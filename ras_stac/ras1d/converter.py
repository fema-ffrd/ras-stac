import io
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from pyproj import CRS
from pystac.extensions.projection import AssetProjectionExtension
from pystac.extensions.storage import StorageExtension
from pystac.item import Item
from ras_stac.ras1d.utils.classes import (
    GenericAsset,
    GeometryAsset,
    GeopackageAsset,
    NullGeometryAsset,
    PlanAsset,
    SteadyFlowAsset,
    ThumbAsset,
    UnsteadyFlowAsset,
)
from ras_stac.ras1d.utils.common import (
    create_non_spatial_table,
    file_location,
    gather_dir_local,
    get_huc8,
    make_thumbnail,
)
from ras_stac.ras1d.utils.s3_utils import (
    gather_dir_s3,
    save_bytes_s3,
    save_file_s3,
    str_from_s3,
)
from ras_stac.ras1d.utils.stac_utils import generate_asset
from shapely import to_geojson


class Converter:

    def __init__(self, asset_paths: list, crs: str) -> None:
        self.assets = [generate_asset(i) for i in asset_paths]
        self.crs = crs
        [a.set_crs(crs) for a in self.assets if isinstance(a, GeometryAsset)]
        self.custom_properties = {}
        # find root
        self.root = ""
        for i in range(len(asset_paths[0])):
            if all([tmp[i] == asset_paths[0][i] for tmp in asset_paths]):
                self.root += asset_paths[0][i]
            else:
                break

    def export_stac(self, output_path: str) -> None:
        """Export the converted STAC item."""
        out_obj = json.dumps(self.stac_item.to_dict(), indent=4).encode()
        if file_location(output_path) == "local":
            with open(output_path, "wb") as f:
                f.write(out_obj)
        else:
            save_bytes_s3(out_obj, output_path)

    def export_thumbnail(self, thumb_path: str) -> None:
        """Generate STAC thumbnail, save to S3, and log path."""
        gdfs = self.primary_geometry.gdfs
        if len(gdfs) == 0 or "null" in gdfs:
            return
        thumb = make_thumbnail(gdfs)
        if file_location(thumb_path) == "local":
            thumb.savefig(thumb_path, dpi=80)
        else:
            img_data = io.BytesIO()
            thumb.savefig(img_data, format="png")
            img_data.seek(0)
            save_bytes_s3(img_data, thumb_path)

        self.assets.append(ThumbAsset(thumb_path))
        plt.close(thumb)

    @property
    def stac_item(self) -> dict:
        """Generate STAC item for this model."""
        stac = Item(
            id=self.idx,
            geometry=self.get_footprint("epsg:4326"),
            bbox=self.get_bbox("epsg:4326"),
            datetime=self.last_update,
            properties=self.stac_properties,
            assets=self.stac_assets,
        )
        stor_ext = StorageExtension.ext(stac, add_if_missing=True)
        stor_ext.apply(platform="AWS", region="us-east-1")
        if self.crs:
            prj_ext = AssetProjectionExtension.ext(stac, add_if_missing=True)
            og_crs = CRS(self.crs)
            prj_ext.apply(
                epsg=og_crs.to_epsg(),
                wkt2=og_crs.to_wkt(),
                geometry=self.get_footprint(),
                bbox=self.get_bbox(),
                centroid=to_geojson(self.get_centroid()),
            )
        return stac

    @property
    def idx(self):
        """Generate STAC item id from RAS name."""
        return self.ras_prj_file.basename.replace(".prj", "").replace(".PRJ", "").replace(" ", "_")

    def get_footprint(self, crs: str = None):
        """Return a geojson of the primary geometry cross-section concave hull"""
        # This reformatting is weird because of how pystac wants the geometry
        cchull = self.primary_geometry.concave_hull
        if crs:
            cchull = cchull.to_crs(crs)
        return json.loads(to_geojson(cchull.iloc[0]["geometry"]))

    def get_bbox(self, crs: str = None):
        """Return bbox for all geometry components in the primary geometry"""
        if self.primary_geometry.gdfs is None:
            return [0, 0, 1, 1]
        all_geom = pd.concat(self.primary_geometry.gdfs)
        if crs:
            all_geom = all_geom.to_crs(crs)
        return all_geom.total_bounds.tolist()

    def get_centroid(self, crs: str = None):
        """Return centroid for XS concave hull of the primary geometry"""
        centroid = self.primary_geometry.concave_hull.centroid
        if crs:
            centroid = centroid.to_crs(crs)
        return centroid.iloc[0]

    @property
    def huc8(self):
        if not self.crs:
            return None
        centroid = self.get_centroid("epsg:4326")
        return get_huc8(centroid.x, centroid.y)

    @property
    def last_update(self):
        """Return the last update time for the primary ras geometry"""
        last = self.primary_geometry.last_update
        if last is None:
            return datetime.now()  # logging of processing_time vs model_geometry is handled in self.stac_properties
        else:
            return last

    @property
    def stac_properties(self):
        """Build properties dict for STAC item"""
        properties = {
            "model_name": self.idx,
            "ras_version": self.primary_geometry.ras_version,
            "ras_units": self.ras_prj_file.units,
            "project_title": self.ras_prj_file.title,
            "plans": {a.title: a.suffix for a in self.assets if isinstance(a, PlanAsset)},
            "geometries": {a.title: a.suffix for a in self.assets if isinstance(a, GeometryAsset)},
            "flows": {a.title: a.suffix for a in self.assets if isinstance(a, SteadyFlowAsset)},
            "river_miles": str(self.primary_geometry.get_river_miles()),
            "datetime_source": "processing_time" if self.primary_geometry.last_update is None else "model_geometry",
            "assigned_HUC8": self.huc8,
            "has_2d": any([a.has_2d for a in self.assets if isinstance(a, GeometryAsset)]),
            "has_1d": any([a.has_1d for a in self.assets if isinstance(a, GeometryAsset)]),
        }
        for p in self.custom_properties:
            properties[p] = self.custom_properties[p]
        return properties

    @property
    def stac_assets(self):
        return {a.name: a.to_stac() for a in self.assets}

    @property
    def extension_dict(self):
        return {a.suffix: a for a in self.assets}

    @property
    def ras_prj_file(self) -> GenericAsset:
        """The RAS project file in this directory."""
        potentials = [a for a in self.assets if a.is_ras_prj]
        if len(potentials) != 1:
            raise RuntimeError(
                f"Model directory did not contain one RAS project file.  Found: {[str(p) for p in potentials]}"
            )
        return potentials[0]

    @property
    def primary_plan(self) -> PlanAsset:
        """The primary plan in the HEC-RAS project"""
        plans = [self.extension_dict[k] for k in self.ras_prj_file.plans]
        assert len(plans) > 0, f"No plans listed for prj file {self.ras_prj_file}"

        if len(plans) == 1:
            return plans[0]
        non_encroached = [p for p in plans if not p.is_encroached]
        if len(non_encroached) == 0:
            return plans[0]
        else:
            return non_encroached[0]

    @property
    def primary_geometry(self) -> GeometryAsset:
        """The geometry file listed in the primary plan"""
        if not self.crs:
            return NullGeometryAsset()
        try:
            geom = self.extension_dict[self.primary_plan.geometry]
        except Exception:
            return NullGeometryAsset()
        if not geom.has_1d:
            return NullGeometryAsset()
        else:
            return geom

    @property
    def primary_flow(self) -> SteadyFlowAsset:
        """The flow file listed in the primary plan"""
        try:
            return self.extension_dict[self.primary_plan.flow]
        except Exception:
            return None

    def check_for_mip(self) -> None:
        mip_data = [a for a in self.assets if a.name == "mip_package_geolocation_metadata.json"]
        if len(mip_data) == 0:
            return
        elif len(mip_data) > 1:
            raise RuntimeError(
                f"More than one mip_package_geolocation_metadata.json found in s3 dir: {[m.title for m in mip_data]}"
            )
        else:
            mip_data = mip_data[0]
            self.assets.remove(mip_data)
            mip_data.download_asset_str()
            mip_json = json.loads(mip_data.file_str)
            self.custom_properties["fema_case_number"] = mip_json["case"]
            self.custom_properties["fema_case_counties"] = mip_json["county"]

    @property
    def metadata(self):
        """Generate dictionary of metadata for HEC-RAS model"""
        meta = {}
        meta["plans_files"] = "\n".join([a.url.replace(self.root, "") for a in self.assets if isinstance(a, PlanAsset)])
        meta["geom_files"] = "\n".join(
            [a.url.replace(self.root, "") for a in self.assets if isinstance(a, GeometryAsset)]
        )
        meta["steady_flow_files"] = "\n".join(
            [a.url.replace(self.root, "") for a in self.assets if isinstance(a, SteadyFlowAsset)]
        )
        meta["unsteady_flow_files"] = "\n".join(
            [a.url.replace(self.root, "") for a in self.assets if isinstance(a, UnsteadyFlowAsset)]
        )

        meta["plans_titles"] = "\n".join([a.title for a in self.assets if isinstance(a, PlanAsset)])
        meta["geom_titles"] = "\n".join([a.title for a in self.assets if isinstance(a, GeometryAsset)])
        meta["steady_flow_titles"] = "\n".join([a.title for a in self.assets if isinstance(a, SteadyFlowAsset)])

        meta["ras_project_file"] = self.ras_prj_file.url.replace(self.root, "")
        meta["ras_project_title"] = self.ras_prj_file.title
        meta["primary_plan_file"] = self.primary_plan.url.replace(self.root, "")
        meta["primary_plan_title"] = self.primary_plan.title
        meta["primary_flow_file"] = self.primary_flow.url.replace(self.root, "")
        meta["primary_flow_title"] = self.primary_flow.title
        meta["primary_geom_file"] = self.primary_geometry.url.replace(self.root, "")
        meta["primary_geom_title"] = self.primary_geometry.title

        meta["ras_version"] = self.primary_geometry.ras_version
        flow_changes = pd.DataFrame(self.primary_flow.flow_change_locations)
        meta["profile_names"] = "\n".join(flow_changes["profile_names"].iloc[0])
        meta["units"] = self.ras_prj_file.units

        return meta

    @property
    def gdfs(self):
        """Create geodataframes from primary geometry and attribute with data from primary plan and flow files"""
        # Get primary geometry GDF
        primary_gdfs = self.primary_geometry.gdfs

        # Attribute XS layer with flow data
        xs = primary_gdfs["XS"]
        flow_changes = pd.DataFrame(self.primary_flow.flow_change_locations)
        flow_changes["river_reach"] = flow_changes["river"] + flow_changes["reach"]
        for river_reach in flow_changes["river_reach"].unique():
            # get flow change locations for this reach
            tmp_flow_changes = flow_changes.loc[flow_changes["river_reach"] == river_reach, :].sort_values(
                by="rs", ascending=False
            )
            # iterate through this reaches flow change locations and set cross section flows/profile names
            for _, row in tmp_flow_changes.iterrows():
                mask = (
                    (xs["river"] == row["river"]) & (xs["reach"] == row["reach"]) & (xs["river_station"] <= row["rs"])
                )
                # add flows to xs_gdf
                xs.loc[mask, "flows"] = "\n".join([str(f) for f in row["flows"]])
                # add profile names to xs_gdf
                xs.loc[mask, "profile_names"] = "\n".join(row["profile_names"])
        primary_gdfs["XS"] = xs

        return primary_gdfs

    def export_gpkg(self, out_path: str) -> None:
        """Save a geopackage file representing the primary geometry"""
        if file_location(out_path) != "local":
            tmp_out_path = f"{self.idx}.gpkg"
        else:
            tmp_out_path = out_path
        gdfs = self.gdfs
        for layer in gdfs:
            gdfs[layer].to_file(tmp_out_path, driver="GPKG", layer=layer)
        create_non_spatial_table(tmp_out_path, self.metadata)
        if file_location(out_path) != "local":
            save_file_s3(tmp_out_path, out_path)

        # Make an asset
        self.assets.append(GeopackageAsset(out_path))


def from_directory(model_dir: str, crs: str) -> Converter:
    """Scrape assets from directory and return Converter object."""
    if file_location(model_dir) == "local":
        assets = gather_dir_local(model_dir)
    else:
        assets = gather_dir_s3(model_dir)
    return Converter(assets, crs)


def ras_to_stac(ras_dir: str, crs: str):
    """Convert a HEC-RAS model to a STAC item and save to same directory."""
    converter = from_directory(ras_dir, crs)
    converter.export_gpkg(str(Path(ras_dir) / "geopackage.gpkg"))
    # converter.export_thumbnail(str(Path(ras_dir) / "thumbnail.png"))
    # converter.export_stac(str(Path(ras_dir) / "debugging.json"))


def process_in_place_s3(in_prefix: str, crs: str, out_prefix: str):
    """Convert a HEC-RAS model to a STAC item and save to same directory."""
    logging.info(f"Processing model with crs {crs} at prefix {in_prefix}")
    logging.info("Discovering model contents")
    converter = from_directory(in_prefix, crs)
    converter.check_for_mip()

    # Define paths
    thumb_path = out_prefix + "Thumbnail.png"
    gpkg_path = out_prefix + f"{converter.idx}.gpkg"
    stac_path = out_prefix + f"{converter.idx}.json"

    # Process
    if converter.crs is not None:
        logging.info(f"Generating thumbnail at {thumb_path}")
        converter.export_thumbnail(thumb_path)

        logging.info(f"Generating geopackage at {gpkg_path}")
        converter.export_gpkg(gpkg_path)

    logging.info(f"Generating STAC item at {stac_path}")
    converter.export_stac(stac_path)
    return {"in_path": in_prefix, "crs": crs, "thumb_path": thumb_path, "stac_path": stac_path}


def append_geopackage(in_prefix: str, crs: str, out_prefix: str):
    """Add geopackage to stac items (fix earlier omission for OWP deliverable)."""
    logging.info(f"Processing model with crs {crs} at prefix {in_prefix}")
    logging.info("Discovering model contents")
    converter = from_directory(in_prefix, crs)
    converter.check_for_mip()

    stac_path = out_prefix + f"{converter.idx}.json"
    stac_item = json.loads(str_from_s3(stac_path))
    if "proj:wkt2" not in stac_item["properties"]:  # no CRS
        logging.info("Skipping {in_prefix} for lack of CRS")
        return {"in_path": in_prefix, "crs": crs, "thumb_path": None, "stac_path": stac_path}
    elif not stac_item["properties"]["has_1d"]:
        logging.info("Skipping {in_prefix} for lack of 1D")
        return {"in_path": in_prefix, "crs": crs, "thumb_path": None, "stac_path": stac_path}

    gpkg_path = out_prefix + f"{converter.idx}.gpkg"
    logging.info(f"Generating geopackage at {gpkg_path}")
    converter.export_gpkg(gpkg_path)

    logging.info(f"Updating STAC item at {stac_path}")
    gpkg_asset = [a for a in converter.assets if isinstance(a, GeopackageAsset)][0]
    stac_item["assets"]["GeoPackage_file"] = gpkg_asset.to_stac().to_dict()
    out_obj = json.dumps(stac_item).encode()
    save_bytes_s3(out_obj, stac_path)
    return {"in_path": in_prefix, "crs": crs, "thumb_path": None, "stac_path": stac_path, "gpkg_path": gpkg_path}


if __name__ == "__main__":
    ras_dir = sys.argv[1]
    crs = sys.argv[2]
    if crs == "None":
        crs = None
    out_dir = ras_dir.replace("source_models", "stac_items")
    # process_in_place_s3(ras_dir, crs, out_dir)
    # ras_to_stac(ras_dir, crs)
    append_geopackage(ras_dir, crs, out_dir)
