import datetime
import io
import json
import os
import sys
from pathlib import Path

import pandas as pd
from pyproj import CRS
from pystac.extensions.projection import AssetProjectionExtension
from pystac.extensions.storage import StorageExtension
from pystac.item import Item
from shapely import to_geojson

from ras_stac.ras1d.utils.classes import (
    GenericAsset,
    GeometryAsset,
    PlanAsset,
    ProjectAsset,
    SteadyFlowAsset,
)
from ras_stac.ras1d.utils.common import file_location, get_huc8, make_thumbnail
from ras_stac.ras1d.utils.s3_utils import s3listdir, save_bytes_s3
from ras_stac.ras1d.utils.stac_utils import generate_asset


class Converter:

    def __init__(self, asset_paths: list, crs: str) -> None:
        self.assets = [generate_asset(i) for i in asset_paths]
        self.crs = crs
        [a.set_crs(crs) for a in self.assets if isinstance(a, GeometryAsset)]
        self.thumb_path = None

    def stac_to_s3(self, output_path: str) -> dict:
        """Export the converted STAC item."""
        return Item

    def save_thumbnail(self, thumb_path: str) -> None:
        """Generate STAC thumbnail, save to S3, and log path."""
        gdfs = self.primary_geometry.gdf
        thumb = make_thumbnail(gdfs)
        if file_location(thumb_path) == "local":
            thumb.savefig(thumb_path, dpi=300)
        else:
            img_data = io.BytesIO()
            thumb.savefig(img_data, format="png")
            img_data.seek(0)
            save_bytes_s3(img_data, thumb_path)
        self.thumb_path = thumb_path

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
        prj_ext = AssetProjectionExtension.ext(stac, add_if_missing=True)
        og_crs = CRS(self.crs)
        prj_ext.apply(
            epsg=og_crs.to_epsg(),
            wkt2=og_crs.to_wkt(),
            geometry=self.get_footprint(),
            bbox=self.get_bbox(),
            centroid=self.get_centroid(),
        )
        return stac

    @property
    def idx(self):
        """Generate STAC item id from RAS name."""
        return str(self.ras_prj_file).replace(".prj", "").replace(" ", "_")

    def get_footprint(self, crs: str = None):
        """Return a geojson of the primary geometry cross-section concave hull"""
        # This reformatting is weird because of how pystac wants the geometry
        cchull = self.primary_geometry.concave_hull
        if crs:
            cchull = cchull.to_crs(crs)
        return json.loads(to_geojson(cchull.iloc[0]["geometry"]))

    def get_bbox(self, crs: str = None):
        """Return bbox for all geometry components in the primary geometry"""
        all_geom = pd.concat(self.primary_geometry.gdfs)
        if crs:
            all_geom = all_geom.to_crs(crs)
        return all_geom.total_bounds.tolist()

    def get_centroid(self, crs: str = None):
        """Return centroid for XS concave hull of the primary geometry"""
        centroid = self.primary_geometry.concave_hull
        if crs:
            centroid = centroid.to_crs(crs)
        return centroid.iloc[0]

    @property
    def huc8(self):
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
            "ras_units": self.primary_geometry.units,
            "project_title": self.ras_prj_file.title,
            "plans": {a.title: a.suffix for a in self.assets if isinstance(a, PlanAsset)},
            "geometries": {a.title: a.suffix for a in self.assets if isinstance(a, GeometryAsset)},
            "flows": {a.title: a.suffix for a in self.assets if isinstance(a, SteadyFlowAsset)},
            "river_miles": str(self.primary_geometry.get_river_miles()),
            "datetime_source": "processing_time" if self.primary_geometry.last_update is None else "model_geometry",
            "assigned_HUC8": self.huc8,
            "has_2d": any([a.has_2d for a in self.assets if isinstance(a, GeometryAsset)]),
        }
        return properties

    @property
    def stac_assets(self):
        return [a.to_stac() for a in self.assets]

    @property
    def extension_dict(self):
        return {a.suffix: a for a in self.assets}

    @property
    def ras_prj_file(self) -> GenericAsset:
        """The RAS project file in this directory."""
        potentials = [a for a in self.assets if a.is_ras_prj]
        if len(potentials) != 1:
            raise RuntimeError(f"Model directory did not contain one RAS project file.  Found: {potentials}")
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
        return self.extension_dict[self.primary_plan.geometry]


def from_directory(model_dir: str, crs: str) -> Converter:
    """Scrape assets from directory and return Converter object."""
    if file_location(model_dir) == "local":
        assets = [os.path.join(model_dir, f) for f in os.listdir(model_dir)]
    else:
        assets = s3listdir(model_dir)
    return Converter(assets, crs)


def ras_to_stac(ras_dir: str, crs: str):
    """Convert a HEC-RAS model to a STAC item and save to same directory."""
    converter = from_directory(ras_dir, crs)
    converter.export_thumbnail(Path(ras_dir) / "thumbnail.png")
    return converter.export_stac("debugging.json")


if __name__ == "__main__":
    ras_dir = sys.argv[1]
    crs = sys.argv[2]
    ras_to_stac(ras_dir, crs)
