import os
import sys
from pathlib import Path

from pystac.extensions.projection import AssetProjectionExtension
from pystac.extensions.storage import StorageExtension
from pystac.item import Item

from ras_stac.ras1d.utils.classes import GenericAsset
from ras_stac.ras1d.utils.common import file_location
from ras_stac.ras1d.utils.s3_utils import s3listdir
from ras_stac.ras1d.utils.stac_utils import generate_asset


class Converter:

    def __init__(self, asset_paths: list, crs: str) -> None:
        self.assets = [generate_asset(i) for i in asset_paths]
        self.crs = crs

    def stac_to_s3(self, output_path: str) -> dict:
        """Export the converted STAC item."""

        return Item

    def thumb_to_s3(self, thumb_path: str) -> None:
        """Generate STAC thumbnail, save to S3, and log path."""
        pass

    @property
    def stac_item(self) -> dict:
        """Generate STAC item for this model."""
        stac = Item(id=self.idx, geometry=None, bbox=None, datetime=None, properties=None, collection=None, assets=None)
        stor_ext = StorageExtension.ext(stac, add_if_missing=True)
        stor_ext.apply(platform="AWS", region="us-east-1")
        prj_ext = AssetProjectionExtension.ext(stac, add_if_missing=True)
        prj_ext.apply(
            epsg=og_crs.to_epsg(),
            wkt2=og_crs.to_wkt(),
            geometry=proj_ext_geom,
            bbox=proj_ext_bbox,
            centroid=proj_ext_centroid,
        )
        return stac

    @property
    def idx(self):
        """Generate STAC item id from RAS name."""
        return str(self.ras_prj_file).replace(" ", "_")

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
    def primary_plan(self):
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
    def primary_geometry(self):
        """The geometry file listed in the primary plan"""
        return self.extension_dict[self.primary_plan.geometry]


def from_directory(model_dir: str, crs: str) -> Converter:
    """Scrape assets from directory and return Converter object."""
    if file_location(model_dir) == "local":
        assets = os.listdir(model_dir)
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
    ras_to_stac(ras_dir)
