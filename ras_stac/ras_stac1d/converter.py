import os
import sys
from pathlib import Path

from pystac.extensions.projection import AssetProjectionExtension
from pystac.extensions.storage import StorageExtension
from pystac.item import Item

from ras_stac.ras_stac1d.utils.classes import GenericAsset
from ras_stac.ras_stac1d.utils.common import file_location
from ras_stac.ras_stac1d.utils.s3_utils import s3listdir


class Converter:

    def __init__(self):
        pass

    def stac_to_s3(self, output_path: str) -> dict:
        """Export the converted STAC item."""

        return Item

    def thumb_to_s3(self, thumb_path: str) -> None:
        """Generate STAC thumbnail, save to S3, and log path."""
        pass

    def import_model(self, model_dir: str, crs: str) -> None:
        """Load HEC-RAS model files to class."""
        if file_location(model_dir) == "local":
            self.assets = [GenericAsset(i) for i in os.listdir(model_dir)]
        else:
            self.assets = [GenericAsset(i) for i in s3listdir(model_dir)]

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
    def ras_prj_file(self) -> GenericAsset:
        """The RAS project file in this directory."""
        potentials = [a for a in self.assets if a.endswith("prj")]
        potentials = [a for a in potentials if a.is_ras_prj]
        if len(potentials) != 1:
            raise RuntimeError(f"Model directory did not contain one RAS project file.  Found: {potentials}")
        return potentials[0]


def ras_to_stac(ras_dir: str, crs: str):
    """Convert a HEC-RAS model to a STAC item and save to same directory."""
    converter = Converter().import_model(ras_dir, crs)
    converter.export_thumbnail(Path(ras_dir) / "thumbnail.png")
    return converter.export_stac("debugging.json")


if __name__ == "__main__":
    ras_dir = sys.argv[1]
    ras_to_stac(ras_dir)