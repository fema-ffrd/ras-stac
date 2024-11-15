import re
from pathlib import Path

from pystac import Asset
from pystac.extensions.storage import StorageExtension

from ras_stac.ras1d.classes import GeometryAsset, PlanAsset, ProjectAsset
from ras_stac.utils.fileio import file_location
from ras_stac.utils.s3_utils import get_metadata


def asset_factory(url: str | Path) -> Asset:
    """Create a PySTAC Asset from a URL."""
    # This is a placeholder for the real implementation
    url = str(url)
    suffix = Path(url).suffix.lower()
    if suffix == ".prj" and is_ras_prj(url):
        asset = ProjectAsset(url)
        asset.title = Path(url).name
    elif re.match(".[Pp][0-9]{2}", suffix):
        asset = PlanAsset(url)
        asset.title = Path(url).name
    elif re.match(".[Gg][0-9]{2}", suffix):
        asset = GeometryAsset(url)
        asset.title = Path(url).name
    else:
        asset = Asset(url)
        asset.title = Path(url).name
    asset = check_storage_extension(asset)
    return asset


def check_storage_extension(asset: Asset) -> Asset:
    """If the file is hosted on S3, add the storage extension."""
    if file_location(asset.href) == "s3":
        stor_ext = StorageExtension.ext(asset)
        meta = get_metadata(asset.href)
        stor_ext.apply(platform="AWS", region=meta["storage:region"], tier=meta["storage:tier"])
    return asset


def is_ras_prj(url: str) -> bool:
    """Check if a file is a HEC-RAS project file."""
    with open(url) as f:
        file_str = f.read()
    if "Proj Title" in file_str.split("\n")[0]:
        return True
    else:
        return False
