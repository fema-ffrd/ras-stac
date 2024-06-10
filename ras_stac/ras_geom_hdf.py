import logging
import numpy as np
import pystac
import sys

from dotenv import load_dotenv, find_dotenv
from papipyplug import parse_input, plugin_logger, print_results
from typing import List

from .utils.common import check_params, GEOM_HDF_IGNORE_PROPERTIES
from .utils.ras_utils import RasStacGeom, new_geom_assets, ras_geom_asset_info
from .utils.s3_utils import (
    verify_safe_prefix,
    s3_key_public_url_converter,
    split_s3_key,
    init_s3_resources,
    get_basic_object_metadata,
    copy_item_to_s3,
    read_ras_geom_from_s3,
)

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)

load_dotenv(find_dotenv())


def new_geom_item(
    geom_hdf: str,
    new_item_s3_key: str,
    topo_assets: list = None,
    lulc_assets: list = None,
    mannings_assets: list = None,
    other_assets: list = None,
    item_props_to_remove: List = None,
    item_props_to_add: dict = None,
    minio_mode=False,
):
    verify_safe_prefix(new_item_s3_key)
    logging.info(f"Creating geom item: {new_item_s3_key}")
    item_public_url = s3_key_public_url_converter(
        new_item_s3_key, minio_mode=minio_mode
    )
    logging.debug(f"item_public_url: {item_public_url}")

    # Prep parameters
    bucket_name, _ = split_s3_key(geom_hdf)
    other_assets.append(geom_hdf)

    _, s3_client, s3_resource = init_s3_resources(minio_mode=minio_mode)
    bucket = s3_resource.Bucket(bucket_name)
    # Create geometry item
    geom_hdf_obj, ras_model_name = read_ras_geom_from_s3(geom_hdf, minio_mode)
    ras_stac_geom = RasStacGeom(geom_hdf_obj)
    if item_props_to_remove:
        item = ras_stac_geom.to_item(item_props_to_remove, ras_model_name)
    else:
        item = ras_stac_geom.to_item(GEOM_HDF_IGNORE_PROPERTIES, ras_model_name)

    if item_props_to_add:
        item.properties.update(item_props_to_add)
    # Create list of assets to add to item
    geom_assets = new_geom_assets(
        topo_assets=topo_assets,
        lulc_assets=lulc_assets,
        mannings_assets=mannings_assets,
        other_assets=other_assets,
    )

    # Add assets to item
    for asset_type, asset_list in geom_assets.items():
        logging.debug(asset_type)
        for asset_file in asset_list:
            _, asset_key = split_s3_key(asset_file)
            logging.info(f"Adding asset {asset_file} to item")
            obj = bucket.Object(asset_key)
            try:
                metadata = get_basic_object_metadata(obj)
            except Exception as e:
                logging.error(f"unable to fetch metadata for {obj}:{e}")
                metadata = {}
            asset_info = ras_geom_asset_info(asset_file, asset_type)
            asset = pystac.Asset(
                s3_key_public_url_converter(asset_file, minio_mode=minio_mode),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            item.add_asset(asset_info["title"], asset)

    # Transform cell size properties to square root of area
    for prop in [
        "2d_flow_areas:cell_average_size",
        "2d_flow_areas:cell_maximum_size",
        "2d_flow_areas:cell_minimum_size",
    ]:
        try:
            item.properties[prop] = int(np.sqrt(float(item.properties[prop])))
        except KeyError:
            logging.warning(f"property {prop} not found")

    logging.info("Writing geom item to s3")
    item.set_self_href(item_public_url)
    copy_item_to_s3(item, new_item_s3_key, s3_client)

    logging.info("Program completed successfully")

    results = [
        {
            "href": item_public_url,
            "rel": "self",
            "title": "public_url",
            "type": "application/json",
        },
        {
            "href": new_item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results


def main(params: dict, minio_mode=False):
    # Required parameters
    geom_hdf = params.get("geom_hdf", None)
    item_s3_key = params.get("new_item_s3_key", None)

    # Optional parameters
    topo_assets = params.get("topo_assets", [])
    lulc_assets = params.get("lulc_assets", [])
    mannings_assets = params.get("mannings_assets", [])
    other_assets = params.get("other_assets", [])
    item_props_to_remove = params.get("item_props_to_remove", [])
    item_props_to_add = params.get("item_props", {})

    return new_geom_item(
        geom_hdf,
        item_s3_key,
        topo_assets,
        lulc_assets,
        mannings_assets,
        other_assets,
        item_props_to_remove,
        item_props_to_add,
        minio_mode,
    )


if __name__ == "__main__":
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    PLUGIN_PARAMS = check_params(new_geom_item)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params, minio_mode=True)
    print_results(result)
