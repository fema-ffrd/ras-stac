import logging
from utils.s3_utils import *
from utils.ras_hdf import *
from utils.ras_stac import *
from rasterio.session import AWSSession
from dotenv import find_dotenv, load_dotenv
import numpy as np
from utils.common import GEOM_HDF_IGNORE_PROPERTIES
import logging

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

PLUGIN_PARAMS = {
    "required": ["geom_hdf", "new_item_s3_key"],
    "optional": [
        "topo_assets",
        "lulc_assets",
        "mannings_assets",
        "other_assets",
        "simplify",
    ],
}


def main(params: dict):
    #  Required parameters
    geom_hdf = params.get("geom_hdf", None)
    item_s3_key = params.get("new_item_s3_key", None)

    verify_safe_prefix(item_s3_key)

    item_public_url = s3_key_public_url_converter(item_s3_key)

    # Optional parameters
    topo_assets = params.get("topo_assets", [])
    lulc_assets = params.get("lulc_assets", [])
    mannings_assets = params.get("mannings_assets", [])
    other_assets = params.get("other_assets", [])
    simplify = params.get("simplify", 100)

    # Prep parameters
    bucket, key = split_s3_key(geom_hdf)
    other_assets.append(geom_hdf)

    # load env for local testing
    try:
        load_dotenv(find_dotenv())
    except:
        logging.debug("no .env file found")

    # Instantitate S3 resources
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    AWS_SESSION = AWSSession(boto3.Session())

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
        handlers=[logging.StreamHandler()],
    )

    logging.info("Creating geom item")

    # Create geometry ite,
    item = create_model_item(geom_hdf, simplify=simplify)

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
            metadata = get_basic_object_metadata(obj)
            asset_info = ras_geom_asset_info(asset_file, asset_type)
            asset = pystac.Asset(
                s3_key_public_url_converter(asset_file),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            item.add_asset(asset_info["title"], asset)

    # Remove unwanted properties
    for prop in GEOM_HDF_IGNORE_PROPERTIES:
        try:
            del item.properties[prop]
        except KeyError:
            logging.warning(f"property {prop} not found")

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
    copy_item_to_s3(item, item_s3_key)

    logging.info("Program completed successfully")

    results = [
        {
            "href": item_public_url,
            "rel": "self",
            "title": "plublic_url",
            "type": "application/json",
        },
        {
            "href": item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results
