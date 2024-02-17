import logging
from utils.s3_utils import *
from utils.ras_hdf import *
from utils.ras_stac import *
from pathlib import Path
from rasterio.session import AWSSession
from dotenv import find_dotenv, load_dotenv
import numpy as np
from utils.common import PLAN_HDF_IGNORE_PROPERTIES

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

PLUGIN_PARAMS = {
    "required": ["plan_hdf", "new_item_s3_key", "geom_item_s3_key", "sim_id"],
    "optional": ["ras_assets", "item_props"],
}


def main(params: dict):
    #  Required parameters
    plan_hdf = params.get("plan_hdf", None)
    sim_id = params.get("sim_id", None)
    sim_item_s3_key = params.get("new_item_s3_key", None)
    geom_item_s3_key = params.get("geom_item_s3_key", None)

    verify_safe_prefix(sim_item_s3_key)
    sim_item_public_url = s3_key_public_url_converter(sim_item_s3_key)
    geom_item_public_url = s3_key_public_url_converter(geom_item_s3_key)

    # Optional parameters
    asset_list = params.get("ras_assets", [])
    sim_item_props = params.get("item_props", {})

    # Prep parameters
    bucket, _ = split_s3_key(plan_hdf)
    asset_list.append(plan_hdf)

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

    logging.info("Creating plan item")

    # Create geometry item
    geom_item = pystac.Item.from_file(geom_item_public_url)

    logging.info("fetching plan metadata")
    sim_meta = get_simulation_metadata(plan_hdf, sim_id)

    try:
        logging.info("creating plan item")
        sim_item = create_model_simulation_item(geom_item, sim_meta, sim_id)
    except TypeError:
        return logging.error("unable to retrieve model results. please verify plan was executed and results exist")

    sim_item.add_derived_from(geom_item)
    sim_item.properties.update(sim_item_props)

    for asset_file in asset_list:
        _, asset_key = split_s3_key(asset_file)
        obj = bucket.Object(asset_key)
        metadata = get_basic_object_metadata(obj)
        asset_info = ras_plan_asset_info(asset_file)
        asset = pystac.Asset(
            s3_key_public_url_converter(asset_file),
            extra_fields=metadata,
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        sim_item.add_asset(asset_info["title"], asset)

    for prop in PLAN_HDF_IGNORE_PROPERTIES:
        try:
            del sim_item.properties[prop]
        except KeyError:
            logging.warning(f"property {prop} not found")

    logging.info("Writing geom item to s3")
    sim_item.set_self_href(sim_item_public_url)
    copy_item_to_s3(sim_item, sim_item_s3_key)

    logging.info("Program completed successfully")

    results = [
        {
            "href": sim_item_public_url,
            "rel": "self",
            "title": "plublic_url",
            "type": "application/json",
        },
        {
            "href": sim_item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results
