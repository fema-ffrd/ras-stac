import logging
from utils.s3_utils import *
from utils.dg_utils import *
from utils.ras_stac import *
from pathlib import Path
from rasterio.session import AWSSession
from dotenv import find_dotenv, load_dotenv
import numpy as np
from utils.common import PLAN_HDF_IGNORE_PROPERTIES

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

PLUGIN_PARAMS = {
    "required": ["plan_dg", "new_item_s3_key", "plan_item_s3_key", "dg_id"],
    "optional": [
        "item_props",
        "assets"
    ],
}

def main(params:dict):
    #  Required parameters
    plan_dg = params.get("plan_dg", None)
    dg_id = params.get("dg_id", None)
    dg_item_s3_key = params.get("new_item_s3_key", None)
    plan_item_s3_key = params.get("plan_item_s3_key", None)

    verify_safe_prefix(dg_item_s3_key)
    dg_item_public_url = s3_key_public_url_converter(dg_item_s3_key)
    plan_item_public_url = s3_key_public_url_converter(plan_item_s3_key)

    # Optional parameters
    asset_list = params.get("assets", [])
    dg_item_props = params.get("item_props", {})

    # Prep parameters
    bucket, _ = split_s3_key(plan_dg)

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

    logging.info("pulling plan item")
    plan_item = pystac.Item.from_file(plan_item_public_url)

    _, key = split_s3_key(plan_dg)
    dg_obj = bucket.Object(key)

    logging.info("fetching dg metadata")
    dg_item = create_depth_grid_item(dg_obj, dg_id, AWS_SESSION)
    dg_item.properties.update(dg_item_props)
    dg_item.add_derived_from(plan_item)

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
        dg_item.add_asset(asset_info["title"], asset)

    logging.info("Writing dg item to s3")
    dg_item.set_self_href(dg_item_public_url)
    copy_item_to_s3(dg_item, dg_item_s3_key)


    logging.info("Program completed successfully")
    
    results = [
        {
            "href": dg_item_public_url,
            "rel": "self",
            "title": "plublic_url",
            "type": "application/json",
        },
        {
            "href": dg_item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results
