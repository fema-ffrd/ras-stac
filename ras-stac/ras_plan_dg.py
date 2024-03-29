import logging
import sys
from utils.s3_utils import *
from utils.dg_utils import *
from utils.ras_stac import *
from pathlib import Path
from rasterio.session import AWSSession
from dotenv import find_dotenv, load_dotenv
import numpy as np
from utils.common import check_params, PLAN_HDF_IGNORE_PROPERTIES
from papipyplug import parse_input, plugin_logger, print_results
from utils.dg_utils import create_depth_grid_item

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def new_plan_dg_item(
    plan_dg: str,
    new_dg_item_s3_key: str,
    plan_item_s3_key: str,
    dg_id: str,
    item_props: dict = None,
    asset_list: list = None,
    dev_mode: bool = False,
):
    logging.info("Creating plan item")
    verify_safe_prefix(new_dg_item_s3_key)

    dg_item_public_url = s3_key_public_url_converter(new_dg_item_s3_key, dev_mode=dev_mode)
    plan_item_public_url = s3_key_public_url_converter(plan_item_s3_key, dev_mode=dev_mode)

    # Prep parameters
    bucket_name, _ = split_s3_key(plan_dg)

    # Instantitate S3 resources
    session, s3_client, s3_resource = init_s3_resources(dev_mode)
    bucket = s3_resource.Bucket(bucket_name)
    AWS_SESSION = AWSSession(session)

    logging.info("pulling plan item")
    plan_item = pystac.Item.from_file(plan_item_public_url)

    _, key = split_s3_key(plan_dg)
    dg_obj = bucket.Object(key)

    logging.info("fetching dg metadata")
    dg_item = create_depth_grid_item(dg_obj, dg_id, AWS_SESSION, dev_mode=dev_mode)
    dg_item.properties.update(item_props)
    dg_item.add_derived_from(plan_item)


    for asset_file in asset_list:
        _, asset_key = split_s3_key(asset_file)
        obj = bucket.Object(asset_key)
        metadata = get_basic_object_metadata(obj)
        asset_info = ras_plan_asset_info(asset_file)
        asset = pystac.Asset(
            s3_key_public_url_converter(asset_file, dev_mode=dev_mode),
            extra_fields=metadata,
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        dg_item.add_asset(asset_info["title"], asset)

    logging.info("Writing dg item to s3")
    dg_item.set_self_href(dg_item_public_url)
    copy_item_to_s3(dg_item, new_dg_item_s3_key, s3_client)

    logging.info("Program completed successfully")

    results = [
        {
            "href": dg_item_public_url,
            "rel": "self",
            "title": "public_url",
            "type": "application/json",
        },
        {
            "href": new_dg_item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results


def main(params: dict, dev_mode=False):
    # Required parameters
    plan_dg = params.get("plan_dg", None)
    dg_id = params.get("dg_id", None)
    dg_item_s3_key = params.get("new_dg_item_s3_key", None)
    plan_item_s3_key = params.get("plan_item_s3_key", None)

    # Optional parameters
    asset_list = params.get("assets", [])
    dg_item_props = params.get("item_props", {})

    return new_plan_dg_item(
        plan_dg,
        dg_item_s3_key,
        plan_item_s3_key,
        dg_id,
        dg_item_props,
        asset_list,
        dev_mode,
    )


if __name__ == "__main__":
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    PLUGIN_PARAMS = check_params(new_plan_dg_item)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params, dev_mode=True)
    print_results(result)
