from .utils.logger import setup_logging
import logging
import pystac
import sys

from dotenv import find_dotenv, load_dotenv
from rasterio.session import AWSSession
from papipyplug import parse_input, plugin_logger, print_results

from .utils.common import check_params
from .utils.dg_utils import create_depth_grid_item
from .utils.ras_utils import get_ras_asset_info
from .utils.s3_utils import (
    verify_safe_prefix,
    s3_path_public_url_converter,
    split_s3_path,
    init_s3_resources,
    get_basic_object_metadata,
    copy_item_to_s3,
)


def new_plan_dg_item(
    plan_dg: str,
    new_dg_item_s3_path: str,
    plan_item_s3_path: str,
    dg_id: str,
    item_props: dict = None,
    asset_list: list = None,
):
    logging.info("Creating plan item")
    verify_safe_prefix(new_dg_item_s3_path)

    dg_item_public_url = s3_path_public_url_converter(new_dg_item_s3_path)
    plan_item_public_url = s3_path_public_url_converter(plan_item_s3_path)

    # Prep parameters
    bucket_name, _ = split_s3_path(plan_dg)

    # Instantitate S3 resources
    session, s3_client, s3_resource = init_s3_resources()
    bucket = s3_resource.Bucket(bucket_name)
    AWS_SESSION = AWSSession(session)

    logging.info("pulling plan item")
    plan_item = pystac.Item.from_file(plan_item_public_url)

    _, key = split_s3_path(plan_dg)
    dg_obj = bucket.Object(key)

    logging.info("fetching dg metadata")
    dg_item = create_depth_grid_item(dg_obj, dg_id, AWS_SESSION)
    dg_item.properties.update(item_props)
    dg_item.add_derived_from(plan_item)

    for asset_file in asset_list:
        _, asset_key = split_s3_path(asset_file)
        obj = bucket.Object(asset_key)
        metadata = get_basic_object_metadata(obj)
        asset_info = get_ras_asset_info(asset_file)
        asset = pystac.Asset(
            s3_path_public_url_converter(asset_file),
            extra_fields=metadata,
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        dg_item.add_asset(asset_info["title"], asset)

    logging.info("Writing dg item to s3")
    dg_item.set_self_href(dg_item_public_url)
    copy_item_to_s3(dg_item, new_dg_item_s3_path, s3_client)

    logging.info("Program completed successfully")

    results = [
        {
            "href": dg_item_public_url,
            "rel": "self",
            "title": "public_url",
            "type": "application/json",
        },
        {
            "href": new_dg_item_s3_path,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results


def main(params: dict):
    # Required parameters
    plan_dg = params.get("plan_dg", None)
    dg_id = params.get("dg_id", None)
    dg_item_s3_path = params.get("new_dg_item_s3_path", None)
    plan_item_s3_path = params.get("plan_item_s3_path", None)

    # Optional parameters
    asset_list = params.get("assets", [])
    dg_item_props = params.get("item_props", {})

    return new_plan_dg_item(
        plan_dg, dg_item_s3_path, plan_item_s3_path, dg_id, dg_item_props, asset_list
    )


if __name__ == "__main__":
    setup_logging()
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    PLUGIN_PARAMS = check_params(new_plan_dg_item)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params)
    print_results(result)
