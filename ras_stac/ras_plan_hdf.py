from .utils.logger import setup_logging
import logging
import pystac
import sys

from dotenv import find_dotenv, load_dotenv
from papipyplug import parse_input, plugin_logger, print_results
from typing import List

from rashdf import RasPlanHdf
from .utils.common import check_params, PLAN_HDF_IGNORE_PROPERTIES
from .utils.ras_utils import RasStacPlan, add_assets_to_item
from .utils.s3_utils import (
    verify_safe_prefix,
    init_s3_resources,
    copy_item_to_s3,
    read_plan_hdf_from_s3,
)


def new_plan_item(
    plan_hdf_obj: RasPlanHdf,
    item_id: str,
    asset_list: list = None,
    item_props_to_remove: List = None,
    item_props_to_add: dict = {},
    s3_resource=None,
):
    ras_stac_plan = RasStacPlan(plan_hdf_obj)
    stac_properties = ras_stac_plan.get_stac_plan_attrs(item_id)

    if not stac_properties:
        raise AttributeError(
            f"Could not find properties while creating model item for {item_id}."
        )

    if item_props_to_remove:
        all_props_to_remove = PLAN_HDF_IGNORE_PROPERTIES + item_props_to_remove
    else:
        all_props_to_remove = PLAN_HDF_IGNORE_PROPERTIES

    for prop in all_props_to_remove:
        try:
            del stac_properties[prop]
        except KeyError:
            logging.warning(f"Failed removing {prop}, property not found")
    plan_item = ras_stac_plan.to_item(stac_properties, item_id)

    plan_item.properties.update(item_props_to_add)

    if asset_list:
        add_assets_to_item(plan_item, asset_list, s3_resource)

    return plan_item


def main(
    ras_plan_hdf: str,
    plan_item_s3_path: str,
    item_id: str = None,
    asset_list: list = None,
    item_props_to_add: dict = None,
    item_props_to_remove: list = None,
):
    """
    Main function with individual parameters instead of using a dict.
    """
    # Handle optional parameters
    asset_list = asset_list or []
    item_props_to_add = item_props_to_add or {}
    item_props_to_remove = item_props_to_remove or []

    # Verify the S3 path
    verify_safe_prefix(plan_item_s3_path)

    # Add the ras_plan HDF to the asset list
    asset_list.append(ras_plan_hdf)

    # Instantiate S3 resources
    _, s3_client, s3_resource = init_s3_resources()

    if item_id:
        plan_hdf_obj, _ = read_plan_hdf_from_s3(ras_plan_hdf)
    else:
        plan_hdf_obj, item_id = read_plan_hdf_from_s3(ras_plan_hdf)

    plan_item = new_plan_item(
        plan_hdf_obj,
        item_id,
        asset_list,
        item_props_to_remove,
        item_props_to_add,
        s3_resource,
    )

    # Copy the plan item to S3
    copy_item_to_s3(plan_item, plan_item_s3_path, s3_client)

    result = [
        {
            "href": plan_item_s3_path,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]
    return result


if __name__ == "__main__":
    setup_logging()
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    # Parse the input parameters
    PLUGIN_PARAMS = check_params(main)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    # Extract required parameters
    ras_plan_hdf = input_params.get("ras_plan_hdf", None)
    plan_item_s3_path = input_params.get("plan_item_s3_path", None)

    # Extract optional parameters
    item_id = input_params.get("item_id", None)
    asset_list = input_params.get("asset_list", [])
    item_props_to_add = input_params.get("item_props_to_add", {})
    item_props_to_remove = input_params.get("item_props_to_remove", [])

    result = main(
        ras_plan_hdf,
        plan_item_s3_path,
        item_id,
        asset_list,
        item_props_to_add,
        item_props_to_remove,
    )

    print_results(result)
