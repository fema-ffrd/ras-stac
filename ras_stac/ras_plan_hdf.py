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
    s3_path_public_url_converter,
    init_s3_resources,
    copy_item_to_s3,
    read_plan_hdf_from_s3,
)


def new_plan_item(
    plan_hdf_obj: RasPlanHdf,
    sim_id: str,
    asset_list: list = None,
    item_props_to_remove: List = None,
    item_props_to_add: dict = {},
    s3_resource=None,
):
    ras_stac_plan = RasStacPlan(plan_hdf_obj)
    stac_properties = ras_stac_plan.get_stac_plan_attrs(sim_id)

    if not stac_properties:
        raise AttributeError(
            f"Could not find properties while creating model item for {sim_id}."
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
    plan_item = ras_stac_plan.to_item(stac_properties, sim_id)

    plan_item.properties.update(item_props_to_add)

    if asset_list:
        add_assets_to_item(plan_item, asset_list, s3_resource)

    return plan_item


def main(params: dict):
    #  Required parameters
    plan_hdf = params.get("plan_hdf", None)
    sim_id = params.get("sim_id", None)
    plan_item_s3_path = params.get("new_plan_item_s3_path", None)
    geom_item_s3_path = params.get("geom_item_s3_path", None)

    # Optional parameters
    asset_list = params.get("asset_list", [])
    plan_item_props = params.get("item_props", {})
    item_props_to_remove = params.get("item_props_to_remove", [])

    verify_safe_prefix(plan_item_s3_path)
    geom_item_public_url = s3_path_public_url_converter(geom_item_s3_path)

    # Prep parameters
    asset_list.append(plan_hdf)

    # Instantitate S3 resources
    _, s3_client, s3_resource = init_s3_resources()
    plan_hdf_obj = read_plan_hdf_from_s3(plan_hdf)

    # Create geometry item
    geom_item = pystac.Item.from_file(geom_item_public_url)

    plan_item = new_plan_item(
        plan_hdf_obj,
        geom_item,
        sim_id,
        asset_list,
        plan_item_props,
        item_props_to_remove,
        s3_resource,
    )

    copy_item_to_s3(plan_item, plan_item_s3_path, s3_client)


if __name__ == "__main__":
    setup_logging()
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    PLUGIN_PARAMS = check_params(new_plan_item)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params)
    print_results(result)
