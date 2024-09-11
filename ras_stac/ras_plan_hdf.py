from .utils.logger import setup_logging
import logging
import pystac
import sys

from dotenv import find_dotenv, load_dotenv
from papipyplug import parse_input, plugin_logger, print_results
from typing import List

from rashdf import RasPlanHdf
from .utils.common import check_params, PLAN_HDF_IGNORE_PROPERTIES
from .utils.ras_utils import ras_plan_asset_info, RasStacPlan
from .utils.s3_utils import (
    verify_safe_prefix,
    s3_path_public_url_converter,
    split_s3_path,
    init_s3_resources,
    get_basic_object_metadata,
    copy_item_to_s3,
    read_plan_hdf_from_s3,
)


def new_plan_item(
    plan_hdf_obj: RasPlanHdf,
    geom_item: pystac.Item,
    sim_id: str,
    asset_list: list = None,
    item_props: dict = {},
    item_props_to_remove: List = None,
    s3_resource=None,
):
    ras_stac_plan = RasStacPlan(plan_hdf_obj)
    plan_meta = ras_stac_plan.get_simulation_metadata(sim_id)
    if plan_meta:
        try:
            logging.info("creating plan item")
            if item_props_to_remove:
                plan_item = ras_stac_plan.to_item(geom_item, plan_meta, sim_id, item_props_to_remove)
            else:
                plan_item = ras_stac_plan.to_item(geom_item, plan_meta, sim_id, PLAN_HDF_IGNORE_PROPERTIES)
        except TypeError:
            return logging.error(
                f"unable to retrieve model results with geom data from the given geometry item and metadata \
                    from ras stac plan. please verify plan was executed and results exist"
            )
    else:
        raise AttributeError(f"No simulation metadata retrieved from given ras stac plan")

    plan_item.add_derived_from(geom_item)
    plan_item.properties.update(item_props)

    if asset_list:
        for asset_file in asset_list:
            bucket, asset_key = split_s3_path(asset_file)
            asset_bucket = s3_resource.Bucket(bucket)
            obj = asset_bucket.Object(asset_key)
            metadata = get_basic_object_metadata(obj)
            asset_info = ras_plan_asset_info(asset_file)
            asset = pystac.Asset(
                s3_path_public_url_converter(asset_file),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            plan_item.add_asset(asset_info["title"], asset)

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
