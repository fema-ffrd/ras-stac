import logging
import pystac
import sys

from dotenv import find_dotenv, load_dotenv
from papipyplug import parse_input, plugin_logger, print_results
from typing import List

from .utils.common import check_params, PLAN_HDF_IGNORE_PROPERTIES
from .utils.ras_stac import (
    get_simulation_metadata,
    create_model_simulation_item,
    ras_plan_asset_info,
)
from .utils.s3_utils import (
    verify_safe_prefix,
    s3_key_public_url_converter,
    split_s3_key,
    init_s3_resources,
    get_basic_object_metadata,
    copy_item_to_s3,
    read_ras_plan_from_s3,
)


logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def new_plan_item(
    plan_hdf: str,
    new_plan_item_s3_key: str,
    geom_item_s3_key: str,
    sim_id: str,
    asset_list: list = None,
    item_props: dict = None,
    item_props_to_remove: List = None,
    minio_mode: bool = False,
):
    verify_safe_prefix(new_plan_item_s3_key)
    plan_item_public_url = s3_key_public_url_converter(new_plan_item_s3_key, minio_mode=minio_mode)
    geom_item_public_url = s3_key_public_url_converter(geom_item_s3_key, minio_mode=minio_mode)

    # Prep parameters
    bucket_name, _ = split_s3_key(plan_hdf)
    asset_list.append(plan_hdf)

    # Instantitate S3 resources
    session, s3_client, s3_resource = init_s3_resources(minio_mode)
    bucket = s3_resource.Bucket(bucket_name)
    # AWSSession(session)

    logging.info("Creating plan item")

    # Create geometry item
    geom_item = pystac.Item.from_file(geom_item_public_url)

    logging.info("fetching plan metadata")
    plan_hdf_obj = read_ras_plan_from_s3(plan_hdf, minio_mode)
    plan_meta = get_simulation_metadata(plan_hdf_obj, sim_id)
    if plan_meta:
        try:
            logging.info("creating plan item")
            if item_props_to_remove:
                plan_item = create_model_simulation_item(geom_item, plan_meta, sim_id, item_props_to_remove)
            else:
                plan_item = create_model_simulation_item(geom_item, plan_meta, sim_id, PLAN_HDF_IGNORE_PROPERTIES)
        except TypeError:
            return logging.error(
                f"unable to retrieve model results with geom data from {geom_item_public_url} and metadata \
                    from {plan_hdf}. please verify plan was executed and results exist"
            )
    else:
        raise AttributeError(f"No simulation metadata retrieved from {plan_hdf}")

    plan_item.add_derived_from(geom_item)
    plan_item.properties.update(item_props)

    if asset_list:
        for asset_file in asset_list:
            _, asset_key = split_s3_key(asset_file)
            obj = bucket.Object(asset_key)
            metadata = get_basic_object_metadata(obj)
            asset_info = ras_plan_asset_info(asset_file)
            asset = pystac.Asset(
                s3_key_public_url_converter(asset_file, minio_mode=minio_mode),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            plan_item.add_asset(asset_info["title"], asset)

    logging.info("Writing geom item to s3")
    plan_item.set_self_href(plan_item_public_url)
    copy_item_to_s3(plan_item, new_plan_item_s3_key, s3_client)

    logging.info("Program completed successfully")

    results = [
        {
            "href": plan_item_public_url,
            "rel": "self",
            "title": "public_url",
            "type": "application/json",
        },
        {
            "href": new_plan_item_s3_key,
            "rel": "self",
            "title": "s3_key",
            "type": "application/json",
        },
    ]

    return results


def main(params: dict, minio_mode=False):
    #  Required parameters
    plan_hdf = params.get("plan_hdf", None)
    sim_id = params.get("sim_id", None)
    plan_item_s3_key = params.get("new_plan_item_s3_key", None)
    geom_item_s3_key = params.get("geom_item_s3_key", None)

    # Optional parameters
    asset_list = params.get("asset_list", [])
    plan_item_props = params.get("item_props", {})
    item_props_to_remove = params.get("item_props_to_remove", [])

    return new_plan_item(
        plan_hdf,
        plan_item_s3_key,
        geom_item_s3_key,
        sim_id,
        asset_list,
        plan_item_props,
        item_props_to_remove,
        minio_mode,
    )


if __name__ == "__main__":
    plugin_logger()

    if not load_dotenv(find_dotenv()):
        logging.warning("No local .env found")

    PLUGIN_PARAMS = check_params(new_plan_item)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)
    result = main(input_params, minio_mode=True)
    print_results(result)
