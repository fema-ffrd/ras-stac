from .utils.logger import setup_logging
import logging
import numpy as np
import sys

from dotenv import load_dotenv, find_dotenv
from papipyplug import parse_input, plugin_logger, print_results
from typing import List

from rashdf import RasGeomHdf
from .utils.common import check_params, GEOM_HDF_IGNORE_PROPERTIES
from .utils.ras_utils import (
    RasStacGeom,
    new_geom_assets,
    add_assets_to_item,
    cell_area_to_distance,
)
from .utils.s3_utils import (
    verify_safe_prefix,
    init_s3_resources,
    copy_item_to_s3,
    read_geom_hdf_from_s3,
)


def new_geom_item(
    ras_geom_hdf: RasGeomHdf,
    item_id: str,
    asset_list: list = None,
    item_props_to_remove: List = None,
    item_props_to_add: dict = None,
    s3_resource=None,
):
    ras_stac_geom = RasStacGeom(ras_geom_hdf)
    stac_properties = ras_stac_geom.get_stac_geom_attrs()

    if not stac_properties:
        raise AttributeError(
            f"Could not find properties while creating model item for {item_id}."
        )

    if item_props_to_remove:
        all_props_to_remove = GEOM_HDF_IGNORE_PROPERTIES + item_props_to_remove
    else:
        all_props_to_remove = GEOM_HDF_IGNORE_PROPERTIES

    for prop in all_props_to_remove:
        try:
            del stac_properties[prop]
        except KeyError:
            logging.warning(f"Failed removing {prop}, property not found")

    geom_item = ras_stac_geom.to_item(stac_properties, item_id)

    if item_props_to_add:
        geom_item.properties.update(item_props_to_add)

    if asset_list:
        add_assets_to_item(geom_item, asset_list, s3_resource)

    # Transform cell size properties (take square root of area)
    cell_area_to_distance(
        geom_item,
        [
            "2d_flow_areas:cell_average_size",
            "2d_flow_areas:cell_maximum_size",
            "2d_flow_areas:cell_minimum_size",
        ],
    )
    # Make projection seperate from general metadata
    if "projection" in geom_item.properties:
        geom_item.properties["proj:wkt2"] = geom_item.properties.pop("projection")

    return geom_item


def main(
    ras_geom_hdf: str,
    new_item_s3_path: str,
    item_id: str = None,
    asset_list: list = None,
    item_props_to_remove: list = None,
    item_props_to_add: dict = None,
):

    verify_safe_prefix(new_item_s3_path)
    logging.info(f"Creating geom item: {new_item_s3_path}")

    # Add the geom HDF file to the asset_list
    asset_list.append(ras_geom_hdf)

    _, s3_client, s3_resource = init_s3_resources()

    # Create geometry item
    if item_id:
        geom_hdf_obj, _ = read_geom_hdf_from_s3(ras_geom_hdf)
    else:
        geom_hdf_obj, item_id = read_geom_hdf_from_s3(ras_geom_hdf)

    geom_item = new_geom_item(
        geom_hdf_obj,
        item_id,
        asset_list,
        item_props_to_remove,
        item_props_to_add,
        s3_resource,
    )

    logging.info("Writing geom item to s3")
    copy_item_to_s3(geom_item, new_item_s3_path, s3_client)

    result = [
        {
            "href": new_item_s3_path,
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

    PLUGIN_PARAMS = check_params(main)
    input_params = parse_input(sys.argv, PLUGIN_PARAMS)

    ras_geom_hdf = input_params.get("ras_geom_hdf", None)
    new_item_s3_path = input_params.get("new_item_s3_path", None)

    # Optional parameters
    item_id = input_params.get("item_id", None)
    asset_list = input_params.get("asset_list", [])
    item_props_to_remove = input_params.get("item_props_to_remove", [])
    item_props_to_add = input_params.get("item_props", {})

    result = main(
        ras_geom_hdf,
        new_item_s3_path,
        item_id,
        asset_list,
        item_props_to_remove,
        item_props_to_add,
    )
    print_results(result)
