import pystac
import json
from datetime import datetime
from pathlib import Path
import re

import logging

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

from .ras_hdf import *


def create_model_item(ras_geom_hdf_url: str, simplify: float = 100.0) -> pystac.Item:
    """
    This function creates a STAC (SpatioTemporal Asset Catalog) item from a given HDF (Hierarchical Data Format)
    file URL.

    Parameters:
    ras_geom_hdf_url (str): The URL of the HDF file.
    simplify (float): The factor to simplify the 2D flow area perimeter. Default is 100.0.

    Returns:
    pystac.Item: The created STAC item.

    Raises:
    ValueError: If the provided URL does not have a '.hdf' suffix.

    The function performs the following steps:
    1. Checks if the provided URL has a '.hdf' suffix. If not, it raises a ValueError.
    2. Extracts the model name from the URL by removing the '.hdf' suffix and getting the stem of the path.
    3. Logs the creation of the STAC item for the model.
    4. Opens the HDF file from the URL using the `RasGeomHdf.open_url` method.
    5. Gets the perimeter of the 2D flow area from the HDF file and simplifies it using the provided `simplify`
      parameter.
    6. Gets the attributes of the geometry from the HDF file.
    7. Extracts the geometry time from the properties.
    8. Creates a new STAC item with the model ID, the geometry converted to GeoJSON, the bounding box of the perimeter,
      the geometry time converted to a datetime object, and the properties.
    9. Returns the created STAC item.
    """
    if Path(ras_geom_hdf_url).suffix != ".hdf":
        raise ValueError(f"Expected pattern: `s3://bucket/prefix/ras-model-name.g**.hdf`, got {ras_geom_hdf_url}")

    ras_model_name = Path(ras_geom_hdf_url.replace(".hdf", "")).stem

    logging.info(f"Creating STAC item for model {ras_model_name}")

    ras_hdf = RasGeomHdf.open_url(ras_geom_hdf_url)
    perimeter = ras_hdf.get_2d_flow_area_perimeter(simplify=simplify)
    properties = ras_hdf.get_geom_attrs()
    geometry_time = properties.get("geometry:geometry_time")
    model_id = ras_model_name

    item = pystac.Item(
        id=model_id,
        geometry=json.loads(shapely.to_geojson(perimeter)),
        bbox=perimeter.bounds,
        datetime=datetime.fromisoformat(geometry_time),
        properties=properties,
    )
    return item


def new_geom_assets(
    topo_assets: list = None,
    lulc_assets: list = None,
    mannings_assets: list = None,
    other_assets: list = None,
):
    """
    This function creates a dictionary of geometric assets.

    Parameters:
        topo_assets (list): The topographic assets. Default is None.
        lulc_assets (list): The land use and land cover assets. Default is None.
        mannings_assets (list): The Manning's roughness coefficient assets. Default is None.
        other_assets (list): Any other assets. Default is None.

    Returns:
        dict: A dictionary with keys "topo", "lulc", "mannings", and "other", and values being the corresponding input
        parameters.
    """
    geom_assets = {
        "topo": topo_assets,
        "lulc": lulc_assets,
        "mannings": mannings_assets,
        "other": other_assets,
    }
    return geom_assets


def ras_geom_asset_info(s3_key: str, asset_type: str) -> dict:
    """
    This function generates information about a geometric asset used in a HEC-RAS model.

    Parameters:
        asset_type (str): The type of the asset. Must be one of: "mannings", "lulc", "topo", "other".
        s3_key (str): The S3 key of the asset.

    Returns:
        dict: A dictionary with the roles, the description, and the title of the asset.

    Raises:
        ValueError: If the provided asset_type is not one of: "mannings", "lulc", "topo", "other".
    """

    if asset_type not in ["mannings", "lulc", "topo", "other"]:
        raise ValueError(f"asset_type must be one of: mannings, lulc, topo, other")

    file_extension = Path(s3_key).suffix
    title = Path(s3_key).name

    if asset_type == "mannings":
        description = "Friction surface used in HEC-RAS model geometry"

    elif asset_type == "lulc":
        description = "Land Use / Land Cover data used in HEC-RAS model geometry"

    elif asset_type == "topo":
        description = "Topo data used in HEC-RAS model geometry"

    elif asset_type == "other":
        description = "Other data used in HEC-RAS model geometry"

    else:
        description = ""

    if file_extension == ".hdf":
        roles = [pystac.MediaType.HDF, f"ras-{asset_type}"]
    elif file_extension == ".tif":
        roles = [pystac.MediaType.GEOTIFF, f"ras-{asset_type}"]
    else:
        roles = [f"ras-{asset_type}"]

    return {"roles": roles, "description": description, "title": title}


def ras_plan_asset_info(s3_key: str) -> dict:
    """
    This function generates information about a plan asset used in a HEC-RAS model.

    Parameters:
        s3_key (str): The S3 key of the asset.

    Returns:
        dict: A dictionary with the roles, the description, and the title of the asset.

    The function performs the following steps:
    1. Extracts the file extension and the file name from the provided `s3_key`.
    2. If the file extension is ".hdf", it sets the `ras_extension` to the extension of the file name without the
      ".hdf" suffix and adds `pystac.MediaType.HDF5` to the roles. Otherwise, it sets the `ras_extension` to the file
        extension.
    3. Removes the leading dot from the `ras_extension`.
    4. Depending on the `ras_extension`, it sets the roles and the description for the asset. The `ras_extension` is
      expected to match one of the following patterns: "g[0-9]{2}", "p[0-9]{2}", "u[0-9]{2}", "s[0-9]{2}", "prj",
      "dss", "log". If it doesn't match any of these patterns, it adds "ras-file" to the roles.
    5. Returns a dictionary with the roles, the description, and the title of the asset.
    """
    file_extension = Path(s3_key).suffix
    title = Path(s3_key).name
    description = ""
    roles = []

    if file_extension == ".hdf":
        ras_extension = Path(s3_key.replace(".hdf", "")).suffix
        roles.append(pystac.MediaType.HDF5)
    else:
        ras_extension = file_extension

    ras_extension = ras_extension.lstrip(".")

    if re.match("g[0-9]{2}", ras_extension):
        roles.append("ras-geometry")
        if file_extension != ".hdf":
            roles.append(pystac.MediaType.TEXT)
            description = """The geometry file contains the 2D flow area perimeter and other geometry information."""

    elif re.match("p[0-9]{2}", ras_extension):
        roles.append("ras-plan")
        if file_extension != ".hdf":
            roles.append(pystac.MediaType.TEXT)
            description = """The plan file contains the simulation plan and other simulation information."""

    elif re.match("u[0-9]{2}", ras_extension):
        roles.extend(["ras-unsteady", pystac.MediaType.TEXT])
        description = """The unsteady file contains the unsteady flow results and other simulation information."""

    elif re.match("s[0-9]{2}", ras_extension):
        roles.extend(["ras-steady", pystac.MediaType.TEXT])
        description = """The steady file contains the steady flow results and other simulation information."""

    elif ras_extension == "prj":
        roles.extend(["ras-project", pystac.MediaType.TEXT])
        description = """The project file contains the project information and other simulation information."""

    elif ras_extension == "dss":
        roles.extend(["ras-dss"])
        description = """The dss file contains the dss results and other simulation information."""

    elif ras_extension == "log":
        roles.extend(["ras-log", pystac.MediaType.TEXT])
        description = """The log file contains the log information and other simulation information."""

    else:
        roles.extend(["ras-file"])

    return {"roles": roles, "description": description, "title": title}


def get_simulation_metadata(ras_plan_hdf_url: str, simulation: str) -> dict:
    """
    This function retrieves the metadata of a simulation from a HEC-RAS plan HDF file.

    Parameters:
        ras_plan_hdf_url (str): The URL of the HEC-RAS plan HDF file.
        simulation (str): The name of the simulation.

    Returns:
        dict: A dictionary with the metadata of the simulation.

    The function performs the following steps:
    1. Opens the HEC-RAS plan HDF file from the provided URL in read-binary mode.
    2. Initializes a dictionary `metadata` with the key "ras:simulation" and the value being the provided
      `simulation` such as the plan id.
    3. Tries to open the HEC-RAS plan HDF file in read mode. If the file is not found, it logs an error
      and returns.
    4. Tries to get the plan attributes from the HDF file and update the `metadata` dictionary with them.
      If an exception occurs, it logs an error and returns.
    5. Tries to get the plan results attributes from the HDF file and update the `metadata` dictionary with
      them. If an exception occurs, it logs an error and returns.
    6. Returns the `metadata` dictionary.
    """
    s3f = fsspec.open(ras_plan_hdf_url, mode="rb")
    metadata = {
        "ras:simulation": simulation,
    }

    try:
        plan_hdf = RasPlanHdf(s3f.open(), mode="r")
    except FileNotFoundError as e:
        return logging.error(f"file not found: {ras_plan_hdf_url}")

    try:
        plan_attrs = plan_hdf.get_plan_attrs()
        metadata.update(plan_attrs)
    except Exception as e:
        return logging.error(f"unable to extract plan_attrs: {e}")

    try:
        results_attrs = plan_hdf.get_plan_results_attrs()
        metadata.update(results_attrs)
    except Exception as e:
        return logging.error(f"unable to extract results_attrs: {e}")

    return metadata


def create_model_simulation_item(ras_item: pystac.Item, results_meta: dict, model_sim_id: str) -> pystac.Item:
    """
    This function creates a PySTAC Item for a model simulation.

    Parameters:
        ras_item (pystac.Item): The PySTAC Item of the RAS model.
        results_meta (dict): The metadata of the simulation results.
        model_sim_id (str): The ID of the model simulation.

    Returns:
        pystac.Item: A PySTAC Item for the model simulation.

    The function performs the following steps:
    1. Retrieves the runtime window from the `results_meta` dictionary.
    2. Converts the start and end times of the runtime window from ISO format to datetime objects.
    3. Creates a PySTAC Item with the ID being the `model_sim_id`, the geometry and the bounding box being those of
      the `ras_item`, the start and end datetimes being the converted start and end times of the runtime window,
      the datetime being the start datetime, and the properties being the `results_meta`.
    4. Returns the created PySTAC Item.
    """
    runtime_window = results_meta.get("results_summary:run_time_window")
    start_datetime = datetime.fromisoformat(runtime_window[0])
    end_datetime = datetime.fromisoformat(runtime_window[1])
    item = pystac.Item(
        id=model_sim_id,
        geometry=ras_item.geometry,
        bbox=ras_item.bbox,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        datetime=start_datetime,
        properties=results_meta,
    )
    return item
