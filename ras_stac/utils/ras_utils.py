import logging
from dotenv import load_dotenv, find_dotenv
import pystac
import json
from typing import List
import shapely
from pathlib import Path
import re
from datetime import datetime

from rashdf import RasPlanHdf, RasGeomHdf
from rashdf.utils import parse_duration

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

load_dotenv(find_dotenv())


class RasStacGeom:
    def __init__(self, rg: RasGeomHdf):
        self.rg = rg

    def get_stac_geom_attrs(self) -> dict:
        """
        This function retrieves the geometry attributes of a HEC-RAS HDF file, converting them to STAC format.

        Returns:
            stac_geom_attrs (dict): A dictionary with the organized geometry attributes.
        """

        stac_geom_attrs = self.rg.get_root_attrs()
        if stac_geom_attrs is not None:
            stac_geom_attrs = prep_stac_attrs(stac_geom_attrs)
        else:
            stac_geom_attrs = {}
            logging.warning("No root attributes found.")

        geom_attrs = self.rg.get_geom_attrs()
        if geom_attrs is not None:
            geom_stac_attrs = prep_stac_attrs(geom_attrs, prefix="Geometry")
            stac_geom_attrs.update(geom_stac_attrs)
        else:
            logging.warning("No base geometry attributes found.")

        structures_attrs = self.rg.get_geom_structures_attrs()
        if structures_attrs is not None:
            structures_stac_attrs = prep_stac_attrs(
                structures_attrs, prefix="Structures"
            )
            stac_geom_attrs.update(structures_stac_attrs)
        else:
            logging.warning("No geometry structures attributes found.")

        d2_flow_area_attrs = self.rg.get_geom_2d_flow_area_attrs()
        if d2_flow_area_attrs is not None:
            d2_flow_area_stac_attrs = prep_stac_attrs(
                d2_flow_area_attrs, prefix="2D Flow Areas"
            )
            cell_average_size = d2_flow_area_stac_attrs.get(
                "2d_flow_area:cell_average_size", None
            )
            if cell_average_size is not None:
                d2_flow_area_stac_attrs["2d_flow_area:cell_average_length"] = (
                    cell_average_size**0.5
                )
            else:
                logging.warning("Unable to add cell average size to attributes.")
            stac_geom_attrs.update(d2_flow_area_stac_attrs)
        else:
            logging.warning("No flow area attributes found.")

        return stac_geom_attrs

    def get_perimeter(self, simplify: float = None, crs: str = "EPSG:4326"):
        return ras_perimeter(self.rg, simplify, crs)

    def to_item(
        self,
        props_to_remove: List,
        ras_model_name: str,
        simplify: float = None,
        stac_item_id: str = None,
    ) -> pystac.Item:
        """
        Creates a STAC (SpatioTemporal Asset Catalog) item from a given RasGeomHdf object.

        Parameters:
        props_to_remove (List): List of properties to be removed from the item.
        ras_model_name (str): The name of the RAS model.
        simplify (float, optional): Tolerance for simplifying the perimeter polygon. Defaults to None.

        Returns:
        pystac.Item: The created STAC item.

        Raises:
        AttributeError: If the properties cannot be extracted from the RasGeomHdf object.

        The function performs the following steps:
        1. Gets the perimeter of the 2D flow area from the RasGeomHdf object.
        2. Extracts the attributes of the geometry from the RasGeomHdf object.
        3. Extracts the geometry time from the properties.
        4. Removes unwanted properties specified in `props_to_remove`.
        5. Creates a new STAC item with the model ID, the geometry converted to GeoJSON, the bounding box of the perimeter, and the properties.
        6. Returns the created STAC item.
        """

        perimeter_polygon = self.get_perimeter(simplify)

        properties = self.get_stac_geom_attrs()
        if not properties:
            raise AttributeError(
                f"Could not find properties while creating model item for {ras_model_name}."
            )

        geometry_time = properties.get("geometry:geometry_time")
        if not geometry_time:
            raise AttributeError(
                f"Could not find data for 'geometry:geometry_time' while creating model item for {ras_model_name}."
            )

        for prop in props_to_remove:
            try:
                del properties[prop]
            except KeyError:
                logging.warning(f"Failed removing {prop}, property not found")

        iso_properties = properties_to_isoformat(properties)

        if not stac_item_id:
            stac_item_id = ras_model_name

        item = pystac.Item(
            id=stac_item_id,
            geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
            bbox=perimeter_polygon.bounds,
            datetime=geometry_time,
            properties=iso_properties,
        )
        return item


class RasStacPlan(RasStacGeom):
    def __init__(self, rp: RasPlanHdf):
        super().__init__(rp)
        self.rp = rp

    def to_item(
        self,
        ras_item: pystac.Item,
        results_meta: dict,
        model_sim_id: str,
        item_props_to_remove: List,
    ) -> pystac.Item:
        """
        This function creates a PySTAC Item for a model simulation.

        Parameters:
            ras_item (pystac.Item): The PySTAC Item of the RAS model.
            results_meta (dict): The metadata of the simulation results.
            model_sim_id (str): The ID of the model simulation.
            item_props_to_remove (List): List of properties to be removed from the item.

        Returns:
            pystac.Item: A PySTAC Item for the model simulation.

        The function performs the following steps:
        1. Retrieves the runtime window from the `results_meta` dictionary.
        2. Removes unwanted properties.
        3. Creates a PySTAC Item with the ID being the `model_sim_id`, the geometry and the bounding box being those of
        the `ras_item`, the start and end datetimes being the converted start and end times of the runtime window,
        the datetime being the start datetime, and the properties being the `results_meta` with unwanted properties removed.
        4. Returns the created PySTAC Item.
        """
        runtime_window = results_meta.get("results_summary:run_time_window")
        if not runtime_window:
            raise AttributeError(
                f"Could not find data for 'results_summary:run_time_window' while creating model item for model id:{model_sim_id}."
            )
        start_datetime = runtime_window[0]
        end_datetime = runtime_window[1]

        for prop in item_props_to_remove:
            try:
                del results_meta[prop]
            except KeyError:
                logging.warning(
                    f"Failed to remove property:{prop} not found in simulation results metadata."
                )

        properties = properties_to_isoformat(results_meta)

        item = pystac.Item(
            id=model_sim_id,
            geometry=ras_item.geometry,
            bbox=ras_item.bbox,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            datetime=start_datetime,
            properties=properties,
        )
        return item

    def get_stac_plan_attrs(self, include_results: bool = False) -> dict:
        """
        This function retrieves the attributes of a plan from a HEC-RAS plan HDF file, converting them to STAC format.

        Parameters:
            include_results (bool, optional): Whether to include the results attributes in the returned dictionary.
                Defaults to False.

        Returns:
            stac_plan_attrs (dict): A dictionary with the attributes of the plan.
        """
        stac_plan_attrs = self.rp.get_root_attrs()
        if stac_plan_attrs is not None:
            stac_plan_attrs = prep_stac_attrs(stac_plan_attrs)
        else:
            stac_plan_attrs = {}
            logging.warning("No root attributes found.")

        plan_info_attrs = self.rp.get_plan_info_attrs()
        if plan_info_attrs is not None:
            plan_info_stac_attrs = prep_stac_attrs(
                plan_info_attrs, prefix="Plan Information"
            )
            stac_plan_attrs.update(plan_info_stac_attrs)
        else:
            logging.warning("No plan information attributes found.")

        plan_params_attrs = self.rp.get_plan_param_attrs()
        if plan_params_attrs is not None:
            plan_params_stac_attrs = prep_stac_attrs(
                plan_params_attrs, prefix="Plan Parameters"
            )
            stac_plan_attrs.update(plan_params_stac_attrs)
        else:
            logging.warning("No plan parameters attributes found.")

        precip_attrs = self.rp.get_meteorology_precip_attrs()
        if precip_attrs is not None:
            precip_stac_attrs = prep_stac_attrs(precip_attrs, prefix="Meteorology")
            precip_stac_attrs.pop("meteorology:projection", None)
            stac_plan_attrs.update(precip_stac_attrs)
        else:
            logging.warning("No meteorology precipitation attributes found.")

        if include_results:
            stac_plan_attrs.update(self.rp.get_stac_plan_results_attrs())
        return stac_plan_attrs

    def get_stac_plan_results_attrs(self):
        """
        This function retrieves the results attributes of a plan from a HEC-RAS plan HDF file, converting
        them to STAC format. For summary atrributes, it retrieves the total computation time, the run time window,
        and the solution from it, and calculates the total computation time in minutes if it exists.

        Returns:
            results_attrs (dict): A dictionary with the results attributes of the plan.
        """
        results_attrs = {}

        unsteady_results_attrs = self.rp.get_results_unsteady_attrs()
        if unsteady_results_attrs is not None:
            unsteady_results_stac_attrs = prep_stac_attrs(
                unsteady_results_attrs, prefix="Unsteady Results"
            )
            results_attrs.update(unsteady_results_stac_attrs)
        else:
            logging.warning("No unsteady results attributes found.")

        summary_attrs = self.rp.get_results_unsteady_summary_attrs()
        if summary_attrs is not None:
            summary_stac_attrs = prep_stac_attrs(
                summary_attrs, prefix="Results Summary"
            )
            computation_time_total = str(
                summary_stac_attrs.get("results_summary:computation_time_total")
            )
            results_summary = {
                "results_summary:computation_time_total": computation_time_total,
                "results_summary:run_time_window": summary_stac_attrs.get(
                    "results_summary:run_time_window"
                ),
                "results_summary:solution": summary_stac_attrs.get(
                    "results_summary:solution"
                ),
            }
            if computation_time_total is not None:
                computation_time_total_minutes = (
                    parse_duration(computation_time_total).total_seconds() / 60
                )
                results_summary["results_summary:computation_time_total_minutes"] = (
                    computation_time_total_minutes
                )
            results_attrs.update(results_summary)
        else:
            logging.warning("No unsteady results summary attributes found.")

        volume_accounting_attrs = self.rp.get_results_volume_accounting_attrs()
        if volume_accounting_attrs is not None:
            volume_accounting_stac_attrs = prep_stac_attrs(
                volume_accounting_attrs, prefix="Volume Accounting"
            )
            results_attrs.update(volume_accounting_stac_attrs)
        else:
            logging.warning("No results volume accounting attributes found.")

        return results_attrs

    def get_simulation_metadata(self, simulation: str) -> dict:
        """
        This function retrieves the metadata of a simulation from a HEC-RAS plan HDF file.

        Parameters:
            simulation (str): The name of the simulation.

        Returns:
            dict: A dictionary with the metadata of the simulation.

        The function performs the following steps:
        1. Initializes a metadata dictionary with the key "ras:simulation" and the value being the provided simulation.
        2. Tries to get the plan attributes from the RasPlanHdf object and update the `metadata` dictionary with them.
        3. Tries to get the plan results attributes from the RasPlanHdf object and update the `metadata` dictionary with them.
        4. Returns the `metadata` dictionary.
        """
        metadata = {"ras:simulation": simulation}

        try:
            plan_attrs = self.get_stac_plan_attrs()
            metadata.update(plan_attrs)
        except Exception as e:
            return logging.error(f"unable to extract plan_attrs from plan: {e}")

        try:
            results_attrs = self.get_stac_plan_results_attrs()
            metadata.update(results_attrs)
        except Exception as e:
            return logging.error(f"unable to extract results_attrs from plan: {e}")

        return metadata


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
        raise ValueError("asset_type must be one of: mannings, lulc, topo, other")

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


def to_snake_case(text):
    """
    Convert a string to snake case, removing punctuation and other symbols.

    Parameters:
        text (str): The string to be converted.

    Returns:
        str: The snake case version of the string.
    """
    import re

    # Remove all non-word characters (everything except numbers and letters)
    text = re.sub(r"[^\w\s]", "", text)

    # Replace all runs of whitespace with a single underscore
    text = re.sub(r"\s+", "_", text)

    return text.lower()


def prep_stac_attrs(attrs: dict, prefix: str = None) -> dict:
    """
    Converts an unformatted HDF attributes dictionary to STAC format by converting values to snake case
    and adding a prefix if one is given.

    Parameters:
        attrs (dict): Unformatted attribute dictionary.
        prefix (str): Optional prefix to be added to each key of formatted dictionary.

    Returns:
        results (dict): The new attribute dictionary snake case values and prefix.
    """
    results = {}
    for k, value in attrs.items():
        if prefix:
            key = f"{to_snake_case(prefix)}:{to_snake_case(k)}"
        else:
            key = to_snake_case(k)
        results[key] = value

    return results


def ras_perimeter(rg: RasGeomHdf, simplify: float = None, crs: str = "EPSG:4326"):
    """
    Calculate the perimeter of a HEC-RAS geometry as a GeoDataFrame in the specified coordinate reference system.

    Parameters:
        rg (RasGeomHdf): A HEC-RAS geometry HDF file object which provides mesh areas.
        simplify (float, optional): A tolerance level to simplify the perimeter geometry to reduce complexity.
                                    If None, the geometry will not be simplified. Defaults to None.
        crs (str): The coordinate reference system which the perimeter geometry will be converted to. Defaults to "EPSG:4326".

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the calculated perimeter polygon in the specified CRS.
    """

    perimeter = rg.mesh_areas()
    perimeter = perimeter.to_crs(crs)
    if simplify:
        perimeter_polygon = perimeter.geometry.unary_union.simplify(tolerance=simplify)
    else:
        perimeter_polygon = perimeter.geometry.unary_union
    return perimeter_polygon


def properties_to_isoformat(properties):
    """Converts datetime objects in properties to isoformat"""
    for k, v in properties.items():
        if isinstance(v, list):
            properties[k] = [
                item.isoformat() if isinstance(item, datetime) else item for item in v
            ]
        elif isinstance(v, datetime):
            properties[k] = v.isoformat()
    return properties
