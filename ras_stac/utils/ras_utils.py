import logging
from dotenv import load_dotenv, find_dotenv
import pystac
import json
import shapely
from pathlib import Path
import re
from datetime import datetime, timezone
import numpy as np
from rashdf import RasPlanHdf, RasGeomHdf
from rashdf.utils import parse_duration
from .s3_utils import (
    s3_path_public_url_converter,
    split_s3_path,
    get_basic_object_metadata,
)

load_dotenv(find_dotenv())


class RasStacGeom:
    def __init__(self, rg: RasGeomHdf):
        self.rg = rg

    def get_stac_geom_attrs(self) -> dict:
        """
        Retrieves the geometry attributes of a HEC-RAS HDF file, converting them to STAC format.

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
        item_properties: dict,
        stac_item_id: str,
        simplify: float = None,
    ) -> pystac.Item:
        """
        Creates a STAC (SpatioTemporal Asset Catalog) item from a given RasGeomHdf object.

        Parameters:
        item_properties (dict): A dictionary containing the properties for the STAC item. Should include geometry time or runtime window for STAC item date.
        ras_model_name (str): The name of the RAS model.
        simplify (float, optional): Tolerance for simplifying the perimeter polygon. Defaults to None.

        Returns:
        pystac.Item: The created STAC item.

        Raises:
        AttributeError: Raised if neither 'geometry_time' nor 'runtime_window' is present in the provided item properties.

        The function performs the following steps:
        1. Retrieves the perimeter polygon of the 2D flow area from the RasGeomHdf object.
        2. Extracts the runtime window or geometry time from the `item_properties` dictionary.
        3. If a runtime window is present, it sets the start and end datetime for the STAC item using the values from the runtime window.
        4. If no runtime window is available, it uses the geometry time as the datetime for the STAC item.
        5. Converts the perimeter geometry to GeoJSON format and adds it to the STAC item.
        6. Returns the created STAC item with the geometry, bounding box, temporal properties, and additional item properties.
        """

        perimeter_polygon = self.get_perimeter(simplify)

        runtime_window = item_properties.get("results_summary:run_time_window")
        geometry_time = item_properties.get("geometry:geometry_time")
        iso_properties = properties_to_isoformat(item_properties)

        if runtime_window:
            start_datetime = runtime_window[0]
            end_datetime = runtime_window[1]
            item = pystac.Item(
                id=stac_item_id,
                geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
                bbox=perimeter_polygon.bounds,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                datetime=start_datetime,
                properties=iso_properties,
            )
            return item

        elif geometry_time:
            item = pystac.Item(
                id=stac_item_id,
                geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
                bbox=perimeter_polygon.bounds,
                datetime=geometry_time,
                properties=iso_properties,
            )
            return item

        else:
            logging.warning(
                "No runtime window or geometry time found in properties for item: {stac_item_id}. Using current time as item datetime."
            )
            datetime_utc = datetime.now(tz=timezone.utc)
            item = pystac.Item(
                id=stac_item_id,
                geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
                bbox=perimeter_polygon.bounds,
                datetime=datetime_utc,
                properties=iso_properties,
            )
            return item


class RasStacPlan(RasStacGeom):
    def __init__(self, rp: RasPlanHdf):
        super().__init__(rp)
        self.rp = rp

    def get_plan_attrs(self, include_results: bool = False) -> dict:
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
            stac_plan_attrs.update(self.rp.get_plan_results_attrs())
        return stac_plan_attrs

    def get_plan_results_attrs(self):
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

    def get_stac_plan_attrs(self, simulation: str) -> dict:
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
            plan_attrs = self.get_plan_attrs()
            metadata.update(plan_attrs)
        except Exception as e:
            return logging.error(f"unable to extract plan_attrs from plan: {e}")

        try:
            results_attrs = self.get_plan_results_attrs()
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
        s3_key (str): The S3 key of the asset.
        asset_type (str): The type of the asset. Must be one of: "mannings", "lulc", "topo", "other".

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


def get_ras_asset_info(s3_key: str) -> dict:
    """
    This function generates information about a HEC-RAS model asset including roles, descriptions, and titles.

    Parameters:
        s3_key (str): The S3 key of the asset.

    Returns:
        dict: A dictionary with the roles, the description, and the title of the asset.

    The function performs the following steps:
    1. Extracts the file extension and the file name from the provided s3_key.
    2. If the file extension is '.hdf', it adjusts the ras_extension to the file names extension without '.hdf'
       and adds pystac.MediaType.HDF5 to the roles. Otherwise, it sets the ras_extension based on the file extension.
    3. Strips the leading dot (.) from ras_extension.
    4. Based on the value of ras_extension, the function determines the asset's type, assigns appropriate roles,
       and provides a descriptive message for the asset. If the file doesn't match a known pattern, the generic role
       "ras-file" is assigned.
    5. Returns a dictionary with the roles, the description, and the title of the asset.
    """
    file_extension = Path(s3_key).suffix
    full_extension = s3_key.rsplit("/")[-1].split(".", 1)[1]
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
        roles.extend(["geometry-file", "ras-file"])
        description = """The geometry file which contains cross-sectional, hydraulic structures, and modeling approach data."""
        if file_extension != ".hdf":
            roles.extend([pystac.MediaType.TEXT])

    elif re.match("p[0-9]{2}", ras_extension):
        roles.extend(["plan-file", "ras-file"])
        description = """The plan file which contains a list of associated input files and all simulation options."""
        if file_extension != ".hdf":
            roles.extend([pystac.MediaType.TEXT])
    elif re.match("f[0-9]{2}", ras_extension):
        roles.extend(["steady-flow-file", "ras-file", pystac.MediaType.TEXT])
        description = """Steady Flow file which contains profile information, flow data, and boundary conditions."""

    elif re.match("q[0-9]{2}", ras_extension):
        roles.extend(["quasi-unsteady-flow-file", "ras-file", pystac.MediaType.TEXT])
        description = """Quasi-Unsteady Flow file."""

    elif re.match("u[0-9]{2}", ras_extension):
        roles.extend(["unsteady-file", "ras-file", pystac.MediaType.TEXT])
        description = """The unsteady file contains hydrographs amd initial conditions, as well as any flow options."""

    elif re.match("r[0-9]{2}", ras_extension):
        roles.extend(["run-file", "ras-file", pystac.MediaType.TEXT])
        description = """Run file for steady flow analysis which contains all the necessary input data required for the RAS computational engine."""

    elif re.match("hyd[0-9]{2}", ras_extension):
        roles.extend(
            ["computational-level-output-file", "ras-file", pystac.MediaType.TEXT]
        )
        description = """Detailed Computational Level output file."""

    elif re.match("c[0-9]{2}", ras_extension):
        roles.extend(
            ["geometric-preprocessor-output-file", "ras-file", pystac.MediaType.TEXT]
        )
        description = """Geomatric Pre-Processor output file. Contains the hydraulic properties tables, rating curves, and family of rating curves for each cross-section, bridge, culvert, storage area, inline and lateral structure."""

    elif re.match("b[0-9]{2}", ras_extension):
        roles.extend(["boundary-condition-file", "ras-file", pystac.MediaType.TEXT])
        description = """Boundary Condition file."""

    elif re.match("bco[0-9]{2}", ras_extension):
        roles.extend(["unsteady-flow-log-file", "ras-file", pystac.MediaType.TEXT])
        description = """Unsteady Flow Log output file."""

    elif re.match("S[0-9]{2}", ras_extension):
        roles.extend(["sediment-data-file", "ras-file", pystac.MediaType.TEXT])
        description = """Sediment data file which contains flow data, boundary conditions, and sediment data."""

    elif re.match("H[0-9]{2}", ras_extension):
        roles.extend(["hydraulic-design-file", "ras-file", pystac.MediaType.TEXT])
        description = """Hydraulic Design data file."""

    elif re.match("W[0-9]{2}", ras_extension):
        roles.extend(["water-quality-file", "ras-file", pystac.MediaType.TEXT])
        description = """Water Quality data file which contains temperature boundary conditions, initial conditions, advection dispersion parameters and meteorological data."""

    elif re.match("SedCap[0-9]{2}", ras_extension):
        roles.extend(
            ["sediment-transport-capacity-file", "ras-file", pystac.MediaType.TEXT]
        )
        description = """Sediment Transport Capacity data."""

    elif re.match("SedXS[0-9]{2}", ras_extension):
        roles.extend(["xs-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Cross section output file."""

    elif re.match("SedHeadXS[0-9]{2}", ras_extension):
        roles.extend(["xs-output-header-file", "ras-file", pystac.MediaType.TEXT])
        description = """Header file for the cross section output."""

    elif re.match("wqrst[0-9]{2}", ras_extension):
        roles.extend(["water-quality-restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """The water quality restart file."""

    elif ras_extension == "sed":
        roles.extend(["sediment-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """Detailed sediment output file."""

    elif ras_extension == "blf":
        roles.extend(["binary-log-file", "ras-file", pystac.MediaType.TEXT])
        description = """Binary Log file."""

    elif ras_extension == "prj" and title != "MMC_Projection.prj":
        roles.extend(["project-file", "ras-file", pystac.MediaType.TEXT])
        description = """Project file for ras. Contains current plan files, units, and project description."""

    elif ras_extension == "prj" and title == "MMC_Projection.prj":
        roles.extend(["projection-file", "ras-file", pystac.MediaType.TEXT])
        description = """Projection file."""

    elif ras_extension == "dss":
        roles.extend(["ras-dss", "ras-file"])
        description = """The dss file contains the dss results and other simulation information."""

    elif ras_extension == "log":
        roles.extend(["ras-log", "ras-file", pystac.MediaType.TEXT])
        description = """The log file contains the log information and other simulation information."""

    elif ras_extension == "png":
        roles.extend(["thumbnail", pystac.MediaType.PNG])
        description = """PNG of geometry with OpenStreetMap basemap."""
        title = "Thumbnail"

    elif ras_extension == "gpkg":
        roles.extend(["ras-geometry-gpkg", pystac.MediaType.GEOPACKAGE])
        description = """GeoPackage file with geometry data extracted from .gxx file."""
        title = "GeoPackage_file"

    elif ras_extension == "rst":
        roles.extend(["restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """Restart file."""
        title = "Restart_file"

    elif ras_extension == "SiamInput":
        roles.extend(["siam-input-file", "ras-file", pystac.MediaType.TEXT])
        description = """SIAM Input Data file."""

    elif ras_extension == "SiamOutput":
        roles.extend(["siam-output-file", "ras-file", pystac.MediaType.TEXT])
        description = """SIAM Output Data file."""

    elif re.match("bco[0-9]{2}", ras_extension):
        roles.extend(["water-quality-log", "ras-file", pystac.MediaType.TEXT])
        description = """Water quality log file."""
        title = "Water_quality_log_file"

    elif ras_extension == "color_scales":
        roles.extend(["color-scales", "ras-file", pystac.MediaType.TEXT])
        description = """File that contains the water quality color scale."""

    elif full_extension == "comp_msgs.txt":
        roles.extend(["computational-message-file", "ras-file", pystac.MediaType.TEXT])
        description = """Computational Message text file which contains the computational messages that pop up in the computation window."""
        title = "Computational_message_file"

    elif re.match("x[0-9]{2}", ras_extension):
        roles.extend(["run-file", "ras-file", pystac.MediaType.TEXT])
        description = """Run file for Unsteady Flow."""

    elif re.match("O[0-9]{2}", full_extension):
        roles.extend(["output-file", "ras-file", pystac.MediaType.TEXT])
        description = (
            """Output file for ras which contains all of the computed results."""
        )

    elif re.match("IC.O[0-9]{2}", full_extension):
        roles.extend(["initial-conditions-file", "ras-file", pystac.MediaType.TEXT])
        description = """Initial conditions file for unsteady flow plan."""

    elif re.match("p[0-9]{2}.rst", full_extension):
        roles.extend(["restart-file", "ras-file", pystac.MediaType.TEXT])
        description = """Restart file."""

    elif full_extension == "rasmap":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Ras Mapper file."""

    elif full_extension == "rasmap.backup":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Backup Ras Mapper file."""

    elif full_extension == "rasmap.original":
        roles.extend(["ras-mapper-file", "ras-file", pystac.MediaType.TEXT])
        description = """Original Ras Mapper file."""
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
        perimeter_polygon = perimeter.geometry.union_all().simplify(tolerance=simplify)
    else:
        perimeter_polygon = perimeter.geometry.union_all()
    return perimeter_polygon


def properties_to_isoformat(properties: dict):
    """Converts datetime objects in properties to isoformat

    Parameters:
        properties (dict): Properties dictionary with datetime object values

    Returns:
        properties (dict): Properties dictionary with datetime objects converted to isoformat
    """
    for k, v in properties.items():
        if isinstance(v, list):
            properties[k] = [
                item.isoformat() if isinstance(item, datetime) else item for item in v
            ]
        elif isinstance(v, datetime):
            properties[k] = v.isoformat()
    return properties


def add_assets_to_item(item, asset_list: list, s3_resource: None):
    """
    Adds assets to a STAC item using the asset list and fetches metadata from S3.
    """

    for asset_file in asset_list:
        bucket, asset_key = split_s3_path(asset_file)
        logging.info(f"Adding asset {asset_file} to item")

        if s3_resource is not None:
            assets_bucket = s3_resource.Bucket(bucket)
            obj = assets_bucket.Object(asset_key)
            try:
                metadata = get_basic_object_metadata(obj)
            except Exception as e:
                logging.error(f"unable to fetch metadata for {obj}: {e}")
                metadata = {}
        else:
            logging.warning(
                f"No S3 resource provided, unable to fetch metadata for asset file: {asset_file}"
            )
            metadata = {}

        asset_info = get_ras_asset_info(asset_file)
        asset = pystac.Asset(
            s3_path_public_url_converter(asset_file),
            extra_fields=metadata,
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        item.add_asset(asset_info["title"], asset)


def cell_area_to_distance(item, properties_to_transform):
    """
    Converts the given properties (representing area) to distance by taking the square root
    of their values. Capitalizes '2d' to '2D' in the property names.

    Parameters:
    - item: The item thats having its properties transformed.
    - properties_to_transform: List of properties to transform.
    """
    for prop in properties_to_transform:
        capitalized_prop = prop.replace("2d", "2D")
        try:
            item.properties[capitalized_prop] = int(
                np.sqrt(float(item.properties[prop]))
            )
            del item.properties[prop]
        except KeyError:
            logging.warning(f"Property {prop} not found")
