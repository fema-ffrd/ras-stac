import logging
from dotenv import load_dotenv, find_dotenv
import pystac
from pathlib import Path
import re
import numpy as np
from rashdf import RasPlanHdf, RasGeomHdf
from rashdf.utils import parse_duration
from .s3_utils import (
    s3_path_public_url_converter,
    split_s3_path,
    get_basic_object_metadata,
)

load_dotenv(find_dotenv())


def get_stac_geom_attrs(ras_geom: RasGeomHdf) -> dict:
    """Retrieve the geometry attributes of a HEC-RAS HDF file, converting them to STAC format.

    Returns
    -------
        stac_geom_attrs (dict): A dictionary with the organized geometry attributes.

    """
    stac_geom_attrs = ras_geom.get_root_attrs()
    if stac_geom_attrs is not None:
        stac_geom_attrs = prep_stac_attrs(stac_geom_attrs)
    else:
        stac_geom_attrs = {}
        logging.warning("No root attributes found.")

    geom_attrs = ras_geom.get_geom_attrs()
    if geom_attrs is not None:
        geom_stac_attrs = prep_stac_attrs(geom_attrs, prefix="Geometry")
        stac_geom_attrs.update(geom_stac_attrs)
    else:
        logging.warning("No base geometry attributes found.")

    structures_attrs = ras_geom.get_geom_structures_attrs()
    if structures_attrs is not None:
        structures_stac_attrs = prep_stac_attrs(structures_attrs, prefix="Structures")
        stac_geom_attrs.update(structures_stac_attrs)
    else:
        logging.warning("No geometry structures attributes found.")

    d2_flow_area_attrs = ras_geom.get_geom_2d_flow_area_attrs()
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


def to_snake_case(text):
    """Convert a string to snake case, removing punctuation and other symbols.

    Parameters
    ----------
        text (str): The string to be converted.

    Returns
    -------
        str: The snake case version of the string.

    """
    import re

    # Remove all non-word characters (everything except numbers and letters)
    text = re.sub(r"[^\w\s]", "", text)

    # Replace all runs of whitespace with a single underscore
    text = re.sub(r"\s+", "_", text)

    return text.lower()


def prep_stac_attrs(attrs: dict, prefix: str = None) -> dict:
    """Convert an unformatted HDF attributes dictionary to STAC format by converting values to snake case and adding a prefix if one is given.

    Parameters
    ----------
        attrs (dict): Unformatted attribute dictionary.
        prefix (str): Optional prefix to be added to each key of formatted dictionary.

    Returns
    -------
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


def add_assets_to_item(item, asset_list: list, s3_resource: None):
    """Add assets to a STAC item using the asset list and fetches metadata from S3."""
    for asset_file in asset_list:
        logging.info(f"Adding asset {asset_file} to item")

        if "s3://" in asset_file:
            bucket, asset_key = split_s3_path(asset_file)
            asset_href = s3_path_public_url_converter(asset_file)

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

        else:
            asset_href = asset_file
            metadata = {}

        asset_info = get_ras_asset_info(asset_file)
        asset = pystac.Asset(
            href=asset_href,
            extra_fields=metadata,
            roles=asset_info["roles"],
            description=asset_info["description"],
        )
        item.add_asset(asset_info["title"], asset)


def get_ras_asset_info(s3_key: str) -> dict:
    """Generate information about a HEC-RAS model asset including roles, descriptions, and titles.

    Parameters
    ----------
        s3_key (str): The S3 key of the asset.

    Returns
    -------
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


def cell_area_to_distance(properties, properties_to_transform):
    """Convert the given properties (representing area) to distance by taking the square root of their values.

    Parameters
    ----------
    - item: The item thats having its properties transformed.
    - properties_to_transform: List of properties to transform.

    """
    for prop in properties_to_transform:
        try:
            properties[prop] = int(np.sqrt(float(properties[prop])))
        except KeyError:
            logging.warning(f"Property {prop} not found")

    return properties


def ras_perimeter(ras_geom: RasGeomHdf, simplify: float = None, crs: str = "EPSG:4326"):
    """Calculate the perimeter of a HEC-RAS geometry as a GeoDataFrame in the specified coordinate reference system.

    Parameters
    ----------
        rg (RasGeomHdf): A HEC-RAS geometry HDF file object which provides mesh areas.
        simplify (float, optional): A tolerance level to simplify the perimeter geometry to reduce complexity.
                                    If None, the geometry will not be simplified. Defaults to None.
        crs (str): The coordinate reference system which the perimeter geometry will be converted to. Defaults to "EPSG:4326".

    Returns
    -------
        gpd.GeoDataFrame: A GeoDataFrame containing the calculated perimeter polygon in the specified CRS.

    """
    perimeter = ras_geom.mesh_areas()
    perimeter = perimeter.to_crs(crs)
    if simplify:
        perimeter_polygon = perimeter.geometry.union_all().simplify(tolerance=simplify)
    else:
        perimeter_polygon = perimeter.geometry.union_all()
    return perimeter_polygon


def get_plan_attrs(ras_plan: RasPlanHdf) -> dict:
    """Retrieve the attributes of a plan from a HEC-RAS plan HDF file, converting them to STAC format.

    Returns
    -------
        stac_plan_attrs (dict): A dictionary with the attributes of the plan.

    """
    stac_plan_attrs = ras_plan.get_root_attrs()
    if stac_plan_attrs is not None:
        stac_plan_attrs = prep_stac_attrs(stac_plan_attrs)
    else:
        stac_plan_attrs = {}
        logging.warning("No root attributes found.")

    plan_info_attrs = ras_plan.get_plan_info_attrs()
    if plan_info_attrs is not None:
        plan_info_stac_attrs = prep_stac_attrs(
            plan_info_attrs, prefix="Plan Information"
        )
        stac_plan_attrs.update(plan_info_stac_attrs)
    else:
        logging.warning("No plan information attributes found.")

    plan_params_attrs = ras_plan.get_plan_param_attrs()
    if plan_params_attrs is not None:
        plan_params_stac_attrs = prep_stac_attrs(
            plan_params_attrs, prefix="Plan Parameters"
        )
        stac_plan_attrs.update(plan_params_stac_attrs)
    else:
        logging.warning("No plan parameters attributes found.")

    precip_attrs = ras_plan.get_meteorology_precip_attrs()
    if precip_attrs is not None:
        precip_stac_attrs = prep_stac_attrs(precip_attrs, prefix="Meteorology")
        precip_stac_attrs.pop("meteorology:projection", None)
        stac_plan_attrs.update(precip_stac_attrs)
    else:
        logging.warning("No meteorology precipitation attributes found.")

    return stac_plan_attrs


def get_plan_results_attrs(ras_plan: RasPlanHdf) -> dict:
    """Retrieve the results attributes of a plan from a HEC-RAS plan HDF file, converting them to STAC format.

    Returns
    -------
        results_attrs (dict): A dictionary with the results attributes of the plan.

    """
    results_attrs = {}

    unsteady_results_attrs = ras_plan.get_results_unsteady_attrs()
    if unsteady_results_attrs is not None:
        unsteady_results_stac_attrs = prep_stac_attrs(
            unsteady_results_attrs, prefix="Unsteady Results"
        )
        results_attrs.update(unsteady_results_stac_attrs)
    else:
        logging.warning("No unsteady results attributes found.")

    summary_attrs = ras_plan.get_results_unsteady_summary_attrs()
    if summary_attrs is not None:
        summary_stac_attrs = prep_stac_attrs(summary_attrs, prefix="Results Summary")
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

    volume_accounting_attrs = ras_plan.get_results_volume_accounting_attrs()
    if volume_accounting_attrs is not None:
        volume_accounting_stac_attrs = prep_stac_attrs(
            volume_accounting_attrs, prefix="Volume Accounting"
        )
        results_attrs.update(volume_accounting_stac_attrs)
    else:
        logging.warning("No results volume accounting attributes found.")

    return results_attrs


def get_stac_plan_attrs(ras_plan: RasPlanHdf) -> dict:
    """Retrieve the metadata of a simulation from a HEC-RAS plan HDF file.

    Parameters
    ----------
        simulation (str): The name of the simulation.

    Returns
    -------
        dict: A dictionary with the metadata of the simulation.

    The function performs the following steps:
    1. Initializes a metadata dictionary.
    2. Tries to get the plan attributes from the RasPlanHdf object and update the `metadata` dictionary with them.
    3. Tries to get the plan results attributes from the RasPlanHdf object and update the `metadata` dictionary with them.
    4. Returns the `metadata` dictionary.

    """
    metadata = {}

    try:
        plan_attrs = get_plan_attrs(ras_plan)
        metadata.update(plan_attrs)
    except Exception as e:
        return logging.error(f"unable to extract plan_attrs from plan: {e}")

    try:
        results_attrs = get_plan_results_attrs(ras_plan)
        metadata.update(results_attrs)
    except Exception as e:
        return logging.error(f"unable to extract results_attrs from plan: {e}")

    return metadata
