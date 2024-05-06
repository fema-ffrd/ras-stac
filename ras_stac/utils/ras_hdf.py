import pyproj
import shapely
import shapely.ops
import logging
from dotenv import load_dotenv, find_dotenv

from rashdf import RasPlanHdf, RasGeomHdf
from rashdf.utils import parse_duration

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

load_dotenv(find_dotenv())


def geom_to_4326(s: shapely.Geometry, proj_wkt: str) -> shapely.Geometry:
    """
    Convert a geometry to the WGS 84 coordinate reference system.

    This function creates a transformer from the source coordinate reference system (CRS), specified by the
    Well-Known Text (WKT) representation, to the WGS 84 CRS (EPSG:4326). It then applies this transformer to the
    input geometry.

    Parameters:
        s (shapely.geometry.base.BaseGeometry): The input geometry.
        proj_wkt (str): The WKT representation of the source CRS.

    Returns:
        shapely.geometry.base.BaseGeometry: The input geometry transformed to the WGS 84 CRS.
    """
    source_crs = pyproj.CRS.from_wkt(proj_wkt)
    target_crs = pyproj.CRS.from_epsg(4326)
    transformer = pyproj.Transformer.from_proj(source_crs, target_crs, always_xy=True)
    return shapely.ops.transform(transformer.transform, s)


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
            key = k
        results[key] = value

    return results


def get_stac_plan_attrs(ras_hdf: RasPlanHdf, include_results: bool = False) -> dict:
    """
    This function retrieves the attributes of a plan from a HEC-RAS plan HDF file, converting them to STAC format.

    Parameters:
        ras_hdf (RasPlanHdf): An instance of RasPlanHdf which the attributes will be retrieved from.
        include_results (bool, optional): Whether to include the results attributes in the returned dictionary.
            Defaults to False.

    Returns:
        stac_plan_attrs (dict): A dictionary with the attributes of the plan.
    """
    stac_plan_attrs = ras_hdf.get_root_attrs()
    if stac_plan_attrs is not None:
        stac_plan_attrs = prep_stac_attrs(stac_plan_attrs)
    else:
        stac_plan_attrs = {}

    plan_info_attrs = ras_hdf.get_plan_info_attrs()
    if plan_info_attrs is not None:
        plan_info_stac_attrs = prep_stac_attrs(plan_info_attrs, prefix="Plan Information")
        stac_plan_attrs.update(plan_info_stac_attrs)

    plan_params_attrs = ras_hdf.get_plan_param_attrs()
    if plan_params_attrs is not None:
        plan_params_stac_attrs = prep_stac_attrs(plan_params_attrs, prefix="Plan Parameters")
        stac_plan_attrs.update(plan_params_stac_attrs)

    precip_attrs = ras_hdf.get_meteorology_precip_attrs()
    if precip_attrs is not None:
        precip_stac_attrs = prep_stac_attrs(precip_attrs, prefix="Meteorology")
        precip_stac_attrs.pop("meteorology:projection", None)
        stac_plan_attrs.update(precip_stac_attrs)

    if include_results:
        stac_plan_attrs.update(get_stac_plan_results_attrs(ras_hdf))
    return stac_plan_attrs


def get_stac_plan_results_attrs(ras_hdf: RasPlanHdf):
    """
    This function retrieves the results attributes of a plan from a HEC-RAS plan HDF file, converting them to STAC format.
    For summary atrributes, it retrieves the total computation time, the run time window,
    and the solution from it, and calculates the total computation time in minutes if it exists.

    Parameters:
        ras_hdf (RasPlanHdf): An instance of RasPlanHdf which the results attributes will be retrieved from.

    Returns:
        results_attrs (dict): A dictionary with the results attributes of the plan.
    """
    results_attrs = {}

    unsteady_results_attrs = ras_hdf.get_results_unsteady_attrs()
    if unsteady_results_attrs is not None:
        unsteady_results_stac_attrs = prep_stac_attrs(unsteady_results_attrs, prefix="Unsteady Results")
        results_attrs.update(unsteady_results_stac_attrs)

    summary_attrs = ras_hdf.get_results_unsteady_summary_attrs()
    if summary_attrs is not None:
        summary_stac_attrs = prep_stac_attrs(summary_attrs, prefix="Results Summary")
        computation_time_total = summary_stac_attrs.get("results_summary:computation_time_total")
        results_summary = {
            "results_summary:computation_time_total": computation_time_total,
            "results_summary:run_time_window": summary_stac_attrs.get("results_summary:run_time_window"),
            "results_summary:solution": summary_stac_attrs.get("results_summary:solution"),
        }
        if computation_time_total is not None:
            computation_time_total_minutes = parse_duration(computation_time_total).total_seconds() / 60
            results_summary["results_summary:computation_time_total_minutes"] = computation_time_total_minutes
        results_attrs.update(results_summary)

    volume_accounting_attrs = ras_hdf.get_results_volume_accounting_attrs()
    if volume_accounting_attrs is not None:
        volume_accounting_stac_attrs = prep_stac_attrs(volume_accounting_attrs, prefix="Volume Accounting")
        results_attrs.update(volume_accounting_stac_attrs)

    return results_attrs


def get_stac_geom_attrs(ras_hdf: RasGeomHdf):
    """
    This function retrieves the geometry attributes of a HEC-RAS plan HDF file, converting them to STAC format.

    Parameters:
        ras_hdf (RasGeomHdf): An instance of RasGeomHdf which the geometry attributes will be retrieved from.

    Returns:
        stac_geom_attrs (dict): A dictionary with the geometry attributes of the plan.
    """

    stac_geom_attrs = ras_hdf.get_root_attrs()
    if stac_geom_attrs is not None:
        stac_geom_attrs = prep_stac_attrs(stac_geom_attrs)
    else:
        stac_geom_attrs = {}

    geom_attrs = ras_hdf.get_geom_attrs()
    if geom_attrs is not None:
        geom_stac_attrs = prep_stac_attrs(geom_attrs, prefix="Geometry")
        stac_geom_attrs.update(geom_stac_attrs)

    structures_attrs = ras_hdf.get_geom_structures_attrs()
    if structures_attrs is not None:
        structures_stac_attrs = prep_stac_attrs(structures_attrs, prefix="Structures")
        stac_geom_attrs.update(structures_stac_attrs)

    d2_flow_area_attrs = ras_hdf.get_geom_2d_flow_area_attrs()
    if d2_flow_area_attrs is not None:
        d2_flow_area_stac_attrs = prep_stac_attrs(d2_flow_area_attrs, prefix="2D Flow Areas")
        cell_average_size = d2_flow_area_stac_attrs.get("2d_flow_area:cell_average_size", None)
        if cell_average_size is not None:
            d2_flow_area_stac_attrs["2d_flow_area:cell_average_length"] = cell_average_size**0.5
        stac_geom_attrs.update(d2_flow_area_stac_attrs)
    return stac_geom_attrs
