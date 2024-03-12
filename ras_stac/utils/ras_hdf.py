from datetime import datetime, timedelta
import re
from typing import Any, Optional, Union, List, Tuple
import zipfile
import fsspec
import h5py
import numpy as np
import pyproj
import shapely
import shapely.ops
import boto3
from io import BytesIO
import os
import logging
from dotenv import load_dotenv, find_dotenv

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

load_dotenv(find_dotenv())


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

    # Convert to lower case
    return text.lower()


def parse_simulation_time_window(window: str) -> Tuple[datetime, datetime]:
    split = window.split(" to ")
    format = "%d%b%Y %H%M"
    begin = datetime.strptime(split[0], format)
    end = datetime.strptime(split[1], format)
    return begin, end


def parse_ras_datetime(datetime_str: str) -> datetime:
    """
    Parse a datetime string from a RAS file into a datetime object.

    Parameters:
        datetime_str (str): The datetime string to be parsed. The string should be in the format "ddMMMyyyy HHmm".

    Returns:
        datetime: A datetime object representing the parsed datetime.
    """
    format = "%d%b%Y %H:%M:%S"
    return datetime.strptime(datetime_str, format)


def parse_ras_simulation_window_datetime(datetime_str) -> datetime:
    """
    Parse a datetime string from a RAS simulation window into a datetime object.

    Parameters:
        datetime_str: The datetime string to be parsed.

    Returns:
        datetime: A datetime object representing the parsed datetime.
    """
    format = "%d%b%Y %H%M"
    return datetime.strptime(datetime_str, format)


def parse_run_time_window(window: str) -> Tuple[datetime, datetime]:
    """
    Parse a run time window string into a tuple of datetime objects.

    Parameters:
        window (str): The run time window string to be parsed.

    Returns:
        Tuple[datetime, datetime]: A tuple containing two datetime objects representing the start and end of the run
        time window.
    """
    split = window.split(" to ")
    begin = parse_ras_datetime(split[0])
    end = parse_ras_datetime(split[1])
    return begin, end


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse a duration string into a timedelta object.

    Parameters:
        duration_str (str): The duration string to be parsed. The string should be in the format "HH:MM:SS".

    Returns:
        timedelta: A timedelta object representing the parsed duration.
    """
    hours, minutes, seconds = map(int, duration_str.split(":"))
    duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return duration


def convert_hdf5_string(value: str) -> Union[bool, str, List[str]]:
    """
    Convert a string value from an HDF5 file into a Python object.

    This function handles several specific string formats:
    - "True" and "False" are converted to boolean values.
    - Strings matching the format "ddMMMyyyy HH:mm:ss" or "ddMMMyyyy HHmm" are parsed into datetime objects.
    - Strings matching the format "ddMMMyyyy HH:mm:ss to ddMMMyyyy HH:mm:ss" or "ddMMMyyyy HHmm to ddMMMyyyy HHmm"
    are parsed into a list of two datetime objects.

    Parameters:
        value (str): The string value to be converted.

    Returns:
        The converted value, which could be a boolean, a datetime string, a list of datetime strings, or the original
        string if no other conditions are met.
    """
    ras_datetime_format1_re = r"\d{2}\w{3}\d{4} \d{2}:\d{2}:\d{2}"
    ras_datetime_format2_re = r"\d{2}\w{3}\d{4} \d{2}\d{2}"
    s = value.decode("utf-8")
    if s == "True":
        return True
    elif s == "False":
        return False
    elif re.match(rf"^{ras_datetime_format1_re}", s):
        if re.match(rf"^{ras_datetime_format1_re} to {ras_datetime_format1_re}$", s):
            split = s.split(" to ")
            return [
                parse_ras_datetime(split[0]).isoformat(),
                parse_ras_datetime(split[1]).isoformat(),
            ]
        return parse_ras_datetime(s).isoformat()
    elif re.match(rf"^{ras_datetime_format2_re}", s):
        if re.match(rf"^{ras_datetime_format2_re} to {ras_datetime_format2_re}$", s):
            split = s.split(" to ")
            return [
                parse_ras_simulation_window_datetime(split[0]).isoformat(),
                parse_ras_simulation_window_datetime(split[1]).isoformat(),
            ]
        return parse_ras_simulation_window_datetime(s).isoformat()
    return s


def convert_hdf5_value(
    value: Any,
) -> Union[None, bool, str, List[str], int, float, List[int], List[float]]:
    """
    Convert a value from an HDF5 file into a Python object.

    This function handles several specific types:
    - NaN values are converted to None.
    - Byte strings are converted using the `convert_hdf5_string` function.
    - NumPy integer or float types are converted to Python int or float.
    - Regular ints and floats are left as they are.
    - Lists, tuples, and NumPy arrays are recursively processed.
    - All other types are converted to string.

    Parameters:
        value (Any): The value to be converted.

    Returns:
        The converted value, which could be None, a boolean, a string, a list of strings, an integer, a float, a list
        of integers, a list of floats, or the original value as a string if no other conditions are met.
    """
    # TODO (?): handle "8-bit bitfield" values in 2D Flow Area groups

    # Check for NaN (np.nan)
    if isinstance(value, np.floating) and np.isnan(value):
        return None

    # Check for byte strings
    elif isinstance(value, bytes) or isinstance(value, np.bytes_):
        return convert_hdf5_string(value)

    # Check for NumPy integer or float types
    elif isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)

    # Leave regular ints and floats as they are
    elif isinstance(value, (int, float)):
        return value

    elif isinstance(value, (list, tuple, np.ndarray)):
        if len(value) > 1:
            return [convert_hdf5_value(v) for v in value]
        else:
            return convert_hdf5_value(value[0])

    # Convert all other types to string
    else:
        return str(value)


def hdf5_attrs_to_dict(attrs: dict, prefix: str = None) -> dict:
    """
    Convert a dictionary of attributes from an HDF5 file into a Python dictionary.

    This function handles several specific types:
    - The attribute "Simulation Time Window" is skipped.
    - Other attributes are converted using the `convert_hdf5_value` function.
    - The keys are converted to snake case. If a prefix is provided, it is also converted to snake case and prepended
      to the key.

    Parameters:
        attrs (dict): The attributes to be converted.
        prefix (str, optional): An optional prefix to prepend to the keys.

    Returns:
        dict: A dictionary with the converted attributes.
    """
    results = {}
    for k, v in attrs.items():
        if k == "Simulation Time Window":
            # Not sure what the issue is but this is a quick fix
            continue
        else:
            value = convert_hdf5_value(v)
            if prefix:
                key = f"{to_snake_case(prefix)}:{to_snake_case(k)}"
            else:
                key = to_snake_case(k)
            results[key] = value
    return results


def get_first_hdf_group(parent_group: h5py.Group) -> Optional[h5py.Group]:
    """
    Get the first HDF5 group from a parent group.

    This function iterates over the items in the parent group and returns the first item that is an instance of
     h5py.Group. If no such item is found, it returns None.

    Parameters:
        parent_group (h5py.Group): The parent group to search in.

    Returns:
        Optional[h5py.Group]: The first HDF5 group in the parent group, or None if no group is found.
    """
    for _, item in parent_group.items():
        if isinstance(item, h5py.Group):
            return item
    return None


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


class RasHdf(h5py.File):
    """
    A class to handle HDF5 files in the context of RAS data.
    """

    def __init__(self, name, mode="r", **kwargs):
        """
        Initialize the RasHdf object.

        Parameters:
            name (str): The name (path) of the HDF5 file.
            mode (str, optional): The mode in which to open the file. Defaults to "r".
        """
        super().__init__(name, mode, **kwargs)

    @classmethod
    def open_url(cls, url: str, mode: str = "r", dev_mode: bool = False, **kwargs):
        """
        Open an HDF5 file from a URL.

        Parameters:
            url (str): The URL of the HDF5 file.
            mode (str, optional): The mode in which to open the file. Defaults to "r".

        Returns:
            RasHdf: The opened HDF5 file as a RasHdf object.
        """
        if dev_mode:
            s3f = fsspec.open(
                url,
                client_kwargs={"endpoint_url": os.environ.get("MINIO_S3_ENDPOINT")},
                mode="rb",
            )
        else:
            s3f = fsspec.open(url, mode="rb")

        return cls(s3f.open(), mode, **kwargs)

    def get_attrs(self) -> dict:
        """
        Get the attributes of the HDF5 file.

        Returns:
            dict: The attributes of the HDF5 file.
        """
        attrs = hdf5_attrs_to_dict(self.attrs)
        projection = attrs.pop("projection", None)
        if projection is not None:
            attrs["proj:wkt2"] = projection
        return attrs

    @classmethod
    def hdf_from_zip(
        cls, bucket_name, zip_file_key, hdf_file_name, ras_hdf_type, mode="r", **kwargs
    ):
        """
        Extract an HDF5 file from a ZIP file stored in an S3 bucket.

        Parameters:
            bucket_name (str): The name of the S3 bucket.
            zip_file_key (str): The key of the ZIP file in the S3 bucket.
            hdf_file_name (str): The name of the HDF5 file in the ZIP file.
            ras_hdf_type (str): The type of the RAS HDF5 file. Acceptable parameters are `plan` or `geom`.
            mode (str, optional): The mode in which to open the HDF5 file. Defaults to "r".

        Returns:
            RasHdf: The extracted HDF5 file as a RasHdf object.
        """
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=zip_file_key)

        with zipfile.ZipFile(BytesIO(response["Body"].read())) as zip_ref:
            if hdf_file_name not in zip_ref.namelist():
                raise FileNotFoundError(f"{hdf_file_name} not found in the zip file.")

            with zip_ref.open(hdf_file_name) as hdf_file:
                hdf_content = hdf_file.read()

        if ras_hdf_type == "plan":
            return RasPlanHdf(BytesIO(hdf_content), mode, **kwargs)
        elif ras_hdf_type == "geom":
            return RasGeomHdf(BytesIO(hdf_content), mode, **kwargs)
        else:
            return cls(BytesIO(hdf_content), mode, **kwargs)


class RasPlanHdf(RasHdf):
    def __init__(self, name: str, mode="r", **kwargs):
        """
        A class to handle RAS Plan HDF5 files.

        Parameters:
            name (str): The name (path) of the HDF5 file.
            mode (str, optional): The mode in which to open the file. Defaults to "r".
        """
        super().__init__(name, mode, **kwargs)

    def get_plan_attrs(self, include_results: bool = False) -> dict:
        """
        This method retrieves the attributes of a plan from a HEC-RAS plan HDF file.

        Parameters:
            include_results (bool, optional): Whether to include the results attributes in the returned dictionary.
            Defaults to False.

        Returns:
            dict: A dictionary with the attributes of the plan.

        The method performs the following steps:
            1. Retrieves the attributes of the HDF file.
            2. Tries to get the "Plan Data/Plan Information" group from the HDF file. If it exists, it converts its
              attributes to a dictionary and updates the attributes dictionary with it.
            3. Tries to get the "Plan Data/Plan Parameters" group from the HDF file. If it exists, it converts its
              attributes to a dictionary and updates the attributes dictionary with it.
            4. Tries to get the "Event Conditions/Meteorology/Precipitation" group from the HDF file. If it exists,
              it converts its attributes to a dictionary, removes the "meteorology:projection" key from it, and updates
              the attributes dictionary with it.
            5. If `include_results` is True, it updates the attributes dictionary with the results attributes.
            6. Returns the attributes dictionary.
        """

        attrs = self.get_attrs()

        plan_info = self.get("Plan Data/Plan Information")
        if plan_info is not None:
            plan_info_attrs = hdf5_attrs_to_dict(
                plan_info.attrs, prefix="Plan Information"
            )
            attrs.update(plan_info_attrs)

        plan_params = self.get("Plan Data/Plan Parameters")
        if plan_params is not None:
            plan_params_attrs = hdf5_attrs_to_dict(
                plan_params.attrs, prefix="Plan Parameters"
            )
            attrs.update(plan_params_attrs)

        precip = self.get("Event Conditions/Meteorology/Precipitation")
        if precip is not None:
            precip_attrs = hdf5_attrs_to_dict(precip.attrs, prefix="Meteorology")
            precip_attrs.pop("meteorology:projection", None)
            attrs.update(precip_attrs)

        if include_results:
            attrs.update(self.get_plan_results_attrs())

        return attrs

    def get_plan_results_attrs(self) -> dict:
        """
        This method retrieves the results attributes of a plan from a HEC-RAS plan HDF file.

        Returns:
            dict: A dictionary with the results attributes of the plan.

        The method performs the following steps:
            1. Initializes an empty dictionary `attrs`.
            2. Tries to get the "Results/Unsteady" group from the HDF file. If it exists, it converts its attributes
                to a dictionary and updates `attrs` with it.
            3. Tries to get the "Results/Unsteady/Summary" group from the HDF file. If it exists, it converts its
                attributes to a dictionary, retrieves the total computation time, the run time window, and the solution
                from it, calculates the total computation time in minutes if it exists, and updates `attrs` with these.
            4. Tries to get the "Results/Unsteady/Summary/Volume Accounting" group from the HDF file. If it exists, it
                converts its attributes to a dictionary and updates `attrs` with it.
            5. Returns `attrs`.
        """
        attrs = {}
        unsteady_results = self.get("Results/Unsteady")
        if unsteady_results is not None:
            unsteady_results_attrs = hdf5_attrs_to_dict(
                unsteady_results.attrs, prefix="Unsteady Results"
            )
            attrs.update(unsteady_results_attrs)

        summary = self.get("Results/Unsteady/Summary")
        if summary is not None:
            summary_attrs = hdf5_attrs_to_dict(summary.attrs, prefix="Results Summary")
            computation_time_total = summary_attrs.get(
                "results_summary:computation_time_total"
            )
            results_summary = {
                "results_summary:computation_time_total": computation_time_total,
                "results_summary:run_time_window": summary_attrs.get(
                    "results_summary:run_time_window"
                ),
                "results_summary:solution": summary_attrs.get(
                    "results_summary:solution"
                ),
            }
            if computation_time_total is not None:
                computation_time_total_minutes = (
                    parse_duration(computation_time_total).total_seconds() / 60
                )
                results_summary[
                    "results_summary:computation_time_total_minutes"
                ] = computation_time_total_minutes
            attrs.update(results_summary)

        volume_accounting = self.get("Results/Unsteady/Summary/Volume Accounting")
        if volume_accounting is not None:
            volume_accounting_attrs = hdf5_attrs_to_dict(
                volume_accounting.attrs, prefix="Volume Accounting"
            )
            attrs.update(volume_accounting_attrs)
        return attrs


class RasGeomHdf(RasHdf):
    def __init__(self, name, mode="r", **kwargs):
        """
        A class to handle RAS Geometry HDF5 files.

        This class provides methods to get various attributes from the HDF5 file, such as the projection and the
        perimeter of the 2D flow area. It inherits from the RasHdf class.
        """
        super().__init__(name, mode, **kwargs)

    def get_geom_attrs(self) -> dict:
        """
        This method retrieves the geometry attributes of a HEC-RAS plan HDF file.

        Returns:
            dict: A dictionary with the geometry attributes of the plan.

        The method performs the following steps:
            1. Retrieves the attributes of the HDF file.
            2. Tries to get the "Geometry" group from the HDF file. If it exists, it converts its attributes to a
                dictionary and updates the attributes dictionary with it.
            3. Tries to get the "Geometry/Structures" group from the HDF file. If it exists, it converts its attributes
                to a dictionary and updates the attributes dictionary with it.
            4. Tries to get the first group from the "Geometry/2D Flow Areas" group in the HDF file. If it does not
                exist, it logs a warning and returns the attributes dictionary.
            5. If the first group from the "Geometry/2D Flow Areas" group exists, it converts its attributes to a
                dictionary, retrieves the cell average size from it, calculates the cell average length if the cell
                average size exists, and updates the attributes dictionary with these.
            6. Returns the attributes dictionary.
        """
        attrs = self.get_attrs()

        geometry = self.get("Geometry")
        if geometry is not None:
            geometry_attrs = hdf5_attrs_to_dict(geometry.attrs, prefix="Geometry")
            attrs.update(geometry_attrs)

        structures = self.get("Geometry/Structures")
        if structures is not None:
            structures_attrs = hdf5_attrs_to_dict(structures.attrs, prefix="Structures")
            attrs.update(structures_attrs)

        try:
            d2_flow_area = get_first_hdf_group(self.get("Geometry/2D Flow Areas"))
        except AttributeError:
            logging.warning(
                "Unable to get 2D Flow Area; Geometry/2D Flow Areas group not found in HDF5 file."
            )
            return attrs

        if d2_flow_area is not None:
            d2_flow_area_attrs = hdf5_attrs_to_dict(
                d2_flow_area.attrs, prefix="2D Flow Areas"
            )
            cell_average_size = d2_flow_area_attrs.get(
                "2d_flow_area:cell_average_size", None
            )
            if cell_average_size is not None:
                d2_flow_area_attrs["2d_flow_area:cell_average_length"] = (
                    cell_average_size**0.5
                )
            attrs.update(d2_flow_area_attrs)

        return attrs

    def get_projection(self) -> Optional[str]:
        """
        Get the projection of the HDF5 file.

        This method tries to get the "Projection" attribute from the HDF5 file. If the attribute is not found, it logs
        a warning and returns None. If the attribute is found, it decodes it from bytes to string and returns it.

        Returns:
            Optional[str]: The projection of the HDF5 file, or None if the "Projection" attribute is not found.
        """
        try:
            projection = self.attrs.get("Projection")
        except AttributeError:
            logging.warning(
                "Unable to get projection; Projection attribute not found in HDF5 file."
            )
            return None
        if projection is not None:
            return projection.decode("utf-8")

    def get_2d_flow_area_perimeter(
        self, simplify: float = 0.001, wgs84: bool = True
    ) -> Optional[shapely.Polygon]:
        """
        This method retrieves the perimeter of a 2D flow area from a HEC-RAS plan HDF5 file and returns it
        as a Shapely Polygon.

        Parameters:
            simplify (float, optional): The tolerance for the simplification of the polygon. Defaults to 0.001.
            wgs84 (bool, optional): Whether to convert the polygon to the WGS 84 (EPSG:4326) coordinate reference
            system. Defaults to True.

        Returns:
            Optional[shapely.Polygon]: The perimeter of the 2D flow area as a Shapely Polygon, or None if the
              "Geometry/2D Flow Areas" group or the "Perimeter" dataset does not exist in the HDF5 file or if
              the projection of the HDF5 file is not specified and `wgs84` is True.
        """
        try:
            d2_flow_area = get_first_hdf_group(self.get("Geometry/2D Flow Areas"))
        except AttributeError:
            logging.warning(
                "Unable to get 2D Flow Area perimeter; Geometry/2D Flow Areas group not found in HDF5 file."
            )
            return None

        if d2_flow_area is None:
            return None

        perim = d2_flow_area.get("Perimeter")
        if perim is None:
            return None

        perim_coords = perim[:]
        perim_polygon = shapely.Polygon(perim_coords)
        if simplify is not None:
            perim_polygon = perim_polygon.simplify(simplify)
        if wgs84:
            proj_wkt = self.get_projection()
            if proj_wkt is not None:
                return geom_to_4326(perim_polygon, proj_wkt)
            logging.warn(
                "Unable to convert 2D Flow Area perimeter to WGS 84 (EPSG:4326); projection not specified in HDF5 file."
            )

        return perim_polygon
