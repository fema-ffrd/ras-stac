import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import contextily as ctx
import fsspec
import geopandas as gpd
import matplotlib.pyplot as plt
from pystac import Asset, Item, Link
from shapely import wkb

from utils.s3_utils import copy_item_to_s3, init_s3_resources


def copy_thumbnail_to_s3(
    thumbnail_path: str, s3_uri: str, s3_client: boto3.client
) -> None:
    """Copy the thumbnail to s3

    args
        thumbnail_path (str): local path to the thumbnail
        s3_uri (str): full s3 uri for the thumbnail (e.g., s3://bucket/prefix)
        s3_client (boto3.client): s3 client
    """

    # Parse the S3 URI
    parsed_uri = urlparse(s3_uri)
    bucket = parsed_uri.netloc
    thumbnail_prefix = parsed_uri.path.lstrip('/')

    # upload the thumbnail to s3
    try:
        s3_client.upload_file(thumbnail_path, bucket, thumbnail_prefix)
    except Exception as e:
        print(f"Error uploading thumbnail to s3: {e}")
        sys.exit(1)


def define_hms_file_types() -> Dict[str, Dict[str, Any]]:
    """Define the HMS file types and their descriptions in a dictionary for use in STAC item creation"""

    # setup dictionary of hms file extensions, types, and descriptions
    hms_file_types = {
        ".met": {
            "href": None,
            "title": ".met file",
            "description": "Contains meteorological data such as precipitation, temperature, and evapotranspiration data",
            "type": "application/octet-stream",
            "roles": ["hms-file", "meteorological-file"],
        },
        ".control": {
            "href": None,
            "title": ".control file",
            "description": "Defines the time control information for the simulation, including start and end times, time step, and other temporal parameters",
            "type": "application/octet-stream",
            "roles": ["hms-file", "control-file"],
        },
        ".basin": {
            "href": None,
            "title": ".basin file",
            "description": "Describes the physical characteristics of the watershed, including sub-basins, reaches, junctions, reservoirs, and other hydrologic elements",
            "type": "application/octet-stream",
            "roles": ["hms-file", "basin-file"],
        },
        ".sqlite": {
            "href": None,
            "title": ".sqlite file",
            "description": "A SQLite database file that stores various project data in a structured format, including model parameters, results, and metadata",
            "type": "application/octet-stream",
            "roles": ["hms-file", "sqlite-file"],
        },
        ".dss": {
            "href": None,
            "title": ".dss file",
            "description": "A Data Storage System (DSS) file storing time series data, paired data, and other types of data",
            "type": "application/octet-stream",
            "roles": ["hms-file", "dss-file"],
        },
        ".gage": {
            "href": None,
            "title": ".gage file",
            "description": "Contains information about gages used in the model, including location, type, and observed data",
            "type": "application/octet-stream",
            "roles": ["hms-file", "gage-file"],
        },
        ".hms": {
            "href": None,
            "title": ".hms file",
            "description": "The main project file that contains references to all other files and overall project settings",
            "type": "application/octet-stream",
            "roles": ["hms-file", "model-file"],
        },
        ".log": {
            "href": None,
            "title": ".log file",
            "description": "Contains log information from the model run, including errors, warnings, and other messages",
            "type": "application/octet-stream",
            "roles": ["hms-file", "log-file"],
        },
        ".out": {
            "href": None,
            "title": ".out file",
            "description": "Contains the output results of the simulation, including flow, stage, and other computed variables",
            "type": "application/octet-stream",
            "roles": ["hms-file", "output-file"],
        },
        ".pdata": {
            "href": None,
            "title": ".pdata file",
            "description": "Stores paired data, such as rating curves or other relationships used in the model",
            "type": "application/octet-stream",
            "roles": ["hms-file", "paired-data-file"],
        },
        ".run": {
            "href": None,
            "title": ".run file",
            "description": "Defines the simulation runs, including the control file, basin file, and meteorological file to be used",
            "type": "application/octet-stream",
            "roles": ["hms-file", "run-file"],
        },
        ".terrain": {
            "href": None,
            "title": ".terrain file",
            "description": "Contains elevation data for the watershed, used to define the topography and flow paths",
            "type": "application/octet-stream",
            "roles": ["hms-file", "terrain-file"],
        },
    }

    return hms_file_types


def list_keys(
    s3_client: boto3.client, bucket: str, prefix: str, suffix: str = ""
) -> List[str]:
    """List keys in an S3 bucket with a given prefix and suffix

    args
        s3_client: boto3.client
        bucket: str
        prefix: str
        suffix: str

    returns
        keys: list
    """

    keys = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys += [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(suffix)]
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return keys


def extract_geom_bbox(
    bucket: str, sqlite_key: str, spatial_ref: str
) -> Tuple[gpd.GeoDataFrame, Dict[str, Any], List[float]]:
    """Extract geometry and bbox from an HMS model's SQLite file
    
    args
        bucket (str): S3 bucket
        sqlite_key (str): S3 key to the SQLite file
    
    returns
        gdf (geopandas.GeoDataFrame): GeoDataFrame containing the geometry data
        geometry (dict): Geometry data in dictionary format: {"type": "Polygon", "coordinates": [[[x1, y1], [x2, y2], ...]]}
        bbox (list): Bounding box coordinates [minx, miny, maxx, maxy]
        spatial_ref (str): Spatial reference in WKT format
    """

    try:
        # open the sqlite file
        uri = f"s3://{bucket}/{sqlite_key}"
        connection = open_sqlite_uri(uri)

        # retrieve the subbasins2d table, then the geometry field
        cursor = connection.cursor()
        cursor.execute("SELECT GEOMETRY FROM subbasin2d")
        geometry_data = cursor.fetchall()

        # create a geodataframe from the geometry data
        gdf = gpd.GeoDataFrame(geometry=[wkb.loads(g[0]) for g in geometry_data])

        # assign the wkt spatial reference to the geodataframe
        gdf.crs = spatial_ref

        # generate the geometry dictionary
        coordinates = [list(map(list, gdf.geometry[0].exterior.coords))]
        geometry = {
            "type": "Polygon",
            "coordinates": coordinates,
        }

        # generate the bounding box
        bbox = gdf.total_bounds.tolist()
        return gdf, geometry, bbox
    except Exception as e:
        print(f"Error extracting geometry and bbox: {e}")
        return None, None, None


def create_hms_thumbnail(
        bucket: str, sqlite_key: str
) -> Optional[str]:
    """Generate a thumbnail image for an HMS model using the SQLite's geometry data
    
    args
        bucket (str): S3 bucket name
        sqlite_key (str): S3 key to the hms SQLite file
    
    returns
        thumbnail_path (str): local path to the thumbnail image (written to local cwd)
    """

    try:
        # open the sqlite file
        uri = f"s3://{bucket}/{sqlite_key}"
        connection = open_sqlite_uri(uri)

        # retrieve the subbasins2d table, then the geometry field
        cursor = connection.cursor()
        cursor.execute("SELECT GEOMETRY FROM subbasin2d")
        geometry_data = cursor.fetchall()

        # create a geodataframe from the geometry data
        gdf = gpd.GeoDataFrame(geometry=[wkb.loads(g[0]) for g in geometry_data])

        # Get the spatial reference
        spatial_ref = get_hms_spatial_ref(bucket, sqlite_key)
        if spatial_ref:
            gdf.crs = spatial_ref
        else:
            # Set the crs to EPSG:4326 if spatial reference is not found
            gdf.crs = 4326

        # Plot the GeoDataFrame with a basemap
        ax = gdf.plot(figsize=(10, 10), alpha=0.5, edgecolor="k")
        ctx.add_basemap(ax, crs=gdf.crs.to_string())

        # Save the plot as a local file
        thumbnail_path = "thumbnail.png"
        plt.savefig(thumbnail_path, bbox_inches="tight")
        plt.close()

        # Return the local file path of the thumbnail
        return thumbnail_path

    except Exception as e:
        print(f"Error generating thumbnail: {e}")
        print("Consider retrying again (network issues may have caused the error)")
        return None


def open_sqlite_uri(
    uri: str, fsspec_kwargs: Dict = {}, sqlite_kwargs: Dict = {}
) -> sqlite3.Connection:
    """Open a SQLite file from a URI.
    
    args
        uri (str): The URI of the SQLite file. Note this should be a path
            recognized by fsspec, such as an S3 path or a Google Cloud
            Storage path. See https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.open
        fsspec_kwargs (dict): Additional keyword arguments to pass to fsspec.open
        sqlite_kwargs (dict): Additional keyword arguments to pass to sqlite3.connect
        
    returns
        sqlite3.Connection: The SQLite file opened from the URI.
    """

    remote_file = fsspec.open(uri, mode="rb", **fsspec_kwargs)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        # Write the content of the remote file to the temporary file
        with remote_file.open() as f:
            tmp_file.write(f.read())
        tmp_file_path = tmp_file.name

    connection = sqlite3.connect(tmp_file_path, **sqlite_kwargs)

    return connection


def get_hms_spatial_ref(bucket: str, sqlite_key: str) -> str:
    """Get the spatial reference from HMS model's sqlite file
    
    args
        bucket (str): S3 bucket name
        sqlite_key (str): S3 key to the sqlite file
        
    returns
        spatial_ref (str): spatial reference in WKT format
    """

    try:
        # open the sqlite file
        uri = f"s3://{bucket}/{sqlite_key}"
        connection = open_sqlite_uri(uri)

        # retrieve the spatial_ref_sys table, then the srtext field
        cursor = connection.cursor()
        cursor.execute("SELECT srtext FROM spatial_ref_sys")
        spatial_ref = cursor.fetchone()[0]
    except Exception as e:
        print(f"Error acquiring spatial reference, returning None: {e}")
        spatial_ref = None

    return spatial_ref


def open_hms_txt_uri(
    uri: str, fsspec_kwargs: dict = {}
) -> Optional[fsspec.core.OpenFile]:
    """Open a text file from a URI.
    
    args
        uri (str): The URI of the text file. Note this should be a path
            recognized by fsspec, such as an S3 path or a Google Cloud
            Storage path. See https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.open
        fsspec_kwargs (dict): Additional keyword arguments to pass to fsspec.open
        
    returns
        file: The text file opened from the URI.
    """

    try:
        remote_file = fsspec.open(uri, mode="r", **fsspec_kwargs)
        return remote_file
    except Exception as e:
        print(f"Error opening hms control file: {e}")
        remote_file = None
        sys.exit(1)


def get_hms_version(bucket: str, control_key: str) -> Optional[str]:
    """Get the HMS version from the control file
    
    args
        bucket (str): S3 bucket name
        control_key (str): S3 key to the control file
    
    returns
        hms_version (str): HMS version number
    """

    try:
        # open the control file
        uri = f"s3://{bucket}/{control_key}"
        control_file = open_hms_txt_uri(uri)

        # Read the contents of the file
        with control_file as f:
            contents = f.read()

        # retrieve the version from the control file (prefaced by 'Version:')
        for line in contents.split("\n"):
            if "Version:" in line:
                hms_version = line.split(":")[1].strip()
                break
        else:
            raise ValueError("Version not found in control file")
    except Exception as e:
        print(f"Error acquiring HMS version, returning None: {e}")
        hms_version = None

    return hms_version


def get_hms_terrain(bucket: str, terrain_key: str) -> Optional[str]:
    """Get the terrain file from the HMS model's .terrain file
    
    args
        terrain_key (str): S3 key to the terrain file
        
    returns
        terrain_file_name (str): name of the terrain file
    """

    try:
        # open the terrain file
        uri = f"s3://{bucket}/{terrain_key}"
        terrain_file = open_hms_txt_uri(uri)

        # Read the contents of the file
        with terrain_file as f:
            contents = f.read()

        terrain_file_name = None

        # retrieve the terrain file name (prefaced by "Elevation File Name:")
        for line in contents.split("\n"):
            if "Elevation File Name:" in line:
                terrain_file_name = line.split(":")[1].strip()

    except Exception as e:
        print(f"Error acquiring terrain file, returning None: {e}")
        terrain_file_name = None

    return terrain_file_name


def create_hms_stac_item(
    model_name: str,
    bucket_name: str,
    model_prefix: str,
    parent_collection: str,
    stac_output_uri: str,
    stac_thumbnail_uri: str,
    sqlite_key: str,
) -> None:
    """Create a STAC item for a given HMS model and upload to s3
    
    args
        model_name (str): User-defined model name that ideally matches the .hms file name without suffix
        bucket_name (str): S3 bucket name
        model_prefix (str): S3 prefix where model files are stored
        parent_collection (str): URL to the stac item's parent collection
        stac_output_uri (str): S3 prefix where STAC item will be uploaded
        stac_thumbnail_uri (str): S3 prefix where STAC thumbnail will be uploaded
        sqlite_key (str): s3 key to the model's sqlite file
        
    returns
        None

    NOTE: model name will be used for the stac json name (model_name.json) and thumbnail (model_name.png)
    """

    # init s3 resources
    session, s3_client, s3_resource = init_s3_resources()

    # list keys in model folder
    try:
        hms_keys = list_keys(s3_client, bucket_name, model_prefix)
    except Exception as e:
        print(f"Error listing model keys: {e}")
        sys.exit(1)

    # define the s3 uri key (parent of the model files)
    s3_uri = f"s3://{bucket_name}/{model_prefix}"

    # define the stac item
    item_id = model_name
    datetime_var = datetime.now()

    # handle spatial reference, geometry, bbox, and thumbnail creation

    # get the spatial reference
    sqlite_key = [key for key in hms_keys if key.endswith(".sqlite")]
    if sqlite_key:
        sqlite_key = sqlite_key[0]
        spatial_ref = get_hms_spatial_ref(bucket_name, sqlite_key)
    else:
        spatial_ref = None

    # extract geometry and bbox from sqlite file
    _, geometry, bbox = extract_geom_bbox(bucket_name, sqlite_key, spatial_ref)

    # generate the thumbnail
    thumbnail_path = create_hms_thumbnail(
        bucket_name, sqlite_key
    )

    # Get the HMS version
    control_files = [key for key in hms_keys if key.endswith(".control")]
    if control_files:
        control_key = control_files[0]
        hms_version = get_hms_version(bucket_name, control_key)
    else:
        hms_version = None

    # Get the HMS terrain file
    terrain_files = [key for key in hms_keys if key.endswith(".terrain")]
    if terrain_files:
        terrain_key = terrain_files[0]
        terrain_file_name = get_hms_terrain(bucket_name, terrain_key)
    else:
        terrain_file_name = None

    item_id = model_name
    datetime_var = datetime.now()
    properties = {
        "model_name": model_name,
        "hms_version": hms_version,
        "spatial_reference": spatial_ref,
        "terrain_file": terrain_file_name,
    }

    item = Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime_var,
        properties=properties,
        collection=parent_collection,
    )

    if thumbnail_path:
        item.add_asset(
            "thumbnail",
            Asset(
                href=stac_thumbnail_uri,
                media_type="image/png",
                roles=["Thumbnail"],
                title="Thumbnail Image",
            ),
        )

    # add hms assets to item
    hms_assets_dict = define_hms_file_types()
    for key in hms_keys:

        asset_name = Path(key).name
        key_suffix = Path(key).suffix

        if key_suffix in hms_assets_dict:
            # retrieve dictionary of standard hms file info
            asset_dict = hms_assets_dict[key_suffix]

            # assign specific asset info
            asset_dict["title"] = asset_name
            asset_dict["href"] = os.path.join(s3_uri, key)
            asset_dict["e-tag"] = s3_resource.Object(bucket_name, key).e_tag.strip('"')
            asset_dict["file-size"] = s3_resource.Object(
                bucket_name, key
            ).content_length
            asset_dict["last-modified"] = s3_resource.Object(
                bucket_name, key
            ).last_modified.isoformat()
            asset_dict["storage-region"] = s3_resource.Object(
                bucket_name, key
            ).meta.client.meta.region_name
            asset_dict["storage-platform"] = "AWS"

            # Create Asset object by unpacking the dictionary
            asset = Asset(
                href=asset_dict["href"],
                title=asset_dict.get("title"),
                description=asset_dict.get("description"),
                media_type=asset_dict.get("type"),
                roles=asset_dict.get("roles"),
            )

            # Add additional properties to the asset
            asset.extra_fields.update(
                {
                    "e-tag": asset_dict["e-tag"],
                    "file-size": asset_dict["file-size"],
                    "last-modified": asset_dict["last-modified"],
                    "storage-region": asset_dict["storage-region"],
                    "storage-platform": asset_dict["storage-platform"],
                }
            )

            item.add_asset(asset_name, asset)

    
    # Add title to stac item
    item.common_metadata.title = model_name


    # add links to the item
    item.add_link(Link("collection", parent_collection))
    item.add_link(Link("parent", s3_uri))  # TODO: confirm what parent link should be
    # item.add_link(Link("root", "")) # TODO: confirm what root link should be
    item.add_link(Link("self", stac_output_uri))

    # validate the item, returning an error if not valid
    item.validate()

    # write the items to the stac_output_uri. TODO: test with correct S3 privileges
    try:
        copy_item_to_s3(item, stac_output_uri, s3_client)
        copy_thumbnail_to_s3(thumbnail_path, bucket_name, stac_thumbnail_uri, s3_client) 
    except Exception as e:
        print(f"Error copying item to s3: {e}")

    # # write the item locally - TODO: remove once tested with correct S3 privileges
    # item_path = f"{model_name}.json"
    # stac_dict = item.to_dict()
    # with open(item_path, "w") as f:
    #     json.dump(stac_dict, f)


if __name__ == "__main__":

    # USER INPUTS

    # model info
    model_name = "KanawhaHMS"  # for labeling the stac item
    bucket_name = "kanawha-pilot"
    model_prefix = (
        "FFRD_Kanawha_Compute/hms/"  # parent key of the individual hms model files
    )

    # stac references
    parent_collection = (
        "https://uampjfpbwi.us-east-1.awsapprunner.com/collections/Kanawha-R01"
    )

    # s3 outputs
    thumbnail_output_s3_uri = (
        f"s3://kanawha-pilot/stac/Kanawha-0505/thumbnails/{model_name}.png"
    )
    stac_item_output_s3_uri = (
        f"s3://kanawha-pilot/stac/Kanawha-0505/model_items/{model_name}.json"
    )

    sqlite_key = "FFRD_Kanawha_Compute/hms/KanawhaCWMS___1996.sqlite"

    create_hms_stac_item(
        model_name,
        bucket_name,
        model_prefix,
        parent_collection,
        stac_item_output_s3_uri,
        thumbnail_output_s3_uri,
        sqlite_key
    )

    print(f"Successfully created HMS STAC item for {model_name}")

