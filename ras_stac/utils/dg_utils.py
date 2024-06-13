import json
import logging
import os
import pystac
import rasterio
import rasterio.warp

from datetime import datetime, timezone
from mypy_boto3_s3.service_resource import Object
from pathlib import Path
from rasterio.session import AWSSession
from shapely import to_geojson
from shapely.geometry import Polygon
from typing import Tuple

from .s3_utils import s3_key_public_url_converter, get_basic_object_metadata

logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


def get_raster_bounds(
    s3_key: str, aws_session: AWSSession, minio_mode: bool = False
) -> Tuple[float, float, float, float]:
    """
    This function retrieves the geographic bounds of a raster file stored in an AWS S3 bucket and returns them in
    the WGS 84 (EPSG:4326) coordinate reference system.

    Parameters:
        s3_key (str): The key of the raster file in the S3 bucket.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns:
        Tuple[float, float, float, float]: The geographic bounds of the raster file in the WGS 84 (EPSG:4326)
        coordinate reference system. The bounds are returned as a tuple of four floats: (west, south, east, north).
    """
    if minio_mode:
        with rasterio.open(s3_key.replace("s3://", f"/vsicurl/{os.environ.get('MINIO_S3_ENDPOINT')}/")) as src:
            bounds = src.bounds
            crs = src.crs
            bounds_4326 = rasterio.warp.transform_bounds(crs, "EPSG:4326", *bounds)
            return bounds_4326

    else:
        with rasterio.Env(aws_session):
            with rasterio.open(s3_key) as src:
                bounds = src.bounds
                crs = src.crs
                bounds_4326 = rasterio.warp.transform_bounds(crs, "EPSG:4326", *bounds)
                return bounds_4326


def get_raster_metadata(s3_key: str, aws_session: AWSSession, minio_mode: bool = False) -> dict:
    """
    This function retrieves the metadata of a raster file stored in an AWS S3 bucket.

    Parameters:
        s3_key (str): The key of the raster file in the S3 bucket.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns:
        dict: The metadata of the raster file. The metadata is returned as a dictionary
        where the keys are the names of the metadata items and the values are the values of the metadata items.
    """
    if minio_mode:
        with rasterio.open(s3_key.replace("s3://", f"/vsicurl/{os.environ.get('MINIO_S3_ENDPOINT')}/")) as src:
            return src.tags(1)
    else:
        with rasterio.Env(aws_session):
            with rasterio.open(s3_key) as src:
                return src.tags(1)


def bbox_to_polygon(bbox) -> Polygon:
    """
    This function converts a bounding box to a Shapely Polygon.

    Parameters:
        bbox: The bounding box to convert. It should be a sequence of four numbers: (min_x, min_y, max_x, max_y).

    Returns:
        shapely.Polygon: The Shapely Polygon representing the bounding box. The Polygon is a rectangle with the lower
          left corner at (min_x, min_y) and the upper right corner at (max_x, max_y).
    """
    min_x, min_y, max_x, max_y = bbox
    return Polygon(
        [
            [min_x, min_y],
            [max_x, min_y],
            [max_x, max_y],
            [min_x, max_y],
        ]
    )


def create_depth_grid_item(
    s3_obj: Object, item_id: str, aws_session: AWSSession, minio_mode: bool = False
) -> pystac.Item:
    """
    This function creates a PySTAC Item for a depth grid raster file stored in an AWS S3 bucket.

    Parameters:
        s3_obj (Object): The s3 object of the raster file in the S3 bucket.
        item_id (str): The ID to assign to the PySTAC Item.
        aws_session (AWSSession): The AWS session to use to access the S3 bucket.

    Returns:
        pystac.Item: The PySTAC Item representing the raster file. The Item has an Asset with the href set to the S3
        key of the raster file, the title set to the name of the raster file, the media type set to COG, and the role
        set to "ras-depth-grid". The Asset's extra fields are updated with the basic object metadata of the raster file
        and the metadata of the raster file. The Item's bbox is set to the geographic bounds of the raster file in the
        WGS 84 (EPSG:4326) coordinate reference system, the datetime is set to the current datetime, and the geometry
        is set to the GeoJSON representation of the bbox.
    """
    s3_full_key = f"s3://{s3_obj.bucket_name}/{s3_obj.key}"
    title = Path(s3_obj.key).name
    bbox = get_raster_bounds(s3_full_key, aws_session, minio_mode=minio_mode)
    geometry = bbox_to_polygon(bbox)
    item = pystac.Item(
        id=item_id,
        properties={},
        bbox=bbox,
        datetime=datetime.now(timezone.utc),
        geometry=json.loads(to_geojson(geometry)),
    )
    # non_null = not raster_is_all_null(depth_grid.key)
    asset = pystac.Asset(
        href=s3_key_public_url_converter(s3_full_key, minio_mode=minio_mode),
        title=title,
        media_type=pystac.MediaType.COG,
        roles=["ras-depth-grid"],
    )
    asset.extra_fields.update(get_basic_object_metadata(s3_obj))
    asset.extra_fields = dict(sorted(asset.extra_fields.items()))
    metadata = get_raster_metadata(s3_full_key, aws_session, minio_mode=minio_mode)
    if metadata:
        asset.extra_fields.update(metadata)
    item.add_asset(key=asset.title, asset=asset)
    return item
