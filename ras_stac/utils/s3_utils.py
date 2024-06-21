import boto3
import botocore
import json
import logging
import re
import os
from rashdf import RasPlanHdf, RasGeomHdf
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
from mypy_boto3_s3.service_resource import ObjectSummary


load_dotenv(find_dotenv())


def read_ras_geom_from_s3(ras_geom_hdf_url: str, minio_mode: bool = False):
    """
    Reads a RAS geometry HDF file from an S3 URL.

    Parameters:
        ras_geom_hdf_url (str): The URL of the RAS geometry HDF file.
        minio_mode (bool, optional): If True, uses MinIO endpoint for S3. Defaults to False.

    Returns:
        geom_hdf_obj (RasGeomHdf): The RasGeomHdf object.
        ras_model_name (str): The RAS model name.

    Raises:
        ValueError: If the provided URL does not have a '.hdf' suffix.
    """
    pattern = r".*\.g[0-9]{2}\.hdf$"
    if not re.fullmatch(pattern, ras_geom_hdf_url):
        raise ValueError(
            f"RAS geom URL does not match pattern {pattern}: {ras_geom_hdf_url}"
        )

    ras_model_name = Path(ras_geom_hdf_url.replace(".hdf", "")).stem

    logging.info(f"Reading hdf file from {ras_geom_hdf_url}")
    if minio_mode:
        geom_hdf_obj = RasGeomHdf.open_uri(
            ras_geom_hdf_url,
            fsspec_kwargs={"endpoint_url": os.environ["MINIO_S3_ENDPOINT"]},
        )
    else:
        geom_hdf_obj = RasGeomHdf.open_uri(ras_geom_hdf_url)

    return geom_hdf_obj, ras_model_name


def read_ras_plan_from_s3(ras_plan_hdf_url: str, minio_mode: bool = False):
    """
    Reads a RAS plan HDF file from an S3 URL.

    Parameters:
        ras_plan_hdf_url (str): The URL of the RAS plan HDF file.
        minio_mode (bool, optional): If True, uses MinIO endpoint for S3. Defaults to False.

    Returns:
        plan_hdf_obj (RasPlanHdf): The RasPlanHdf object.

    Raises:
        ValueError: If the provided URL does not have a '.hdf' suffix.
    """
    pattern = r".*\.p[0-9]{2}\.hdf$"
    if not re.fullmatch(pattern, ras_plan_hdf_url):
        raise ValueError(
            f"RAS plan URL does not match pattern {pattern}: {ras_plan_hdf_url}"
        )

    logging.info(f"Reading hdf file from {ras_plan_hdf_url}")
    if minio_mode:
        plan_hdf_obj = RasPlanHdf.open_uri(
            ras_plan_hdf_url,
            fsspec_kwargs={"endpoint_url": os.environ["MINIO_S3_ENDPOINT"]},
        )
    else:
        plan_hdf_obj = RasPlanHdf.open_uri(ras_plan_hdf_url)

    return plan_hdf_obj


def get_basic_object_metadata(obj: ObjectSummary) -> dict:
    """
    This function retrieves basic metadata of an AWS S3 object.

    Parameters:
        obj (ObjectSummary): The AWS S3 object.

    Returns:
        dict: A dictionary with the size, ETag, last modified date, storage platform, region, and
              storage tier of the object.
    """
    try:
        _ = obj.load()
        return {
            "file:size": obj.content_length,
            "e_tag": obj.e_tag.strip('"'),
            "last_modified": obj.last_modified.isoformat(),
            "storage:platform": "AWS",
            "storage:region": obj.meta.client.meta.region_name,
            "storage:tier": obj.storage_class,
        }
    except botocore.exceptions.ClientError:
        raise KeyError(
            f"Unable to access {obj.key} check that key exists and you have access"
        )


def copy_item_to_s3(item, s3_path, s3client):
    """
    This function copies an item to an AWS S3 bucket.

    Parameters:
        item: The item to copy. It must have a `to_dict` method that returns a dictionary representation of it.
        s3_key (str): The file path in the S3 bucket to copy the item to.

    The function performs the following steps:
        1. Initializes a boto3 S3 client and splits the s3_key into the bucket name and the key.
        2. Converts the item to a dictionary, serializes it to a JSON string, and encodes it to bytes.
        3. Puts the encoded JSON string to the specified file path in the S3 bucket.
    """
    # s3 = boto3.client("s3")
    bucket, key = split_s3_path(s3_path)

    item_json = json.dumps(item.to_dict()).encode("utf-8")

    s3client.put_object(Body=item_json, Bucket=bucket, Key=key)


def split_s3_path(s3_path: str) -> tuple[str, str]:
    """
    This function splits an S3 path into the bucket name and the key.

    Parameters:
        s3_path (str): The S3 path to split. It should be in the format 's3://bucket/key'.

    Returns:
        tuple: A tuple containing the bucket name and the key. If the S3 path does not contain a key, the second element
          of the tuple will be None.

    The function performs the following steps:
        1. Removes the 's3://' prefix from the S3 path.
        2. Splits the remaining string on the first '/' character.
        3. Returns the first part as the bucket name and the second part as the key. If there is no '/', the key will
          be None.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"s3_path does not start with s3://: {s3_path}")
    bucket, _, key = s3_path[5:].partition("/")
    if not key:
        raise ValueError(f"s3_path contains bucket only, no key: {s3_path}")
    return bucket, key


def s3_path_public_url_converter(url: str, minio_mode: bool = False) -> str:
    """
    This function converts an S3 URL to an HTTPS URL and vice versa.

    Parameters:
        url (str): The URL to convert. It should be in the format 's3://bucket/' or 'https://bucket.s3.amazonaws.com/'.

    Returns:
        str: The converted URL. If the input URL is an S3 URL, the function returns an HTTPS URL. If the input URL is
        an HTTPS URL, the function returns an S3 URL.

    The function performs the following steps:
        1. Checks if the input URL is an S3 URL or an HTTPS URL.
        2. If the input URL is an S3 URL, it converts it to an HTTPS URL.
        3. If the input URL is an HTTPS URL, it converts it to an S3 URL.
    """

    if url.startswith("s3"):
        bucket = url.replace("s3://", "").split("/")[0]
        key = url.replace(f"s3://{bucket}", "")[1:]
        if minio_mode:
            logging.info(
                f"minio_mode | using minio endpoint for s3 url conversion: {url}"
            )
            return f"{os.environ['MINIO_S3_ENDPOINT']}/{bucket}/{key}"
        else:
            return f"https://{bucket}.s3.amazonaws.com/{key}"

    elif url.startswith("http"):
        if minio_mode:
            logging.info(
                f"minio_mode | using minio endpoint for s3 url conversion: {url}"
            )
            bucket = url.replace(os.environ["MINIO_S3_ENDPOINT"], "").split("/")[0]
            key = url.replace(os.environ["MINIO_S3_ENDPOINT"], "")
        else:
            bucket = url.replace("https://", "").split(".s3.amazonaws.com")[0]
            key = url.replace(f"https://{bucket}.s3.amazonaws.com/", "")

        return f"s3://{bucket}/{key}"

    else:
        raise ValueError(f"Invalid URL format: {url}")


def verify_safe_prefix(s3_key: str):
    """
    TODO: discuss this with the team. Would like some safety mechanism to ensure that the S3 key is limited to
    certain prefixes. Should there be some restriction where these files can be written?
    """
    parts = s3_key.split("/")
    logging.debug(f"parts of the s3_key: {parts}")
    if parts[3] != "stac":
        raise ValueError(
            f"prefix must begin with stac/, user provided {s3_key} needs to be corrected"
        )


def init_s3_resources(minio_mode: bool = False):
    if minio_mode:
        session = boto3.Session(
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["MINIO_SECRET_ACCESS_KEY"],
        )

        s3_client = session.client("s3", endpoint_url=os.environ["MINIO_S3_ENDPOINT"])

        s3_resource = session.resource(
            "s3", endpoint_url=os.environ["MINIO_S3_ENDPOINT"]
        )

        return session, s3_client, s3_resource
    else:
        # Instantitate S3 resources
        session = boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

        s3_client = session.client("s3")
        s3_resource = session.resource("s3")
        return session, s3_client, s3_resource


def list_keys(s3_client, bucket, prefix, suffix=""):
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


def list_keys_regex(s3_client, bucket, prefix_includes, suffix=""):
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": prefix_includes}
    prefix_pattern = re.compile(prefix_includes.replace("*", ".*"))
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys += [
            obj["Key"]
            for obj in resp["Contents"]
            if prefix_pattern.match(obj["Key"]) and obj["Key"].endswith(suffix)
        ]
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return keys
