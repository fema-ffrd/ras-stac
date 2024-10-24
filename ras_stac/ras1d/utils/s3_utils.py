from ras_stac.utils.s3_utils import (
    get_basic_object_metadata,
    init_s3_resources,
    list_keys,
    split_s3_key,
)


def gather_dir_s3(s3_prefix_url: str) -> list:
    """Get list of files with same prefix."""
    _, s3_client, _ = init_s3_resources()
    bucket, key = split_s3_key(s3_prefix_url)
    keys = list_keys(s3_client, bucket, key)
    return [f"s3://{bucket}/{k}" for k in keys]


def str_from_s3(s3_key: str) -> str:
    """Read a text file from s3 and return its contents as a string."""
    _, s3_client, _ = init_s3_resources()
    bucket, key = split_s3_key(s3_key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def save_bytes_s3(byte_obj: bytes, s3_key: str, content_type: str = "image/png") -> None:
    """Save bytes to S3."""
    _, s3_client, _ = init_s3_resources()
    bucket, key = split_s3_key(s3_key)
    s3_client.put_object(Body=byte_obj, ContentType=content_type, Bucket=bucket, Key=key)


def key_metadata(s3_key: str) -> dict:
    """Wrap get_basic_object_metadata"""
    _, _, s3_resource = init_s3_resources()
    bucket, key = split_s3_key(s3_key)
    obj = s3_resource.Bucket(bucket).Object(key)
    return get_basic_object_metadata(obj)
