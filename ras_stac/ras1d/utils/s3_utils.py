from ras_stac.utils.s3_utils import init_s3_resources, list_keys, split_s3_key


def s3listdir(s3_prefix_url: str) -> list:
    """Get list of files with same prefix."""
    _, s3_client, _ = init_s3_resources()
    bucket, key = split_s3_key(s3_prefix_url)
    return list_keys(bucket, key)


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
