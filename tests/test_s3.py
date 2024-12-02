from moto import mock_aws
import pytest
import boto3

from ras_stac.utils.s3_utils import (
    list_keys,
    s3_path_public_url_converter,
    split_s3_path,
)


@pytest.fixture
def s3_setup():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_resource = boto3.resource("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        yield s3_client, s3_resource


def test_split_s3_path():
    s3_path = "s3://test-bucket/test-item.json"
    bucket, key = split_s3_path(s3_path)
    assert bucket == "test-bucket"
    assert key == "test-item.json"


def test_s3_path_public_url_converter():
    s3_url = "s3://test-bucket/test-item.json"
    https_url = "https://test-bucket.s3.amazonaws.com/test-item.json"
    assert s3_path_public_url_converter(s3_url) == https_url
    assert s3_path_public_url_converter(https_url) == s3_url


def test_list_keys(s3_setup):
    s3_client, _ = s3_setup

    s3_client.put_object(Bucket="test-bucket", Key="prefix/test1.txt", Body="")
    s3_client.put_object(Bucket="test-bucket", Key="prefix/test2.txt", Body="")

    keys = list_keys(s3_client, "test-bucket", "prefix/")
    assert "prefix/test1.txt" in keys
    assert "prefix/test2.txt" in keys
