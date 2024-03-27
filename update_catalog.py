import pystac
import json
import boto3
import os
import sys

from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    item_key_to_add_to_catalog = sys.argv[1]

    endpoint_url = os.getenv("MINIO_S3_ENDPOINT")

    bucket_name = os.getenv("STORAGE_BUCKET", "pilot")
    catalog_prefix = "stac"

    catalog_key = f"{catalog_prefix}/catalog.json"
    catalog_s3_uri = f"{endpoint_url}/{bucket_name}/{catalog_key}"
    catalog = pystac.Catalog.from_file(catalog_s3_uri)

    item_public_uri = f"{endpoint_url}/{bucket_name}/{item_key_to_add_to_catalog}"

    item = pystac.Item.from_file(item_public_uri)

    item.set_parent(catalog)
    catalog.add_item(item)

    item.set_self_href(item_public_uri)
    catalog.set_self_href(catalog_s3_uri)
    catalog.make_all_asset_hrefs_relative()

    session = boto3.Session(
       aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "user"),
       aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
       region_name=os.getenv("AWS_SECRET_ACCESS_KEY", "us-east-1"),
    )

    s3_client = session.client("s3", endpoint_url=endpoint_url)
    s3_client.put_object(
        Body=json.dumps(catalog.to_dict()).encode(), Bucket=bucket_name, Key=catalog_key
    )

    s3_client = session.client("s3", endpoint_url=endpoint_url)
    s3_client.put_object(
        Body=json.dumps(item.to_dict()).encode(),
        Bucket=bucket_name,
        Key=item_key_to_add_to_catalog,
    )
