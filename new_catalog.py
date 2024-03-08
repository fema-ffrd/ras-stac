import pystac
import json
import boto3
import os
import sys


def create_catalog(cat_id: str = "dev-catalog"):
    catalog = pystac.Catalog(
        id=cat_id,
        description=f"Sandbox catalog for local development with stac-browser",
        title=f"{cat_id}",
    )
    return catalog


if __name__ == "__main__":
    item_key = sys.argv[1]
    endpoint_url = os.getenv("MINIO_S3_ENDPOINT")

    bucket_name = "pilot"
    catalog_prefix = "stac"
    catalog_key = f"{catalog_prefix}/catalog.json"
    catalog_s3_uri = f"{endpoint_url}/{bucket_name}/{catalog_key}"

    item_public_uri = f"{endpoint_url}/{item_key}"
    item = pystac.Item.from_file(f"{endpoint_url}/{bucket_name}/{item_key}")

    catalog = create_catalog()
    item.set_parent(catalog)
    catalog.add_item(item)

    catalog.set_self_href(catalog_s3_uri)
    catalog.make_all_asset_hrefs_relative()

    session = boto3.Session(
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
        region_name=os.environ.get("AWS_REGION"),
    )

    s3_client = session.client("s3", endpoint_url=endpoint_url)
    s3_client.put_object(
        Body=json.dumps(catalog.to_dict()).encode(), Bucket=bucket_name, Key=catalog_key
    )

    s3_client = session.client("s3", endpoint_url=endpoint_url)
    s3_client.put_object(
        Body=json.dumps(item.to_dict()).encode(), Bucket=bucket_name, Key=item_key
    )
