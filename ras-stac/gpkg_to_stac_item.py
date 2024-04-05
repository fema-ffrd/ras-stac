import geopandas as gpd
import pystac
import json
from utils.s3_utils import *
from utils.gpkg_utils import *
from utils.dg_utils import *
import logging
import boto3
import tempfile

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def new_gpkg_item(
    gpkg_s3_path: str,
    new_gpkg_item_s3_path: str,
    thumbnail_png_s3_path: str,
    metadata_json_s3_path: str,
    asset_list: list = None,
    dev_mode: bool = True,
):
    logging.info("Creating item from gpkg")
    # Prep parameters
    bucket_name, gpkg_key = split_s3_key(gpkg_s3_path)
    _, metadata_json_key = split_s3_key(metadata_json_s3_path)

    # Instantitate S3 resources
    session, s3_client, s3_resource = init_s3_resources(dev_mode)
    bucket = s3_resource.Bucket(bucket_name)

    # Read in item metadata from json
    logging.info(f"Fetching metadata from json located in s3://{bucket_name}/{metadata_json_key}")
    s3_response_object = s3_client.get_object(Bucket=bucket_name, Key=metadata_json_key)
    content = s3_response_object["Body"].read()
    meta_json = json.loads(content)

    # Download gpkg as temp file
    with tempfile.NamedTemporaryFile() as tmp:
        gpkg_local_path = tmp.name
        s3_client.download_file(Bucket=bucket_name, Key=gpkg_key, Filename=gpkg_local_path)
        # Convert gpkg to geodataframe for easy handling
        gpkg_gdf = gpkg_to_geodataframe(gpkg_local_path)

        logging.info("Creating png thumbnail")
        create_thumbnail_from_gpkg(gpkg_gdf, thumbnail_png_s3_path, s3_client)

        # Create item
        bbox = gpkg_gdf.total_bounds
        footprint = bbox_to_polygon(bbox)
        item = create_geom_item(gpkg_key, bbox, footprint)
        # Organize metadata
        metadata_to_remove = ["FileExt", "Path"]
        item_meta_data = parse_metadata(meta_json, metadata_to_remove)

        asset_list = asset_list + [thumbnail_png_s3_path, gpkg_s3_path]
        for asset_file in asset_list:
            _, asset_key = split_s3_key(asset_file)
            obj = bucket.Object(asset_key)
            metadata = get_basic_object_metadata(obj)
            hash = find_hash(item_meta_data, asset_file)
            metadata.update(hash)
            asset_info = get_asset_info(asset_file)
            asset = pystac.Asset(
                s3_key_public_url_converter(asset_file, dev_mode),
                extra_fields=metadata,
                roles=asset_info["roles"],
                description=asset_info["description"],
            )
            item.add_asset(asset_info["title"], asset)
        # Remove hashes from item metadata, theyve been added to the asset metadata
        item_metadata_no_hash = remove_hash_from_metadata(item_meta_data)
        item.properties.update(item_metadata_no_hash)

        copy_item_to_s3(item, new_gpkg_item_s3_path, s3_client)

        logging.info("Program completed successfully")


def main():

    asset_list = [
        "s3://pilot/assets/BUMS CREEK.f01",
        "s3://pilot/assets/BUMS CREEK.f02",
        "s3://pilot/assets/BUMS CREEK.g01",
        "s3://pilot/assets/BUMS CREEK.p01",
    ]
    json_s3_path = "s3://pilot/metadata/example_1.json"
    gpkg_s3_path = "s3://pilot/gpkgs/BENS BRANCH.gpkg"
    png_s3_path = "s3://pilot/pngs/bens_branch.png"
    new_gpkg_item_s3_path = "s3://pilot/stac/new_items/bums_creek.json"

    new_gpkg_item(gpkg_s3_path, new_gpkg_item_s3_path, png_s3_path, json_s3_path, asset_list)


main()
