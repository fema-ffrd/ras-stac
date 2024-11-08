import json
import logging
import re

import pystac_client
import requests
from pystac import TemporalExtent

from ras_stac.ras1d.headers import get_auth_header


class DewStacClient:
    """pystac_client with some Dewberry-specific features"""

    def __init__(self, stac_endpoint: str = "https://stac2.dewberryanalytics.com") -> None:
        self.stac_endpoint = stac_endpoint
        self.stac_client = pystac_client.Client.open(self.stac_endpoint)

        self.collection_url = "{}/collections/{}"
        self.item_url = "{}/collections/{}/items/{}"

    def __getattr__(self, name):
        """Delegate undefined methods to the underlying stac_client"""
        return getattr(self.stac_client, name)

    def add_collection_summary(self, stac_collection_id: str) -> None:
        """Update STAC collections to include item summaries."""

        version_summary = {}
        coverage_summary = {
            "1D_HEC-RAS_models": 0,
            "1D_HEC-RAS_river_miles": 0,
        }
        collection = self.get_collection(stac_collection_id)
        daterange = []
        for item in collection.get_all_items():
            if item.properties["river_miles"] == "None":
                river_miles = 0
            else:
                river_miles = float(item.properties["river_miles"])
            coverage_summary["1D_HEC-RAS_river_miles"] += river_miles
            coverage_summary["1D_HEC-RAS_models"] += 1

            ras_version = item.properties.get("ras_version", None)

            if ras_version not in version_summary:
                version_summary[ras_version] = {"river_miles": 0, "ras_models": 0}

            version_summary[ras_version]["river_miles"] += river_miles
            version_summary[ras_version]["ras_models"] += 1
            if item.properties["datetime_source"] == "model_geometry":
                daterange.append(item.datetime)

        collection.extent.spatial = collection.extent.from_items(collection.get_all_items()).spatial
        if len(daterange) > 0:
            collection.extent.temporal = TemporalExtent(intervals=[[min(daterange), max(daterange)]])
            temp_notes = {"Temporal extent": "Timestamps reflect data scraped from HEC-RAS model files"}
        else:
            temp_notes = {
                "Temporal extent": "Timestamps not available in HEC-RAS model files, temporal extent is limited to data upload / processing date"
            }

        coverage_summary["1D_HEC-RAS_river_miles"] = int(coverage_summary["1D_HEC-RAS_river_miles"])

        for ras_version in version_summary.keys():
            version_summary[ras_version]["river_miles"] = int(version_summary[ras_version]["river_miles"])

        collection.summaries.add("coverage", coverage_summary)
        collection.summaries.add("model-versions", version_summary)
        collection.summaries.add("notes", temp_notes)

        header = get_auth_header()
        response = requests.put(
            self.collection_url.format(self.stac_endpoint, stac_collection_id),
            json=collection.to_dict(),
            headers=header,
        )
        response.raise_for_status

    def add_all_summaries(self, overwrite: bool = False, re_filter: re.Pattern = re.compile("(.*?)")) -> list:
        """Add summaries to all collections of the stac endpoint"""
        collections = self.stac_client.get_collections()
        collections = [c for c in collections if re_filter.fullmatch(c.id) is not None]

        for c in collections:
            collection = self.get_collection(c.id)
            summary = collection.summaries

            # If schema exists and not overwriting, go to next
            if len(summary.schemas) > 0 and not overwrite:
                continue
            # Otherwise, add summary
            try:
                print(f"Found missing summary for {c.id}")
                self.add_collection_summary(c.id)
            except requests.exceptions.HTTPError as e:
                print(f"Error uploading summary for {c.id}")
                print(e)

    def export_to_json(self, out_path: str, catalog_filter: re.Pattern = re.compile("(.*?)")):
        """Save all catalogs and items to a local json"""
        collections = self.stac_client.get_collections()
        collections = [c for c in collections if catalog_filter.fullmatch(c.id) is not None]
        out_dict = {}

        for c in collections:
            collection = self.get_collection(c.id)
            out_dict[c.id] = [i.id for i in collection.get_all_items()]

        with open(out_path, "w") as out_file:
            json.dump(out_dict, out_file, indent=4)

    def add_collection(self, collection_id: str) -> None:
        """Create new collection at endpoint"""
        header = get_auth_header()
        # check if exists first
        response = requests.get(self.collection_url.format(self.stac_endpoint, collection_id))
        if response.status_code != 404:
            return

        response = requests.post(
            self.collection_url.format(self.stac_endpoint, ""),
            json={
                "id": collection_id,
                "description": f"HEC-RAS models for {collection_id}",
                "stac_version": "1.0.0",
                "links": [],
                "title": collection_id,
                "type": "Collection",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                    "temporal": {"interval": [[None, None]]},
                },
            },
            headers=header,
        )
        response.raise_for_status()

    def add_collection_item(self, stac_collection_id: str, item_id: str, item: dict) -> None:
        header = get_auth_header()
        response = requests.post(
            self.item_url.format(self.stac_endpoint, stac_collection_id, ""),
            json=item,
            headers=header,
        )
        response.raise_for_status()

    def remove_collection_item(self, stac_collection_id: str, item_id: str) -> None:
        header = get_auth_header()
        response = requests.delete(
            self.item_url.format(self.stac_endpoint, stac_collection_id, item_id),
            headers=header,
        )
        response.raise_for_status()

    def remove_collection(self, stac_collection_id: str) -> None:
        header = get_auth_header()
        response = requests.delete(
            self.collection_url.format(self.stac_endpoint, stac_collection_id),
            headers=header,
        )
        response.raise_for_status()

    def update_item(self, collection_id, item_id, stac):
        header = get_auth_header()
        response = requests.put(
            self.item_url.format(self.stac_endpoint, collection_id, item_id), headers=header, json=stac
        )
        response.raise_for_status()


def post_item(stac):
    if "proj:wkt2" not in stac["properties"]:  # no CRS
        if not stac["properties"]["has_1d"]:  # 2D
            collection = "mip_2D_only"
        else:
            collection = "mip_no_crs"
    else:
        collection = "mip_" + stac["properties"]["assigned_HUC8"]

    client = DewStacClient()
    collection_ref = client.get_collection(collection)
    update_stac_item(stac, collection_ref, client)


def update_stac_item(stac, collection, client):
    # Check if item already exists
    stac["id"] = stac["id"].replace("(", "_").replace(")", "_")
    stac_id = stac["id"]
    api_item = collection.get_item(stac_id)

    ind = 0
    searching = True
    candidate_hash_set = {
        f'{stac["assets"][i]["title"]} {stac["assets"][i]["e_tag"]}'
        for i in stac["assets"]
        if "thumbnail" not in stac["assets"][i]["roles"] and "ras-geometry-gpkg" not in stac["assets"][i]["roles"]
    }
    while searching:
        api_hash_set = {
            f'{api_item.assets[i].title} {api_item.assets[i].extra_fields["e_tag"]}'
            for i in api_item.assets
            if "thumbnail" not in api_item.assets[i].roles and "ras-geometry-gpkg" not in api_item.assets[i].roles
        }
        if candidate_hash_set == api_hash_set:
            searching = False
            client.update_item(collection.id, stac["id"], stac)
            logging.info(f"updated STAC item {stac["id"]} in collection {collection.id}")
            continue

        ind += 1
        stac["id"] = stac_id + f"_{ind}"
        api_item = collection.get_item(stac["id"])
        if api_item is None:
            searching = False
            logging.warning("No STAC item found in API to update")

    return stac_id
