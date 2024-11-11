import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Union
from pystac import Item
import pystac
import shapely
from shapely.geometry import Polygon
from rashdf import RasGeomHdf, RasPlanHdf
from utils.s3_utils import save_bytes_s3
from utils.class_utils import (
    get_stac_geom_attrs,
    add_assets_to_item,
    cell_area_to_distance,
    ras_perimeter,
    get_stac_plan_attrs,
    properties_to_isoformat,
)
from pathlib import Path
import re


class RASItem(Item):
    def __init__(
        self,
        hdf_path: str,
        item_id: str,
        asset_list: Optional[List] = None,
        item_props_to_remove: Optional[List] = None,
        item_props_to_add: Optional[Dict] = None,
        item_href: Optional[str] = None,
        item_links: Optional[List[Dict]] = None,
        extra_fields: Optional[Dict] = None,
        simplify: Optional[float] = None,
        s3_resource=None,
        crs: str = "EPSG:4326",
    ):
        self.hdf_path = hdf_path
        self.item_id = item_id
        self.asset_list = asset_list
        self.item_props_to_remove = item_props_to_remove or []
        self.item_props_to_add = item_props_to_add or {}
        self.item_links = item_links or []
        self.href = item_href
        self.extra_fields = extra_fields or {}
        self.simplify = simplify
        self.s3_resource = s3_resource
        self.crs = crs

        self.asset_list.append(hdf_path)

        self.ras_hdf = self._load_hdf_file(hdf_path)
        self.properties = self._process_properties()
        perimeter_polygon = self._create_perimeter(simplify, crs)

        item_datetime = self._determine_item_time()

        super().__init__(
            id=self.item_id,
            geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
            bbox=perimeter_polygon.bounds,
            datetime=item_datetime,
            properties=properties_to_isoformat(self.properties),
            href=self.href,
            extra_fields=self.extra_fields,
        )
        if self.asset_list:
            add_assets_to_item(self, self.asset_list, self.s3_resource)

        if self.item_links:
            self.add_model_links(self.item_links)

    def _load_hdf_file(self, hdf_path: str) -> Union[RasGeomHdf, RasPlanHdf]:
        """
        Determine if the HDF file is geometry or plan based on its extension and initialize the respective rashdf class.
        """
        file_extension = Path(hdf_path).suffixes
        if re.match(r"\.g\d{2}", file_extension[0]):
            self.file_type = "geometry"
            return RasGeomHdf(hdf_path)
        elif re.match(r"\.p\d{2}", file_extension[0]):
            self.file_type = "plan"
            return RasPlanHdf(hdf_path)
        else:
            raise ValueError(f"Unknown HDF file type for path: {hdf_path}")

    def _get_geom_attrs(self) -> Dict:
        """
        Retrieve geometry attributes from the HDF file and raise an error if none found.
        """
        geom_properties = get_stac_geom_attrs(self.ras_hdf)
        if not geom_properties:
            raise AttributeError(f"Could not find geom properties for {self.item_id}.")
        return geom_properties

    def _get_plan_attrs(self) -> Dict:
        """
        Retrieve geometry attributes from the HDF file and raise an error if none found.
        """
        plan_properties = get_stac_plan_attrs(self.ras_hdf)
        if not plan_properties:
            raise AttributeError(f"Could not find plan properties for {self.item_id}.")
        return plan_properties

    def _remove_unwanted_properties(self, properties: Dict) -> Dict:
        """
        Remove properties specified in item_props_to_remove.
        """
        for prop in self.item_props_to_remove:
            properties.pop(prop, None)
        return properties

    def _add_custom_properties(self, properties: Dict) -> Dict:
        """
        Add custom properties specified in item_props_to_add.
        """
        properties.update(self.item_props_to_add)
        return properties

    def _apply_2d_cell_transformations(self, properties: Dict) -> None:
        """
        Transform 2D cell size properties to distance values.
        """
        properties = cell_area_to_distance(
            properties,
            [
                "2d_flow_areas:cell_average_size",
                "2d_flow_areas:cell_maximum_size",
                "2d_flow_areas:cell_minimum_size",
            ],
        )
        return properties

    def _process_properties(self) -> Dict:
        """
        Retrieve and process properties from the HDF file.
        """
        properties = self._get_geom_attrs()

        if self.file_type == "plan":
            properties.update(self._get_plan_attrs())

        properties = self._remove_unwanted_properties(properties)
        properties = self._add_custom_properties(properties)
        properties = self._apply_2d_cell_transformations(properties)

        return properties

    def _create_perimeter(self, simplify: Optional[float], crs: str) -> Polygon:
        """
        Retrieves and simplifies the perimeter polygon.
        """
        perimeter = ras_perimeter(self.ras_hdf, simplify, crs)
        return perimeter

    def _determine_item_time(self):
        """
        Determines the appropriate datetime value for the item.
        TODO: Figure out how to handle different times.
        """
        runtime_window = self.properties.get("results_summary:run_time_window")
        geometry_time = self.properties.get("geometry:geometry_time")

        if runtime_window:
            start_datetime, end_datetime = runtime_window
            item_datetime = start_datetime
        elif geometry_time:
            item_datetime = geometry_time
        else:
            item_datetime = datetime.now(tz=timezone.utc)

        return item_datetime

    def add_model_links(self, item_links: List[Dict]):
        """
        Adds model links to the item.
        """
        for item_link in item_links:
            link = pystac.Link(
                rel=item_link["rel"],
                target=item_link["href"],
                media_type=item_link.get("media_type", None),
                title=item_link.get("title", None),
                extra_fields=item_link.get("extra_fields", None),
            )
            self.add_link(link)

    def export_stac(self, output_path: str) -> None:
        """Export the converted STAC item."""
        out_obj = json.dumps(self.to_dict(), indent=4).encode()
        if output_path.startswith("s3://"):
            save_bytes_s3(out_obj, output_path)
        else:
            with open(output_path, "wb") as f:
                f.write(out_obj)


geom_hdf_path = "Muncie.g05.hdf"
plan_hdf_path = "Muncie.p04.hdf"

item_id = "test_item"
test_props = {"test_prop": "test_value"}
props_to_remove = ["2d_flow_areas:property_tables_last_computed"]
asset_list = ["s3://test_bucket/test_prefix/test_model.f03"]
test_links = [{"href": "https://example.com", "rel": "test", "title": "test_title"}]
test_href = "https://example.com/item.json"
ras_item = RASItem(
    hdf_path=plan_hdf_path,
    item_id=item_id,
    asset_list=asset_list,
    item_href=test_href,
    item_props_to_add=test_props,
    item_props_to_remove=props_to_remove,
)
