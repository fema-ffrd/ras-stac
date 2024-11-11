import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Union
from pystac import Item
import shapely
from shapely.geometry import Polygon
from rashdf import RasGeomHdf, RasPlanHdf
from utils.class_utils import (
    get_stac_geom_attrs,
    add_assets_to_item,
    cell_area_to_distance,
    ras_perimeter,
)
from pathlib import Path


class RASItem(Item):
    def __init__(
        self,
        hdf_path: str,
        item_id: str,
        asset_list: Optional[List] = None,
        item_props_to_remove: Optional[List] = None,
        item_props_to_add: Optional[Dict] = None,
        simplify: Optional[float] = None,
        s3_resource=None,
        crs: str = "EPSG:4326",
    ):
        self.hdf_path = hdf_path
        self.item_id = item_id
        self.asset_list = asset_list
        self.item_props_to_remove = item_props_to_remove or []
        self.item_props_to_add = item_props_to_add or {}
        self.simplify = simplify
        self.s3_resource = s3_resource
        self.crs = crs

        self.ras_hdf = self._load_hdf_file(hdf_path)
        self.stac_properties = self._prepare_stac_properties()
        perimeter_polygon = self._create_perimeter(simplify, crs)

        item_datetime = self._determine_item_time()

        super().__init__(
            id=self.item_id,
            geometry=json.loads(shapely.to_geojson(perimeter_polygon)),
            bbox=perimeter_polygon.bounds,
            datetime=item_datetime,
            properties=self.stac_properties,
        )
        if self.asset_list:
            add_assets_to_item(self, self.asset_list, self.s3_resource)

    def _load_hdf_file(self, hdf_path: str) -> Union[RasGeomHdf, RasPlanHdf]:
        """
        Determine if the HDF file is geometry or plan based on its extension and initialize the respective rashdf class.
        """
        file_extension = Path(hdf_path).suffixes
        if ".g" in file_extension[0]:
            self.file_type = "geometry"
            return RasGeomHdf(hdf_path)
        elif ".p" in file_extension[0]:
            self.file_type = "plan"
            return RasPlanHdf(hdf_path)
        else:
            raise ValueError(f"Unknown HDF file type for path: {hdf_path}")

    def _get_stac_geom_attrs(self) -> Dict:
        """
        Retrieve geometry attributes from the HDF file and raise an error if none found.
        """
        stac_properties = get_stac_geom_attrs(self.ras_hdf)
        if not stac_properties:
            raise AttributeError(f"Could not find properties for {self.item_id}.")
        return stac_properties

    def _remove_unwanted_properties(self, stac_properties: Dict) -> Dict:
        """
        Remove properties specified in item_props_to_remove.
        """
        for prop in self.item_props_to_remove:
            stac_properties.pop(prop, None)
        return stac_properties

    def _add_custom_properties(self, stac_properties: Dict) -> Dict:
        """
        Add custom properties specified in item_props_to_add.
        """
        stac_properties.update(self.item_props_to_add)
        return stac_properties

    def _apply_2d_cell_transformations(self, stac_properties: Dict) -> None:
        """
        Transform 2D cell size properties to distance values.
        """
        stac_properties = cell_area_to_distance(
            stac_properties,
            [
                "2d_flow_areas:cell_average_size",
                "2d_flow_areas:cell_maximum_size",
                "2d_flow_areas:cell_minimum_size",
            ],
        )
        return stac_properties

    def _prepare_stac_properties(self) -> Dict:
        """
        Retrieve and process STAC properties from the HDF file.
        """
        stac_properties = self._get_stac_geom_attrs()
        stac_properties = self._remove_unwanted_properties(stac_properties)
        stac_properties = self._add_custom_properties(stac_properties)
        stac_properties = self._apply_2d_cell_transformations(stac_properties)

        return stac_properties

    def _create_perimeter(self, simplify: Optional[float], crs: str) -> Polygon:
        """
        Retrieves and simplifies the perimeter polygon.
        """
        perimeter = ras_perimeter(self.ras_hdf, simplify, crs)
        return perimeter

    def _determine_item_time(self):
        """
        Determines the appropriate datetime value for the STAC item.
        TODO: Figure out how to handle different times.
        """
        runtime_window = self.stac_properties.get("results_summary:run_time_window")
        geometry_time = self.stac_properties.get("geometry:geometry_time")

        if runtime_window:
            start_datetime, end_datetime = runtime_window
            item_datetime = start_datetime
        elif geometry_time:
            item_datetime = geometry_time
        else:
            item_datetime = datetime.now(tz=timezone.utc)

        return item_datetime


hdf_path = "Muncie.g05.hdf"

item_id = "test_item"
asset_list = ["s3://test_bucket/test_prefix/test_model.f03"]
ras_item = RASItem(hdf_path=hdf_path, item_id=item_id, asset_list=asset_list)

# TODO: remove 'STAC' from naming conventions
