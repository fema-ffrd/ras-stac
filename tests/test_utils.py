import json
import shapely
import pystac
import sys

sys.path.append("../")
from ras_stac.utils.ras_utils import RasStacGeom, properties_to_isoformat, RasStacPlan


def stac_item_to_json(item: pystac.Item, filename: str):
    """Writes a STAC item to a JSON file."""
    item_json = json.dumps(item.to_dict(), indent=4)
    with open(filename, "w") as f:
        f.write(item_json)


def create_perimeter_json(ras_stac_geom: RasStacGeom, output_json_fn: str = "test_perimeter.json"):
    perimeter = ras_stac_geom.get_perimeter()
    geometry = json.loads(shapely.to_geojson(perimeter))
    bounds = perimeter.bounds
    test_perimeter = {"geometry": geometry, "bounds": bounds}
    with open(output_json_fn, "w") as f:
        json.dump(test_perimeter, f)


def geom_properties_to_json(ras_stac_geom: RasStacGeom, output_json_fn: str = "test_geom_properties.json"):

    properties = ras_stac_geom.get_stac_geom_attrs()
    iso_properties = properties_to_isoformat(properties)

    with open(output_json_fn, "w") as f:
        json.dump(iso_properties, f)


def plan_attrs_to_json(ras_stac_plan: RasStacPlan, output_json_fn: str = "test_plan_attrs.json"):

    plan_attrs = ras_stac_plan.get_stac_plan_attrs()
    iso_attrs = properties_to_isoformat(plan_attrs)

    with open(output_json_fn, "w") as f:
        json.dump(iso_attrs, f)


def plan_results_attrs_to_json(ras_stac_plan: RasStacPlan, output_json_fn: str = "test_plan_results_attrs.json"):

    plan_results_attrs = ras_stac_plan.get_stac_plan_results_attrs()
    iso_attrs = properties_to_isoformat(plan_results_attrs)

    with open(output_json_fn, "w") as f:
        json.dump(iso_attrs, f)
