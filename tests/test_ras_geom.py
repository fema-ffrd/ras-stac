from pathlib import Path
from rashdf import RasGeomHdf
import shapely
import json

import sys

sys.path.append("../")
from ras_stac.utils.ras_utils import RasStacGeom, to_snake_case, prep_stac_attrs, properties_to_isoformat

TEST_DATA = Path("data")
TEST_JSON = TEST_DATA / "json"
TEST_RAS = TEST_DATA / "ras"
TEST_GEOM = TEST_RAS / "Muncie.g05.hdf"
TEST_GEOM_ITEM = TEST_JSON / "test_geom_item.json"
TEST_GEOM_PERIMETER = TEST_JSON / "test_perimeter.json"
TEST_GEOM_PROPERTIES = TEST_JSON / "test_geom_properties.json"


def test_geom_stac_item():

    ghdf = RasGeomHdf(TEST_GEOM)
    ras_stac_geom = RasStacGeom(ghdf)
    item = ras_stac_geom.to_item(props_to_remove=[], ras_model_name="test-1")
    item.validate()

    with open(TEST_GEOM_ITEM, "r") as f:
        test_item_content = json.load(f)

    item_dict = json.loads(json.dumps(item.to_dict()))

    assert item_dict == test_item_content


def test_geom_properties():
    ghdf = RasGeomHdf(TEST_GEOM)
    ras_stac_geom = RasStacGeom(ghdf)
    test_properties = properties_to_isoformat(ras_stac_geom.get_stac_geom_attrs())

    with open(TEST_GEOM_PROPERTIES, "r") as f:
        properties_json = json.load(f)

    assert test_properties == properties_json


def test_geom_perimeter():
    ghdf = RasGeomHdf(TEST_GEOM)
    ras_stac_geom = RasStacGeom(ghdf)

    perimeter = ras_stac_geom.get_perimeter()

    test_geom = json.loads(shapely.to_geojson(perimeter))
    test_bounds = list(perimeter.bounds)

    with open(TEST_GEOM_PERIMETER, "r") as f:
        perimeter_json = json.load(f)

    json_geometry = perimeter_json["geometry"]
    json_bounds = perimeter_json["bounds"]

    assert test_geom == json_geometry
    assert test_bounds == json_bounds


def test_to_snake_case():
    assert to_snake_case("Hello World") == "hello_world"
    assert to_snake_case("Hello, World!") == "hello_world"
    assert to_snake_case("Hello   World") == "hello_world"


def test_prep_stac_attrs():
    attrs = {"Attribute One": "Value1", "Attribute Two": "Value2"}
    expected_result = {"attribute_one": "Value1", "attribute_two": "Value2"}
    assert prep_stac_attrs(attrs) == expected_result

    expected_result_with_prefix = {"prefix:attribute_one": "Value1", "prefix:attribute_two": "Value2"}
    assert prep_stac_attrs(attrs, prefix="prefix") == expected_result_with_prefix
