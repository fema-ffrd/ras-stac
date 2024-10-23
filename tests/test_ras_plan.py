from pathlib import Path
from rashdf import RasPlanHdf
import json

from ras_stac.utils.ras_utils import RasStacPlan, properties_to_isoformat
from ras_stac.ras_plan_hdf import new_plan_item


TEST_DATA = Path("./tests/data")
TEST_JSON = TEST_DATA / "json"
TEST_RAS = TEST_DATA / "ras"
TEST_PLAN = TEST_RAS / "Muncie.p04.hdf"
TEST_PLAN_ATTRS = TEST_JSON / "test_plan_attrs.json"
TEST_PLAN_RESULTS_ATTRS = TEST_JSON / "test_plan_results_attrs.json"
TEST_PLAN_ITEM = TEST_JSON / "test_plan_item.json"


def test_geom_item():

    ras_plan_hdf = RasPlanHdf(TEST_PLAN)
    ras_model_name = "test_model"
    test_asset = "s3://test_bucket/test_prefix/test_model.f03"
    item = new_plan_item(ras_plan_hdf, ras_model_name, asset_list=[test_asset])
    item.validate()

    item_json = json.dumps(item.to_dict(), indent=4)
    with open(TEST_PLAN_ITEM, "r") as f:
        test_item_content = json.load(f)

    test_item_json = json.dumps(test_item_content, indent=4)

    assert item_json == test_item_json


def test_plan_attrs():
    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    test_attrs = properties_to_isoformat(ras_stac_plan.get_plan_attrs())

    with open(TEST_PLAN_ATTRS, "r") as f:
        attrs_json = json.load(f)

    assert test_attrs == attrs_json


def test_plan_results_attrs():
    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    test_attrs = properties_to_isoformat(ras_stac_plan.get_plan_results_attrs())

    with open(TEST_PLAN_RESULTS_ATTRS, "r") as f:
        attrs_json = json.load(f)

    assert test_attrs == attrs_json
