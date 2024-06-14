from pathlib import Path
from rashdf import RasPlanHdf
import pystac
import json

import sys

sys.path.append("../")
from ras_stac.utils.ras_utils import RasStacPlan, properties_to_isoformat

TEST_DATA = Path("data")
TEST_JSON = TEST_DATA / "json"
TEST_RAS = TEST_DATA / "ras"
TEST_PLAN = TEST_RAS / "Muncie.p04.hdf"
TEST_PLAN_ATTRS = TEST_JSON / "test_plan_attrs.json"
TEST_PLAN_RESULTS_ATTRS = TEST_JSON / "test_plan_results_attrs.json"
TEST_GEOM_ITEM = TEST_JSON / "test_geom_item.json"
TEST_PLAN_ITEM = TEST_JSON / "test_plan_item.json"


def test_plan_stac_item():
    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    geom_item = pystac.Item.from_file(TEST_GEOM_ITEM)
    plan_meta = ras_stac_plan.get_simulation_metadata(simulation="test-1")
    plan_item = ras_stac_plan.to_item(
        geom_item, plan_meta, model_sim_id="test-1", item_props_to_remove=[]
    )
    plan_item.validate()

    with open(TEST_PLAN_ITEM, "r") as f:
        test_item_content = json.load(f)

    item_dict = json.loads(json.dumps(plan_item.to_dict()))

    assert item_dict == test_item_content


def test_plan_attrs():
    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    test_attrs = properties_to_isoformat(ras_stac_plan.get_stac_plan_attrs())

    with open(TEST_PLAN_ATTRS, "r") as f:
        attrs_json = json.load(f)

    assert test_attrs == attrs_json


def test_plan_results_attrs():
    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    test_attrs = properties_to_isoformat(ras_stac_plan.get_stac_plan_results_attrs())

    with open(TEST_PLAN_RESULTS_ATTRS, "r") as f:
        attrs_json = json.load(f)

    assert test_attrs == attrs_json
