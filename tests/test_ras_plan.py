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
