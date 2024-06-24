from moto import mock_aws
import pytest

from ras_stac.utils.s3_utils import *
import pystac
from rashdf import RasGeomHdf
from ras_stac.utils.ras_utils import RasStacGeom, RasStacPlan


TEST_DATA = Path("./tests/data")
TEST_JSON = TEST_DATA / "json"
TEST_RAS = TEST_DATA / "ras"
TEST_GEOM = TEST_RAS / "Muncie.g05.hdf"
TEST_PLAN = TEST_RAS / "Muncie.p04.hdf"
TEST_GEOM_ITEM = TEST_JSON / "test_geom_item.json"
TEST_PLAN_ITEM = TEST_JSON / "test_plan_item.json"


@pytest.fixture
def s3_setup():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_resource = boto3.resource("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test-bucket")
        yield s3_client, s3_resource


def test_split_s3_path():
    s3_path = "s3://test-bucket/test-item.json"
    bucket, key = split_s3_path(s3_path)
    assert bucket == "test-bucket"
    assert key == "test-item.json"


def test_s3_path_public_url_converter():
    s3_url = "s3://test-bucket/test-item.json"
    https_url = "https://test-bucket.s3.amazonaws.com/test-item.json"
    assert s3_path_public_url_converter(s3_url) == https_url
    assert s3_path_public_url_converter(https_url) == s3_url


def test_list_keys(s3_setup):
    s3_client, _ = s3_setup

    s3_client.put_object(Bucket="test-bucket", Key="prefix/test1.txt", Body="")
    s3_client.put_object(Bucket="test-bucket", Key="prefix/test2.txt", Body="")

    keys = list_keys(s3_client, "test-bucket", "prefix/")
    assert "prefix/test1.txt" in keys
    assert "prefix/test2.txt" in keys


def test_geom_item_to_s3(s3_setup):
    s3_client, s3_resource = s3_setup

    ghdf = RasGeomHdf(TEST_GEOM)
    ras_stac_geom = RasStacGeom(ghdf)
    item = ras_stac_geom.to_item(props_to_remove=[], ras_model_name="test-1")

    s3_path = "s3://test-bucket/test-item.json"
    copy_item_to_s3(item, s3_path, s3_client)

    bucket, key = split_s3_path(s3_path)
    obj = s3_resource.Object(bucket, key)
    item_json = obj.get()["Body"].read().decode("utf-8")
    item_dict = json.loads(item_json)

    with open(TEST_GEOM_ITEM, "r") as f:
        test_item_content = json.load(f)

    assert item_dict == test_item_content


def test_plan_item_to_s3(s3_setup):
    s3_client, s3_resource = s3_setup

    phdf = RasPlanHdf(TEST_PLAN)
    ras_stac_plan = RasStacPlan(phdf)
    geom_item = pystac.Item.from_file(TEST_GEOM_ITEM)
    plan_meta = ras_stac_plan.get_simulation_metadata(simulation="test-1")
    plan_item = ras_stac_plan.to_item(
        geom_item, plan_meta, model_sim_id="test-1", item_props_to_remove=[]
    )
    plan_item.validate()

    s3_path = "s3://test-bucket/test-plan-item.json"
    copy_item_to_s3(plan_item, s3_path, s3_client)

    bucket, key = split_s3_path(s3_path)
    obj = s3_resource.Object(bucket, key)
    item_json = obj.get()["Body"].read().decode("utf-8")
    item_dict = json.loads(item_json)

    with open(TEST_PLAN_ITEM, "r") as f:
        test_item_content = json.load(f)

    assert item_dict == test_item_content
