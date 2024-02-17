import requests
import os
from pathlib import Path 
from headers import get_auth_header
from dotenv  import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

geom_payload = """{"inputs": 
    {
    "geom_hdf": "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.g01.hdf",
    "new_item_s3_key": "s3://kanawha-pilot/stac/testing/BluestoneLocal.g01.hdf.json",
    "lulc_assets": [
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/LandCover.hdf",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/LandCover.tif"
    ],
    "other_assets": [
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.g01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/Features/Profile Lines.shp",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/Backup/BluestoneLocal.2024-Jan-22(1).g01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/Backup/BluestoneLocal.2024-Jan-22(2).g01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/MMC_Projection.prj"
    ]
    }
}
"""

plan_payload = """{"inputs": 
  {
    "plan_hdf": "s3://kanawha-pilot/FFRD_Kanawha_Compute/runs/10/ras/Kanawha_0505_Bluestone_Upper/Kanawha_0505_Bluestone_Upper.p01.hdf",
    "new_item_s3_key": "s3://kanawha-pilot/stac/testing/BluestoneLocal.p01.hdf.json",
    "geom_item_s3_key": "s3://kanawha-pilot/stac/testing/BluestoneLocal.g01.hdf.json",
    "sim_id": "BluestoneLocal-r0001-s0010",
    "ras_assets": [
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.b01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.bco01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.c01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.dsc.h5",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.dss",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.IC.O01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.p01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.prj",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.rasmap",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.u01",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.u01.hdf",
        "s3://kanawha-pilot/FFRD_Kanawha_Compute/ras/BluestoneLocal/BluestoneLocal.x01"
    ],
    "item_props": {
        "cloud_wat:version": "dev",
        "cloud_wat:realization": "1",
        "cloud_wat:simulation": "10"
    }
  }
}
"""

dg_payload = """{"inputs": 
    {
    "plan_dg": "s3://kanawha-pilot/FFRD_Kanawha_Compute/runs/10/depth-grids/Kanawha_0505_Bluestone_Upper/grid_96.tif",
    "new_item_s3_key": "s3://kanawha-pilot/stac/testing/BluestoneLocal.p01-grid_96.json",
    "plan_item_s3_key": "s3://kanawha-pilot/stac/testing/BluestoneLocal.p01.hdf.json",
    "dg_id": "BluestoneLocal.p01-grid_96",
    "item_props": {
        "cloud_wat:version": "dev",
        "cloud_wat:realization": "1",
        "cloud_wat:simulation": "10"
        }
    }
}
"""

def main(process_id:str, json_payload:str):
    papi_url = os.environ["PAPI_URL"]

    # Authentication
    headers = get_auth_header()

    # Put/Post Job 
    process_url = f"{papi_url}/processes/{process_id}/execution"
    response = requests.request("POST", process_url, headers=headers, data=json_payload)

    # Check Response
    print(response.text)

if __name__ == "__main__":
    """
    Examples expect process_id to be the same as the process file name
    """
    # process_id, payload = "ras-plan-to-stac", plan_payload
    process_id, payload = "ras-dg-to-stac", dg_payload
    # process_id, payload = "ras-geom-to-stac", geom_payload

    main(process_id, payload)