import requests
import json
import os
from pathlib import Path 
from headers import get_auth_header
from dotenv  import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def main(process_id:str, process_json:str, new_process:bool):
    papi_url = os.environ["PAPI_URL"]

    # Authentication
    headers = get_auth_header()

    # Load process from file
    with open(process_json, "r") as f:
        process_payload = json.load(f)

    # uri for process 
    if new_process:
        http_verb = "POST"
    else:
        http_verb = "PUT"

    # Put/Post Job 
    process_url = f"{papi_url}/processes/{process_id}"
    response = requests.request(http_verb, process_url, headers=headers, data=json.dumps(process_payload))

    # Check Response
    print(response.text)

if __name__ == "__main__":
    """
    Examples expect process_id to be the same as the process file name
    """
    # process_id = "ras-plan-to-stac"
    process_id = "ras-dg-to-stac"
    # process_id = "ras-geom-to-stac"

    process_definition = Path(os.getcwd())/f"process-api/processes/{process_id}.json"

    main(process_id, process_definition, new_process=True)
