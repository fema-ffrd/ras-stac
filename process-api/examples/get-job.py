import requests
import os
from headers import get_auth_header
from dotenv  import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def main(job_id:str):
    papi_url = os.environ["PAPI_URL"]

    # Authentication
    headers = get_auth_header()

    process_url = f"{papi_url}/jobs/{job_id}"

    response = requests.request("GET", process_url, headers=headers)

    # Check Response
    print(response.text)

if __name__ == "__main__":
    job_id = "d4afdd57-d28f-4aa0-abdf-84874a58fbf3"
    main(job_id)
