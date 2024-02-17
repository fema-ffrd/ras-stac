import requests
import json
import os
from dotenv  import load_dotenv

load_dotenv()

def get_auth_header(username:str = os.environ["USERNAME"]):
    auth_server = os.environ["AUTH_SERVER"]
    client_id = os.environ["CLIENT_ID"]
    client_secret = os.environ["CLIENT_SECRET"]
    password = os.environ["PASSWORD"]

    auth_payload = f"username={username}&password={password}&client_id={client_id}&grant_type=password&client_secret={client_secret}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer null",
    }

    auth_response = requests.request("POST", auth_server, headers=headers, data=auth_payload)

    try:
        token = json.loads(auth_response.text)["access_token"]
    except KeyError:
        print(auth_response.text)
        raise KeyError

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-ProcessAPI-User-Email": username,
    }

    return headers
