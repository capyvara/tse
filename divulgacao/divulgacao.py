import os
import re
import json
import datetime
import requests
import urllib.parse

HOST="https://resultados-sim.tse.jus.br"
ENVIRONMENT="teste"
BASEURL=f"{HOST}/{ENVIRONMENT}"

# Stuff from comum/config/ele-c.json
CYCLE="ele2022"
ELECTIONS=[9240, 9238]

BASE_DOWNLOAD_PATH="data/download"

STATES = "BR AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ".split()

def download_file(url, static=True):
    url_path = os.path.relpath(urllib.parse.urlparse(url).path, "/")
    target_path = os.path.join(BASE_DOWNLOAD_PATH, url_path)

    if static and os.path.exists(target_path):
        return

    print(f"Downloading {url_path}")

    with requests.get(url) as response:
        if response.status_code == 403:
            print(f"Error downloading {url} {response.status_code}")
            return
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(response.content)

        return response.text

def download_index(election):
    config_url = f"{BASEURL}/{CYCLE}/{election}/config"

    index = {}
        
    for state in STATES:
        state = state.lower()
        text = download_file(f"{config_url}/{state}/{state}-e{election:06}-i.json", False)

        data = json.loads(text)
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")
            index[filename] = filedate

    return index

def get_path(filename):
    if filename == "ele-c.json":
        return f"comum/config/{filename}"

    result = re.match(r"^(?P<state>cert|mun|\w{2}).*-e(?P<election>\d{6}).*-(?P<type>[^\.]+?)\.\w+", filename)
    if result:
        result_state = result.group("state")
        result_election = result.group("election").lstrip('0')
        result_type = result.group("type")

        if result_state == "cert" or result_state == "mun":
            return f"{CYCLE}/{result_election}/config/{filename}"

        if result_type == "i":
            return f"{CYCLE}/{result_election}/config/{result_state}/{filename}"

        if result_type == "r":
            return f"{CYCLE}/{result_election}/dados-simplificados/{result_state}/{filename}"

        return f"{CYCLE}/{result_election}/dados/{result_state}/{filename}"
        

print(f"Host: {HOST}")
print(f"Environment: {ENVIRONMENT}")
print(f"Cycle: {CYCLE}")

current_index = {}

for election in ELECTIONS:
    print(f"Dowloading election: {election}")
    downloaded_index = download_index(election)

    for filename, filedate in downloaded_index.items():
        download_file(f"{BASEURL}/{get_path(filename)}")