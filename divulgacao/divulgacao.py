import os
import re
import requests
import urllib.parse

HOST="https://resultados-sim.tse.jus.br"
ENVIRONMENT="teste"

# Stuff from comum/config/ele-c.json
CYCLE="ele2022"
ELECTIONS=[9240, 9238]

BASE_DOWNLOAD_PATH="data/download"

STATES = "BR AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ".split()

def get_baseurl_common():
    return f"{HOST}/{ENVIRONMENT}/comum"

def get_baseurl_cycle():
    return f"{HOST}/{ENVIRONMENT}/{CYCLE}"

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


print(f"Host: {HOST}")
print(f"Environment: {ENVIRONMENT}")
print(f"Cycle: {CYCLE}")

download_file(f"{get_baseurl_common()}/config/ele-c.json")

def download_configs(election):
    base_url = f"{get_baseurl_cycle()}/{election}/config"
    
    download_file(f"{base_url}/cert-e{election:06}-a.cer")
    download_file(f"{base_url}/mun-e{election:06}-cm.json")
    download_file(f"{base_url}/mun-e{election:06}-cm.sig")
    
    # Indexes
    for state in STATES:
        state = state.lower()
        download_file(f"{base_url}/{state}/{state}-e{election:06}-i.json", False)

def get_fullpath(filename):
    if filename == "ele-c.json":
        return f"comum/config/{filename}"

    result = re.match(r"^(cert|mun|\w{2}).*-e(?P<election>\d{6}).*-(?P<type>[^\.]+?)\.\w+")
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

def download_dados(election):
    base_url = f"{get_baseurl_cycle()}/{election}/dados"
    for state in STATES:
        state = state.lower()
        

for election in ELECTIONS:
    print(f"Dowloading election: {election}")
    download_configs(election)