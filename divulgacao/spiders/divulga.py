import os
import re
import json
import datetime
import requests
import scrapy
import logging
import urllib.parse

class DivulgaSpider(scrapy.Spider):
    name = 'divulga'
    
    HOST="https://resultados-sim.tse.jus.br"
    ENVIRONMENT="teste"
    BASEURL=f"{HOST}/{ENVIRONMENT}"

    # Stuff from comum/config/ele-c.json
    CYCLE="ele2022"
    ELECTIONS=[9240, 9238]

    STATES = "BR AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ".lower().split()

    def extract_path_info(self, filename):
        if filename == "ele-c.json":
            return f"comum/config/{filename}", "c"

        result = re.match(r"^(?P<state>cert|mun|\w{2}).*-e(?P<election>\d{6}).*-(?P<type>[^\.]+?)\.\w+", filename)
        if result:
            state = result.group("state")
            election = result.group("election").lstrip('0')
            type = result.group("type")

            if state == "cert" or state == "mun":
                return f"{self.CYCLE}/{election}/config/{filename}", type

            if type == "i":
                return f"{self.CYCLE}/{election}/config/{state}/{filename}", type

            if type == "r":
                return f"{self.CYCLE}/{election}/dados-simplificados/{state}/{filename}", type

            return f"{self.CYCLE}/{election}/dados/{state}/{filename}", type

    def persist_response(self, response, filedate=None):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(response.body)

        if filedate:
            dt_epoch = filedate.timestamp()
            os.utime(target_path, (dt_epoch, dt_epoch))

    def load_index(self, election):
        if not "index" in self.state:
            self.state["index"] = {}

        files_store = self.settings['FILES_STORE']
        base_path = f"{files_store}/{self.ENVIRONMENT}/{self.CYCLE}/{election}/config"
        for state in self.STATES:
            file_path = f"{base_path}/{state}/{state}-e{election:06}-i.json"
            if not os.path.exists(file_path):
                continue

            with open(file_path, "r") as f:
                data = json.loads(f.read())
                for entry in data["arq"]:
                    filename = entry["nm"]
                    filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")

                    path, type = self.extract_path_info(filename)
                    if os.path.exists(f"{files_store}/{self.ENVIRONMENT}/{path}"):
                        self.state["index"][filename] = filedate
    
            logging.info(f"Loading index from: {election}-{state}, total index size {len(self.state['index'])}")

    def start_requests(self):
        logging.info(f"Host: {self.HOST}")
        logging.info(f"Environment: {self.ENVIRONMENT}")
        logging.info(f"Cycle: {self.CYCLE}")

        for election in self.ELECTIONS:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def generate_requests_index(self, election):
        self.load_index(election)

        config_url = f"{self.BASEURL}/{self.CYCLE}/{election}/config"
            
        for state in self.STATES:
            yield scrapy.Request(f"{config_url}/{state}/{state}-e{election:06}-i.json", 
                self.parse_index, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response)

        data = json.loads(response.body)

        index = {}
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")
            index[filename] = filedate

        logging.info(f"Parsed index for {election}-{state}, size {len(index)}")

        current_index = self.state["index"]
        
        for filename, filedate in index.items():
            if not filename in current_index or current_index[filename] != filedate:
                path, type = self.extract_path_info(filename)
                priority = 1 if type in {"c", "a", "cm", "r"} else 0
                yield scrapy.Request(f"{self.BASEURL}/{path}", self.parse_file, priority=priority,
                    cb_kwargs={"filename": filename, "filedate": filedate})

    def parse_file(self, response, filename, filedate):
        self.persist_response(response, filedate)
        self.state["index"][filename] = filedate
                
