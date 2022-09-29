import os
import re
import json
import datetime
import scrapy
import logging
import urllib.parse

class DivulgaSpider(scrapy.Spider):
    name = 'divulga'
    
    # Sim env
    #HOST="https://resultados-sim.tse.jus.br"
    #ENVIRONMENT="teste"
    #CYCLE="ele2022"
    #ELECTIONS=[9240, 9238]
    #custom_settings = { "JOBDIR": "data/crawls/divulga-sim" }

    # Prod env
    HOST="https://resultados.tse.jus.br"
    ENVIRONMENT="oficial"
    CYCLE="ele2022"
    ELECTIONS=[544, 546, 548]
    custom_settings = { "JOBDIR": "data/crawls/divulga-prod" }

    STATES = "BR AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ".lower().split()

    BASEURL=f"{HOST}/{ENVIRONMENT}"

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
    
            logging.info(f"Loading index from: {election}-{state}, size {len(data['arq'])}")

    def start_requests(self):
        logging.info(f"Host: {self.HOST}")
        logging.info(f"Environment: {self.ENVIRONMENT}")
        logging.info(f"Cycle: {self.CYCLE}")

        if not "index" in self.state:
            self.state["index"] = {}
            for election in self.ELECTIONS:
                self.load_index(election)

        if not "pending" in self.state:
            self.state["pending"] = set()

        logging.info(f"Total current index size {len(self.state['index'])}")

        for election in self.ELECTIONS:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def generate_requests_index(self, election):
        config_url = f"{self.BASEURL}/{self.CYCLE}/{election}/config"
            
        for state in self.STATES:
            yield scrapy.Request(f"{config_url}/{state}/{state}-e{election:06}-i.json", 
                self.parse_index, dont_filter=True, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response)

        data = json.loads(response.body)

        index = {}
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")
            index[filename] = filedate

        current_index = self.state["index"]
        
        for filename, filedate in index.items():
            if not filename in current_index or current_index[filename] != filedate:
                if filename in self.state["pending"]:
                    logging.debug(f"Skipping pending duplicated query {filename}")
                    continue

                self.state["pending"].add(filename)
                path, type = self.extract_path_info(filename)
                priority = 1 if type in {"c", "a", "cm", "r"} else 0
                yield scrapy.Request(f"{self.BASEURL}/{path}", self.parse_file, priority=priority,
                    dont_filter=True, cb_kwargs={"filename": filename, "filedate": filedate})

        logging.info(f"Parsed index for {election}-{state}, size {len(index)}, total queue size {len(self.crawler.engine.slot.scheduler)}")

    def parse_file(self, response, filename, filedate):
        self.persist_response(response, filedate)
        self.state["index"][filename] = filedate
        self.state["pending"].remove(filename)
                
