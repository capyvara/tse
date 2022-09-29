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
    HOST="https://resultados-sim.tse.jus.br"
    ENVIRONMENT="teste"
    CYCLE="ele2022"
    ELECTIONS=[9240, 9238]
    custom_settings = { "JOBDIR": "data/crawls/divulga-sim" }

    # Prod env
    # HOST="https://resultados.tse.jus.br"
    # ENVIRONMENT="oficial"
    # CYCLE="ele2022"
    # ELECTIONS=[544, 546, 548]
    # custom_settings = { "JOBDIR": "data/crawls/divulga-prod" }

    STATES = "BR AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ".lower().split()

    BASEURL=f"{HOST}/{ENVIRONMENT}"

    class FileInfo:
        _regex = re.compile(r"^(?P<state>cert|mun|\w{2})(?P<mun>\d{5})?(?:-c(?P<cand>\d{4}))?-e(?P<election>\d{6})(?:-(?P<ver>\d{3}))?-(?P<type>\w{1,2}?)\.(?P<ext>\w+)")

        def __init__(self, filename):
            self.filename = filename

            if filename == "ele-c.json":
                self.path = f"comum/config/{filename}"
                self.type = "c"
                self.ext = "json"
                return

            result = type(self)._regex.match(filename)
            if result:
                self.state = result.group("state")
                self.mun = result.group("mun")
                self.cand = result.group("cand")
                self.election = result.group("election")
                self.cand = result.group("cand")
                self.type = result.group("type")
                self.ext = result.group("ext")

                uelection = self.election.lstrip("0")

                if self.state == "cert" or self.state == "mun":
                    self.path = f"{DivulgaSpider.CYCLE}/{uelection}/config/{filename}"
                elif self.type == "i":
                    self.path = f"{DivulgaSpider.CYCLE}/{uelection}/config/{self.state}/{filename}"
                elif self.type == "r":
                    self.path = f"{DivulgaSpider.CYCLE}/{uelection}/dados-simplificados/{self.state}/{filename}"
                else:
                    self.path = f"{DivulgaSpider.CYCLE}/{uelection}/dados/{self.state}/{filename}"
            else:
                raise ValueError("Filename format not recognized")
            
    def persist_response(self, response, filedate=None):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            f.write(response.body)

        if filedate:
            dt_epoch = filedate.timestamp()
            os.utime(target_path, (dt_epoch, dt_epoch))

    def expand_index(self, state, data):
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")

            if filename == "ele-c.json":
                continue

            info = self.FileInfo(filename)
            if (info.state == "cert" or info.state == "mun") and state != "br":
                continue

            # Uncomment to skip signature files
            #if info.ext == "sig":
            #    continue

            yield info, filedate

    def load_index(self, election):
        files_store = self.settings['FILES_STORE']
        base_path = f"{files_store}/{self.ENVIRONMENT}/{self.CYCLE}/{election}/config"
        for state in self.STATES:
            file_path = f"{base_path}/{state}/{state}-e{election:06}-i.json"
            if not os.path.exists(file_path):
                continue

            with open(file_path, "r") as f:
                data = json.loads(f.read())
                for info, filedate in self.expand_index(state, data):
                    if os.path.exists(f"{files_store}/{self.ENVIRONMENT}/{info.path}"):
                        self.state["index"][info.filename] = filedate
    
            logging.info(f"Loaded index from: {election}-{state}, size {len(data['arq'])}")

    def start_requests(self):
        logging.info(f"Host: {self.HOST}")
        logging.info(f"Environment: {self.ENVIRONMENT}")
        logging.info(f"Cycle: {self.CYCLE}")

        if not "index" in self.state:
            self.state["index"] = {}
            for election in self.ELECTIONS:
                self.load_index(election)
        
        self.state["pending"] = set()

        logging.info(f"Index size {len(self.state['index'])}")
        logging.info(f"Pending size {len(self.state['pending'])}")

        yield from self.query_common()

    def query_common(self):
        yield scrapy.Request(f"{self.BASEURL}/comum/config/ele-c.json", self.parse_config, dont_filter=True)

    def parse_config(self, response):
        self.persist_response(response)

        for election in self.ELECTIONS:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def generate_requests_index(self, election):
        config_url = f"{self.BASEURL}/{self.CYCLE}/{election}/config"
            
        for state in self.STATES:
            filename = f"{state}-e{election:06}-i.json"
            logging.debug(f"Queueing index file {filename}")
            yield scrapy.Request(f"{config_url}/{state}/{filename}", self.parse_index, errback=self.errback_index,
                dont_filter=True, priority=2, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response)

        current_index = self.state["index"]

        data = json.loads(response.body)
        for info, filedate in self.expand_index(state, data):
            if info.filename in self.state["pending"]:
                logging.debug(f"Skipping pending duplicated query {info.filename}")
                continue

            self.state["pending"].add(info.filename)

            priority = 0 if info.type == "v" else 1

            logging.debug(f"Queueing file {info.filename} - priority:{priority} old:{current_index.get(info.filename)} new:{filedate}")

            yield scrapy.Request(f"{self.BASEURL}/{info.path}", self.parse_file, errback=self.errback_file, priority=priority,
                dont_filter=True, cb_kwargs={"info": info, "filedate": filedate})

        logging.info(f"Parsed index for {election}-{state}, total pending {len(self.state['pending'])}")

    def errback_index(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def parse_file(self, response, info, filedate):
        self.persist_response(response, filedate)
        self.state["index"][info.filename] = filedate
        self.state["pending"].discard(info.filename)

        if info.type == "r":
            data = json.loads(response.body)
            yield from self.download_pictures(data)

    def errback_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
        self.state["pending"].discard(failure.request.cb_kwargs["info"].filename)

    def download_pictures(self, data):
        return