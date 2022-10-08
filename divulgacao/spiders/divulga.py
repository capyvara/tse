import os
import json
import scrapy
import logging

from divulgacao.common.index import Index
from divulgacao.common.fileinfo import FileInfo
from divulgacao.common.basespider import BaseSpider

class DivulgaSpider(BaseSpider):
    name = "divulga"
    
    def append_states_index(self, election):
        base_path = self.get_local_path(f"{election}/config")
        for state in self.states:
            file_path = f"{base_path}/{state}/{state}-e{election:0>6}-i.json"
            if not os.path.exists(file_path):
                continue
    
            self.index.append_state(state, file_path)

    def load_index(self):
        self.index = Index()
        index_path = self.get_local_path("index.json", no_cycle=True)
        try:
            self.index.load(index_path)
        except Exception as e:
            logging.info("No valid saved index found, loading from downloaded index files")
            for election in self.elections:
                self.append_states_index(election)

        self.index.validate(self.get_local_path(""))

        logging.info(f"Index size {len(self.index)}")

    def save_index(self):
        index_path = self.get_local_path(f"index.json", no_cycle=True)
        self.index.save(index_path)

    def start_requests(self):
        self.load_settings()
        self.load_index()        
        self.pending = set()

        yield from self.query_common()

    def closed(self, reason):
        self.save_index()
        return

    def query_common(self):
        yield scrapy.Request(self.get_full_url(f"comum/config/ele-c.json", no_cycle=True), self.parse_config, dont_filter=True)

    def parse_config(self, response):
        self.persist_response(response)

        for election in self.elections:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def generate_requests_index(self, election):
        config_url = self.get_full_url(f"{election}/config")
            
        for state in self.states:
            filename = f"{state}-e{election:0>6}-i.json"
            logging.debug(f"Queueing index file {filename}")
            yield scrapy.Request(f"{config_url}/{state}/{filename}", self.parse_index, errback=self.errback_index,
                dont_filter=True, priority=2, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response)

        size = 0
        added = 0

        data = json.loads(response.body)
        for info, filedate in Index.expand(state, data):
            size += 1

            if self.ignore_pattern and self.ignore_pattern.match(info.filename):
                continue

            if info.filename in self.index and filedate <= self.index[info.filename]:
                continue

            if info.filename in self.pending:
                logging.debug(f"Skipping pending duplicated query {info.filename}")
                continue

            self.pending.add(info.filename)
            added += 1

            priority = 0 if info.type == "v" else 2

            logging.debug(f"Queueing file {info.filename} [{self.index.get(info.filename)} > {filedate}]")

            yield scrapy.Request(self.get_full_url(info.path), self.parse_file, errback=self.errback_file, priority=priority,
                dont_filter=True, cb_kwargs={"info": info, "filedate": filedate})

        logging.info(f"Parsed index for {election}-{state}, size {size}, added {added}, total pending {len(self.pending)}")

    def errback_index(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def parse_file(self, response, info, filedate):
        self.persist_response(response, filedate)
        self.index[info.filename] = filedate
        self.pending.discard(info.filename)

        if info.type == "f" and info.ext == "json" and info.settings["DOWNLOAD_PICTURES"]:
            try:
                data = json.loads(response.body)
                yield from self.query_pictures(data, info)
            except json.JSONDecodeError:
                logging.warning(f"Malformed json at {info.filename}, skipping parse")
                pass

    def errback_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
        self.pending.discard(failure.request.cb_kwargs["info"].filename)

    def expand_candidates(self, data):
        for agr in data["carg"]["agr"]:
            for par in agr["par"]:
                for cand in par["cand"]:
                    yield cand

    def query_pictures(self, data, info):
        added = 0

        for cand in self.expand_candidates(data):
            sqcand = cand["sqcand"]
            filename = f"{sqcand}.jpeg"

            if filename in self.pending:
                continue

            # President is br, others go on state specific directories
            cand_state = info.state if info.cand != "1" else "br"

            path = f"{info.election}/fotos/{cand_state}/{filename}"

            target_path = self.get_local_path(path)
            if not os.path.exists(target_path):
                self.pending.add(filename)
                added += 1
                logging.debug(f"Queueing picture {sqcand}.jpeg")
                yield scrapy.Request(self.get_full_url(path), self.parse_picture, priority=1,
                    dont_filter=True, cb_kwargs={"filename": filename})

        if added > 0:
            logging.info(f"Added pictures {added}, total pending {len(self.pending)}")

    def parse_picture(self, response, filename):
        self.persist_response(response)
        self.pending.discard(filename)