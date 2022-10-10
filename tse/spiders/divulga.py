import os
import json
import scrapy
import logging

from tse.common.index import Index
from tse.common.basespider import BaseSpider
from scrapy.core.downloader import Slot

class DivulgaSpider(BaseSpider):
    name = "divulga"

    def __init__(self, continuous=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.continuous = continuous
    
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
        self.pending = Index()

        if self.continuous:
            self.crawler.engine.downloader.slots["reindex"] = Slot(
                concurrency=1, 
                delay=1,
                randomize_delay=False)

        yield from self.query_common()

    def closed(self, reason):
        self.save_index()

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
                dont_filter=True, priority=4, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response)

        size = 0
        added = 0

        data = json.loads(response.body)
        for info, filedate in Index.expand(state, data):
            size += 1

            if self.ignore_pattern and self.ignore_pattern.match(info.filename):
                continue

            if info.filename in self.index and filedate == self.index[info.filename]:
                continue

            dupe = info.filename in self.pending
            self.pending[info.filename] = filedate

            if dupe:
                logging.debug(f"Skipping pending duplicated query {info.filename}")
                continue

            added += 1

            # Priorities (higher to lower)
            # 4 - Initial indexes
            # 3 - Static files (ex: configs, fixed data)
            # 2 - Aggregated results
            # 1 - Re-indexing continuous
            # 0 - Variable files 

            priority = 3

            if info.type == "r": 
                priority = 2
            elif info.type == "v":
                priority = 0

            logging.debug(f"Queueing file {info.filename} [{self.index.get(info.filename)} > {filedate}]")

            yield scrapy.Request(self.get_full_url(info.path), self.parse_file, errback=self.errback_file, priority=priority,
                dont_filter=True, cb_kwargs={"info": info})

        if added > 0 or response.request.meta.get("reindex_count", 0) == 0:
            logging.info(f"Parsed index for {election}-{state}, size {size}, added {added}, total pending {len(self.pending)}")

        if self.continuous and self.crawler.crawling:
            reindex_request = response.request.copy()
            reindex_request.priority = 1
            reindex_request.meta["depth"] = 0
            reindex_request.meta["download_slot"] = "reindex"
            reindex_request.meta["reindex_count"] = reindex_request.meta.get("reindex_count", 0) + 1
            self.crawler.engine.downloader.slots.get("reindex").delay = 1 # Autothrottle keeps overriding 
            logging.debug(f"Queueing re-indexing of {election}-{state}, count: {reindex_request.meta['reindex_count']}")
            yield reindex_request

    def errback_index(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def parse_file(self, response, info):
        filedate = self.pending[info.filename]
        self.persist_response(response, filedate)
        self.index[info.filename] = filedate
        self.pending.discard(info.filename)

        self.index_dirty_count = self.index_dirty_count + 1
        if self.index_dirty_count % 10 == 0:
            self.save_index()
            self.index_dirty_count = 0

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
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
                self.pending[filename] = None
                added += 1
                logging.debug(f"Queueing picture {sqcand}.jpeg")
                yield scrapy.Request(self.get_full_url(path), self.parse_picture, priority=1,
                    dont_filter=True, cb_kwargs={"filename": filename})

        if added > 0:
            logging.info(f"Added pictures {added}, total pending {len(self.pending)}")

    def parse_picture(self, response, filename):
        self.persist_response(response)
        self.pending.discard(filename)