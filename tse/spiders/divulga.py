import json
import logging
import os

import scrapy

from tse.common.basespider import BaseSpider
from tse.common.fileinfo import FileInfo
from tse.common.index import Index
from tse.middlewares import defer_request


class DivulgaSpider(BaseSpider):
    name = "divulga"

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
           'tse.middlewares.DeferMiddleware': 543,
        }
    }

    def __init__(self, continuous=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.continuous = continuous
    
    def append_states_index(self, election):
        # TUDO: scandir
        for state in self.states:
            file_path = self.get_local_path(FileInfo.get_state_index_path(election, state))
            if not os.path.exists(file_path):
                continue
    
            self.index.append_state(state, file_path)

    def load_index(self):
        self.index = Index(self.get_local_path("index.db", no_cycle=True))

        base_local_path = self.get_local_path("")
        if len(self.index) == 0:
            logging.info("Empty index found, loading from downloaded index files")

            with os.scandir(base_local_path) as it:
                for entry in it:
                    if entry.is_file() or entry.name.startswith('.'): 
                        continue
                    try:
                        election = int(entry.name)
                        self.append_states_index(str(election))
                    except ValueError:
                        pass
                    
        self.index.validate(base_local_path)

        logging.info(f"Index size {len(self.index)}")

    def continue_requests(self, config_response):
        self.load_index()        
        self.pending = dict()

        for election in self.elections:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def closed(self, reason):
        if self.settings["INDEX_SAVE_JSON"]:
            self.index.save_json(self.get_local_path(f"index.json", no_cycle=True))

        self.index.close()

    def generate_requests_index(self, election):
        for state in self.states:
            logging.debug(f"Queueing index file for {election}-{state}")
            path = FileInfo.get_state_index_path(election, state)
            yield scrapy.Request(self.get_full_url(path), self.parse_index, errback=self.errback_index,
                dont_filter=True, priority=4, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response, check_identical=True)

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
            reindex_request = defer_request(30.0, response.request)
            reindex_request.priority = 1
            reindex_request.meta["reindex_count"] = reindex_request.meta.get("reindex_count", 0) + 1
            logging.debug(f"Queueing re-indexing of {election}-{state}, count: {reindex_request.meta['reindex_count']}")
            yield reindex_request

    def errback_index(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def parse_file(self, response, info):
        filedate = self.pending[info.filename]
        self.persist_response(response, filedate)
        self.index[info.filename] = filedate
        self.pending.pop(info.filename, None)

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
            try:
                data = json.loads(response.body)
                yield from self.query_pictures(data, info)
            except json.JSONDecodeError:
                logging.warning(f"Malformed json at {info.filename}, skipping parse")
                pass

    def errback_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
        self.pending.pop(failure.request.cb_kwargs["info"].filename, None)

    def expand_candidates(self, data):
        for agr in data["carg"]["agr"]:
            for par in agr["par"]:
                for cand in par["cand"]:
                    yield cand

    def query_pictures(self, data, info):
        added = 0

        for cand in self.expand_candidates(data):
            sqcand = cand["sqcand"]
            # President is br, others go on state specific directories
            cand_state = info.state if info.cand != "1" else "br"
            
            path = FileInfo.get_picture_path(info.election, cand_state, sqcand)
            filename = os.path.basename(path)
            if filename in self.pending:
                continue

            target_path = self.get_local_path(path)
            if not os.path.exists(target_path):
                self.pending[filename] = None
                added += 1
                logging.debug(f"Queueing picture {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_picture, priority=1,
                    dont_filter=True, cb_kwargs={"filename": filename})

        if added > 0:
            logging.info(f"Added pictures {added}, total pending {len(self.pending)}")

    def parse_picture(self, response, filename):
        self.persist_response(response)
        self.pending.pop(filename, None)