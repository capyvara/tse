import datetime
import glob
import json
import logging
import os

import scrapy

from tse.common.basespider import BaseSpider
from tse.common.index import Index
from tse.common.pathinfo import PathInfo
from tse.middlewares import defer_request
from tse.parsers import FixedParser, IndexParser, get_dh_timestamp


class DivulgaSpider(BaseSpider):
    name = "divulga"

    # Priorities (higher to lower)
    # 4 - Initial indexes
    # 3 - Static files (ex: configs, fixed data)
    # 2 - Aggregated results
    # 1 - Re-indexing continuous
    # 0 - Variable files, .sig files 

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
           'tse.middlewares.DeferMiddleware': 543,
        }
    }

    def __init__(self, continuous=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.continuous = continuous

    def append_state_index(self, state_index_path):
        info = PathInfo(os.path.basename(state_index_path))
        if info.filename in self.index:
            return

        state_index_data = self.load_json(state_index_path)

        def expand_state_index():
            for f, d in IndexParser.expand(info.state, state_index_data): 
                self.get_current_version(self.get_local_path(f.path))
                yield (f.filename, Index.Entry(d))

        self.index.add_many(expand_state_index())

        logging.info(f"Appended index from: {state_index_path}")

        self.index[info.filename] = Index.Entry()

    def validate_index_entry(self, filename, entry: Index.Entry):
        info = PathInfo(filename)
        if not info.path or info.type == "i":
            return True

        target_path = self.get_local_path(info.path, info.no_cycle)
        if not os.path.exists(target_path):
            logging.debug(f"Target path not found, skipping index {info.filename}")
            return False

        modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(target_path))
        if entry.index_date != modified_time:
            logging.debug(f"Index date mismatch, skipping index {info.filename} {modified_time} > {entry.index_date}")
            return False

        return True

    def validate_index(self):
        logging.info(f"Validating index...")

        invalid = [f for f, e in self.index.items() if not self.validate_index_entry(f, e)]
        if len(invalid) > 0:
            self.index.remove_many(invalid)
            logging.info(f"Removed {len(invalid)} invalid index entries")

    def load_index(self):
        for state_index_path in glob.glob(f"{self.get_local_path('')}/[0-9]*/config/[a-z][a-z]/*.json", recursive=True):
            self.append_state_index(state_index_path)

        self.validate_index()

        logging.info(f"Index size {len(self.index)}")

    def continue_requests(self, config_data):
        self.load_index()
        self.pending = dict()

        for election in self.elections:
            logging.info(f"Queueing election: {election}")
            yield from self.generate_requests_index(election)

    def closed(self, reason):
        self.index.close()

    def generate_requests_index(self, election):
        for state in self.states:
            logging.debug(f"Queueing index file for {election}-{state}")
            path = PathInfo.get_state_index_path(election, state)
            yield scrapy.Request(self.get_full_url(path), self.parse_index, errback=self.errback_index,
                dont_filter=True, priority=4, cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        self.persist_response(response, check_identical=True)

        size = 0
        added = 0

        data = json.loads(response.body)
        for info, filedate in IndexParser.expand(state, data):
            size += 1

            if self.ignore_pattern and self.ignore_pattern.match(info.filename):
                continue

            if info.filename in self.index and filedate <= self.index[info.filename].index_date:
                continue

            dupe = info.filename in self.pending

            # Pending always stores the latest known filedate
            self.pending[info.filename] = filedate

            if dupe:
                logging.debug(f"Skipping pending duplicated query {info.filename}")
                continue

            added += 1

            priority = 3

            if info.type == "r": 
                priority = 2
            elif info.type == "v" or info.ext == "sig":
                priority = 0

            logging.debug(f"Queueing file {info.filename} [{self.index.get(info.filename).index_date} > {filedate}]")

            yield scrapy.Request(self.get_full_url(info.path), self.parse_file, errback=self.errback_file, priority=priority,
                dont_filter=True, cb_kwargs={"info": info})

        if added > 0 or response.request.meta.get("reindex_count", 0) == 0:
            logging.info(f"Parsed index for {election}-{state}, size {size}, added {added}, total pending {len(self.pending)}")

        if self.continuous and self.crawler.crawling:
            reindex_request = defer_request(60.0, response.request)
            reindex_request.priority = 1
            reindex_request.meta["reindex_count"] = reindex_request.meta.get("reindex_count", 0) + 1
            logging.debug(f"Queueing re-indexing of {election}-{state}, count: {reindex_request.meta['reindex_count']}")
            yield reindex_request

    def errback_index(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def parse_file(self, response, info):
        filedate = self.pending[info.filename]
        self.persist_response(response, filedate)
        self.pending.pop(info.filename, None)

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
            try:
                yield from self.query_pictures(json.loads(response.body), info, filedate)
            except json.JSONDecodeError:
                logging.warning(f"Malformed json at {info.filename}, skipping parse")

    def errback_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
        self.pending.pop(failure.request.cb_kwargs["info"].filename, None)

    def query_pictures(self, data, info, filedate):
        added = 0

        for cand in FixedParser.expand_candidates(data):
            sqcand = cand["sqcand"]
            # President is br, others go on state specific directories
            cand_state = info.state if info.cand != "1" else "br"
            
            path = PathInfo.get_picture_path(info.election, cand_state, sqcand)
            filename = os.path.basename(path)
            if filename in self.pending:
                continue

            target_path = self.get_local_path(path)
            if not os.path.exists(target_path):
                self.pending[filename] = None
                added += 1
                logging.debug(f"Queueing picture {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_picture, priority=1,
                    dont_filter=True, cb_kwargs={"filename": filename, "filedate": filedate})
            else:
                self.update_file_timestamp(target_path, filedate)

        if added > 0:
            logging.info(f"Added pictures {added}, total pending {len(self.pending)}")

    def parse_picture(self, response, filename, filedate):
        self.persist_response(response, filedate)
        self.pending.pop(filename, None)