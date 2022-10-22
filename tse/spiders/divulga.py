import datetime
import glob
import json
import logging
import os
import urllib.parse

import scrapy
from scrapy import signals

from tse.common.basespider import BaseSpider
from tse.common.index import Index
from tse.common.pathinfo import PathInfo
from tse.middlewares import defer_request
from tse.parsers import FixedParser, IndexParser


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
        },
        "EXTENSIONS": {
            'tse.extensions.LogStatsDivulga': 543,
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
                self.update_current_version(self.get_local_path(f.path))
                yield (f.filename, Index.Entry(d))

        self.index.add_many(expand_state_index())

        logging.info("Appended index from: %s", state_index_path)

        self.index[info.filename] = Index.Entry()

    def validate_index_entry(self, filename, entry: Index.Entry):
        info = PathInfo(filename)
        if not info.path or info.type == "i":
            return True

        local_path = self.get_local_path(info.path)
        if not os.path.exists(local_path):
            logging.debug("Local path not found, skipping index %s", info.filename)
            return False

        modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_path))
        if entry.index_date != modified_time:
            logging.debug("Index date mismatch, skipping index %s %s > %s", info.filename, modified_time, entry.index_date)
            return False

        return True

    def validate_index(self):
        logging.info("Validating index...")

        invalid = [f for f, e in self.index.items() if not self.validate_index_entry(f, e)]
        if len(invalid) > 0:
            self.index.remove_many(invalid)
            logging.info("Removed %d invalid index entries", len(invalid))

    def load_index(self):
        # for state_index_path in glob.glob(f"{self.get_local_path('')}/[0-9]*/config/[a-z][a-z]/*.json", recursive=True):
        #     self.append_state_index(state_index_path)

        # self.validate_index()

        logging.info("Index size %d", len(self.index))

    def continue_requests(self, config_data):
        self.crawler.signals.connect(self.request_reached_downloader, signals.request_reached_downloader)
        self.crawler.signals.connect(self.request_left_downloader, signals.request_left_downloader)

        self.load_index()
        self.pending = dict()
        self.downloading = set()

        for election in self.elections:
            logging.info("Queueing election: %s", election)
            yield from self.generate_requests_index(election)

    def request_reached_downloader(self, request, spider):
        filename = os.path.basename(urllib.parse.urlparse(request.url).path)
        self.downloading.add(filename)

    def request_left_downloader(self, request, spider):
        filename = os.path.basename(urllib.parse.urlparse(request.url).path)
        self.downloading.discard(filename)

    def closed(self, reason):
        self.index.close()

    def generate_requests_index(self, election):
        for state in self.states:
            logging.debug("Queueing index file for %s-%s", election, state)
            path = PathInfo.get_state_index_path(election, state)
            yield self.make_request(path, self.parse_index, errback=self.errback_index, priority = 4,  
                cb_kwargs={"election": election, "state":state})

    def parse_index(self, response, election, state):
        result = self.persist_response(response)

        if not self.crawler.crawling:
            return

        size = 0
        added = 0

        transferring = self.crawler.engine.downloader.slots[response.meta["download_slot"]].transferring

        data = json.loads(result.body)
        for info, index_date in IndexParser.expand(state, data):
            size += 1

            if self.ignore_pattern and self.ignore_pattern.match(info.filename):
                continue

            if info.filename in self.index and index_date <= self.index[info.filename].index_date:
                continue

            def find_req(r):
                return r.cb_kwargs["info"].filename == info.filename if "info" in r.cb_kwargs else False

            if info.filename in self.pending:
                # There may be some time between the enqueue of the request and the actual http get
                # So if it isn't sent yet and a newer date is available use that instead
                if index_date > self.pending[info.filename]:
                    in_transfer = next(filter(find_req, transferring), None)
                    if in_transfer == None:
                        self.pending[info.filename] = index_date
                        logging.debug("Bumped date for %s to %s > %s", info.filename, self.pending[info.filename], index_date)
                        self.crawler.stats.inc_value("divulga/bumped")
                
                logging.debug("Skipping dupe %s %s > %s", info.filename, self.pending[info.filename], index_date)
                self.crawler.stats.inc_value("divulga/dupes")
                continue

            self.pending[info.filename] = index_date

            added += 1

            priority = 3

            if info.type == "r": 
                priority = 2
            elif info.type == "v" or info.ext == "sig":
                priority = 0

            logging.debug("Queueing file %s [%s > %s]", info.filename, self.index.get(info.filename).index_date, index_date)

            yield self.make_request(info.path, self.parse_file, errback=self.errback_file, priority=priority,
                cb_kwargs={"info": info})

        if added > 0 or response.request.meta.get("reindex_count", 0) == 0:
            logging.info("Parsed index for %s-%s, size %d, added %d, total pending %s", election, state, size, added, len(self.pending))

        if self.continuous:
            reindex_request = defer_request(60.0, response.request)
            reindex_request.priority = 1
            reindex_request.meta["reindex_count"] = reindex_request.meta.get("reindex_count", 0) + 1
            logging.debug("Queueing re-indexing of %s-%s, count: %d", election, state, reindex_request.meta['reindex_count'])
            self.crawler.stats.inc_value("divulga/reindexes")
            yield reindex_request

    def errback_index(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def parse_file(self, response, info):
        index_date = self.pending[info.filename]
        result = self.persist_response(response, index_date)
        self.pending.pop(info.filename, None)

        if not self.crawler.crawling:
            return

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
            try:
                yield from self.query_pictures(json.loads(result.body), info, index_date)
            except json.JSONDecodeError:
                logging.warning("Malformed json at %s, skipping parse", info.filename)

    def errback_file(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))
        self.pending.pop(failure.request.cb_kwargs["info"].filename, None)

    def query_pictures(self, data, info, index_date):
        added = 0

        for cand in FixedParser.expand_candidates(data):
            sqcand = cand["sqcand"]
            # President is br, others go on state specific directories
            cand_state = info.state if info.cand != "1" else "br"
            
            path = PathInfo.get_picture_path(info.election, cand_state, sqcand)
            filename = os.path.basename(path)
            if filename in self.pending:
                continue

            local_path = self.get_local_path(path)
            if not os.path.exists(local_path):
                self.pending[filename] = None
                added += 1
                logging.debug("Queueing picture %s", filename)
                yield self.make_request(path, self.parse_picture, priority=1, 
                    cb_kwargs={"filename": filename, "index_date": index_date})
            else:
                self.update_file_timestamp(local_path, index_date)

        if added > 0:
            logging.info("Added pictures %d, total pending %d", added, len(self.pending))

    def parse_picture(self, response, filename, index_date):
        self.persist_response(response, index_date)
        self.pending.pop(filename, None)