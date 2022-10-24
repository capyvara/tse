import json
import logging
import os
import urllib.parse

from scrapy import signals
from scrapy.downloadermiddlewares.retry import get_retry_request

from tse.common.basespider import BaseSpider
from tse.common.pathinfo import PathInfo
from tse.middlewares import defer_request
from tse.parsers import FixedParser, IndexParser


class DivulgaSpider(BaseSpider):
    name = "divulga"

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

    def continue_requests(self, config_data):
        self.crawler.signals.connect(self.request_reached_downloader, signals.request_reached_downloader)
        self.crawler.signals.connect(self.request_left_downloader, signals.request_left_downloader)

        self.pending = dict()
        self.downloading = set()

        for election in self.elections:
            logging.info("Scheduling election: %s", election)
            yield from self.generate_requests_index(election)

    def request_reached_downloader(self, request, spider):
        filename = os.path.basename(urllib.parse.urlparse(request.url).path)
        self.downloading.add(filename)

    def request_left_downloader(self, request, spider):
        filename = os.path.basename(urllib.parse.urlparse(request.url).path)
        self.downloading.discard(filename)

    def generate_requests_index(self, election):
        for state in self.states:
            logging.debug("Scheduling index file for %s-%s", election, state)
            path = PathInfo.get_state_index_path(election, state)
            yield self.make_request(path, self.parse_index, errback=self.errback_index, 
                priority = 1000 if state == "br" else 900,
                cb_kwargs={"election": election, "state":state})

    # Higher is scheduled first
    def get_file_priority(self, info):
        # Reindexes
        if info.type == "i":
            return 3
        
        priority = 0
        
        if info.state:
            # Countrywise
            if info.state == "br":
                priority += 20
            # Statewise        
            elif not info.city:
                priority += 10
    
        # Configuration, fixed, etc (mostly unchanging files)
        if info.type in ("c", "a", "cm", "f", "jpeg"):
            priority += 6
        # Simplified results, totalling status
        elif info.type in ("r", "ab", "t", "e"):
            priority += 4
        # Variable results
        elif info.type == "v":
            priority += 2

        # Signatures
        if info.ext == "sig":
            priority -= 2

        return priority

    def parse_index(self, response, election, state):
        result = self.persist_response(response)

        if not self.crawler.crawling:
            return

        size = 0
        added = 0

        transferring = self.crawler.engine.downloader.slots[response.meta["download_slot"]].transferring

        data = json.loads(result.contents)
        
        priorities = ((i, d, self.get_file_priority(i)) for i,d in IndexParser.expand(state, data))
        sorted_index = sorted(priorities, key = lambda t: t[2], reverse=True)
        for info, new_index_date, priority in sorted_index:
            size += 1

            if self.ignore_pattern and self.ignore_pattern.match(info.filename):
                continue

            index_entry = self.index.get(info.filename)
            if index_entry and index_entry.index_date and new_index_date <= index_entry.index_date:
                continue

            def find_req(r):
                return r.cb_kwargs["info"].filename == info.filename if "info" in r.cb_kwargs else False

            if info.filename in self.pending:
                # There may be some time between the schedule of the request and the actual http get
                # So if it isn't sent yet and a newer date is available use that instead
                if new_index_date > self.pending[info.filename]:
                    in_transfer = next(filter(find_req, transferring), None)
                    if in_transfer == None:
                        self.pending[info.filename] = new_index_date
                        logging.debug("Bumped date for %s to %s > %s", info.filename, self.pending[info.filename], new_index_date)
                        self.crawler.stats.inc_value("divulga/bumped")
                
                continue

            self.pending[info.filename] = new_index_date
            added += 1

            logging.debug("Scheduling file %s [%s > %s], p:%d", 
                info.filename, index_entry.index_date if index_entry else None, new_index_date, priority)

            yield self.make_request(info.path, self.parse_file, errback=self.errback_file, 
                priority=priority, cb_kwargs={"info": info})

        if added > 0 or response.request.meta.get("reindex_count", 0) == 0:
            logging.info("Parsed index for %s-%s, size %d, added %d, total pending %s", election, state, size, added, len(self.pending))

        if self.continuous and self.crawler.crawling:
            reindex_request = defer_request(60.0, response.request)
            reindex_request.priority = self.get_file_priority(PathInfo(os.path.basename(result.local_path)))
            reindex_request.meta["reindex_count"] = reindex_request.meta.get("reindex_count", 0) + 1
            logging.debug("Scheduling re-indexing of %s-%s, count: %d", election, state, reindex_request.meta['reindex_count'])
            self.crawler.stats.inc_value("divulga/reindexes")
            yield reindex_request

    def errback_index(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def parse_file(self, response, info):
        index_date = self.pending.pop(info.filename, None)
        if not index_date:
            return

        result = self.persist_response(response, index_date)

        # Server may send a version that wasn't updated yet so keep the old index date
        if not result.is_new_file:
            self.crawler.stats.inc_value("divulga/dupes")
            
            retry_request = get_retry_request(response.request, 
                spider=self, reason='outdated_index', max_retry_times=1, priority_adjust=-1)
            
            if retry_request:
                self.pending[info.filename] = index_date
                yield retry_request

            return

        if not self.crawler.crawling:
            return

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
            try:
                yield from self.query_pictures(json.loads(result.contents), info, index_date)
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
                self.pending[filename] = index_date
                added += 1
                logging.debug("Scheduling picture %s", filename)
                yield self.make_request(path, self.parse_picture, priority=self.get_file_priority(info), 
                    cb_kwargs={"filename": filename, "index_date": index_date})

        if added > 0:
            logging.info("Added pictures %d, total pending %d", added, len(self.pending))

    def parse_picture(self, response, filename, index_date):
        self.persist_response(response, index_date)
        self.pending.pop(filename, None)