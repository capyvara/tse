import datetime
import orjson
import logging
import os

from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.spidermiddlewares.httperror import HttpError

from tse.common.index import Index
from tse.common.basespider import BaseSpider
from tse.common.pathinfo import PathInfo
from tse.middlewares import defer_request
from tse.parsers import FixedParser, IndexParser


class DivulgaSpider(BaseSpider):
    name = "divulga"

    custom_settings = {
        "EXTENSIONS": {
            'tse.extensions.LogStatsDivulga': 543,
        }
    }

    def __init__(self, continuous=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.continuous = continuous

    def continue_requests(self, config_data):
        self.pending = dict()

        for election in self.elections:
            logging.info("Scheduling election: %s", election)
            yield from self.generate_requests_index(election)

        if self.settings["DOWNLOAD_PICTURES"]:
            yield from self.generate_missing_pictures_requests()


    def generate_requests_index(self, election):
        idx = self.elections.index(election)
        priority = 1000 + ((len(self.elections) - idx) * 100) 

        for state in self.states:
            logging.debug("Scheduling index file for %s-%s", election, state)
            path = PathInfo.get_state_index_path(election, state)
            yield self.make_request(path, self.parse_index, errback=self.errback_index, 
                priority = priority + (50 if state == "br" else 0),
                cb_kwargs={"election": election, "state":state})

    # Higher is scheduled first
    def get_file_priority(self, info):
        if info.type == "i": # Reindexes
            return 3
        
        priority = 0

        if info.election:
            idx = self.elections.index(info.election)
            priority += (len(self.elections) - 1 - idx) * 30 # Favors federal elections
        
        if info.state:
            if info.state == "br": # Countrywise
                priority += 20       
            elif not info.city: # No city = Statewise 
                priority += 10
    
        if info.type in ("c", "a", "cm", "f"): # Configuration, fixed, etc (mostly unchanging files)
            priority += 6
        elif info.type in ("r", "ab", "t", "e"): # Simplified results, totalling status
            priority += 4
        elif info.type == "v": # Variable results
            priority += 2
        
        if info.ext == "jpeg": # Pics
            priority += 1
        elif info.ext == "sig": # Signatures
            priority -= 2

        return priority

    def expand_index(self, state, data):
        for filename, filedate in IndexParser.expand(data):
            if filename == "ele-c.json":
                continue

            info = PathInfo(filename)
            if (info.prefix == "cert" or info.prefix == "mun") and state != "br":
                continue
            
            if info.state and state != info.state:
                continue

            yield info, filedate

    def parse_index(self, response, election, state):
        result = self.persist_response(response)

        if not self.crawler.crawling:
            return

        size = 0
        added = 0

        transferring = self.crawler.engine.downloader.slots[response.meta["download_slot"]].transferring

        data = orjson.loads(result.contents)
        
        priorities = ((i, d, self.get_file_priority(i)) for i,d in self.expand_index(state, data))
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
                        logging.debug("Bumped date for %s to [%s > %s]", info.filename, self.pending[info.filename], new_index_date)
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

        result = self.persist_response(response)

        # Server may send a version that wasn't updated yet
        if not result.is_new_file and self.crawler.crawling:
            # Re-attempt couple times after a delay
            retry_times = response.meta.get("retry_times", 0) + 1
            if retry_times <= 3:
                self.crawler.stats.inc_value("divulga/dupes")
                logging.debug("File %s dupe retrying [%s > %s], rt:%d", info.filename, result.index_entry.index_date, index_date, retry_times)

                retry_request = defer_request(min(5.0 * retry_times, 15.0), response.request)
                retry_request.meta["retry_times"] = retry_times
                self.pending[info.filename] = index_date
                yield retry_request
            else:
                self.crawler.stats.inc_value("divulga/skipped_dupes")
                logging.debug("File %s dupe skipped up [%s > %s]", info.filename, result.index_entry.index_date, index_date)
                self.index[info.filename] = result.index_entry._replace(index_date=index_date)

            return

        self.index[info.filename] = result.index_entry._replace(index_date=index_date)

        if not self.crawler.crawling:
            return

        if info.type == "f" and info.ext == "json" and self.settings["DOWNLOAD_PICTURES"]:
            try:
                yield from self.process_fixed(orjson.loads(result.contents), info.election)
            except orjson.JSONDecodeError:
                logging.debug("Malformed json at %s, skipping parse", info.filename)

    def errback_file(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))
        self.pending.pop(failure.request.cb_kwargs["info"].filename, None)

    def process_fixed(self, data, election):
        sqcands = (cand["sqcand"] for cand in FixedParser.expand_candidates(data))
        yield from self.generate_pictures_requests(election, sqcands)

    def generate_pictures_requests(self, election, sqcands):
        added = 0
        for sqcand in sqcands:
            request = self.make_picture_request(election, sqcand)
            if request:
                added += 1
                yield request

        if added > 10:
            logging.info("Added pictures %d, total pending %d", added, len(self.pending))

    def make_picture_request(self, election, sqcand):
        info = PathInfo(PathInfo.get_picture_filename(sqcand))

        path = info.make_picture_path(election)
        filename = os.path.basename(path)
        if filename in self.pending or filename in self.index:
            return None

        metadata = {"election": election}

        self.pending[filename] = True
        priority = self.get_file_priority(info)

        logging.debug("Scheduling picture %s, p: %d", filename, priority)
        return self.make_request(path, self.parse_picture, errback=self.errback_picture, priority=priority, 
            cb_kwargs={"filename": info.filename, "metadata": metadata})

    def parse_picture(self, response, filename, metadata):
        if not self.pending.pop(filename, False):
            return    
        
        result = self.persist_response(response)
        self.index[filename] = result.index_entry._replace(metadata=metadata)

    def errback_picture(self, failure):
        if failure.check(HttpError) and failure.value.response.status == 403:
            path = self.get_path_from_url(failure.request.url)
            filename = os.path.basename(path)
            self.index[filename] = Index.Entry(datetime.datetime.now().replace(microsecond=0), "")
            logging.debug("Picture not found %s", str(failure.request))
            return

        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def generate_missing_pictures_requests(self):
        sqcands_map = {}
        indexed_sqcands = {os.path.splitext(p)[0] for p,_ in self.index.search("%%.jpeg")}

        for filename, _ in self.index.search("%%-f.json"):
            info = PathInfo(filename)

            try:
                with open(self.get_local_path(info.path), "rb") as f:
                    data = orjson.loads(f.read())
                    sqcands = (cand["sqcand"] for cand in FixedParser.expand_candidates(data))
                    sqcands_map.setdefault(info.election, set()).update(sqcands)
            except orjson.JSONDecodeError:
                logging.debug("Malformed json at %s, skipping parse", info.filename)

        for election, sqcands in sqcands_map.items():
            diff = sqcands - indexed_sqcands
            yield from self.generate_pictures_requests(election, diff)