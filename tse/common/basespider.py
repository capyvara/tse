import datetime
import logging
import os
import re
import hashlib
import signal

import urllib.parse
from email.utils import parsedate_to_datetime, format_datetime
import orjson
from pyparsing import NamedTuple

import scrapy
from twisted.web.client import ResponseFailed
from scrapy.utils.python import to_unicode

from tse.common.index import Index
from tse.common.pathinfo import PathInfo
from tse.parsers import CityConfigParser
from tse.utils import log_progress


class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdown = False
        self._sigHandler = None

        self._city_state_map = None

    def _handle_sigint(self, signum, frame):
        self.shutdown = True
        if self._sigHandler:
            self._sigHandler(signum, frame)

    def continue_requests(self, config_data, config_entry):
        raise NotImplementedError(f"{self.__class__.__name__}.continue_requests callback is not defined")

    def start_requests(self):
        self.initialize()
        if self.shutdown:
            return

        yield from self.query_common()

    def query_common(self):
        yield self.make_request(PathInfo.get_election_config_path(), self.parse_config)

    def parse_config(self, response):
        result = self.persist_response(response)
        config_data = orjson.loads(result.contents)
        yield from self.continue_requests(config_data)

    def get_local_path(self, path):
        return PathInfo.get_local_path(self.settings, path)

    def get_full_url(self, path):
        return PathInfo.get_full_url(self.settings, path)

    def archive_version(self, path):
        dirname, filename = os.path.split(path)
        index_version = self.index.get_current_version(filename)
        if index_version == 0 or not os.path.exists(path):
            return 0
        
        ver_dir = os.path.join(dirname, ".ver")
        root, ext = os.path.splitext(filename)
        ver_path = os.path.join(ver_dir, f"{root}_{index_version:04}{ext}")

        os.makedirs(ver_dir, exist_ok=True)
        os.rename(path, ver_path)

        return index_version
        
    def _rfc2822_to_datetime(self, date_str):
        try:
            date_str = to_unicode(date_str, encoding='ascii')
            return parsedate_to_datetime(date_str).replace(tzinfo=None)
        except Exception:
            return None  
                  
    def get_http_cache_headers(self, response):
        last_modified = self._rfc2822_to_datetime(response.headers[b"Last-Modified"]) if b"Last-Modified" in response.headers else None
        etag = to_unicode(response.headers[b"ETag"]).strip('"') if b"ETag" in response.headers else None
        date = self._rfc2822_to_datetime(response.headers[b"Date"]) if b"Date" in response.headers else None
        return (last_modified, etag, date)

    def make_request(self, path: str, *args, **kwargs):
        headers = kwargs.setdefault("headers", {})

        filename = os.path.basename(path)
        entry = self.get_valid_index_entry(filename)
        if entry:
            if entry.last_modified != None:
                headers[b"If-Modified-Since"] = format_datetime(entry.last_modified.replace(tzinfo=datetime.timezone.utc), True)
            if entry.etag != None:
                headers[b"ETag"] = f'"{entry.etag}"'

            headers[b"Cache-Control"] = "max-age=0"

            meta = kwargs.setdefault("meta", {})
            meta.update({"handle_httpstatus_list": [304]})
        else:
            headers[b"Cache-Control"] = "no-cache"

        kwargs.setdefault("dont_filter", True)
        return scrapy.Request(self.get_full_url(path), *args, **kwargs)

    class PersistedResult(NamedTuple):
        local_path: str
        index_entry: Index.Entry
        body: bytes
        is_new_file: bool

        @property
        def contents(self) -> bytes:
            if self.body: 
                return self.body

            with open(self.local_path, "rb") as f:
                return f.read()

        @property
        def filename(self) -> str:
            return os.path.basename(self.local_path)

    def write_result_file(self, path, body, date):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(body)

        dt_epoch = date.replace(tzinfo=datetime.timezone.utc).timestamp()
        os.utime(path, (dt_epoch, dt_epoch))

    def get_path_from_url(self, url):
        return urllib.parse.urlparse(url).path.strip("/")

    def persist_response(self, response) -> PersistedResult:
        local_path = os.path.join(self.settings["FILES_STORE"], self.get_path_from_url(response.url))
        filename = os.path.basename(local_path)

        last_modified, etag, server_date = self.get_http_cache_headers(response)
        index_entry = self.index.get(filename)

        if response.status == 304:
            if index_entry:
                return self.PersistedResult(local_path, index_entry, None, False)
            else:
                raise ResponseFailed(response)

        last_modified = (last_modified or 
                        server_date or 
                        datetime.datetime.utcnow().replace(tzinfo=None, microsecond=0))

        etag = etag or hashlib.md5(response.body).hexdigest()

        # Same indexed contents (etag or body md5)
        if index_entry and (index_entry.etag == etag):
            if not os.path.exists(local_path):
                self.write_result_file(local_path, response.body, last_modified)
            if index_entry.last_modified != last_modified:    
                self.index[filename] = index_entry._replace(last_modified=last_modified)

            return self.PersistedResult(local_path, index_entry, response.body, False)

        # We have a new file
        index_entry = Index.Entry(last_modified, etag)

        index_version = self.archive_version(local_path) if self.keep_old_versions else 0
        if index_version != 0:
            self.index.add_version(filename, index_version + 1, index_entry)
        else:
            self.index[filename] = index_entry

        self.write_result_file(local_path, response.body, last_modified)

        return self.PersistedResult(local_path, index_entry, response.body, True)

    @property
    def plea(self):
        return self.settings["PLEA"]

    @property
    def elections(self):
        return self.settings["ELECTIONS"]

    @property
    def states(self):
        return self.settings["STATES"]

    @property
    def ignore_pattern(self):
        return re.compile(self.settings["IGNORE_PATTERN"]) if self.settings["IGNORE_PATTERN"] else None

    @property
    def keep_old_versions(self):
        return self.settings["KEEP_OLD_VERSIONS"]

    def initialize(self):
        if type(self) == BaseSpider:
            raise NotImplementedError(f"BaseSpider is meant to be a inherited from")

        self._sigHandler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

        logging.info("Host: %s", self.settings["HOST"])
        logging.info("Environment: %s", self.settings["ENVIRONMENT"])
        logging.info("Cycle: %s", self.settings["CYCLE"])
        
        logging.info("Plea: %s", self.plea)
        logging.info("Elections: %s", self.elections)
        logging.info("States: %s", self.states)

        db_dir = os.path.join(self.settings["FILES_STORE"], self.settings["ENVIRONMENT"])
        os.makedirs(db_dir, exist_ok=True)
        self.index = Index(os.path.join(db_dir, f"index_{self.name}_{self.plea}.db"))
        logging.info("Index size %d", len(self.index))

        if self.settings["VALIDATE_INDEX"]:
            self.validate_index()

    def closed(self, reason):
        if hasattr(self, "index"):
            self.index.close()

    def find_local_path_elections(self):
        for entry in os.scandir(self.get_local_path("")):
            if entry.is_dir and not entry.name.startswith(".") and entry.name.isdigit():
                yield entry.name

    def get_state(self, city):
        if self._city_state_map:
            return self._city_state_map[city]

        config_paths = ["data/mun-default-cm.json", PathInfo.get_cities_config_path(self.elections[0])]

        self._city_state_map = {}

        for config_path in config_paths:
            try:
                with open(config_path, "rb") as f:
                    data = orjson.loads(f.read())
                    map = {ccity: cstate for cstate, ccity, _, _, _, _ in CityConfigParser.expand_cities(data)}
                    self._city_state_map.update(map)
            except FileNotFoundError:
                continue
        
        return self._city_state_map[city]

    def get_valid_index_entry(self, filename):
        entry = self.index.get(filename)
        if not entry:
            return None

        if not self.validate_index_entry(filename, entry):
            self.index.discard(filename)
            return None

        return entry

    def validate_index_entry(self, filename, entry: Index.Entry):
        info = PathInfo(filename)
        
        if not info.path:
            if info.match == "ballot_box":
                if not entry.metadata:
                    logging.debug("Index: Missing meta information %s", info.filename)
                    return False

                info.path = info.make_ballot_box_file_path(entry.metadata["state"], entry.metadata["hash"])
            elif info.match == "picture":
                if not entry.metadata:
                    logging.debug("Index: Missing meta information %s", info.filename)
                    return False

                info.path = info.make_picture_path(entry.metadata["election"])
            else:
                logging.debug("Index: Missing path %s", info.filename)
                return False

        local_path = self.get_local_path(info.path)
        if not os.path.exists(local_path):
            logging.debug("Index: Local path not found %s", info.filename)
            return False

        modified_time = datetime.datetime.utcfromtimestamp(os.path.getmtime(local_path)).replace(microsecond=0)
        
        # Some tolerance, as some processes may change precision (ex: unzipping has two seconds precision)
        delta = modified_time - entry.last_modified
        if abs(delta.total_seconds()) > 2:
            logging.debug("Index: Modified date mismatch %s %s > %s", info.filename, modified_time, entry.last_modified)
            return False

        if info.plea and info.plea != self.plea:
            logging.debug("Index: Plea mismatch %s %s > %s", info.filename, info.plea, self.plea)
            return False

        if info.election and not info.election in self.elections:
            logging.debug("Index: Election mismatch %s %s > %s", info.filename, info.election, self.elections)
            return False

        return True

    def validate_index(self):
        logging.info("Validating index...")

        invalid = []
        for f, e in log_progress(self.index.items(), len(self.index)):
            if self.shutdown:
                return

            if not e.etag:
                continue

            if not self.validate_index_entry(f, e):
                invalid.append(f)

        if len(invalid) > 0:
            self.index.remove_many(invalid)
            self.index.vacuum()
            logging.info("Removed %d invalid index entries, new size: %d", len(invalid), len(self.index))

        self.index.optimize()
            

