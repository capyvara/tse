import datetime
import json
import logging
import os
import re
import time
import urllib.parse
import zipfile
from email.utils import parsedate_to_datetime, format_datetime
from pyparsing import NamedTuple

import scrapy
from twisted.web.client import ResponseFailed
from scrapy.utils.python import to_unicode

from tse.common.index import Index
from tse.common.pathinfo import PathInfo


class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def continue_requests(self, config_data, config_entry):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')

    def start_requests(self):
        self.initialize()
        yield from self.query_common()

    def query_common(self):
        yield self.make_request(PathInfo.get_election_config_path(), self.parse_config)

    def parse_config(self, response):
        result = self.persist_response(response)
        config_data = json.loads(result.contents)
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
        last_modified = self._rfc2822_to_datetime(response.headers[b"Last-Modified"])
        etag = to_unicode(response.headers[b"etag"]).strip('"')
        return (last_modified, etag)

    def make_request(self, path: str, *args, **kwargs):
        entry = self.index.get(os.path.basename(path))
        if entry and os.path.exists(self.get_local_path(path)):
            headers = kwargs.setdefault("headers", {})

            if entry.last_modified != None:
                headers[b"If-Modified-Since"] = format_datetime(entry.last_modified.replace(tzinfo=datetime.timezone.utc), True)
            if entry.etag != None:
                headers[b"ETag"] = f'"{entry.etag}"'

            meta = kwargs.setdefault("meta", {})
            meta.update({"handle_httpstatus_list": [304]})

        kwargs.setdefault("dont_filter", True)
        return scrapy.Request(self.get_full_url(path), *args, **kwargs)

    class PersistedResult(NamedTuple):
        local_path: str
        index_entry: Index.Entry
        body: str = None

        @property
        def contents(self) -> str:
            if self.body: 
                return self.body

            with open(self.local_path, "r") as f:
                return f.read()

        @property
        def is_new_file(self) -> str:
            return self.body is not None

    def persist_response(self, response, index_date = None) -> PersistedResult:
        local_path = os.path.join(self.settings["FILES_STORE"], urllib.parse.urlparse(response.url).path.strip("/"))
        filename = os.path.basename(local_path)

        last_modified, etag = self.get_http_cache_headers(response)
        index_entry = self.index.get(filename)

        if response.status == 304:
            if (index_entry and 
                    (index_entry.last_modified == last_modified) and 
                    (index_entry.etag == etag) and 
                    os.path.exists(local_path)):
                return self.PersistedResult(local_path, index_entry)
            else:
                # TODO invalidate/retry
                raise ResponseFailed(response)

        index_entry = Index.Entry(last_modified, etag, index_date)

        index_version = self.archive_version(filename) if self.keep_old_versions else 0
        if index_version != 0:
            self.index.add_version(filename, index_version + 1, index_entry)
        else:
            self.index[filename] = index_entry

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(response.body)

        dt_epoch = last_modified.timestamp()
        os.utime(local_path, (dt_epoch, dt_epoch))

        return self.PersistedResult(local_path, index_entry, response.body)

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
        logging.info("Host: %s", self.settings["HOST"])
        logging.info("Environment: %s", self.settings["ENVIRONMENT"])
        logging.info("Cycle: %s", self.settings["CYCLE"])
        
        logging.info("Plea: %s", self.plea)
        logging.info("Elections: %s", self.elections)
        logging.info("States: %s", self.states)

        db_dir = os.path.join(self.settings["FILES_STORE"], self.settings["ENVIRONMENT"])
        os.makedirs(db_dir, exist_ok=True)
        self.index = Index(os.path.join(db_dir, f"index_{self.name}.db"))

        self.validate_index()

        logging.info("Index size %d", len(self.index))

    def closed(self, reason):
        self.index.close()

    def validate_index_entry(self, filename, entry: Index.Entry):
        info = PathInfo(filename)
        if not info.path:
            return True

        local_path = self.get_local_path(info.path)
        if not os.path.exists(local_path):
            logging.debug("Local path not found, skipping index %s", info.filename)
            return False

        modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(local_path))
        if entry.last_modified != modified_time:
            logging.debug("Modified date mismatch, skipping index %s %s > %s", info.filename, modified_time, entry.index_date)
            return False

        return True

    def validate_index(self):
        logging.info("Validating index...")

        invalid = [f for f, e in self.index.items() if not self.validate_index_entry(f, e)]
        if len(invalid) > 0:
            self.index.remove_many(invalid)
            logging.info("Removed %d invalid index entries", len(invalid))

