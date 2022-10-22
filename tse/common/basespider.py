import datetime
import json
import logging
import os
import re
import time
import urllib.parse
import zipfile
from email.utils import parsedate_to_datetime, format_datetime

import scrapy
from twisted.web.client import ResponseFailed
from scrapy.utils.python import to_unicode

from tse.common.index import Index
from tse.common.pathinfo import PathInfo


class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._version_path_cache = {}

    def continue_requests(self, config_data, config_entry):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')

    def start_requests(self):
        self.initialize()
        yield from self.query_common()

    def query_common(self):
        yield self.make_request(PathInfo.get_election_config_path(), self.parse_config)

    def load_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def parse_config(self, response):
        result = self.persist_response(response)
        config_data = json.loads(result.body)
        yield from self.continue_requests(config_data)

    def get_local_path(self, path):
        return PathInfo.get_local_path(self.settings, path)

    def get_full_url(self, path):
        return PathInfo.get_full_url(self.settings, path)

    def _scan_version_directory(self, ver_dir):
        with os.scandir(ver_dir) as it:
            for entry in it:
                if not entry.is_file() or entry.name.startswith('.'): 
                    continue

                if os.path.splitext(entry.name)[1] == ".zip":
                    with zipfile.ZipFile(entry.path, "r") as zip:
                        for info in zip.infolist():
                            if not info.is_dir() and not "/" in info.filename:
                                yield (info.filename, time.mktime(info.date_time + (0, 0, -1)))
                    continue

                yield (entry.name, entry.stat().st_mtime)

    def _get_version_path_cache(self, dirname):
        if dirname in self._version_path_cache:
            return self._version_path_cache[dirname]

        cache = {}

        ver_dir = os.path.join(dirname, ".ver")
        if not os.path.exists(ver_dir):
            self._version_path_cache[dirname] = cache
            return cache

        for entry, mtime in self._scan_version_directory(ver_dir):            
            entry_root, entry_ext = os.path.splitext(entry)

            try:
                idx = entry_root.rindex("_")
                entry_version = int(entry_root[idx + 1:])
                filename = f"{entry_root[:idx]}{entry_ext}"
                max_version = max(entry_version, cache.get(filename, 0))
                cache[filename] = max_version

                filedate = datetime.datetime.fromtimestamp(mtime)
                self.index.ensure_version_exists(filename, entry_version, filedate)
            except ValueError:
                logging.debug("Error: skipping version from filename: %s", entry)
                continue

        self._version_path_cache[dirname] = cache
        return cache

    def update_current_version(self, path):
        dirname, filename = os.path.split(path)
        cache = self._get_version_path_cache(dirname)

        ver_dir = os.path.join(dirname, ".ver")
        ver_path = os.path.join(ver_dir, filename)

        root, ext = os.path.splitext(filename)
        version = cache.get(filename, 1)

        while True:
            ver_path = os.path.join(ver_dir, f"{root}_{version:04}{ext}")
            if not os.path.exists(ver_path):
                break

            version += 1
    
        cache[filename] = version
        self.index.set_current_version(filename, version)
        return (version, ver_path)

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

    def update_file_timestamp(self, target_path, filedate):
        dt_epoch = filedate.timestamp()
        if os.path.getmtime(target_path) != dt_epoch:
            os.utime(target_path, (dt_epoch, dt_epoch))
            
        filename = os.path.basename(target_path)            
        old_entry = self.index.get(filename)
        if old_entry.index_date != filedate:
            self.index[filename] = Index.Entry(filedate, old_entry.last_modified, old_entry.etag)

    def make_request(self, path: str, *args, **kwargs):
        self.update_current_version(self.get_local_path(path))

        entry = self.index.get(os.path.basename(path))
        if entry.has_http_cache and os.path.exists(self.get_local_path(path)):
            headers = kwargs.setdefault("headers", {})

            if entry.last_modified != None:
                headers[b"If-Modified-Since"] = format_datetime(entry.last_modified.replace(tzinfo=datetime.timezone.utc), True)
            if entry.etag != None:
                headers[b"ETag"] = f'"{entry.etag}"'

            meta = kwargs.setdefault("meta", {})
            meta.update({"handle_httpstatus_list": [304]})

        kwargs.setdefault("dont_filter", True)
        return scrapy.Request(self.get_full_url(path), *args, **kwargs)

    class PersistedResult:
        def __init__(self, target_path, index_entry, body = None):
            self.target_path = target_path
            self.index_entry = index_entry
            self._body = body

        @property
        def body(self) -> str:
            if self._body: 
                return self._body

            with open(self.target_path, "r") as f:
                return f.read()

    def persist_response(self, response, index_date=None, check_identical = True) -> PersistedResult:
        local_path = os.path.join(self.settings["FILES_STORE"], urllib.parse.urlparse(response.url).path.strip("/"))
        filename = os.path.basename(local_path)

        _, current_version_path = self.update_current_version(local_path)

        last_modified, etag = self.get_http_cache_headers(response)
        index_entry = self.index.get(filename)

        if response.status == 304:
            if index_entry.last_modified == last_modified and index_entry.etag == etag and os.path.exists(local_path):
                return self.PersistedResult(local_path, index_entry)
            else:
                # TODO invalidate/retry
                # os.remove(target_path)
                raise ResponseFailed(response)

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if self.keep_old_versions and os.path.exists(local_path):
            os.makedirs(os.path.dirname(current_version_path), exist_ok=True)
            os.rename(local_path, current_version_path)
            self.update_current_version(local_path)

        with open(local_path, "wb") as f:
            f.write(response.body)

        index_entry = Index.Entry(index_date, last_modified, etag)
        self.index[filename] = index_entry
            
        # TODO: Set to last modified?
        if index_date:
            dt_epoch = index_date.timestamp()
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
