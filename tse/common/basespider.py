import filecmp
import json
import logging
import os
import re
import urllib.parse
import zipfile
from email.utils import parsedate_to_datetime

import scrapy
from scrapy.utils.python import to_unicode

from tse.common.index import Index
from tse.common.pathinfo import PathInfo
from tse.parsers import get_dh_timestamp


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
        yield scrapy.Request(self.get_full_url(PathInfo.get_election_config_path(), no_cycle=True), self.parse_config, dont_filter=True)

    def load_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def parse_config(self, response):
        config_data = json.loads(response.body)
        config_date = get_dh_timestamp(config_data)
        self.persist_response(response, config_date, check_identical=True)
                
        yield from self.continue_requests(config_data)

    def get_local_path(self, path, no_cycle=False):
        return PathInfo.get_local_path(self.settings, path, no_cycle)

    def get_full_url(self, path, no_cycle=False):
        return PathInfo.get_full_url(self.settings, path, no_cycle)

    def _scan_version_directory(self, ver_dir):
        with os.scandir(ver_dir) as it:
            for entry in it:
                if not entry.is_file() or entry.name.startswith('.'): 
                    continue

                if os.path.splitext(entry.name)[1] == ".zip":
                    with zipfile.ZipFile(entry.path, "r") as zip:
                        for info in zip.infolist():
                            if not info.is_dir() and not "/" in info.filename:
                                yield info.filename
                    continue

                yield entry.name

    def _get_version_path_cache(self, dirname):
        if dirname in self._version_path_cache:
            return self._version_path_cache[dirname]

        cache = {}

        ver_dir = os.path.join(dirname, ".ver")
        if not os.path.exists(ver_dir):
            self._version_path_cache[dirname] = cache
            return cache

        for entry in self._scan_version_directory(ver_dir):            
            entry_root, entry_ext = os.path.splitext(entry)

            try:
                idx = entry_root.rindex("_")
                entry_version = int(entry_root[idx + 1:])
                filename = f"{entry_root[:idx]}{entry_ext}"
                max_version = cache.get(filename, 0)
                cache[filename] = max(entry_version, max_version)
            except ValueError:
                logging.debug(f"Error: skipping version from filename: {entry}")
                continue

        self._version_path_cache[dirname] = cache
        return cache

    def _get_next_version_path(self, path):
        dirname, basename = os.path.split(path)
        cache = self._get_version_path_cache(dirname)

        ver_dir = os.path.join(dirname, ".ver")
        ver_path = os.path.join(ver_dir, basename)

        root, ext = os.path.splitext(basename)
        version = cache.get(basename, 0)

        while True:
            version += 1

            ver_path = os.path.join(ver_dir, f"{root}_{version:04}{ext}")
            if not os.path.exists(ver_path):
                break
    
        cache[basename] = version
        return ver_path

    def _rfc1123_to_datetime(self, date_str):
        try:
            date_str = to_unicode(date_str, encoding='ascii')
            return parsedate_to_datetime(date_str).replace(tzinfo=None)
        except Exception:
            return None  
                  
    def get_http_cache_headers(self, response):
        last_modified = self._rfc1123_to_datetime(response.headers[b"Last-Modified"])
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

    def persist_response(self, response, filedate=None, check_identical=False):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        target_dir = os.path.dirname(target_path)
        os.makedirs(target_dir, exist_ok=True)

        # TODO: Select patterns to keep old versions

        if self.keep_old_versions:
            tmp_path = os.path.join(target_dir, f".tmp_{os.path.basename(target_path)}")
            try:
                with open(tmp_path, "wb") as f:
                    f.write(response.body)

                if os.path.exists(target_path):
                    if check_identical and filecmp.cmp(tmp_path, target_path, shallow=False):
                        os.remove(target_path)
                    else:
                        next_version_path = self._get_next_version_path(target_path)
                        os.makedirs(os.path.dirname(next_version_path), exist_ok=True)
                        os.rename(target_path, next_version_path)

                os.rename(tmp_path, target_path)
            except:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
        else:
            with open(target_dir, "wb") as f:
                f.write(response.body)

        last_modified, etag = self.get_http_cache_headers(response)
        self.index[os.path.basename(target_path)] = Index.Entry(filedate, last_modified, etag)
            
        if filedate:
            dt_epoch = filedate.timestamp()
            os.utime(target_path, (dt_epoch, dt_epoch))

        return target_path

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
        logging.info(f"Host: {self.settings['HOST']}")
        logging.info(f"Environment: {self.settings['ENVIRONMENT']}")
        logging.info(f"Cycle: {self.settings['CYCLE']}")
        
        logging.info(f"Plea: {self.plea}")
        logging.info(f"Elections: {self.elections}")
        logging.info(f"States: {self.states}")

        os.makedirs(self.get_local_path(""), exist_ok=True)
        self.index = Index(self.get_local_path(f"index_{self.name}.db", no_cycle=True))
