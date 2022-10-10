import os
import re
import scrapy
import logging
import datetime
import filecmp
import urllib.parse

class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._version_path_cache = {}

    def get_local_path(self, path, no_cycle=False):
        if no_cycle:
            return os.path.join(self.settings["FILES_STORE"], self.environment, path)

        return os.path.join(self.settings["FILES_STORE"], self.environment, self.cycle, path)

    def get_full_url(self, path, no_cycle=False):
        if no_cycle:
            return os.path.join(f"{self.host}/{self.environment}", path)

        return os.path.join(f"{self.host}/{self.environment}/{self.cycle}", path)

    def _get_next_version_path(self, path):
        dirname, basename = os.path.split(path)
        ver_dir = os.path.join(dirname, ".ver")
        ver_path = os.path.join(ver_dir, basename)

        filename, ext = os.path.splitext(basename)
        version = self._version_path_cache.get(path, 1)

        while True:
            ver_path = os.path.join(ver_dir, f"{filename}_{version:04}{ext}")
            if not os.path.exists(ver_path):
                break

            self._version_path_cache[path] = version
            version += 1
    
        return ver_path

    def persist_response(self, response, filedate=None):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        target_dir = os.path.dirname(target_path)
        os.makedirs(target_dir, exist_ok=True)

        if self.keep_old_versions:
            tmp_path = os.path.join(target_dir, f".tmp_{os.path.basename(target_path)}")
            try:
                with open(tmp_path, "wb") as f:
                    f.write(response.body)

                if os.path.exists(target_path):
                    if not filecmp.cmp(tmp_path, target_path, shallow=False):
                        os.renames(target_path, self._get_next_version_path(target_path))
                    else:
                        os.remove(target_path)

                os.renames(tmp_path, target_path)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
        else:
            with open(target_dir, "wb") as f:
                f.write(response.body)

        if filedate:
            dt_epoch = filedate.timestamp()
            os.utime(target_path, (dt_epoch, dt_epoch))

    def load_settings(self):
        self.host = self.settings["HOST"]
        self.environment = self.settings["ENVIRONMENT"]
        self.cycle = self.settings["CYCLE"]
        self.plea = self.settings["PLEA"]
        self.elections = self.settings["ELECTIONS"]
        self.states = self.settings["STATES"].lower().split()
        self.ignore_pattern = re.compile(self.settings["IGNORE_PATTERN"]) if self.settings["IGNORE_PATTERN"] else None
        self.keep_old_versions = self.settings["KEEP_OLD_VERSIONS"]

        logging.info(f"Host: {self.host}")
        logging.info(f"Environment: {self.environment}")
        logging.info(f"Cycle: {self.cycle}")
        logging.info(f"Plea: {self.plea}")
        logging.info(f"Elections: {self.elections}")
        logging.info(f"States: {self.settings['STATES']}")
