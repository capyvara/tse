import os
import re
import scrapy
import logging
import datetime
import filecmp
import urllib.parse

class BaseSpider(scrapy.Spider):
    name = "base"

    def get_local_path(self, path, no_cycle=False):
        if no_cycle:
            return os.path.join(self.settings["FILES_STORE"], self.environment, path)

        return os.path.join(self.settings["FILES_STORE"], self.environment, self.cycle, path)

    def get_full_url(self, path, no_cycle=False):
        if no_cycle:
            return os.path.join(f"{self.host}/{self.environment}", path)

        return os.path.join(f"{self.host}/{self.environment}/{self.cycle}", path)

    _path_ver_regex = re.compile(r"(?P<base_path>.*?)(?:_(?P<ver>\d{1,4}})?)?\.(?P<ext>\w+)")

    def _get_unused_path(self, path):
        while os.path.exists(path):
            result = BaseSpider._path_ver_regex.match(path)
            if not result:
                raise ValueError("Unrecognized path format")

            version = int(result["ver"]) if result["ver"] != None else 0
            version += 1
            path = f"{result['base_path']}_{version:04}.{result['ext']}"

        return path

    def persist_response(self, response, filedate=None):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        backup_path = None
        
        if os.path.exists(target_path) and self.keep_old_versions:
            backup_path = self._get_unused_path(target_path)
            os.rename(target_path, backup_path)
            
        with open(target_path, "wb") as f:
            f.write(response.body)

        if filedate:
            dt_epoch = filedate.timestamp()
            os.utime(target_path, (dt_epoch, dt_epoch))

        if backup_path:
            if filecmp.cmp(target_path, backup_path, shallow=False):
                os.unlink(backup_path)

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
