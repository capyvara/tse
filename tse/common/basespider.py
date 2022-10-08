import os
import re
import scrapy
import logging
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

    def persist_response(self, response, filedate=None):
        url_path = os.path.relpath(urllib.parse.urlparse(response.url).path, "/")
        target_path = os.path.join(self.settings["FILES_STORE"], url_path)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
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

        logging.info(f"Host: {self.host}")
        logging.info(f"Environment: {self.environment}")
        logging.info(f"Cycle: {self.cycle}")
        logging.info(f"Plea: {self.plea}")
        logging.info(f"Elections: {self.elections}")
        logging.info(f"States: {self.settings['STATES']}")
