import os
import json
import datetime
import scrapy
import logging
import urllib.parse

from divulgacao.common.fileinfo import FileInfo
from divulgacao.common.basespider import BaseSpider

class UrnaSpider(BaseSpider):
    name = "urna"

    def start_requests(self):
        self.load_settings()
        yield from self.query_sections_configs()

    def query_sections_configs(self):
        for state in self.states:
            if state == "br":
                continue

            filename = f"{state}-p{self.plea:0>6}-cs.json"
            info = FileInfo(filename)

            try:
                with open(self.get_local_path(info.path), "r") as f:
                    logging.info(f"Reading sections config file {filename}")
                    data = json.load(f)
                    yield from self.query_sections(state, data)
            except (FileNotFoundError, json.JSONDecodeError):
                logging.info(f"Queueing sections config file {filename}")
                yield scrapy.Request(self.get_full_url(info.path), self.parse_section_config, errback=self.errback_section_config,
                    dont_filter=True, priority=3, cb_kwargs={"state": state})

    def parse_section_config(self, response, state):
        self.persist_response(response)
        data = json.loads(response.body)
        yield from self.query_sections(state, data)

    def errback_section_config(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def expand_sections(self, data):
        for mu in data["abr"][0]["mu"]:
            city = mu["cd"].lstrip("0")
            for zon in mu["zon"]:
                zone = zon["cd"].lstrip("0")
                for sec in zon["sec"]:
                    yield (city, zone, sec["ns"].lstrip("0"))

    def query_sections(self, state, data):        
        for city, zone, section in self.expand_sections(data):
            filename = f"p{self.plea:0>6}-{state}-m{city:0>5}-z{zone:0>4}-s{section:0>4}-aux.json"

            info = FileInfo(filename)
            try:
                with open(self.get_local_path(info.path), "r") as f:
                    logging.debug(f"Reading section file {filename}")
                    data = json.load(f)
                    yield from self.download_ballot_files(state, city, zone, section, data)
            except (FileNotFoundError, json.JSONDecodeError):
                logging.debug(f"Queueing section file {filename}")
                yield scrapy.Request(self.get_full_url(info.path), self.parse_section, errback=self.errback_section,
                    dont_filter=True, priority=2, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

    def parse_section(self, response, state, city, zone, section):
        self.persist_response(response)
        data = json.loads(response.body)
        yield from self.download_ballot_files(state, city, zone, section, data)

    def errback_section(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def expand_files(self, data):
        for hash in data["hashes"]:
            for filename in hash["nmarq"]:
                yield (hash["hash"], filename)

    def download_ballot_files(self, state, city, zone, section, data):
        base_path = f"arquivo-urna/{self.plea}/dados/{state}/{city:0>5}/{zone:0>4}/{section:0>4}"
        for hash, filename in self.expand_files(data):
            path = f"{base_path}/{hash}/{filename}"

            if not os.path.exists(path) or data["st"] != "Totalizada":
                logging.debug(f"Queueing ballot file {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_ballot_file, errback=self.errback_ballot_file,
                    dont_filter=True, priority=1, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

    def parse_ballot_file(self, response, state, city, zone, section):
        self.persist_response(response)

    def errback_ballot_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")