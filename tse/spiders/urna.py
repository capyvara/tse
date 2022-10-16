import json
import logging
import os

import scrapy

from tse.common.basespider import BaseSpider
from tse.common.fileinfo import FileInfo


class UrnaSpider(BaseSpider):
    name = "urna"

    def query_sig(self, path, force=False):
        sig_path = os.path.splitext(path)[0] + ".sig"
        if force or not os.path.exists(self.get_local_path(sig_path)):
            return scrapy.Request(self.get_full_url(sig_path), self.parse_sig, errback=self.errback_sig,
                dont_filter=True, priority=3)

        return None

    def parse_sig(self, response):
        self.persist_response(response, check_identical=True)

    def errback_sig(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def start_requests(self):
        self.load_settings()
        yield from self.query_sections_configs()

    def query_sections_configs(self):
        for state in self.states:
            if state == "br":
                continue

            path = FileInfo.get_sections_config_path(self.plea, state)

            sig_query = self.query_sig(path)
            if sig_query:
                 yield sig_query

            try:
                with open(self.get_local_path(path), "r") as f:
                    logging.info(f"Reading sections config file for {self.plea} {state}")
                    yield from self.query_sections(state, json.load(f))
            except (FileNotFoundError, json.JSONDecodeError):
                logging.info(f"Queueing sections config file for {self.plea} {state}")
                yield scrapy.Request(self.get_full_url(path), self.parse_section_config, errback=self.errback_section_config,
                    dont_filter=True, priority=3, cb_kwargs={"state": state})

    def parse_section_config(self, response, state):
        self.persist_response(response, check_identical=True)
        yield from self.query_sections(state, json.loads(response.body))

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
        size = 0
        queued = 0

        for city, zone, section in self.expand_sections(data):
            path = FileInfo.get_section_aux_path(self.plea, state, city, zone, section)
            filename = os.path.basename(path)
            size += 1

            try:
                with open(self.get_local_path(path), "r") as f:
                    logging.debug(f"Reading section file {filename}")

                    aux_data = json.load(f)
                    if aux_data["st"] in ["Não instalada"]:
                        continue

                    if not aux_data["st"] in ["Totalizada", "Recebida", "Anulada"]:
                        raise ValueError("Section not totalled up yet")

                    yield from self.download_ballot_files(state, city, zone, section, aux_data)
            except (FileNotFoundError, ValueError, json.JSONDecodeError):
                logging.debug(f"Queueing section file {filename}")
                queued += 1
                yield scrapy.Request(self.get_full_url(path), self.parse_section, errback=self.errback_section,
                    dont_filter=True, priority=2, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

        logging.info(f"Queued {state} {queued} section files of {size}")

    def parse_section(self, response, state, city, zone, section):
        self.persist_response(response, check_identical=True)
        yield from self.download_ballot_files(state, city, zone, section, json.loads(response.body))

    def errback_section(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def expand_files(self, data):
        for hash in data["hashes"]:
            if not hash["st"] in ["Totalizado", "Recebido", "Excluído"]:
                continue

            for filename in hash["nmarq"]:
                yield (hash["hash"], filename)

    def download_ballot_files(self, state, city, zone, section, data):
        for hash, filename in self.expand_files(data):
            if self.ignore_pattern and self.ignore_pattern.match(filename):
                continue
 
            path = FileInfo.get_ballot_file_path(self.plea, state, city, zone, section, hash, filename)

            if not os.path.exists(self.get_local_path(path)):
                logging.debug(f"Queueing ballot file {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_ballot_file, errback=self.errback_ballot_file,
                    dont_filter=True, priority=1, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

    def parse_ballot_file(self, response, state, city, zone, section):
        self.persist_response(response)

    def errback_ballot_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
