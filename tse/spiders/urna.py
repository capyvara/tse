import json
import logging
import os
import signal

import scrapy

from tse.common.basespider import BaseSpider
from tse.common.pathinfo import PathInfo
from tse.parsers import SectionAuxParser, SectionsConfigParser


class UrnaSpider(BaseSpider):
    name = "urna"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdown = False
        self.sigHandler = None

    def handle_sigint(self, signum, frame):
        self.shutdown = True
        self.sigHandler(signum, frame)

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

    def continue_requests(self, config_response):
        # Allows us to stop in the middle of start_requests
        # TODO: Any way to control consuptiom of the generator?
        self.sigHandler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handle_sigint)

        yield from self.query_sections_configs()

    def query_sections_configs(self):
        for state in self.states:
            if state == "br":
                continue

            if self.shutdown:
                break

            path = PathInfo.get_sections_config_path(self.plea, state)

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

    def query_sections(self, state, data):
        size = 0
        queued = 0

        for city, zone, section in SectionsConfigParser.expand_sections(data):
            if self.shutdown:
                break

            path = PathInfo.get_section_aux_path(self.plea, state, city, zone, section)
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

    def download_ballot_files(self, state, city, zone, section, data):
        for hash, filename in SectionAuxParser.expand_files(data):
            if self.ignore_pattern and self.ignore_pattern.match(filename):
                continue
 
            path = PathInfo.get_ballot_file_path(self.plea, state, city, zone, section, hash, filename)

            if not os.path.exists(self.get_local_path(path)):
                logging.debug(f"Queueing ballot file {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_ballot_file, errback=self.errback_ballot_file,
                    dont_filter=True, priority=1, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

    def parse_ballot_file(self, response, state, city, zone, section):
        self.persist_response(response)

    def errback_ballot_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
