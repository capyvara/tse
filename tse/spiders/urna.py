import json
import logging
import os
import signal

from scrapy.spidermiddlewares.httperror import HttpError

from tse.common.basespider import BaseSpider
from tse.common.pathinfo import PathInfo
from tse.parsers import (SectionAuxParser, SectionsConfigParser)


class UrnaSpider(BaseSpider):
    name = "urna"

    custom_settings = {
        "EXTENSIONS": {
            'tse.extensions.LogStatsUrna': 543,
        }
    }

    # Priorities (higher to lower)
    # 4 - Section configs
    # 3 - Section configs.sig
    # 2 - Aux files
    # 1 - Ballot files

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shutdown = False
        self.sigHandler = None

    def handle_sigint(self, signum, frame):
        self.shutdown = True
        self.sigHandler(signum, frame)

    def load_json(self, path):
        with open(path, "r") as f:
            return json.load(f)

    def query_sigfile(self, source_path, force=False):
        sig_path = os.path.splitext(source_path)[0] + ".sig"
        sig_local_path = self.get_local_path(sig_path)
        
        if force or not os.path.exists(sig_local_path):
            return self.make_request(sig_path, self.parse_sigfile, errback=self.errback_sigfile,
                priority=3, cb_kwargs={"source_path": source_path})

        return None

    def parse_sigfile(self, response, source_path):
        self.persist_response(response, check_identical=True)

    def errback_sigfile(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def continue_requests(self, config_data):
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

            try:
                local_path = self.get_local_path(path)
                config_data = self.load_json(local_path)
                                
                sig_query = self.query_sigfile(path)
                if sig_query:
                    yield sig_query

                yield from self.query_sections(state, config_data)                
            except (FileNotFoundError, json.JSONDecodeError):
                logging.info("Scheduling sections config file for %s %s", self.plea, state)
                yield self.make_request(path, self.parse_section_config, errback=self.errback_section_config,
                    priority=4, cb_kwargs={"state": state})

                sig_query = self.query_sigfile(path)
                if sig_query:
                    yield sig_query

    def parse_section_config(self, response, state):
        result = self.persist_response(response)
        yield from self.query_sections(state, json.loads(result.contents))

    def errback_section_config(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def query_sections(self, state, data):
        logging.info("Processing sections config file for %s %s", self.plea, state)

        for city, zone, section in SectionsConfigParser.expand_sections(data):
            if self.shutdown:
                break

            path = PathInfo.get_section_aux_path(self.plea, state, city, zone, section)
            self.crawler.stats.inc_value("urna/sections")

            try:
                local_path = self.get_local_path(path)
                aux_data = self.load_json(local_path)

                yield from self.download_ballot_box_files(state, city, zone, section, aux_data)
            except (FileNotFoundError, json.JSONDecodeError):
                yield self.make_request(path, self.parse_section, errback=self.errback_section,
                    priority=2, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

    def parse_section(self, response, state, city, zone, section):
        result = self.persist_response(response)
        yield from self.download_ballot_box_files(state, city, zone, section, json.loads(result.contents))

    def errback_section(self, failure):
        if failure.check(HttpError) and failure.value.response.status == 403:
            logging.debug("Section config not found %s", str(failure.request))
            return

        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))

    def download_ballot_box_files(self, state, city, zone, section, data):
        self.crawler.stats.inc_value("urna/processed_sections")

        hash, _, filenames = SectionAuxParser.get_files(data)
        if hash == None:
            return

        for filename in filenames:
            if self.ignore_pattern and self.ignore_pattern.match(filename):
                continue

            self.crawler.stats.inc_value("urna/ballot_box_files")

            path = PathInfo.get_ballot_box_file_path(self.plea, state, city, zone, section, hash, filename)
            local_path = self.get_local_path(path)

            if not os.path.exists(local_path):
                logging.debug("Scheduling ballot box file %s", filename)
                yield self.make_request(path, self.parse_ballot_box_file, errback=self.errback_ballot_box_file,
                    priority=1, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})
            else:
                self.crawler.stats.inc_value("urna/processed_ballot_box_files")

    def parse_ballot_box_file(self, response, state, city, zone, section):
        self.persist_response(response)
        self.crawler.stats.inc_value("urna/processed_ballot_box_files")

    def errback_ballot_box_file(self, failure):
        logging.error("Failure downloading %s - %s", str(failure.request), str(failure.value))
