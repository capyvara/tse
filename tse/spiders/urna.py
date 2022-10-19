import datetime
import json
import logging
import os
import signal

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

from tse.common.basespider import BaseSpider
from tse.common.pathinfo import PathInfo
from tse.parsers import (SectionAuxParser, SectionsConfigParser,
                         get_dh_timestamp)


class UrnaSpider(BaseSpider):
    name = "urna"

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

    def query_sigfile(self, source_path, force=False):
        sig_path = os.path.splitext(source_path)[0] + ".sig"
        sig_local_path = self.get_local_path(sig_path)
        
        if force or not os.path.exists(sig_local_path):
            return scrapy.Request(self.get_full_url(sig_path), self.parse_sigfile, errback=self.errback_sigfile,
                dont_filter=True, priority=3, cb_kwargs={"source_path": source_path})
        else:
            self.match_sigfile_filedate(self.get_local_path(source_path))

        return None

    def match_sigfile_filedate(self, source_local_path):
        sig_local_path = os.path.splitext(source_local_path)[0] + ".sig"
        if os.path.exists(source_local_path) and os.path.exists(sig_local_path):
            source_filedate = datetime.datetime.fromtimestamp(os.path.getmtime(source_local_path))
            self.update_file_timestamp(sig_local_path, source_filedate)

    def parse_sigfile(self, response, source_path):
        self.persist_response(response, check_identical=True)
        self.match_sigfile_filedate(self.get_local_path(source_path))

    def errback_sigfile(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def load_index(self):
        logging.info(f"Index size {len(self.index)}")

    def continue_requests(self, config_data):
        # Allows us to stop in the middle of start_requests
        # TODO: Any way to control consuptiom of the generator?
        self.sigHandler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handle_sigint)

        self.load_index()

        yield from self.query_sections_configs()

    def query_sections_configs(self):
        for state in self.states:
            if state == "br":
                continue

            if self.shutdown:
                break

            path = PathInfo.get_sections_config_path(self.plea, state)

            try:
                logging.info(f"Reading sections config file for {self.plea} {state}")
                local_path = self.get_local_path(path)
                config_data = self.load_json(local_path)
                
                filedate = get_dh_timestamp(config_data, "dg", "hg")
                self.update_file_timestamp(local_path, filedate)
                
                sig_query = self.query_sigfile(path)
                if sig_query:
                    yield sig_query

                yield from self.query_sections(state, config_data)                
            except (FileNotFoundError, json.JSONDecodeError):
                logging.info(f"Queueing sections config file for {self.plea} {state}")
                yield scrapy.Request(self.get_full_url(path), self.parse_section_config, errback=self.errback_section_config,
                    dont_filter=True, priority=4, cb_kwargs={"state": state})

                sig_query = self.query_sigfile(path)
                if sig_query:
                    yield sig_query

    def parse_section_config(self, response, state):
        data = json.loads(response.body)
        filedate = get_dh_timestamp(data, "dg", "hg")
        persisted_path = self.persist_response(response, filedate, check_identical=True)
        self.match_sigfile_filedate(persisted_path)

        yield from self.query_sections(state, data)

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
                logging.debug(f"Reading section file {filename}")
                local_path = self.get_local_path(path)
                aux_data = self.load_json(local_path)

                filedate = get_dh_timestamp(aux_data, "dg", "hg")
                self.update_file_timestamp(local_path, filedate)

                if aux_data["st"] in ("Não instalada"):
                    continue

                yield from self.download_ballot_box_files(state, city, zone, section, aux_data)
            except (FileNotFoundError, json.JSONDecodeError):
                logging.debug(f"Queueing section file {filename}")
                queued += 1
                yield scrapy.Request(self.get_full_url(path), self.parse_section, errback=self.errback_section,
                    dont_filter=True, priority=2, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section})

        logging.info(f"Queued {state} {queued} section files of {size}")

    def parse_section(self, response, state, city, zone, section):
        data = json.loads(response.body)
        filedate = get_dh_timestamp(data, "dg", "hg")
        self.persist_response(response, filedate, check_identical=True)

        yield from self.download_ballot_box_files(state, city, zone, section, data)

    def errback_section(self, failure):
        if failure.check(HttpError) and failure.value.response.status == 403:
            logging.debug(f"Section config not found {str(failure.request)}")
            return

        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")

    def download_ballot_box_files(self, state, city, zone, section, data):
        hash, hashdate, filenames = SectionAuxParser.get_files(data)
        if hash == None:
            return

        for filename in filenames:
            if self.ignore_pattern and self.ignore_pattern.match(filename):
                continue

            path = PathInfo.get_ballot_box_file_path(self.plea, state, city, zone, section, hash, filename)
            local_path = self.get_local_path(path)

            if not os.path.exists(local_path):
                logging.debug(f"Queueing ballot box file {filename}")
                yield scrapy.Request(self.get_full_url(path), self.parse_ballot_box_file, errback=self.errback_ballot_box_file,
                    dont_filter=True, priority=1, cb_kwargs={"state": state, "city": city, "zone": zone, "section": section, "hashdate": hashdate})
            else:
                self.update_file_timestamp(local_path, hashdate)

    def parse_ballot_box_file(self, response, state, city, zone, section, hashdate):
        self.persist_response(response, hashdate)

    def errback_ballot_box_file(self, failure):
        logging.error(f"Failure downloading {str(failure.request)} - {str(failure.value)}")
