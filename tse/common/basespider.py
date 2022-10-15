import filecmp
import logging
import os
import re
import urllib.parse
import zipfile

import scrapy


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
                        os.renames(target_path, self._get_next_version_path(target_path))

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
