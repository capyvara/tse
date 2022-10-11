import os
import json
import datetime
import logging

from tse.common.fileinfo import FileInfo

class Index():
    def expand(state, data):
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")

            if filename == "ele-c.json":
                continue

            info = FileInfo(filename)
            if (info.prefix == "cert" or info.prefix == "mun") and state != "br":
                continue
            
            if info.state and state != info.state:
                continue

            yield info, filedate

    def __init__(self):
        self.index = dict()

    def __getitem__(self, key):
        return self.index[key]

    def __len__(self):
        return len(self.index)

    def __setitem__(self, key, value):
        self.index[key] = value

    def __contains__(self, key):
        return key in self.index

    def add(self, key, value):
        self.index[key] = value

    def get(self, key):
        return self.index.get(key)

    def discard(self, key):
        return self.index.pop(key, None)

    def load(self, path):
        with open(path, "r") as f:
            self.index = json.load(f, object_hook=self._json_parse)

        logging.info(f"Loaded index from {path}")

    def append_state(self, state, path):
        with open(path, "r") as f:
            data = json.load(f)
            for info, filedate in Index.expand(state, data):
                self.index[info.filename] = filedate

        logging.info(f"Appended index from: {path}")

    def validate(self, base_path):
        old_size = len(self.index)
        self.index = {k: v for k, v in self.index.items() if self._validate_entry(base_path, k, v)}
        if len(self.index) != old_size:
            logging.info(f"Removed {old_size - len(self.index)} invalid index entries")

    def _validate_entry(self, base_path, filename, filedate):
        info = FileInfo(filename)
        target_path = os.path.join(base_path, info.path)

        if not os.path.exists(target_path):
            logging.debug(f"Target path not found, skipping index {info.filename}")
            return False

        modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(target_path))
        if filedate != modified_time:
            logging.debug(f"Index date mismatch, skipping index {info.filename} {modified_time} > {filedate}")
            return False

        return True
    
    def _json_serialize(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.strftime("%d/%m/%Y %H:%M:%S")

        raise TypeError("Type %s not serializable" % type(obj))            

    def _json_parse(self, json_dict):
        for (key, value) in json_dict.items():
            try:
                json_dict[key] = datetime.datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
            except:
                pass
        return json_dict

    def save_json(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.index, f, default=self._json_serialize, check_circular=False)
