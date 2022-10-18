import datetime
import json
import logging
import os
import sqlite3

from tse.common.pathinfo import PathInfo
from tse.parsers import IndexParser


class Index():
    def __init__(self, persist_path=None):
        self.con = sqlite3.connect(persist_path if persist_path else ":memory:", 
            detect_types=sqlite3.PARSE_DECLTYPES)

        with self.con:
            self.con.execute((
                "CREATE TABLE IF NOT EXISTS file_dates ("
                "  filename TEXT PRIMARY KEY,"
                "  filedate TIMESTAMP"
                ") WITHOUT ROWID"
            ))

        if persist_path:
            logging.info(f"Index persist path: {persist_path}")

    def close(self):
        if self.con:
            self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

    def __getitem__(self, key):
        row = self.con.execute("SELECT filedate FROM file_dates WHERE filename=:filename", {"filename": key}).fetchone()
        if not row:
            raise KeyError(key)
        
        return row[0]

    def __len__(self):
        row = self.con.execute("SELECT COUNT(*) FROM file_dates").fetchone()
        return row[0]

    def __setitem__(self, key, value):
        with self.con:
            self.con.execute(("INSERT INTO file_dates VALUES (:filename, :filedate) "
                "ON CONFLICT(filename) DO UPDATE SET filedate=:filedate WHERE filename=:filename"), 
                    {"filename": key , "filedate": value})

    def __contains__(self, key):
        row = self.con.execute("SELECT COUNT(*) FROM file_dates WHERE filename=:filename", {"filename": key}).fetchone()
        return row and row[0] != 0

    def _upsert_from_iterator(self, iterator):
        with self.con:
            data = ({ "filename": k, "filedate": v } for k, v in iterator)
            self.con.executemany(("INSERT INTO file_dates VALUES (:filename, :filedate)"
                "ON CONFLICT(filename) DO UPDATE SET filedate=:filedate WHERE filename=:filename"), data)

    def _delete_from_iterator(self, iterator):
        with self.con:
            data = ({ "filename": k } for k in iterator)
            self.con.executemany("DELETE FROM file_dates WHERE filename=:filename", data)

    def items(self):
        for row in self.con.execute("SELECT filename, filedate FROM file_dates"):
            yield row

    def add(self, key, value):
        self[key] = value

    def get(self, key, default = None):
        try:
            return self[key]
        except KeyError:
            return default

    def discard(self, key):
        with self.con:
            self.con.execute("DELETE FROM file_dates WHERE filename=:filename", {"filename": key})

    def load_json(self, path):
        with open(path, "r") as f:
            self._upsert_from_iterator(json.load(f, object_hook=self._json_parse).items())

        logging.info(f"Loaded index from {path}")

    def append_state(self, state, path):
        with open(path, "r") as f:
            data = json.load(f)
            self._upsert_from_iterator([(i.filename, d) for i, d in IndexParser.expand(state, data)])

        logging.info(f"Appended index from: {path}")

    def validate(self, base_path):
        logging.info(f"Validating index...")

        invalid = list([k for k, v in self.items() if not self._validate_entry(base_path, k, v)])
        if len(invalid) > 0:
            self._delete_from_iterator(invalid)
            logging.info(f"Removed {len(invalid)} invalid index entries")

    def _validate_entry(self, base_path, filename, filedate):
        info = PathInfo(filename)
        target_path = os.path.join(base_path, info.path)

        if not os.path.exists(target_path):
            logging.debug(f"Target path not found, skipping index {info.filename}")
            return False

        modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(target_path))
        if filedate != modified_time:
            logging.debug(f"Index date mismatch, skipping index {info.filename} {modified_time} > {filedate}")
            return False

        return True
    
    @staticmethod
    def _json_serialize(obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.strftime("%d/%m/%Y %H:%M:%S")

        raise TypeError("Type %s not serializable" % type(obj))            

    @staticmethod
    def _json_parse(json_dict):
        for (key, value) in json_dict.items():
            try:
                json_dict[key] = datetime.datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
            except:
                pass
        return json_dict

    def save_json(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(dict(self.items()), f, default=self._json_serialize, check_circular=False)
