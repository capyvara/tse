import datetime
import logging
import sqlite3
from collections.abc import Iterable
from typing import NamedTuple, Tuple


class Index():
    class Entry(NamedTuple):
        index_date: datetime.datetime = None
        last_modified: datetime.datetime = None
        etag: bytes = None

    def __init__(self, persist_path=None):
        self.con = sqlite3.connect(persist_path if persist_path else ":memory:", 
            detect_types=sqlite3.PARSE_DECLTYPES)

        with self.con:
            self.con.execute((
                "CREATE TABLE IF NOT EXISTS file_entries ("
                "  filename TEXT PRIMARY KEY,"
                "  index_date TIMESTAMP,"
                "  last_modified TIMESTAMP,"
                "  etag BLOB"
                ") WITHOUT ROWID"
            ))

            self.con.execute("PRAGMA synchronous = OFF")
            self.con.execute("PRAGMA journal_mode = TRUNCATE")

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

    def __getitem__(self, filename: str) -> Entry:
        row = self.con.execute("SELECT index_date, last_modified, etag FROM file_entries WHERE filename=:filename", {"filename": filename}).fetchone()
        if not row:
            raise KeyError(filename)
        
        return Index.Entry(row[0], row[1], row[2])      

    def __len__(self):
        row = self.con.execute("SELECT COUNT(*) FROM file_entries").fetchone()
        return row[0]

    def __setitem__(self, filename: str, entry: Entry):
        with self.con:
            self.con.execute(("INSERT INTO file_entries VALUES (:filename, :index_date, :last_modified, :etag) "
                "ON CONFLICT(filename) DO UPDATE SET index_date=:index_date, last_modified=:last_modified, etag=:etag WHERE filename=:filename"), 
                    {"filename": filename, "index_date": entry.index_date, "last_modified": entry.last_modified, "etag": entry.etag})

    def __contains__(self, filename: str):
        row = self.con.execute("SELECT COUNT(*) FROM file_entries WHERE filename=:filename", {"filename": filename}).fetchone()
        return row and row[0] != 0

    def items(self) -> Tuple[str, Entry]: 
        for row in self.con.execute("SELECT filename, index_date, last_modified, etag FROM file_entries"):
            yield (row[0], Index.Entry(row[1], row[2], row[3]))

    def add(self, filename: str, entry: Entry):
        self[filename] = entry

    def get(self, filename: str, default = Entry()) -> Entry:
        try:
            return self[filename]
        except KeyError:
            return default

    def discard(self, filename: str):
        with self.con:
            self.con.execute("DELETE FROM file_entries WHERE filename=:filename", {"filename": filename})

    def add_many(self, iterable: Iterable[tuple[str,Entry]]):
        with self.con:
            data = ({"filename": f, "index_date": e.index_date, "last_modified": e.last_modified, "etag": e.etag} for f, e in iterable)
            self.con.executemany(("INSERT INTO file_entries VALUES (:filename, :index_date, :last_modified, :etag) "
                "ON CONFLICT(filename) DO UPDATE SET index_date=:index_date, last_modified=:last_modified, etag=:etag WHERE filename=:filename"), data)

    def remove_many(self, iterable: Iterable[str]):
        with self.con:
            data = ({ "filename": f } for f in iterable)
            self.con.executemany("DELETE FROM file_entries WHERE filename=:filename", data)
