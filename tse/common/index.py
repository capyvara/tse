import datetime
import logging
import sqlite3
from collections.abc import Iterable
from typing import NamedTuple, Tuple


class Index():
    class Entry(NamedTuple):
        last_modified: datetime.datetime
        etag: str
        index_date: datetime.datetime = None
        metadata: bytes = None

    def __init__(self, persist_path=None):
        self.con = sqlite3.connect(persist_path if persist_path else ":memory:", 
            detect_types=sqlite3.PARSE_DECLTYPES)

        with self.con:
            self.con.execute("PRAGMA foreign_keys = ON")
            self.con.execute("PRAGMA synchronous = OFF")
            self.con.execute("PRAGMA journal_mode = TRUNCATE")

            self.con.execute((
                "CREATE TABLE IF NOT EXISTS file_versions ("
                "  filename TEXT,"
                "  version INTEGER,"
                "  last_modified TIMESTAMP NOT NULL,"
                "  etag TEXT NOT NULL,"
                "  index_date TIMESTAMP,"
                "  metadata BLOB,"
                "  PRIMARY KEY(filename,version)"
                ") WITHOUT ROWID"
            ))

            self.con.execute((
                "CREATE TABLE IF NOT EXISTS file_entries ("
                "  filename TEXT PRIMARY KEY,"
                "  version INTEGER,"
                "  FOREIGN KEY(filename,version) REFERENCES file_versions(filename,version)"
                ") WITHOUT ROWID"
            ))

            self.con.execute((
                "CREATE VIEW IF NOT EXISTS file_items AS"
                " SELECT file_entries.filename, file_entries.version, last_modified, etag, index_date, metadata"
                " FROM file_entries NATURAL LEFT JOIN file_versions;"
            ))

        if persist_path:
            logging.info("Index persist path: %s",  persist_path)

    def close(self):
        if self.con:
            with self.con:
                self.con.execute("PRAGMA journal_mode = DELETE")
                
            self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return

    def __getitem__(self, filename: str) -> Entry:
        row = self.con.execute((
            "SELECT last_modified, etag, index_date, metadata FROM file_entries" 
            " NATURAL LEFT JOIN file_versions WHERE file_entries.filename = :fn"), {"fn": filename}).fetchone()

        if not row:
            raise KeyError(filename)
        
        return Index.Entry(row[0], row[1], row[2], row[3])      

    def __len__(self):
        row = self.con.execute("SELECT COUNT(*) FROM file_entries").fetchone()
        return row[0]

    def __setitem__(self, filename: str, entry: Entry):
        with self.con:
            row = self.con.execute("SELECT version FROM file_entries WHERE filename=:fn", {"fn": filename}).fetchone()
            version = row[0] if row else 1

            self.con.execute("REPLACE INTO file_versions VALUES (:fn, :ver, :lmod, :etag, :idx, :meta)", 
                    {"fn": filename, "ver": version, 
                    "lmod": entry.last_modified, "etag": entry.etag, "idx": entry.index_date, "meta": entry.metadata})

            self.con.execute("REPLACE INTO file_entries VALUES (:fn, :ver)", 
                    {"fn": filename, "ver": version})

    def __contains__(self, filename: str):
        row = self.con.execute("SELECT COUNT(*) FROM file_entries WHERE filename=:fn", {"fn": filename}).fetchone()
        return row and row[0] != 0

    def files(self) -> Iterable[str]: 
        for row in self.con.execute("SELECT filename FROM file_entries"):
            yield row[0]

    def items(self) -> Iterable[Tuple[str, Entry]]: 
        for row in self.con.execute(("SELECT file_entries.filename, last_modified, etag, index_date, metadata FROM file_entries"
                                     " NATURAL LEFT JOIN file_versions")):
            yield (row[0], Index.Entry(row[1], row[2], row[3], row[4]))

    def add(self, filename: str, entry: Entry):
        self[filename] = entry

    def get(self, filename: str, default: Entry = None) -> Entry:
        try:
            return self[filename]
        except KeyError:
            return default

    def discard(self, filename: str):
        with self.con:
            self.con.execute("DELETE FROM file_entries WHERE filename=:fname", {"fname": filename})
            self.con.execute("DELETE FROM file_versions WHERE filename=:fname", {"fname": filename})

    def add_many(self, iterable: Iterable[tuple[str,Entry]]):
        with self.con:
            data = [{"fn": f, "e": e} for f, e in iterable]

            # TODO: Better to do multiple selects instead?
            filenames = "'" + "','".join((f["fn"] for f in data)) + "'"
            rows = self.con.execute(f"SELECT filename, version FROM file_entries WHERE filename IN ({filenames})").fetchall()

            versions = dict(rows) if rows else dict()

            replace_data = list({"fn": d["fn"], "ver": versions.get(d["fn"], 1), 
                "lmod": d["e"].last_modified, "etag": d["e"].etag, "idx": d["e"].index_date, "meta": d["e"].meta} for d in data)

            self.con.executemany("REPLACE INTO file_versions VALUES (:fn, :ver, :lmod, :etag, :idx, :meta)", replace_data)
            self.con.executemany("REPLACE INTO file_entries VALUES (:fn, :ver)", replace_data)

    def remove_many(self, iterable: Iterable[str]):
        with self.con:
            data = [{"fn": f} for f in iterable]
            self.con.executemany("DELETE FROM file_entries WHERE filename=:fn", data)
            self.con.executemany("DELETE FROM file_versions WHERE filename=:fn", data)

    def get_current_version(self, filename: str, default: int = 0 ) -> int:
        row = self.con.execute("SELECT version FROM file_entries WHERE filename=:fn", {"fn": filename}).fetchone()
        return row[0] if row else default

    def add_version(self, filename: str, version: int, entry: Entry):
        with self.con:
            data = {"fn": filename, "ver": version, 
                "lmod": entry.last_modified, "etag": entry.etag, "idx": entry.index_date, "meta": entry.meta}

            self.con.execute("REPLACE INTO file_versions VALUES (:fn, :ver, :lmod, :etag: :idx, :meta)", data)
            self.con.execute("REPLACE INTO file_entries VALUES (:fn, :ver)", data)

    def optimize(self):
        with self.con:
            self.con.execute("PRAGMA optimize")
 
    def vacuum(self):
        with self.con:
            self.con.execute("VACUUM")
 