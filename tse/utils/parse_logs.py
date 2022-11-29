from collections import deque
import functools
import os
from threading import Thread, Event
import zipfile
import logging
import base64
from datetime import datetime, timezone, time
import mmh3
import re
import numpy as np
import orjson
from distributed.utils_perf import disable_gc_diagnosis
from distributed import Client, LocalCluster, progress, get_worker, wait
import pandas as pd
from tse.common.voting_machine_files import VotingMachineFiles, VotingMachineLogProcessor
from tse.utils import log_progress
from tse.parsers import CityConfigParser

from elasticsearch import Elasticsearch, TransportError
from elasticsearch.helpers import bulk, parallel_bulk

import asyncio
import queue
import concurrent.futures

pd.options.mode.string_storage = "pyarrow"
DOWNLOAD_DIR = "data/download/dadosabertos/transmitted"

ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
CLOUD_ID = os.getenv("CLOUD_ID")

def scan_dir(dir):
    with os.scandir(dir) as it:
        for entry in it:
            if not entry.is_file() or entry.name.startswith('.'): 
                continue

            if os.path.splitext(entry.name)[1] == ".zip":
                # if not "_PI" in entry.name:
                #     continue

                with zipfile.ZipFile(entry.path, "r") as zip:
                    for info in zip.infolist():
                        if not info.is_dir() and not info.filename == "leiame.pdf":
                            yield os.path.join(entry.name, info.filename)

def load_json(path):
    with open(path, "rb") as f:
        return orjson.loads(f.read())

def read_tse_cities():
    data = list(CityConfigParser.expand_cities(load_json("data/mun-default-cm.json")))

    df = pd.DataFrame(data,
                        columns=["SG_UF", "CD_MUNICIPIO",
                            "CD_MUNICIPIO_IBGE", "NM_MUNICIPIO", "MUNICIPIO_CAPITAL", "MUNICIPIO_ZONAS"])

    df["SG_UF"] = df["SG_UF"].str.upper().astype(pd.CategoricalDtype())
    df["CD_MUNICIPIO"] = df["CD_MUNICIPIO"].astype(pd.CategoricalDtype())
    df["CD_MUNICIPIO_IBGE"] = df["CD_MUNICIPIO_IBGE"].astype(pd.CategoricalDtype())
    df["NM_MUNICIPIO"] = df["NM_MUNICIPIO"].astype(pd.CategoricalDtype())
    df["MUNICIPIO_CAPITAL"] = df["MUNICIPIO_CAPITAL"].astype(bool)
    return df.set_index(["SG_UF", "CD_MUNICIPIO"]).sort_index()

def get_index_id(filename, row_num = 0):
    return base64.urlsafe_b64encode(mmh3.hash_bytes(filename + str(row_num))).decode("ascii").rstrip("=") 

def dict_process(d):
    if not d:
        return None

    for k,v in d.items():
        if isinstance(v, time):
            d[k] = v.strftime("%H:%M:%S") 

    return d

def worker_name():
    try:
        return get_worker().name
    except ValueError:
        return ""

def expand_logs_thread(df, q, e, wname):
    logger = logging.getLogger("distributed.worker")
    log_processor = VotingMachineLogProcessor()
    cities = read_tse_cities()

    zip_regex = re.compile(r"^bu_imgbu_logjez_rdv_vscmr_(?P<year>\d{4})_(?P<round>\d{1})t_(?P<state>\w{2})\.(?P<ext>\w+)")
    file_regex = re.compile(r"^(o|s|t)(?P<plea>\d{5})-(?P<city>\d{5})(?P<zone>\d{4})(?P<section>\d{4})\.(?P<ext>\w+)")

    for zip_filename, group in df.groupby(level=0):
        zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)
        logger.info("%s | Opened zip %s", worker_name(), zip_filename)

        zip_match = zip_regex.match(zip_filename)
        year = zip_match.group("year")
        round = zip_match.group("round")
        state = zip_match.group("state")

        with zipfile.ZipFile(zip_path, "r") as zip:
            for entry in group.itertuples(True):
                log_ext = VotingMachineFiles.get_voting_machine_files_map(entry.extensions)[VotingMachineFiles.FileType.LOG]
                log_filename = entry.Index[1] + log_ext

                file_match = file_regex.match(log_filename)
                plea = file_match.group("plea").lstrip("0")
                city = file_match.group("city").lstrip("0")
                zone = file_match.group("zone").lstrip("0")
                section = file_match.group("section").lstrip("0")

                city_info = cities.loc[(state, city)]

                commonfields = {
                    "year": int(year),
                    "round": int(round),
                    "plea": plea,
                    "state": state,
                    "city": city,
                    "city_ibge": city_info["CD_MUNICIPIO_IBGE"],
                    "city_name": city_info["NM_MUNICIPIO"],
                    "zone": zone,
                    "section": section,
                }
            
                def gen_docs():
                    with zip.open(log_filename) as file:
                        for filename, bio in log_processor.read_compressed_logs(file, log_filename):
                            for row in log_processor.parse_log(bio, filename):
                                doc = {
                                    "_index": "voting-machine-logs",
                                    "_id": get_index_id(filename, row.number),
                                }
                                doc.update(commonfields)
                                doc.update({
                                    "logfilename": filename,
                                    "logtype": "contingency" if os.path.splitext(filename)[1] == ".jez" else "main",
                                    "rownum": row.number,
                                    "timestamp": row.timestamp.astimezone(timezone.utc),
                                    "level": row.level,
                                    "vm_id": row.vm_id,
                                    "app": row.app,
                                    "message": row.message,
                                    "message_template": row.message_template,
                                    "message_params": dict_process(row.message_params),
                                    "hash": "{:016X}".format(row.hash),
                                    "event": { "dataset": "vmlogs" }
                                })
                                yield doc
                    
                    doc = {
                        "_index": "voting-machine-logfiles",
                        "_id": get_index_id(filename),
                    }
                    doc.update(commonfields)
                    doc.update({
                        "logfilename": log_filename,
                        "timestamp": datetime.utcnow(),
                        "event": { "dataset": "vmlogfiles" }
                    })
                    
                    yield doc

                docs = list(gen_docs())
                q.put((log_filename, docs))
                logger.info("%s | Processed %s", wname, log_filename)
    e.set()

def part(partition):
    logger = logging.getLogger("distributed.worker")

    df = (
        partition.reset_index().
        assign(zip_filename=lambda p: p["index"].apply(os.path.dirname)).
        assign(log_filename=lambda p: p["index"].apply(os.path.basename)).
        drop(columns="index").
        rename(columns={"paths": "extensions"}).
        set_index(["zip_filename", "log_filename"]).sort_index()
    )

    que = queue.Queue(10)
    evt = Event()
    Thread(target=expand_logs_thread, args=(df, que, evt, worker_name()), daemon=True).start()

    es_loggers = logging.getLogger("elastic_transport.transport")
    es_loggers.setLevel(logging.WARNING)

    es_client = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD),
        http_compress=True,
    )

    def get_docs():
        while not evt.is_set():
            log_filename, docs = que.get()
            logger.info("%s | Sent %s (%d docs)", worker_name(), log_filename, len(docs))
            yield from docs
    
    deque(parallel_bulk(client=es_client, actions=get_docs(), chunk_size=10000, thread_count=8, raise_on_error=False), 0)

    evt.wait()

def collect_all_files() -> pd.Series:
    expected_total = 2360133
    logging.info("Scanning %s...", DOWNLOAD_DIR)
    df = pd.Series(log_progress(scan_dir(DOWNLOAD_DIR), total=expected_total), dtype=pd.StringDtype(), name="paths")
    logging.info("Found %d files", len(df))
    
    logging.info("Grouping...")
    df = df.groupby(lambda x: os.path.splitext(df[x])[0]).apply(list)
    df = df.map(lambda l: [os.path.splitext(x)[1] for x in l])
    logging.info("Grouped into %d items", len(df))
    return df

def collect_indexed_files(client: Elasticsearch):
    def gen_query():
        search_after = None
        while True:
            res = client.search(index="voting-machine-logfiles", 
                fields=["year", "round", "state", "logfilename"],
                size=1000,
                source=False,
                search_after=search_after,
                sort=[
                    {"state": "asc"},
                    {"zone": "asc"},
                    {"section": "asc"}
                ] 
            )

            if not res or len(res["hits"]["hits"]) == 0:
                break

            for field in (h["fields"] for h in res["hits"]["hits"]):
                zipname = f"bu_imgbu_logjez_rdv_vscmr_{field['year'][0]}_{field['round'][0]}t_{field['state'][0]}.zip"
                filename = os.path.splitext(field['logfilename'][0])[0]
                yield os.path.join(zipname, filename)

            search_after = res["hits"]["hits"][-1]["sort"]

    expected_total = 472027
    df = pd.Series(log_progress(gen_query(), total=expected_total), dtype=pd.StringDtype(), name="logfilenames")
    logging.info("Found %d already indexed", len(df))

    return df

def create_voting_machine_logs_index(client: Elasticsearch):
    if client.indices.exists(index="voting-machine-logs"):
        return

    client.indices.create(
        index="voting-machine-logs",
        settings={
            "number_of_shards": 1, 
            "codec": "best_compression",
            "query": {
                "default_field": "row.message"
            }
        },
        mappings={
            "dynamic_templates": [
                {
                    "string_as_keyword": {
                        "path_match": "row.message_params.*",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword",
                            "ignore_above": 1024,
                        }
                    }
                },
            ],
            "properties": {
                "year": { "type": "short" },
                "round": { "type": "byte" },
                "plea": { "type": "keyword" },
                "state": { "type": "keyword" },
                "city": { "type": "keyword" },
                "city_ibge": { "type": "keyword" },
                "city_name": { "type": "keyword" },
                "zone": { "type": "keyword" },
                "section": { "type": "keyword" },
                "logfilename": { "type": "wildcard" },
                "logtype": { "type": "keyword" },
                "rownum": { "type": "integer" },
                "timestamp": { "type": "date" },
                "level": { "type": "keyword" },
                "vm_id": { "type": "keyword" },
                "app": { "type": "keyword" },
                "message": { "type": "match_only_text" },
                "message_template": { "type": "match_only_text" },
                "message_params": { 
                    "type": "object",
                    "subobjects": False,
                    "dynamic": "runtime",
                },
                "hash": { "type": "wildcard" },
                "event": {
                    "properties": {
                        "dataset": { type: "constant_keyword" }
                    }
                }
            }
        }
    )

def create_voting_machine_logfiles_index(client):
    if client.indices.exists(index="voting-machine-logfiles"):
        return

    client.indices.create(
        index="voting-machine-logfiles",
        settings={
            "number_of_shards": 1, 
            "codec": "best_compression",
        },
        mappings={
            "properties": {
                "year": { "type": "short" },
                "round": { "type": "byte" },
                "plea": { "type": "keyword" },
                "state": { "type": "keyword" },
                "city": { "type": "keyword" },
                "city_ibge": { "type": "keyword" },
                "city_name": { "type": "keyword" },
                "zone": { "type": "keyword" },
                "section": { "type": "keyword" },
                "logfilename": { "type": "wildcard" },
                "timestamp": { "type": "date" },
            }
        }
    )

def main():
    logging.basicConfig(level=logging.INFO)
    disable_gc_diagnosis()

    es_client = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD),
    )
    es_loggers = logging.getLogger("elastic_transport.transport")
    es_loggers.setLevel(logging.WARNING)

    create_voting_machine_logs_index(es_client)
    create_voting_machine_logfiles_index(es_client)

    with Client(LocalCluster(processes = True, n_workers=4, threads_per_worker=1, silence_logs=False)) as client:
        logging.info("Init client: %s, dashboard: %s", client, client.dashboard_link)

        all_files = collect_all_files()
        already_indexed = collect_indexed_files(es_client)
        to_index = all_files[~all_files.index.isin(already_indexed)]

        split = max(int(len(to_index) / 500), 1)
        logging.info("Will index %d files in %d chunks", len(to_index), split)

        c = client.map(part, np.array_split(to_index, split), pure=False)
        wait(c)

if __name__ == "__main__":
    main()
