import os
import zipfile
import logging
import base64
from datetime import timezone, time
import mmh3
import re
import orjson
import dask.dataframe as daf
from distributed.utils_perf import disable_gc_diagnosis
from distributed import Client, LocalCluster, progress, get_worker, wait
import pandas as pd
from tse.common.voting_machine_files import VotingMachineFiles, VotingMachineLogProcessor
from tse.utils import log_progress
from tse.parsers import CityConfigParser

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

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
                if not "_AC" in entry.name:
                    continue

                with zipfile.ZipFile(entry.path, "r") as zip:
                    for info in zip.infolist():
                        if not info.is_dir() and not info.filename == "leiame.pdf":
                            yield os.path.join(entry.name, info.filename)

def load_json(path):
    with open(path, "rb") as f:
        return orjson.loads(f.read())

def read_tse_cities():
    data = list(CityConfigParser.expand_cities(load_json("data/download/oficial/ele2022/545/config/mun-e000545-cm.json")))

    df = pd.DataFrame(data,
                        columns=["SG_UF", "CD_MUNICIPIO",
                            "CD_MUNICIPIO_IBGE", "NM_MUNICIPIO", "MUNICIPIO_CAPITAL", "MUNICIPIO_ZONAS"])

    df["SG_UF"] = df["SG_UF"].str.upper().astype(pd.CategoricalDtype())
    df["CD_MUNICIPIO"] = df["CD_MUNICIPIO"].astype(pd.CategoricalDtype())
    df["CD_MUNICIPIO_IBGE"] = df["CD_MUNICIPIO_IBGE"].astype(pd.CategoricalDtype())
    df["NM_MUNICIPIO"] = df["NM_MUNICIPIO"].astype(pd.CategoricalDtype())
    df["MUNICIPIO_CAPITAL"] = df["MUNICIPIO_CAPITAL"].astype(bool)
    return df.set_index(["SG_UF", "CD_MUNICIPIO"]).sort_index()

def get_index_id(filename, row_num):
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
        get_worker()
    except ValueError:
        return ""

def part(partition, partition_info=None):
    if not partition_info:
        return partition

    logger = logging.getLogger("distributed.worker")
    logger.setLevel(logging.INFO)

    es_loggers = logging.getLogger("elastic_transport.transport")
    es_loggers.setLevel(logging.WARNING)

    log_processor = VotingMachineLogProcessor()
    cities = read_tse_cities()

    zip_regex = re.compile(r"^bu_imgbu_logjez_rdv_vscmr_(?P<year>\d{4})_(?P<round>\d{1})t_(?P<state>\w{2})\.(?P<ext>\w+)")
    file_regex = re.compile(r"^(o|s|t)(?P<plea>\d{5})-(?P<city>\d{5})(?P<zone>\d{4})(?P<section>\d{4})\.(?P<ext>\w+)")

    es_client = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD),
        http_compress=True,
    )

    df = (
        partition.reset_index().
        assign(zip_filename=lambda p: p["index"].apply(os.path.dirname)).
        assign(log_filename=lambda p: p["index"].apply(os.path.basename)).
        drop(columns="index").
        rename(columns={"paths": "extensions"}).
        set_index(["zip_filename", "log_filename"]).sort_index()
    )

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
                
                def gen_rows():
                    with zip.open(log_filename) as file:
                        for filename, bio in log_processor.read_compressed_logs(file, log_filename):
                            for row in log_processor.parse_log(bio, filename):
                                doc = {
                                    "_id": get_index_id(filename, row.number),
                                    "year": year,
                                    "round": round,
                                    "plea": plea,
                                    "state": state,
                                    "city": city,
                                    "city_ibge": city_info["CD_MUNICIPIO_IBGE"],
                                    "city_name": city_info["NM_MUNICIPIO"],
                                    "zone": zone,
                                    "section": section,
                                    "logfilename": filename,
                                    "logtype": "contingency" if os.path.splitext(filename)[1] == ".jez" else "main",
                                    "row": {
                                        "num": row.number,
                                        "timestamp": row.timestamp.astimezone(timezone.utc),
                                        "level": row.level,
                                        "vm_id": row.vm_id,
                                        "app": row.app,
                                        "raw_message": row.raw_message,
                                        "message": row.message,
                                        "message_params": dict_process(row.message_params),
                                        "hash": "{:016X}".format(row.hash)
                                    }
                                }
                                yield doc

                rows = list(gen_rows())
                bulk(client=es_client, index="index-logs-voting-machine", actions=rows, chunk_size=8192)
                logger.info("%s | Finished %s", worker_name(), log_filename)

    return partition

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

def main():
    logging.basicConfig(level=logging.INFO)
    disable_gc_diagnosis()

    with Client(LocalCluster(n_workers=32, threads_per_worker=1), asynchronous=True) as client:
    #with Client(LocalCluster(processes=False, threads_per_worker=1), asynchronous=True) as client:
        logging.info("Init client: %s, dashboard: %s", client, client.dashboard_link)
        all_files = daf.from_pandas(collect_all_files(), chunksize=1000)
        a = all_files.map_partitions(client.submit(part))
        x = a.persist()
        progress(x)

if __name__ == "__main__":
    main()
