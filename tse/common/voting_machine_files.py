import csv
import datetime
import io
import orjson
import os
from enum import Enum
from typing import IO, Any

import pandas as pd
import numpy as np
import py7zr

from tse.utils.grok import GrokProcessor


pd.options.mode.string_storage = "pyarrow"

class VotingMachineFileType(Enum):
    BULLETIN = 1
    BULLETIN_IMAGE = 2
    DVR = 3
    LOG = 4
    SIGNATURE = 5

# In contingency order VOTA > RED > SA
VOTING_MACHINE_FILES_EXTENSIONS = {
    VotingMachineFileType.BULLETIN: ["bu", "busa"],
    VotingMachineFileType.BULLETIN_IMAGE: ["imgbu", "imgbusa"],
    VotingMachineFileType.DVR: ["rdv", "rdvred"],
    VotingMachineFileType.LOG: ["logjez", "logsajez"],
    VotingMachineFileType.SIGNATURE: ["vscmr", "vscred", "vscsa"],
}

INV_VOTING_MACHINE_FILES_EXTENSIONS = { v: k for k, l in VOTING_MACHINE_FILES_EXTENSIONS.items() for v in l }

def get_voting_machine_files_map(filenames):
    map = {}
    
    for filename in filenames:
        ext = os.path.splitext(filename)[1][1:]
        type = INV_VOTING_MACHINE_FILES_EXTENSIONS[ext]

        idx = VOTING_MACHINE_FILES_EXTENSIONS[type].index(ext)

        if type in map:
            # Prefer contingency vm files
            old_ext = os.path.splitext(map[type])[1][1:]
            old_idx = VOTING_MACHINE_FILES_EXTENSIONS[type].index(old_ext)
            if idx > old_idx:
                map[type] = filename
        else:
            map[type] = filename

    return map

def read_voting_machine_logs(file, source_filename = None):
    extra_logs = []

    if not source_filename and isinstance(file, str):
        source_filename = os.path.basename(file)

    with py7zr.SevenZipFile(file, 'r') as zip:
        for filename, bio in zip.readall().items():
            if os.path.splitext(filename)[1] == ".jez":
                extra_logs += list(read_voting_machine_logs(bio, filename))
                continue

            if filename != "logd.dat":
                continue

            df = pd.read_table(bio, header=None, encoding="latin_1", 
                names=["timestamp", "level", "id_voting_machine", "app", "message", "hash"],
                parse_dates=["timestamp"], infer_datetime_format = True, dayfirst=True,
                dtype={
                    "level": pd.CategoricalDtype(["ALERTA", "ERRO", "EXTERNO", "INFO"]),
                    "id_voting_machine": pd.CategoricalDtype(),
                    "app": pd.CategoricalDtype(["ATUE", "GAP", "INITJE", "LOGD", "RED", "SA", "SCUE", "VOTA"]),
                    "message": pd.CategoricalDtype(), # pd.StringDtype(),
                },
                converters={"hash": lambda x: int(x, 16)}
            )
            yield (source_filename, df)

    if len(extra_logs) > 0:
        yield from extra_logs


grok_processor = GrokProcessor()
grok_processor.load_matchers_from_file("data/voting_machine_logs_matchers.txt")

def log_parser(buffer: IO[Any]):
    with io.TextIOWrapper(buffer, encoding="latin_1", newline="") as wrapper:
        reader = csv.reader(wrapper, delimiter="\t")

        timestamps = []
        levels = []
        apps = []
        message_categories = {}
        messages = []
        message_params = []
        hashes = []

        lines = 0
        for row in reader:
            # strptime is slower
            # dd/mm/yyyy hh:mm:ss
            dt = row[0]
            timestamps.append((datetime.datetime( 
                    int(dt[6:10]), int(dt[3:5]), int(dt[:2]),
                    int(dt[11:13]), int(dt[14:16]), int(dt[17:]))))

            levels.append(row[1])
            apps.append(row[3])

            result = grok_processor.match(row[4], positional=False)
            message_categories[result[0]] = message_categories.get(result[0], 0) + 1
            
            messages.append(result[0])
            message_params.append(result[1])
            
            hashes.append(int(row[5], 16))
            
        #     print(f"'{result[0]}' % {result[1]}")
        #     lines += 1
        
        # print("*********************************************************************")

        # for k, v in sorted(message_categories.items(), key=lambda t: t[1], reverse=True):
        #     print(f"{k} = {v}")

        # print(len(message_categories))
        # print(lines)

        message_cat = pd.CategoricalDtype(message_categories.keys())

        df = pd.DataFrame({
            "timestamp": pd.Series(timestamps, dtype="datetime64[ns]"),
            "level": pd.Series(levels, dtype=pd.CategoricalDtype(["ALERTA", "ERRO", "EXTERNO", "INFO"])),
            "app": pd.Series(apps, dtype=pd.CategoricalDtype(["ATUE", "GAP", "INITJE", "LOGD", "RED", "SA", "SCUE", "VOTA"])),
            "message": pd.Series(messages, dtype=message_cat),
            "message_params": pd.Series(message_params),
        })

        df2 = df.copy()
        df2["message_params"] = df2["message_params"].apply(lambda p: orjson.dumps(p).decode("utf-8") if p else None).astype(pd.StringDtype("pyarrow"))
        df2.to_parquet(f"data/temp/log_tests/log_pq.parquet", index=False, engine="pyarrow", compression="brotli")
        df2.to_csv(f"data/temp/log_tests/log_ts.tsv", index=False, sep="\t", quotechar="'")

        df3 = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
        df3.index.name = "message"
        df3.to_csv(f"data/temp/log_tests/log_ts_cats.tsv", index=True, sep="\t", quotechar="'")
        pass

test_log_path = "data/download/oficial/ele2022/arquivo-urna/406/dados/ac/01066/0004/0077/395459446c754b34572b56304a706a6a413454646f6f5a6f5664426f5169564241506566444932644f75493d/o00406-0106600040077.logjez"

#test_log_path = "data/temp/DadosDeUrna_o00406-7183804070205sa_1665898395680/o00406-7183804070205.logsajez"
with py7zr.SevenZipFile(test_log_path, 'r') as zip:
    log_parser(zip.read("logd.dat")["logd.dat"])
    pass
