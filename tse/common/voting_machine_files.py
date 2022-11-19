import csv
import datetime
import io
import logging
import orjson
import os
from enum import Enum
from typing import BinaryIO, Iterable, NamedTuple, Optional, Tuple, Union

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

class VotingMachineLogProcessor:

    class Row(NamedTuple):
        count: int
        timestamp: datetime.datetime
        level: str
        vm_id: str
        app: str
        raw_message: str
        message: str
        message_params: Optional[Union[dict, list]]
        hash: int

    _grok_processor: GrokProcessor

    def __init__(self):
        self._grok_processor = GrokProcessor({
                                        "NAPI_EXCEPTION": r"N\dapi\d+C.*ExceptionE - \((?:Código \(%{INT:code:int}\))?\) ",
                                        "ST_ERROR": r"St\d{2}.+?_error - \(\) "
                                    }).load_matchers_from_file("data/voting_machine_logs_matchers.txt")

    def read_compressed_logs(self, file: Union[BinaryIO, str, os.PathLike], source_filename: str = None) ->  Iterable[Tuple[str, BinaryIO]]:
        if not source_filename and isinstance(file, str):
            source_filename = os.path.basename(file)

        # To speed up: https://github.com/miurahr/py7zr/issues/489
        with py7zr.SevenZipFile(file, 'r', mp = False) as zip:
            for filename, bio in zip.readall().items():
                if os.path.splitext(filename)[1] == ".jez":
                    yield from self.read_compressed_logs(bio, filename)
                    continue

                if filename != "logd.dat":
                    continue

                yield (source_filename, bio)

    def parse_log(self, source_filename: str, bio: BinaryIO, *, pos_msg_params: bool = False) ->  Iterable[Row]:
        with io.TextIOWrapper(bio, encoding="latin_1", newline="") as wrapper:
            reader = csv.reader(wrapper, delimiter="\t")

            row_count = 0
            for row in reader:
                row_count += 1

                try:
                    if len(row) != 6:
                        raise ValueError("Doesn't contain 6 fields")

                    hash = 0
                    try:
                        hash=int(row[5], 16)
                    except ValueError:
                        raise ValueError("Malformed hash")

                    # strptime is slower
                    # dd/mm/yyyy hh:mm:ss
                    dt = row[0]
                    timestamp = ((datetime.datetime( 
                            int(dt[6:10]), int(dt[3:5]), int(dt[:2]),
                            int(dt[11:13]), int(dt[14:16]), int(dt[17:]))))

                    message, message_params = self._grok_processor.match(row[4], pos_msg_params=pos_msg_params)
                    row_count += 1
                    yield VotingMachineLogProcessor.Row(count=row_count, timestamp=timestamp, level=row[1], vm_id=row[2], app=row[3], 
                        raw_message=row[4], message=message, message_params=message_params, hash=hash)
                except ValueError as ex:
                    logging.warning("Error reading %s @ %d: %s", source_filename, row_count, repr(ex))
                    continue


from tqdm import tqdm
from scrapy.utils.project import get_project_settings
from tse.common.pathinfo import PathInfo
import pandas as pd

settings = get_project_settings()
plea = settings["PLEA"]
elections = settings["ELECTIONS"]
states= settings["STATES"]

df = pd.read_parquet("data/all_sections_1st_round.parquet")

print(len(df[df["hash"].isna()]))

def message_cat():
    message_categories = {}
    count = -1

    log_processor = VotingMachineLogProcessor()

    def dump():
        df_message_categories = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
        df_message_categories.index.name = "message"
        df_message_categories.to_csv(f"data/temp/all_message_categories_2.tsv", index=True, sep="\t", quotechar="'")

    #for index, row in tqdm(df.iloc[13000:].iterrows(), total=472075):
    for index, row in tqdm(df.iterrows(), total=472075):
        count += 1

        if pd.isna(row["hash"]):
            continue

        log_filename = get_voting_machine_files_map(row["files"]).get(VotingMachineFileType.LOG, None)
        if not log_filename:
            continue

        log_path = PathInfo.get_voting_machine_file_path(plea, *index, row["hash"], log_filename)    

        for filename, bio in log_processor.read_compressed_logs(PathInfo.get_local_path(settings, log_path)):
            for row in log_processor.parse_log(filename, bio):
                if row.message_params:
                    message_categories[row.raw_message] = message_categories.get(row.raw_message, 0) + 1

        if count % 100 == 0:
            dump()

    dump()

message_cat()


#     count += 1
#     if count % 10 == 0:
#         df_message_categories = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
#         df_message_categories.index.name = "message"
#         df_message_categories.to_csv(f"data/temp/all_message_categories_tmp.tsv", index=True, sep="\t", quotechar="'")

# df_message_categories = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
# df_message_categories.index.name = "message"
# df_message_categories.to_csv(f"data/temp/all_message_categories.tsv", index=True, sep="\t", quotechar="'")


# test_log_path = "data/temp/DadosDeUrna_o00406-7183804070205sa_1665898395680/o00406-7183804070205.logsajez"

# message_categories = {}
# for filename, bio in VotingMachineLogFile.read_compressed_logs(test_log_path):
#     for row in VotingMachineLogFile.parse_log(bio):
#         message_categories[row.message] = message_categories.get(row.message, 0) + 1

# a = sorted(message_categories.items(), key=lambda t: t[1], reverse=True)
# pass
# grok_processor = GrokProcessor()
# grok_processor.load_matchers_from_file("data/voting_machine_logs_matchers.txt")

# def log_parser(buffer: IO[Any]):
#     with io.TextIOWrapper(buffer, encoding="latin_1", newline="") as wrapper:
#         reader = csv.reader(wrapper, delimiter="\t")

#         timestamps = []
#         levels = []
#         apps = []
#         message_categories = {}
#         messages = []
#         message_params = []
#         hashes = []

#         lines = 0
#         for row in reader:
#             # strptime is slower
#             # dd/mm/yyyy hh:mm:ss
#             dt = row[0]
#             timestamps.append((datetime.datetime( 
#                     int(dt[6:10]), int(dt[3:5]), int(dt[:2]),
#                     int(dt[11:13]), int(dt[14:16]), int(dt[17:]))))

#             levels.append(row[1])
#             apps.append(row[3])

#             result = grok_processor.match(row[4], positional=False)
#             message_categories[result[0]] = message_categories.get(result[0], 0) + 1
            
#             messages.append(result[0])
#             message_params.append(result[1])
            
#             hashes.append(int(row[5], 16))
            
#         #     print(f"'{result[0]}' % {result[1]}")
#         #     lines += 1
        
#         # print("*********************************************************************")

#         # for k, v in sorted(message_categories.items(), key=lambda t: t[1], reverse=True):
#         #     print(f"{k} = {v}")

#         # print(len(message_categories))
#         # print(lines)

#         message_cat = pd.CategoricalDtype(message_categories.keys())

#         df = pd.DataFrame({
#             "timestamp": pd.Series(timestamps, dtype="datetime64[ns]"),
#             "level": pd.Series(levels, dtype=pd.CategoricalDtype(["ALERTA", "ERRO", "EXTERNO", "INFO"])),
#             "app": pd.Series(apps, dtype=pd.CategoricalDtype(["ATUE", "GAP", "INITJE", "LOGD", "RED", "SA", "SCUE", "VOTA"])),
#             "message": pd.Series(messages, dtype=message_cat),
#             "message_params": pd.Series(message_params),
#         })

#         df2 = df.copy()
#         df2["message_params"] = df2["message_params"].apply(lambda p: orjson.dumps(p).decode("utf-8") if p else None).astype(pd.StringDtype("pyarrow"))
#         df2.to_parquet(f"data/temp/log_tests/log_pq.parquet", index=False, engine="pyarrow", compression="brotli")
#         df2.to_csv(f"data/temp/log_tests/log_ts.tsv", index=False, sep="\t", quotechar="'")

#         df3 = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
#         df3.index.name = "message"
#         df3.to_csv(f"data/temp/log_tests/log_ts_cats.tsv", index=True, sep="\t", quotechar="'")
#         pass

# test_log_path = "data/download/oficial/ele2022/arquivo-urna/406/dados/ac/01066/0004/0077/395459446c754b34572b56304a706a6a413454646f6f5a6f5664426f5169564241506566444932644f75493d/o00406-0106600040077.logjez"

# #test_log_path = "data/temp/DadosDeUrna_o00406-7183804070205sa_1665898395680/o00406-7183804070205.logsajez"
# with py7zr.SevenZipFile(test_log_path, 'r') as zip:
#     log_parser(zip.read("logd.dat")["logd.dat"])
#     pass
