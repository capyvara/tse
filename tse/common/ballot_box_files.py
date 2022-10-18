import os
from enum import Enum

import pandas as pd
import py7zr


class BallotBoxFileType(Enum):
    BULLETIN = 1
    BULLETIN_IMAGE = 2
    DVR = 3
    LOG = 4
    SIGNATURE = 5

# In contingency order VOTA > RED > SA
BALLOT_BOX_FILES_EXTENSIONS = {
    BallotBoxFileType.BULLETIN: ["bu", "busa"],
    BallotBoxFileType.BULLETIN_IMAGE: ["imgbu", "imgbusa"],
    BallotBoxFileType.DVR: ["rdv", "rdvred"],
    BallotBoxFileType.LOG: ["logjez", "logsajez"],
    BallotBoxFileType.SIGNATURE: ["vscmr", "vscred", "vscsa"],
}

INV_BALLOT_BOX_FILES_EXTENSIONS = { v: k for k, l in BALLOT_BOX_FILES_EXTENSIONS.items() for v in l }

def get_ballot_box_files_map(filenames):
    map = {}
    
    for filename in filenames:
        ext = os.path.splitext(filename)[1][1:]
        type = INV_BALLOT_BOX_FILES_EXTENSIONS[ext]

        idx = BALLOT_BOX_FILES_EXTENSIONS[type].index(ext)

        if type in map:
            # Prefer contingency ballot files
            old_ext = os.path.splitext(map[type])[1][1:]
            old_idx = BALLOT_BOX_FILES_EXTENSIONS[type].index(old_ext)
            if idx > old_idx:
                map[type] = filename
        else:
            map[type] = filename

    return map

def read_ballot_box_logs(file, source_filename = None):
    extra_logs = []

    if not source_filename and isinstance(file, str):
        source_filename = os.path.basename(file)

    with py7zr.SevenZipFile(file, 'r') as zip:
        for filename, bio in zip.readall().items():
            if os.path.splitext(filename)[1] == ".jez":
                extra_logs += list(read_ballot_box_logs(bio, filename))
                continue

            if filename != "logd.dat":
                continue

            df = pd.read_csv(bio, sep="\t", header=None, encoding="latin_1", 
                names=["timestamp", "level", "id_ballot_box", "app", "message", "hash"],
                parse_dates=["timestamp"], infer_datetime_format = True, dayfirst=True,
                dtype={
                    "level": pd.CategoricalDtype(["ALERTA", "ERRO", "EXTERNO", "INFO"]),
                    "id_ballot_box": pd.CategoricalDtype(),
                    "app": pd.CategoricalDtype(["ATUE", "GAP", "INITJE", "LOGD", "RED", "SA", "SCUE", "VOTA"]),
                    "message": pd.CategoricalDtype(), # pd.StringDtype(),
                },
                converters={"hash": lambda x: int(x, 16)}
            )
            yield (source_filename, df)

    if len(extra_logs) > 0:
        yield from extra_logs