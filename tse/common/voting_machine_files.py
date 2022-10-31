import csv
import io
import os
from enum import Enum
import re
from typing import IO, Any

import pandas as pd
import py7zr


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

class Grok:
    PATTERNS = {
        "PATH": r"(/([\w_%!$@:.,~-]+|\\.)*)+",
    }

    def compile_grok(pat):
        return re.compile(re.sub(r'%{(\w+):(\w+)}', 
            lambda m: "(?P<" + m.group(2) + ">" + PATTERNS[m.group(1)] + ")", pat))

# TODO: Put more common first
RULES = [
    r"Urna ligada em (?P<date>.+) às (?P<time>.+)",
    r"Iniciando aplicação - (?P<env>.+) - (?P<round>.+)",
    r"Versão da aplicação: (?P<ver_num>.+) - (?P<ver_name>.+)",
    r"Tamanho da (?P<target>.+): (?P<target_size>.+) MB",
    r"Verificação de assinatura de aplicação por etapa \[(?P<stage>\d+)\] - \[(?P<path>\d+)\] - \[(?P<result>\d+)\]"
]

PATTERNS = [re.compile(p) for p in RULES]

def process_message(message: str):
    for pattern in PATTERNS:
        match = pattern.match(message)
        if match:
            params = match.groupdict()
            split = []
            lbound = 0
            for key, l, r in ((key, *match.span(key)) for key in params.keys()):
                split.append(message[lbound:l])
                lbound = r
                split.append(f"%({key})s")
            return ("".join(split), params)
    return (message, None)

def log_parser(buffer: IO[Any]):
    wrapper = io.TextIOWrapper(buffer, encoding="latin_1", newline="")
    reader = csv.reader(wrapper, delimiter="\t")
    for row in reader:
        # print('\t'.join(row))
        print(process_message(row[4]))
    return

test_log_path = "data/download/oficial/ele2022/arquivo-urna/406/dados/ac/01066/0004/0077/395459446c754b34572b56304a706a6a413454646f6f5a6f5664426f5169564241506566444932644f75493d/o00406-0106600040077.logjez"
with py7zr.SevenZipFile(test_log_path, 'r') as zip:
    log_parser(zip.read("logd.dat")["logd.dat"])
    pass
