import csv
import datetime
import io
import logging
import os
from enum import Enum
from typing import BinaryIO, Iterable, NamedTuple, Optional, Tuple, Union

class VotingMachineFiles:

    class FileType(Enum):
        BULLETIN = 1
        BULLETIN_IMAGE = 2
        DVR = 3
        LOG = 4
        SIGNATURE = 5

    # In contingency order VOTA > RED > SA
    VOTING_MACHINE_FILES_EXTENSIONS = {
        FileType.BULLETIN: ["bu", "busa"],
        FileType.BULLETIN_IMAGE: ["imgbu", "imgbusa"],
        FileType.DVR: ["rdv", "rdvred"],
        FileType.LOG: ["logjez", "logsajez"],
        FileType.SIGNATURE: ["vscmr", "vscred", "vscsa"],
    }

    INV_VOTING_MACHINE_FILES_EXTENSIONS = { v: k for k, l in VOTING_MACHINE_FILES_EXTENSIONS.items() for v in l }

    @classmethod
    def get_voting_machine_files_map(cls, filenames: Iterable[str]):
        map = {}
        
        for filename in filenames:
            root, ext = os.path.splitext(filename)
            ext = ext or root
            ext = ext[1:]
            type = cls.INV_VOTING_MACHINE_FILES_EXTENSIONS[ext]

            idx = cls.VOTING_MACHINE_FILES_EXTENSIONS[type].index(ext)

            if type in map:
                # Prefer contingency vm files
                old_ext = os.path.splitext(map[type])[1][1:]
                old_idx = cls.VOTING_MACHINE_FILES_EXTENSIONS[type].index(old_ext)
                if idx > old_idx:
                    map[type] = filename
            else:
                map[type] = filename

        return map

import py7zr
from tse.common.grok import GrokProcessor

class VotingMachineLogProcessor:

    class Row(NamedTuple):
        number: int
        timestamp: datetime.datetime
        level: str
        vm_id: str
        app: str
        message: str
        message_template: str
        message_params: Optional[Union[dict, list]]
        hash: int

    _grok_processor: GrokProcessor

    def __init__(self):
        self._grok_processor = GrokProcessor({
                                        "NAPI_EXCEPTION": r"N\dapi\d+C.*ExceptionE - \((?:Código \(%{INT:code:int}\))?\) ",
                                        "ST_ERROR": r"St\d{2}.+?_error - \(\) "
                                    }).load_matchers_from_file("data/voting_machine_logs_matchers.txt")

    def read_compressed_logs(self, file: Union[BinaryIO, str, os.PathLike], source_name: str = None) ->  Iterable[Tuple[str, BinaryIO]]:
        if not source_name and isinstance(file, str):
            source_name = os.path.relpath(file)

        with py7zr.SevenZipFile(file, 'r', mp = False) as zip:
            for filename, bio in zip.readall().items():
                if os.path.splitext(filename)[1] == ".jez":
                    yield from self.read_compressed_logs(bio, os.path.join(source_name, filename))
                    continue

                if filename != "logd.dat":
                    continue

                yield (source_name, bio)

    def parse_log(self, bio: BinaryIO, source_name: str, *, pos_msg_params: bool = False) ->  Iterable[Row]:
        with io.TextIOWrapper(bio, encoding="latin_1", newline="") as wrapper:
            try:
                reader = csv.reader(wrapper, delimiter="\t")

                row_number = 0
                for row in reader:
                    row_number += 1

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

                        message_template, message_params = self._grok_processor.match(row[4], pos_msg_params=pos_msg_params)
                        yield VotingMachineLogProcessor.Row(number=row_number, timestamp=timestamp, level=row[1], vm_id=row[2], app=row[3], 
                            message=row[4], message_template=message_template, message_params=message_params, hash=hash)
                    except ValueError as ex:
                        logging.warning("Error reading %s @ %d: %s", source_name, row_number, repr(ex))
                        continue
            except csv.Error as ex:
                logging.warning("Invalid file %s: %s", source_name, repr(ex))
