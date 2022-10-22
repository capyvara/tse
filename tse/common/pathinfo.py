import os
import re
from datetime import datetime


class PathInfo:
    _regexes = [
        re.compile(r"^(?P<prefix>cert|mun)?(?P<state>\w{2})?(?P<city>\d{5})?(?:-?p(?P<plea>\d{6}))?(?:-c(?P<cand>\d{4}))?(?:-e(?P<election>\d{6}))?(?:-(?P<ver>\d{3}))?-(?P<type>\w{1,3}?)\.(?P<ext>\w+)"),
        re.compile(r"^p(?P<plea>\d{6})-(?P<state>\w{2})-m(?P<city>\d{5})?-z(?P<zone>\d{4})?-s(?P<section>\d{4})?-(?P<type>\w{1,3}?)\.(?P<ext>\w+)"),
        re.compile(r"^(o|s|t)(?P<plea>\d{5})-(?P<city>\d{5})(?P<zone>\d{4})(?P<section>\d{4})\.(?P<ext>\w+)"),
        re.compile(r"^(?P<id_ballot_box>\d{8})(?P<timestamp>\d{14})-(?P<seq>\d{2})\.(?P<ext>\w+)")
    ]

    def __init__(self, filename):
        self.filename = filename
        self.path = None
        self.prefix = None
        self.state = None
        self.city = None
        self.cand = None
        self.election = None
        self.plea = None
        self.zone = None
        self.section = None
        self.ver = None
        self.type = None
        self.ext = None
        self.id_ballot_box = None
        self.timestamp = None
        self.seq = None

        if filename == "ele-c.json":
            self.path = f"comum/config/{filename}"
            self.type = "c"
            self.ext = "json"
            return

        if os.path.splitext(filename)[1] == ".jpeg":
            self.ext = ".jpeg"
            return 

        # Divulgacao files + Urna section config
        result = self._regexes[0].match(filename)
        if result:
            self.prefix =result["prefix"] 
            self.state = result["state"]
            self.city = result["city"].lstrip("0") if result["city"] else None
            self.cand = result["cand"].lstrip("0") if result["cand"] else None
            self.election = result["election"].lstrip("0") if result["election"] else None
            self.plea = result["plea"].lstrip("0") if result["plea"] else None
            self.ver = result["ver"].lstrip("0") if result["ver"] else None
            self.type = result.group("type")
            self.ext = result.group("ext")

            if self.type in ("a", "cm"):
                self.path = f"{self.election}/config/{filename}"
            elif self.type == "i":
                self.path = f"{self.election}/config/{self.state}/{filename}"
            elif self.type == "r":
                self.path = f"{self.election}/dados-simplificados/{self.state}/{filename}"
            elif self.type in ("f", "v", "t", "e", "ab"):
                self.path = f"{self.election}/dados/{self.state}/{filename}"
            elif self.type == "cs":
                self.path = f"arquivo-urna/{self.plea}/config/{self.state}/{filename}"
            
            return

        # Section aux files
        result = self._regexes[1].match(filename)
        if result:
            self.plea = result["plea"].lstrip("0") if result["plea"] else None
            self.state = result["state"]
            self.city = result["city"].lstrip("0") if result["city"] else None
            self.zone = result["zone"].lstrip("0") if result["zone"] else None
            self.section = result["section"].lstrip("0") if result["section"] else None
            self.type = result.group("type")
            self.ext = result.group("ext")

            if self.type == "aux":
                self.path = f"arquivo-urna/{self.plea}/dados/{self.state}/{self.city:0>5}/{self.zone:0>4}/{self.section:0>4}/{filename}"

            return

        # Ballot box files
        result = self._regexes[2].match(filename)
        if result:
            self.plea = result["plea"].lstrip("0") if result["plea"] else None
            self.city = result["city"].lstrip("0") if result["city"] else None
            self.zone = result["zone"].lstrip("0") if result["zone"] else None
            self.section = result["section"].lstrip("0") if result["section"] else None
            self.ext = result.group("ext")
            return

        # Ballot box log contingency
        result = self._regexes[3].match(filename)
        if result:
            self.id_ballot_box = result["id_ballot_box"].lstrip("0") if result["id_ballot_box"] else None
            self.timestamp = datetime.strptime(self.timestamp, r"%d%m%Y%H%M%S") if result["timestamp"] else None
            self.seq = result.group("seq")
            self.ext = result.group("ext")
            return

        raise ValueError("Filename format not recognized")

    @staticmethod
    def get_local_path(settings, path):
        if path.startswith("comum/"):
            return os.path.join(settings["FILES_STORE"], settings["ENVIRONMENT"], path)

        return os.path.join(settings["FILES_STORE"], settings["ENVIRONMENT"], settings["CYCLE"], path)

    @staticmethod
    def get_full_url(settings, path):
        if path.startswith("comum/"):
            return os.path.join(f"{settings['HOST']}/{settings['ENVIRONMENT']}", path)

        return os.path.join(f"{settings['HOST']}/{settings['ENVIRONMENT']}/{settings['CYCLE']}", path)

    @staticmethod
    def get_state_index_path(election, state):
        return f"{election}/config/{state}/{state}-e{election:0>6}-i.json"
    
    @staticmethod
    def get_election_config_path():
        return "comum/config/ele-c.json"

    @staticmethod
    def get_picture_path(election, cand_state, sqcand):
        return f"{election}/fotos/{cand_state}/{sqcand}.jpeg"

    @staticmethod
    def get_sections_config_path(plea, state):
        return f"arquivo-urna/{plea}/config/{state}/{state}-p{plea:0>6}-cs.json"

    @staticmethod
    def _get_section_base_path(plea, state, city, zone, section):
        return f"arquivo-urna/{plea}/dados/{state}/{city:0>5}/{zone:0>4}/{section:0>4}"

    @classmethod
    def get_section_aux_path(cls, plea, state, city, zone, section):
        return f"{cls._get_section_base_path(plea, state, city, zone, section)}/p{plea:0>6}-{state}-m{city:0>5}-z{zone:0>4}-s{section:0>4}-aux.json"

    @classmethod
    def get_ballot_box_file_path(cls, plea, state, city, zone, section, hash, filename):
        return f"{cls._get_section_base_path(plea, state, city, zone, section)}/{hash}/{filename}"

    @classmethod
    def get_ballot_box_file_path_ext(cls, plea, state, city, zone, section, hash, ext, phase = "o"):
        return cls.get_ballot_box_file_path(plea, state, city, zone, section, hash, f"{phase}{plea:0>5}-{city:0>5}{zone:0>4}{section:0>4}.{ext}")
