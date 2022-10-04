import re
import datetime

class FileInfo:
    _regex1 = re.compile(r"^(?P<prefix>cert|mun)?(?P<state>\w{2})?(?P<mun>\d{5})?(?:-?p(?P<lawsuit>\d{6}))?(?:-c(?P<cand>\d{4}))?(?:-e(?P<election>\d{6}))?(?:-(?P<ver>\d{3}))?-(?P<type>\w{1,3}?)\.(?P<ext>\w+)")

    _regex2 = re.compile(r"^p(?P<lawsuit>\d{6})-(?P<state>\w{2})-m(?P<mun>\d{5})?-z(?P<zone>\d{4})?-s(?P<section>\d{4})?-(?P<type>\w{1,3}?)\.(?P<ext>\w+)")

    def __init__(self, filename):
        self.filename = filename

        if filename == "ele-c.json":
            self.path = f"comum/config/{filename}"
            self.type = "c"
            self.ext = "json"
            return

        result = self._regex1.match(filename)
        if result:
            self.prefix =result["prefix"] 
            self.state = result["state"]
            self.mun = result["mun"].lstrip("0") if result["mun"] else None
            self.cand = result["cand"].lstrip("0") if result["cand"] else None
            self.election = result["election"].lstrip("0") if result["election"] else None
            self.lawsuit = result["lawsuit"].lstrip("0") if result["lawsuit"] else None
            self.ver = result["ver"].lstrip("0") if result["ver"] else None
            self.type = result.group("type")
            self.ext = result.group("ext")

            if self.prefix == "cert" or self.prefix == "mun":
                self.path = f"{self.election}/config/{filename}"
            elif self.type == "i":
                self.path = f"{self.election}/config/{self.state}/{filename}"
            elif self.type == "r":
                self.path = f"{self.election}/dados-simplificados/{self.state}/{filename}"
            elif self.type == "cs":
                self.path = f"arquivo-urna/{self.lawsuit}/config/{self.state}/{filename}"
            else:
                self.path = f"{self.election}/dados/{self.state}/{filename}"
            
            return

        result = self._regex2.match(filename)
        if result:
            self.lawsuit = result["lawsuit"].lstrip("0") if result["lawsuit"] else None
            self.state = result["state"]
            self.mun = result["mun"].lstrip("0") if result["mun"] else None
            self.zone = result["zone"].lstrip("0") if result["zone"] else None
            self.section = result["section"].lstrip("0") if result["section"] else None
            self.type = result.group("type")
            self.ext = result.group("ext")

            if self.type == "aux":
                self.path = f"arquivo-urna/{self.lawsuit}/dados/{self.state}/{self.mun:05}/{self.zone:04}/{filename}"
            else:
                self.path = f"arquivo-urna/{self.lawsuit}/dados/{self.state}/{filename}"

            return

        raise ValueError("Filename format not recognized")

    def expand_index(state, data):
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")

            if filename == "ele-c.json":
                continue

            info = FileInfo(filename)
            if (info.prefix == "cert" or info.prefix == "mun") and state != "br":
                continue
            
            if info.state and state != info.state:
                continue

            yield info, filedate
