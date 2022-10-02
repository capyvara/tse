import re
import datetime

class FileInfo:
    _regex = re.compile(r"^(?P<prefix>cert|mun)?(?P<state>\w{2})?(?P<mun>\d{5})?(?:-c(?P<cand>\d{4}))?-e(?P<election>\d{6})(?:-(?P<ver>\d{3}))?-(?P<type>\w{1,2}?)\.(?P<ext>\w+)")

    def __init__(self, filename):
        self.filename = filename

        if filename == "ele-c.json":
            self.path = f"comum/config/{filename}"
            self.type = "c"
            self.ext = "json"
            return

        result = type(self)._regex.match(filename)
        if result:
            self.prefix =result["prefix"] 
            self.state = result["state"]
            self.mun = result["mun"].lstrip("0") if result["mun"] else None
            self.cand = result["cand"].lstrip("0") if result["cand"] else None
            self.election = result["election"].lstrip("0") if result["election"] else None
            self.ver = result["ver"].lstrip("0") if result["ver"] else None
            self.type = result.group("type")
            self.ext = result.group("ext")

            if self.prefix == "cert" or self.prefix == "mun":
                self.path = f"{self.election}/config/{filename}"
            elif self.type == "i":
                self.path = f"{self.election}/config/{self.state}/{filename}"
            elif self.type == "r":
                self.path = f"{self.election}/dados-simplificados/{self.state}/{filename}"
            else:
                self.path = f"{self.election}/dados/{self.state}/{filename}"
        else:
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
