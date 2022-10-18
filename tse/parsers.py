import datetime
import logging

from tse.common.pathinfo import PathInfo


class IndexParser: 
    @staticmethod
    def expand(state, data):
        for entry in data["arq"]:
            filename = entry["nm"]
            filedate = datetime.datetime.strptime(entry["dh"], "%d/%m/%Y %H:%M:%S")

            if filename == "ele-c.json":
                continue

            info = PathInfo(filename)
            if (info.prefix == "cert" or info.prefix == "mun") and state != "br":
                continue
            
            if info.state and state != info.state:
                continue

            yield info, filedate

class FixedParser:
    @staticmethod
    def expand_candidates(data):
        for agr in data["carg"]["agr"]:
            for par in agr["par"]:
                for cand in par["cand"]:
                    yield cand

class SectionsConfigParser:
    @staticmethod
    def expand_sections(data):
        for mu in data["abr"][0]["mu"]:
            city = mu["cd"].lstrip("0")
            for zon in mu["zon"]:
                zone = zon["cd"].lstrip("0")
                for sec in zon["sec"]:
                    yield (city, zone, sec["ns"].lstrip("0"))


class SectionAuxParser:

    # data["st"] Totalizada, Recebida, Anulada, Não instalada

    # hash["st"] Totalizado, Recebido, Excluído, Rejeitado, Sem arquivo
    # Only Sem arquivo doesn't have actual files hash = "0"
    
    @staticmethod
    def expand_files(data):
        for hash in data["hashes"]:
            if not hash["st"] == "0":
                continue

            for filename in hash["nmarq"]:
                yield (hash["hash"], filename)
