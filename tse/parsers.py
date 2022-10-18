import datetime

from tse.common.pathinfo import PathInfo


def get_dh_timestamp(data, d = "dg", h = "hg"):
    return datetime.datetime.strptime(data[d] + data[h], "%d/%m/%Y%H:%M:%S")

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
    #   Only Sem arquivo doesn't have actual files (hash = "0")
    
    @staticmethod
    def get_files(data):
        if data["st"] not in ["Totalizada", "Recebida"]:
            return (None, None, None)

        valid_hashes = [h for h in data["hashes"] if h["st"] in ["Totalizado", "Recebido"] and h["hash"] != "0"]
        if len(valid_hashes) == 1:
            return (valid_hashes[0]["hash"],  get_dh_timestamp(valid_hashes[0], "dr", "hr"), valid_hashes[0]["nmarq"])

        # Not sure if there's a situation of more than one valid hash, but opt for newest one in the case
        sorted_valid_hashes = sorted(valid_hashes, 
            key=lambda h: get_dh_timestamp(h, "dr", "hr"),
            reverse=True)

        if len(sorted_valid_hashes) > 0:
            return (sorted_valid_hashes[0]["hash"],  get_dh_timestamp(sorted_valid_hashes[0], "dr", "hr"), sorted_valid_hashes[0]["nmarq"])

        return (None, None, None)
        
    @staticmethod
    def expand_all_files(data):
        for hash in data["hashes"]:
            if hash["hash"] == "0":
                continue
            
            yield (hash["hash"], get_dh_timestamp(hash, "dr", "hr"), hash["nmarq"])
