import os
import zipfile
import logging
import dask
import dask.bag as dab
import dask.dataframe as daf
import dask.distributed
from distributed.utils_perf import disable_gc_diagnosis
from distributed import Client, LocalCluster, progress, get_worker
import pandas as pd
from tse.common.voting_machine_files import VotingMachineFiles, VotingMachineLogProcessor
from tse.utils import log_progress

pd.options.mode.string_storage = "pyarrow"
DOWNLOAD_DIR = "data/download/dadosabertos/transmitted"

def scan_dir(dir):
    with os.scandir(dir) as it:
        for entry in it:
            if not entry.is_file() or entry.name.startswith('.'): 
                continue

            if os.path.splitext(entry.name)[1] == ".zip":
                with zipfile.ZipFile(entry.path, "r") as zip:
                    for info in zip.infolist():
                        if not info.is_dir() and not info.filename == "leiame.pdf":
                            yield os.path.join(entry.name, info.filename)
                #break
                continue

            yield entry.name

def part(partition, partition_info=None):
    if not partition_info:
        return partition

    logger = logging.getLogger("distributed.worker")

    df = (
        partition.reset_index().
        assign(zip_filename=lambda p: p["index"].apply(os.path.dirname)).
        assign(log_filename=lambda p: p["index"].apply(os.path.basename)).
        drop(columns="index").
        rename(columns={"paths": "extensions"}).
        set_index(["zip_filename", "log_filename"]).sort_index()
    )

    log_processor = VotingMachineLogProcessor()

    for zip_filename, group in df.groupby(level=0):
        zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)
        logger.info("%s | Opened zip %s", get_worker().name, zip_filename)
        with zipfile.ZipFile(zip_path, "r") as zip:
            for entry in group.itertuples(True):
                log_ext = VotingMachineFiles.get_voting_machine_files_map(entry.extensions)[VotingMachineFiles.FileType.LOG]
                log_filename = entry.Index[1] + log_ext
                with zip.open(log_filename) as file:
                    for filename, bio in log_processor.read_compressed_logs(file, os.path.join(zip_filename, log_filename)):
                        for row in log_processor.parse_log(bio, filename):
                            continue

    return partition

def collect_all_files() -> pd.Series:
    expected_total = 2360133
    logging.info("Scanning %s...", DOWNLOAD_DIR)
    all_files = pd.Series(log_progress(scan_dir(DOWNLOAD_DIR), total=expected_total), dtype=pd.StringDtype(), name="paths")
    logging.info("Found %d files", len(all_files))
    
    logging.info("Grouping...")
    all_files = all_files.groupby(lambda x: os.path.splitext(all_files[x])[0]).apply(list)
    all_files = all_files.map(lambda l: [os.path.splitext(x)[1] for x in l])
    logging.info("Grouped into %d items", len(all_files))
    return all_files

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cluster = LocalCluster(n_workers=12, threads_per_worker=2, silence_logs=logging.INFO)
    client = Client(cluster)
    disable_gc_diagnosis()
    logging.info("Init client: %s, dashboard: %s", client, client.dashboard_link)
    
    #dask.config.set(scheduler='single-threaded') # Debug

    all_files = daf.from_pandas(collect_all_files(), chunksize=1000)
    a = all_files.map_partitions(part)
    x = a.persist()
    progress(x)
    x.compute()
















    # client.close()

    # def get_log(x, y):
    #     return VotingMachineFiles.get_voting_machine_files_map(y)[VotingMachineFiles.FileType.LOG]

    # def addfile(x, y):
    #     ret = x + (y,)
    #     print(ret)
    #     return ret

    # df = all_files

    #ret = all_files.groupby(lambda x: os.path.splitext(x)[0], shuffle="disk")

    # ret = all_files.foldby(lambda x: os.path.splitext(x)[0], 
    #     addfile, initial=(), 
    #     combine=get_log, combine_initial="").compute()

# from tqdm import tqdm
# from scrapy.utils.project import get_project_settings
# from tse.common.pathinfo import PathInfo
# import pandas as pd
# import tse.common.voting_machine_files as vmf

# pd.options.mode.string_storage = "pyarrow"

# settings = get_project_settings()
# plea = settings["PLEA"]
# elections = settings["ELECTIONS"]
# states= settings["STATES"]

# df = pd.read_parquet("data/all_sections_1st_round.parquet")```

# print(len(df[df["hash"].isna()]))

# def message_cat():
#     message_categories = {}
#     count = -1

#     log_processor = vmf.VotingMachineLogProcessor()

#     def dump():
#         df_message_categories = pd.DataFrame.from_dict(message_categories, orient="index", columns=["count"]).sort_values(by="count", ascending=False)
#         df_message_categories.index.name = "message"
#         df_message_categories.to_csv(f"data/temp/all_message_categories_2.tsv", index=True, sep="\t", quotechar="'")

#     #for index, row in tqdm(df.iloc[13000:].iterrows(), total=472075):
#     for index, row in tqdm(df.iterrows(), total=472075):
#         count += 1

#         if pd.isna(row["hash"]):
#             continue

#         log_filename = vmf.get_voting_machine_files_map(row["files"]).getvmf.VotingMachineFileType.LOG, None)
#         if not log_filename:
#             continue

#         log_path = PathInfo.get_voting_machine_file_path(plea, *index, row["hash"], log_filename)    

#         for filename, bio in log_processor.read_compressed_logs(PathInfo.get_local_path(settings, log_path)):
#             for row in log_processor.parse_log(filename, bio):
#                 if row.message_params:
#                     message_categories[row.raw_message] = message_categories.get(row.raw_message, 0) + 1

#         if count % 100 == 0:
#             dump()

#     dump()

# message_cat()
