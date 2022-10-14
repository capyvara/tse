import argparse
import logging
import os
import shutil
import time
import zipfile

from zipfile_remove import *


def getargs():
    def add_path_arg(parser):
        def dir_path(string):
            if os.path.isdir(string):
                return string
            else:
                raise NotADirectoryError(string)

        parser.add_argument("path", type=dir_path, nargs='+', help="Paths to scan for version folders (ex: data/download/oficial/ele2022/[0-9]*/**)")

    parser = argparse.ArgumentParser(description="Manages version directories")
    parser.add_argument('-v', '--verbose',
        action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.INFO,
        help="Be verbose",
    )
    
    subparsers = parser.add_subparsers(help="Command", dest="command", required=True)
    
    pack = subparsers.add_parser("pack", help="Packs the files inside '.ver' directories in a single zip per dir")
    add_path_arg(pack)
    pack.add_argument("--keep", action="store_true", help="Keep original files after they are packed (copy, not move)")

    unpack = subparsers.add_parser("unpack", help="Unpack the files from the packs inside '.ver' dirs")
    add_path_arg(unpack)
    unpack.add_argument("--keep", action="store_true", help="Keep zip files")

    return parser.parse_args()

def zip_root_files(zip):
    for info in zip.infolist():
        if not info.is_dir() and not "/" in info.filename:
            yield info.filename

def pack(ver_dir, files):
    zippable_files = sorted([f for f in files if not f.startswith(".") and os.path.splitext(f)[1] != ".zip"])
    if len(zippable_files) == 0:
        return

    backup_path = os.path.join(ver_dir, ".bpk__pack.zip")
    if os.path.exists(backup_path):
        os.remove(backup_path)
    
    zip_path = os.path.join(ver_dir, "_pack.zip")
    if os.path.exists(zip_path):
        shutil.copyfile(zip_path, backup_path)

    try:
        logging.debug(f"    @ _pack.zip")
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zip:
            zipped_files = set(zip_root_files(zip))
            
            for file in zippable_files:
                if file in zipped_files:
                    zip.remove(file)
                    logging.debug(f"      - {file}")
                
                logging.debug(f"      + {file}")
                zip.write(os.path.join(ver_dir, file), file)
    except Exception as e:
        if os.path.exists(backup_path):
            os.remove(zip_path)
            os.rename(backup_path, zip_path)
        raise
    finally:
        if os.path.exists(backup_path):
            os.remove(backup_path)

    if not args.keep:
        for file in zippable_files:
            logging.debug(f"    - {file}")
            os.remove(os.path.join(ver_dir, file))

def unpack(ver_dir, files):
    zip_files = sorted([f for f in files if not f.startswith(".") and os.path.splitext(f)[1] == ".zip"])
    if len(zip_files) == 0:
        return

    for zip_file in zip_files:
        logging.debug(f"    @ {zip_file}")
        zip_path = os.path.join(ver_dir, zip_file)

        with zipfile.ZipFile(zip_path, "r") as zip:
            for zipinfo in zip.infolist():
                logging.debug(f"    + {zipinfo.filename}")
                zip.extract(zipinfo, ver_dir)
                
                # Restore original mod time
                fullpath = os.path.join(ver_dir, zipinfo.filename)
                date_time = time.mktime(zipinfo.date_time + (0, 0, -1))
                os.utime(fullpath, (date_time, date_time))                

            
    if not args.keep:
        for zip_file in zip_files:
            logging.debug(f"    - {zip_file}")
            os.remove(os.path.join(ver_dir, zip_file))

args = getargs()
logging.basicConfig(level=args.loglevel, format="%(message)s")

for root in args.path:
    logging.info(f"{root}")

    for path, dirs, files in os.walk(root):
        if os.path.basename(path) != ".ver":
            continue
        
        logging.info(f"  {os.path.relpath(path, root)}")

        if args.command == "pack":
           pack(path, files)
        elif args.command == "unpack":
            unpack(path, files)