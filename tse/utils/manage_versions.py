import argparse
import os
import shutil
import zipfile

from zipfile_remove import *


def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)
            
def getargs():
    parser = argparse.ArgumentParser(description="Manages version directories")
    subparsers = parser.add_subparsers(help="Command", dest="command", required=True)
    pack = subparsers.add_parser("pack", help="Packs the files inside '.ver' directories in a single zip per dir")
    pack.add_argument("paths", type=dir_path, nargs='+', help="Paths to scan for version folders (ex: data/download/oficial/ele2022/[0-9]*/**)")
    pack.add_argument("--remove", action=argparse.BooleanOptionalAction, help="Delete files after they are packed")
    return parser.parse_args()

def zip_root_files(zip):
    for info in zip.infolist():
        if not info.is_dir() and not "/" in info.filename:
            yield info.filename

def pack(ver_dir, files):
    zippable_files = [f for f in files if not f.startswith(".") and os.path.splitext(f)[1] != ".zip"]
    if len(zippable_files) == 0:
        return

    backup_path = os.path.join(ver_dir, ".bpk_pack.zip")
    if os.path.exists(backup_path):
        os.remove(backup_path)
    
    zip_path = os.path.join(ver_dir, "pack.zip")
    if os.path.exists(zip_path):
        shutil.copyfile(zip_path, backup_path)

    try:
        print(f"    pack.zip")
        with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zip:
            zip_files = set(zip_root_files(zip))
            
            for file in zippable_files:
                if file in zip_files:
                    zip.remove(file)
                    print(f"      - {file}")
                
                print(f"      + {file}")
                zip.write(os.path.join(ver_dir, file), file)
    except Exception as e:
        if os.path.exists(backup_path):
            os.remove(zip_path)
            os.rename(backup_path, zip_path)
        raise
    finally:
        if os.path.exists(backup_path):
            os.remove(backup_path)

    if args.remove:
        for file in zippable_files:
            print(f"    - {file}")
            os.remove(os.path.join(ver_dir, file))

args = getargs()

for root in args.paths:
    print(f"{root}")

    for path, dirs, files in os.walk(root):
        if os.path.basename(path) != ".ver":
            continue
        
        print(f"  {os.path.relpath(path, root)}")

        if args.command == "pack":
           pack(path, files)