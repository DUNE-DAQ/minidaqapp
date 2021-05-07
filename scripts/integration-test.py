import sys
import tempfile
import os
import subprocess
import os.path
import time
import shutil
from glob import glob

import data_file_checks
import log_file_checks

def setup(tmpdir):
    # TODO: Find a better way to locate frames.bin. This way we have to run the script from a directory containing frames.bin
    shutil.copy("frames.bin", os.path.join(tmpdir, "frames.bin"))
    print(f"Switching to temporary directory {tmpdir}")
    os.chdir(tmpdir)

def create_json_files(module_name, module_arguments, json_dir):
    print("Creating json files")

    try:
        subprocess.run(["python"] + ["-m"] + [module_name] + module_arguments + [json_dir], check=True)
    except subprocess.CalledProcessError as err:
        print(f"Generating json files failed with exit code {err.returncode}")
        sys.exit(1)

def run_nanorc(json_dir, command_list):
    nanorc=os.path.join(os.getenv("DBT_AREA_ROOT"), "sourcecode/nanorc/nanorc.py")
    try:
        subprocess.run([nanorc] + [json_dir] + command_list, check=True)
    except subprocess.CalledProcessError as err:
        print(f"Running nanorc failed with exit code {err.returncode}")
        sys.exit(1)
    
if __name__=="__main__":                
    with tempfile.TemporaryDirectory() as tmpdir:
        json_dir="json"
        
        setup(tmpdir)
        create_json_files("minidaqapp.nanorc.mdapp_multiru_gen", [ "-d", "./frames.bin", "-o", ".", "-s", "10", "-n", "2"], json_dir)
        run_nanorc(json_dir, "boot init conf start 1 resume wait 10 stop scrap terminate".split())

        all_ok=True
        all_ok=all_ok and log_file_checks.logs_are_error_free()

            
        data_file_name=glob("swtest_*.hdf5")
        n_files=len(data_file_name)
        if n_files==1:
            data_file=data_file_checks.DataFile(data_file_name[0])
            print(f"Output file {data_file_name[0]} with {data_file.n_events} events")
            all_ok=all_ok and data_file_checks.sanity_check(data_file)
            all_ok=all_ok and data_file_checks.check_link_presence(data_file, 1)
            all_ok=all_ok and data_file_checks.check_fragment_sizes(data_file, 22344, 22344)
        else:
            print(f"There are {n_files} files instead of 1")
            all_ok=False

        if all_ok:
            print("Test ran successfully")
        else:
            print("Test was unsuccessful")
            
        print(f"Press RETURN to exit and delete temp dir {tmpdir}")
        sys.stdin.readline()

        if all_ok:
            sys.exit(0)
        else:
            sys.exit(1)
