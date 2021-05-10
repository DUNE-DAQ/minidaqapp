import pytest
import os
from glob import glob

import data_file_checks
import log_file_checks

confgen_name="minidaqapp.nanorc.mdapp_multiru_gen"
confgen_arguments=[ "-o", ".", "-s", "10", "-n", "2"]
nanorc_command_list="boot init conf start 1 resume wait 10 stop scrap terminate".split()

def test_nanorc_success(run_nanorc):
    assert run_nanorc.completed_process.returncode==0
    
def test_integration(run_nanorc):
    # os.chdir(setup_dirs[0])
    # data_file_name=glob("swtest_*.hdf5")
    assert len(run_nanorc.data_files)==1

    data_file=data_file_checks.DataFile(run_nanorc.data_files[0])
    assert data_file_checks.sanity_check(data_file)
    assert data_file_checks.check_link_presence(data_file, 1)
    assert data_file_checks.check_fragment_sizes(data_file, 22344, 22344)


def test_log_files(run_nanorc):
    # os.chdir(setup_dirs[0])
    assert log_file_checks.logs_are_error_free(run_nanorc.log_files)
