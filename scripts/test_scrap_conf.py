# test_scrap_conf.py - check that a second conf after scrap works

import pytest

import data_file_checks
import log_file_checks

# The name of the python module for the config generation
confgen_name="minidaqapp.nanorc.mdapp_multiru_gen"
# The arguments to pass to the config generator, excluding the json
# output directory (the test framework handles that)
confgen_arguments=[ "-o", ".", "-s", "10", "-n", "2"]
# The commands to run in nanorc, as a list
nanorc_command_list="boot init conf start 1 resume wait 10 stop scrap conf start 2 resume wait 10 stop scrap terminate".split()

def test_nanorc_success(run_nanorc):
    assert run_nanorc.completed_process.returncode==0

def test_log_files(run_nanorc):
    assert log_file_checks.logs_are_error_free(run_nanorc.log_files)

def test_data_files(run_nanorc):
    # We did 2 runs, so there should be 2 output files
    assert len(run_nanorc.data_files)==2

    for data_file_path in run_nanorc.data_files:
        data_file=data_file_checks.DataFile(data_file_path)
        assert data_file_checks.sanity_check(data_file)
        assert data_file_checks.check_link_presence(data_file, n_links=1)
        assert data_file_checks.check_fragment_sizes(data_file, min_frag_size=22344, max_frag_size=22344)


