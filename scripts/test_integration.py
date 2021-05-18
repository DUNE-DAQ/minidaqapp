# test_integration.py - example integration test
#
# An example integration test using the pytest framework
#
# Prerequisites:
#
# > cd minidaqapp/python/minidaqapp/integration_tests
# > pip install -r requirements.txt # Install pytest
#
# Usage:
#
# > cd minidaqapp/scripts # (This directory)
# > pytest -s test_integration.py --frame-file /path/to/frames.bin
#
# to run just this test, or to run all of the tests in this directory:
#
# > cd minidaqapp/scripts # (This directory)
# > pytest -s . --frame-file /path/to/frames.bin
#
# Remove "-s" from the options to suppress output from confgen and nanorc as they run
#
# If your nanorc is not checked out in $DBT_AREA_ROOT/sourcecode, you
# can specify the location of nanorc.py with the --nanorc-path option
#
# pytest finds the --frame-file option and the `run_nanorc` fixture
# used in these tests via the conftest.py file in this directory. If
# you want to make a test directory in another package, you'll have to
# copy that file there too
import pytest

import integrationtest.data_file_checks as data_file_checks
import integrationtest.log_file_checks as log_file_checks

# The next three variables must be present as globals in the test
# file. They're read by the "fixtures" in conftest.py to determine how
# to run the config generation and nanorc

# The name of the python module for the config generation
confgen_name="minidaqapp.nanorc.mdapp_multiru_gen"
# The arguments to pass to the config generator, excluding the json
# output directory (the test framework handles that)
confgen_arguments=[ "-o", ".", "-s", "10", "-n", "2"]
# The commands to run in nanorc, as a list
nanorc_command_list="boot init conf start 1 resume wait 10 stop scrap terminate".split()


# Each condition you want to test after running nanorc should appear
# as an `assert` inside a function whose name begins with `test_` and
# which takes `run_nanorc` as an argument.
#
# The `run_nanorc` argument in the function refers to the return value
# of the run_nanorc fixture from conftest.py, which has attributes:
#
# completed_process: subprocess.CompletedProcess object with the output of the nanorc process
# run_dir:           pathlib.Path pointing to the directory in which nanorc was run
# json_dir:          pathlib.Path pointing to the directory in which the run configuration json files are stored
# data_files:        list of pathlib.Path with each of the HDF5 data files produced by the run
# log_files:         list of pathlib.Path with each of the log files produced by the run
# opmon_files:       list of pathlib.Path with each of the opmon json files produced by the run


def test_nanorc_success(run_nanorc):
    assert run_nanorc.completed_process.returncode==0
    
def test_data_file(run_nanorc):
    assert len(run_nanorc.data_files)==1

    data_file=data_file_checks.DataFile(run_nanorc.data_files[0])
    assert data_file_checks.sanity_check(data_file)
    assert data_file_checks.check_link_presence(data_file, n_links=1)
    assert data_file_checks.check_fragment_sizes(data_file, min_frag_size=22344, max_frag_size=22344)


def test_log_files(run_nanorc):
    assert log_file_checks.logs_are_error_free(run_nanorc.log_files)
