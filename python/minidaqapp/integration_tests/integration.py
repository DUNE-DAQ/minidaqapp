import pytest
import shutil
import subprocess
import os.path
import os

def pytest_addoption(parser):
    parser.addoption(
        "--frame-file", action="store", help="Path to frame file", required=True
    )

@pytest.fixture(scope="module")
def setup_dirs(request, tmp_path_factory):
    """Create the temporary directory to run nanorc in, and put the frame data file in it"""
    run_dir=tmp_path_factory.mktemp(request.module.__name__+"_run")
    frame_path=request.config.getoption("--frame-file")
    os.symlink(frame_path, run_dir.joinpath("frames.bin"))
    class Dirs:
        pass
    dirs=Dirs()
    dirs.run_dir=run_dir
    # Form the name of the json directory, but don't actually create
    # it, because the confgen script checks that it doesn't already
    # exist
    p=tmp_path_factory.mktemp(request.module.__name__)
    dirs.json_dir=p / "json"
    yield dirs

@pytest.fixture(scope="module")
def create_json_files(request, setup_dirs):
    """Run the confgen to produce the configuration json files

    The name of the module to use is taken from the `confgen_name`
    variable in the global scope of the test module, and the arguments
    for the confgen are taken from the `confgen_arguments` variable in
    the same place

    """
    print("Creating json files")
    module_name=getattr(request.module, "confgen_name")
    module_arguments=getattr(request.module, "confgen_arguments")
    try:
        subprocess.run(["python", "-m"] + [module_name] + module_arguments + [str(setup_dirs.json_dir)], check=True)
    except subprocess.CalledProcessError as err:
        print(f"Generating json files failed with exit code {err.returncode}")
        pytest.fail()

@pytest.fixture(scope="module")
def run_nanorc(request, create_json_files, setup_dirs):
    """Run nanorc with the json files created by `create_json_files`. The
    commands specified by the `nanorc_command_list` variable in the
    test module are executed

    """
    command_list=getattr(request.module, "nanorc_command_list")
    run_dir=setup_dirs.run_dir
    json_dir=setup_dirs.json_dir
    nanorc=os.path.join(os.getenv("DBT_AREA_ROOT"), "sourcecode/nanorc/nanorc.py")

    class RunResult:
        pass
    
    result=RunResult()
    result.completed_process=subprocess.run([nanorc] + [str(json_dir)] + command_list, cwd=run_dir)
    result.run_dir=run_dir
    result.json_dir=json_dir
    result.data_files=list(run_dir.glob("swtest_*.hdf5"))
    result.log_files=list(run_dir.glob("log_*.txt"))
    result.opmon_files=list(run_dir.glob("info_*.json"))
    yield result
