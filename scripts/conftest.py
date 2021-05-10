import pytest
import shutil
import subprocess
import os.path
import os

def pytest_addoption(parser):
    parser.addoption(
        "--frame-file", action="store", help="Path to frame file", required=True
    )

@pytest.fixture(scope="session")
def setup_dirs(request, tmp_path_factory):
    run_dir=tmp_path_factory.mktemp("rundir")
    frame_path=request.config.getoption("--frame-file")
    os.symlink(frame_path, run_dir.joinpath("frames.bin"))
    class Dirs:
        pass
    dirs=Dirs()
    dirs.run_dir=run_dir
    dirs.json_dir=tmp_path_factory.getbasetemp() / "json"
    yield dirs

@pytest.fixture(scope="module")
def create_json_files(request, setup_dirs):
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
    command_list=getattr(request.module, "nanorc_command_list")
    run_dir=setup_dirs.run_dir
    json_dir=setup_dirs.json_dir
    nanorc=os.path.join(os.getenv("DBT_AREA_ROOT"), "sourcecode/nanorc/nanorc.py")

    class RunResult:
        pass
    
    try:
        result=RunResult()
        result.completed_process=subprocess.run([nanorc] + [str(json_dir)] + command_list, cwd=run_dir)
        result.run_dir=run_dir
        result.json_dir=json_dir
        result.data_files=list(run_dir.glob("swtest_*.hdf5"))
        result.log_files=list(run_dir.glob("log_*.txt"))
        result.opmon_files=list(run_dir.glob("info_*.json"))
        yield result
    except subprocess.CalledProcessError as err:
        print(f"Running nanorc failed with exit code {err.returncode}")
        pytest.fail()
