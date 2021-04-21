#!/bin/bash

sourcecode_subdir_name="sourcecode"
dunedaq_release_version="v2.4.0"

current_subdir=`echo ${PWD} | xargs basename`
#echo "cwd = ${current_subdir}"

# Verify that we're in a good location to do the repo clones
if [[ "$current_subdir" != "$sourcecode_subdir_name" ]]; then
    if [[ -d "$sourcecode_subdir_name" ]]; then
        cd ${sourcecode_subdir_name}
    else
        echo "*** Warning: this script needs to be run in a newly-created dunedaq software area."
        echo "*** Warning: unable to find a subdirectory named \"${sourcecode_subdir_name}\", exiting."
        return 1
    fi
fi

# Define a function to handle the clone of a single repo.
# There are two required arguments: repo name and initial branch.
# A third, optional, argument is a commit hash or tag to checkout.
function clone_repo_for_mdapp {
    if [[ $# -lt 2 ]]; then
        return 1
    fi
    git clone https://github.com/DUNE-DAQ/${1}.git -b ${2}
    if [[ $# -gt 2 ]]; then
        cd ${1}
        git checkout ${3}
        cd ..
    fi
}

# Clone the repos that we want


clone_repo_for_mdapp dataformats develop v2.0.0
clone_repo_for_mdapp dfmessages develop v2.0.0
clone_repo_for_mdapp dfmodules develop v2.0.2
clone_repo_for_mdapp flxlibs develop v1.0.0
clone_repo_for_mdapp ipm develop v2.0.1
clone_repo_for_mdapp nwqueueadapters develop v1.2.0
clone_repo_for_mdapp readout develop v1.2.0
clone_repo_for_mdapp serialization develop v1.1.0
clone_repo_for_mdapp trigemu develop v2.1.0
clone_repo_for_mdapp minidaqapp develop v2.1.1
cd ..

# Clone the nanorc repo, if needed
if [[ -d "$sourcecode_subdir_name" ]]; then
    echo "*** Note: the nanorc package has already been cloned and will not be cloned again."
else
    git clone https://github.com/DUNE-DAQ/nanorc.git -b v1.1.1
    dbt-setup-build-environment
    pip install -r nanorc/requirements.txt
fi
