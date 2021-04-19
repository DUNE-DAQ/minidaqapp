#!/bin/bash

sourcecode_subdir_name="sourcecode"
dunedaq_release_version="v2.4.0"

current_subdir=`echo ${PWD} | xargs basename`
#echo "cwd = ${current_subdir}"

# First, check if dbt-create.sh needs to be run.
# If the dbt-create script can't be found, we simply go on to the parts that we *can* do.
#dbt_create_path=`which dbt-create.sh`
#if [[ "$dbt_create_path" != "" ]]; then
#    if [[ "$current_subdir" != "$sourcecode_subdir_name" ]]; then
#        if [[ ! -d "$sourcecode_subdir_name" ]]; then
#            echo ""
#            echo "*** It appears that dbt-create.sh has not yet been run in the current directory."
#            echo "*** (current working directory = \"$PWD\")"
#            echo "*** Do you want to run it now (y/n/a [default is \"a\" for \"abort this script\"])"
#            read response
#            if [[ "$response" == "y" ]]; then
#                dbt-create.sh dunedaq-${dunedaq_release_version}
#            elif [[ "$response" == "n" ]]; then
#                echo "Skipping dbt-create..."
#            else
#                return 1
#            fi
#        fi
#    fi
#fi

# Next, verify that we're in a good location to do the repo clones
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

## Next, update the dbt-build-order.cmake file
#cd sourcecode
#cp -p dbt-build-order.cmake dbt-build-order.cmake.orig
#sed -i 's/"readout" "trigemu"/"readout" "flxlibs" "trigemu"/' dbt-build-order.cmake

## Next, update the dbt-settings file
#cd ..
#cp -p dbt-settings dbt-settings.orig
#sed -i 's,#"/cvmfs/dune.opensciencegrid.org/dunedaq/DUNE/products","/cvmfs/dune.opensciencegrid.org/dunedaq/DUNE/products",' dbt-setting#s
#sed -i 's,#"/cvmfs/dune.opensciencegrid.org/dunedaq/DUNE/products_dev","/cvmfs/dune.opensciencegrid.org/dunedaq/DUNE/products_dev",' dbt#-settings
#sed -i 's/"msgpack_c v3_3_0 e19:prof"/"msgpack_c v3_3_0 e19:prof"\n    "felix v1_1_0 e19:prof"/' dbt-settings
