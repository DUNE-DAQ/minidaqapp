The intention of this page is to provide a few simple instructions that new or casual users can use to quickly demonstrate the operation of a small MiniDAQ system that uses emulators instead of real electronics.

The expected steps will be something like the following
1. setup the environment
2. generate the sample system configuration
3. use _nanorc_ to run the sample system

There is a bit of a chicken-and-egg problem, though, because it will be best to document those steps once the v2.6.0 release is complete, but we want to provide some documentation before the release is done.  

To help give a flavor of what is to come, though, here are the similar steps that one would use for a v2.4.0-based system:
1. log into a system that has access to `/cvmfs/dunedaq.opensciencegrid.org/`
2. `source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh`
3. `setup_dbt dunedaq-v2.4.0`
4. `cd <work_dir>`
5. `dbt-workarea-env`
