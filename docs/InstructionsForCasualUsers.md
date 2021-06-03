The intention of this page is to provide a few simple instructions that new or casual users can use to quickly demonstrate the operation of a small MiniDAQ system that uses emulators instead of real electronics.

The steps fall into a few general categories, and they draw on more detailed instructions from other repositories, for example, _daq-buildtools_ and _nanorc_.
1. setup the environment
2. generate the sample system configuration
3. use _nanorc_ to run the sample system

Here are the steps that should be used when you first create your local software working area (i.e. `<work_dir>`):

1. log into a system that has access to `/cvmfs/dunedaq.opensciencegrid.org/`
2. `source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh`
3. `setup_dbt dunedaq-v2.6.0`
4. `dbt-create.sh dunedaq-v2.6.0 <work_dir>`
5. `cd <work_dir>`
6. `dbt-workarea-env`
9. download a raw data file ([CERNBox link](https://cernbox.cern.ch/index.php/s/VAqNtn7bwuQtff3/download)) and put it into `<work_dir>`
10. `python -m minidaqapp.nanorc.mdapp_multiru_gen -d ./frames.bin -o . -s 10 mdapp_fake`
11. `nanorc mdapp_fake boot init conf start 101 wait 2 resume wait 60 pause wait 2 stop scrap terminate`
12. examine the contents of the HDf5 file with commands like the following:
   * `h5dump-shared -H -A swtest_run000101_0000_*.hdf5`
   * and
   * `python3 $DFMODULES_FQ_DIR/dfmodules/bin/hdf5dump/hdf5_dump.py -p both -f swtest_run000101_0000_*.hdf5`

When you return to this work area (for example, after logging out and back in), you can skip the 'setup' steps in the instructions above.  For example:

1. `cd <work_dir>`
2. `source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh`
3. `setup_dbt dunedaq-v2.6.0`
4. `dbt-workarea-env`
7. `nanorc mdapp_fake boot init conf start 102 wait 2 resume wait 60 pause wait 2 stop scrap terminate`

If/when you want to make code changes in one of the repositories that are part of the MiniDAQ suite, you can clone that package into the /sourcecode subdirectory in your work area and proceed from there.  For example:
1. `cd <work_dir>`
2. `source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh`
3. `setup_dbt dunedaq-v2.6.0`
4. `dbt-workarea-env`
5. `cd sourcecode`
6. `git clone https://github.com/DUNE-DAQ/minidaqapp.git -b dunedaq-v2.6.0` # or `-b develop` or `-b <whatever>`
7.  `cd ..`
8.  `dbt-build.sh`
9.  etc.

  

