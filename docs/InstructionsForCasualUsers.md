The intention of this page is to provide a few simple instructions that new or casual users can use to quickly demonstrate the operation of a small MiniDAQ system that uses emulators instead of real electronics.

The steps fall into a few general categories:
1. setup the environment
2. generate the sample system configuration
3. use _nanorc_ to run the sample system

These steps draw on more detailed instructions in other repositories, for example, _daq-buildtools_ and _nanorc_.

1. log into a system that has access to `/cvmfs/dunedaq.opensciencegrid.org/`
2. `source /cvmfs/dunedaq.opensciencegrid.org/setup_dunedaq.sh`
3. `setup_dbt dunedaq-v2.6.0`
4. `dbt-create.sh dunedaq-v2.6.0 <work_dir>`
5. `cd <work_dir>`
6. `dbt-workarea-env`
7. `pip install https://github.com/DUNE-DAQ/nanorc/archive/refs/tags/dunedaq-v2.6.0.tar.gz`
8. download a raw data file ([CERNBox link](https://cernbox.cern.ch/index.php/s/VAqNtn7bwuQtff3/download)) and put it into `<work_dir>`
9. `python -m minidaqapp.nanorc.mdapp_gen -d ./frames.bin -o . -s 10 mdapp_fake`
11. `nanorc mdapp_fake boot init conf start 101 wait 2 resume wait 60 pause wait 2 stop scrap terminate`
12. examine the contents of the HDf5 file with commands like the following:
   * `h5dump-shared -H -A swtest_run000101_0000_*.hdf5`
   * and
   * `python3 $DFMODULES_FQ_DIR/dfmodules/bin/hdf5dump/hdf5_dump.py -p both -f swtest_run000101_0000_*.hdf5`
