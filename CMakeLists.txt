cmake_minimum_required(VERSION 3.12)
project(minidaqapp VERSION 2.1.1)

find_package(daq-cmake REQUIRED )

daq_setup_environment()

daq_install()
