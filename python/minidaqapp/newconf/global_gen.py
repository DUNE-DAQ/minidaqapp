import json
import os
import math
import sys
import glob
import rich.traceback
from rich.console import Console
from os.path import exists, join
from appfwk.system import System
from appfwk.conf_utils import AppConnection, add_network, make_app_command_data

CLOCK_SPEED_HZ = 50000000

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')
import dunedaq.networkmanager.nwmgr as nwmgr

import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-p', '--partition-name', default="global", help="Name of the partition to use, for ERS and OPMON")
@click.option('--disable-trace', is_flag=True, help="Do not enable TRACE (default TRACE_FILE is /tmp/trace_buffer_\${HOSTNAME}_\${USER})")
@click.option('--host-thi', default='localhost', help='Host to run the (global) timing hardware interface app on')
@click.option('--port-thi', default=12345, help='Port to host running the (global) timing hardware interface app on')
@click.option('--host-tmc', default='localhost', help='Host to run the (global) timing master controller app on')
@click.option('--timing-hw-connections-file', default="${TIMING_SHARE}/config/etc/connections.xml", help='Path to timing hardware connections file')
@click.option('--opmon-impl', type=click.Choice(['json','cern','pocket'], case_sensitive=False),default='json', help="Info collector service implementation to use")
@click.option('--ers-impl', type=click.Choice(['local','cern','pocket'], case_sensitive=False), default='local', help="ERS destination (Kafka used for cern and pocket)")
@click.option('--pocket-url', default='127.0.0.1', help="URL for connecting to Pocket services")
@click.option('--hsi-device-name', default="", help='Real HSI hardware only: device name of HSI hw')
@click.option('--master-device-name', default="", help='Device name of timing master hw')
@click.argument('json_dir', type=click.Path())

def cli(partition_name, disable_trace, host_thi, port_thi, host_tmc, timing_hw_connections_file, opmon_impl, ers_impl, pocket_url, hsi_device_name, master_device_name, json_dir):

    if exists(json_dir):
        raise RuntimeError(f"Directory {json_dir} already exists")

    console.log("Loading timing hardware config generator")
    from .thi_gen import THIApp

    console.log("Loading timing master controller generator")
    from .tmc_gen import TMCApp

    console.log(f"Generating configs for global thi host {host_thi}")

    the_system = System(partition_name, first_port=port_thi)
   
    if opmon_impl == 'cern':
        info_svc_uri = "influx://188.185.88.195:80/write?db=db1"
    elif opmon_impl == 'pocket':
        info_svc_uri = "influx://" + pocket_url + ":31002/write?db=influxdb"
    else:
        info_svc_uri = "file://info_${APP_NAME}_${APP_PORT}.json"

    ers_settings=dict()

    if ers_impl == 'cern':
        use_kafka = True
        ers_settings["INFO"] =    "erstrace,throttle,lstdout,erskafka(dqmbroadcast:9092)"
        ers_settings["WARNING"] = "erstrace,throttle,lstdout,erskafka(dqmbroadcast:9092)"
        ers_settings["ERROR"] =   "erstrace,throttle,lstdout,erskafka(dqmbroadcast:9092)"
        ers_settings["FATAL"] =   "erstrace,lstdout,erskafka(dqmbroadcast:9092)"
    elif ers_impl == 'pocket':
        use_kafka = True
        ers_settings["INFO"] =    "erstrace,throttle,lstdout,erskafka(" + pocket_url + ":30092)"
        ers_settings["WARNING"] = "erstrace,throttle,lstdout,erskafka(" + pocket_url + ":30092)"
        ers_settings["ERROR"] =   "erstrace,throttle,lstdout,erskafka(" + pocket_url + ":30092)"
        ers_settings["FATAL"] =   "erstrace,lstdout,erskafka(" + pocket_url + ":30092)"
    else:
        use_kafka = False
        ers_settings["INFO"] =    "erstrace,throttle,lstdout"
        ers_settings["WARNING"] = "erstrace,throttle,lstdout"
        ers_settings["ERROR"] =   "erstrace,throttle,lstdout"
        ers_settings["FATAL"] =   "erstrace,lstdout"

    # timing commands coming in outside data-taking partitions
    the_system.network_endpoints.append(nwmgr.Connection(name=partition_name + ".timing_cmds",  topics=[], address=f"tcp://{{host_thi}}:{port_thi}"))
    
    # the timing hardware interface application
    the_system.apps["thi"] = THIApp(
        CONNECTIONS_FILE=timing_hw_connections_file,
        MASTER_DEVICE_NAME=master_device_name,
        HSI_DEVICE_NAME=hsi_device_name,
        HOST=host_thi)
        
    the_system.app_connections[f"from_outside.timing_cmds"] = AppConnection(nwmgr_connection=partition_name + ".timing_cmds",
                                                                            msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                                                            msg_module_name="TimingHwCmdNQ",
                                                                            topics=[],
                                                                            receivers=["thi.timing_cmds"])
    add_network("thi", the_system, verbose=True)

    # the timing master controller application
    the_system.app_connections[f"tmc.timing_cmds"] = AppConnection(nwmgr_connection=partition_name + ".timing_cmds",
                                                                            msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                                                            msg_module_name="TimingHwCmdNQ",
                                                                            topics=[],
                                                                            receivers=["thi.timing_cmds"])
    the_system.apps["tmc"] = TMCApp(
        MASTER_DEVICE_NAME=master_device_name,
        HOST=host_tmc)
    add_network("tmc", the_system, verbose=True)

    
    the_system.export("global_system.dot")

    ####################################################################
    # Application command data generation
    ####################################################################

    # Arrange per-app command data into the format used by util.write_json_files()
    app_command_datas = {
        name : make_app_command_data(the_system, app, verbose=True)
        for name,app in the_system.apps.items()
    }

    # Make boot.json config
    from appfwk.conf_utils import make_system_command_datas,generate_boot, write_json_files
    system_command_datas = make_system_command_datas(the_system)
    # Override the default boot.json with the one from minidaqapp
    boot = generate_boot(the_system.apps, partition_name=partition_name, ers_settings=ers_settings, info_svc_uri=info_svc_uri,
                              disable_trace=disable_trace, use_kafka=use_kafka)

    system_command_datas['boot'] = boot

    write_json_files(app_command_datas, system_command_datas, json_dir)

    console.log(f"Global aapp config generated in {json_dir}")


if __name__ == '__main__':
    try:
        cli(show_default=True, standalone_mode=True)
    except Exception as e:
        console.print_exception()
