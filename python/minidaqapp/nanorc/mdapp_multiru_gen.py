import json
import os
import math
import rich.traceback
from rich.console import Console
from os.path import exists, join
from collections import defaultdict

CLOCK_SPEED_HZ = 50000000

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

# Map from application name to the list of endpoint names that are
# connected to that application (specifically, which must be assigned
# on its host)
app_endpoints = defaultdict(list)

def add_endpoint(endpoint_name, app_name):
    app_endpoints[app_name].append(endpoint_name)

def assign_endpoints():
    ret=dict()
    for app, endpoints in app_endpoints.items():
        port = 12345
        for endpoint in endpoints:
            ret[endpoint] = f"tcp://{{host_{app}}}:{port}"
            port+=1
            
    return ret


import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-p', '--partition-name', default="${USER}_test", help="Name of the partition to use, for ERS and OPMON")
@click.option('-n', '--number-of-data-producers', default=2, help="Number of links to use, either per ru (<=10) or total. If total is given, will be adjusted to the closest multiple of the number of rus")
@click.option('-e', '--emulator-mode', is_flag=True, help="If active, timestamps of data frames are overwritten when processed by the readout. This is necessary if the felix card does not set correct timestamps.")
@click.option('-s', '--data-rate-slowdown-factor', default=1)
@click.option('-r', '--run-number', default=333)
@click.option('-t', '--trigger-rate-hz', default=1.0, help='Fake HSI only: rate at which fake HSIEvents are sent. 0 - disable HSIEvent generation.')
@click.option('-b', '--trigger-window-before-ticks', default=1000)
@click.option('-a', '--trigger-window-after-ticks', default=1000)
@click.option('-c', '--token-count', default=10)
@click.option('-d', '--data-file', type=click.Path(), default='./frames.bin', help="File containing data frames to be replayed by the fake cards")
@click.option('-o', '--output-path', type=click.Path(), default='.')
@click.option('--disable-trace', is_flag=True, help="Do not enable TRACE (default TRACE_FILE is /tmp/trace_buffer_\${HOSTNAME}_\${USER})")
@click.option('-f', '--use-felix', is_flag=True, help="Use real felix cards instead of fake ones")
@click.option('--host-df', default='localhost')
@click.option('--host-ru', multiple=True, default=['localhost'], help="This option is repeatable, with each repetition adding an additional ru process.")
@click.option('--host-trigger', default='localhost', help='Host to run the trigger app on')
@click.option('--host-hsi', default='localhost', help='Host to run the HSI app on')
@click.option('--host-timing-hw', default='np04-srv-012.cern.ch', help='Host to run the timing hardware interface app on')
@click.option('--control-timing-hw', is_flag=True, default=False, help='Flag to control whether we are controlling timing hardware')
# hsi readout options
@click.option('--hsi-device-name', default="BOREAS_TLU", help='Real HSI hardware only: device name of HSI hw')
@click.option('--hsi-readout-period', default=1e3, help='Real HSI hardware only: Period between HSI hardware polling [us]')
# hw hsi options
@click.option('--hsi-endpoint-address', default=1, help='Timing address of HSI endpoint')
@click.option('--hsi-endpoint-partition', default=0, help='Timing partition of HSI endpoint')
@click.option('--hsi-re-mask', default=0x20000, help='Rising-edge trigger mask')
@click.option('--hsi-fe-mask', default=0x0, help='Falling-edge trigger mask')
@click.option('--hsi-inv-mask', default=0x0, help='Invert-edge mask')
@click.option('--hsi-source', default=0x1, help='HSI signal source; 0 - hardware, 1 - emulation (trigger timestamp bits)')
# fake hsi options
@click.option('--use-hsi-hw', is_flag=True, default=False, help='Flag to control whether fake or real hardware HSI config is generated. Default is fake')
@click.option('--hsi-device-id', default=0, help='Fake HSI only: device ID of fake HSIEvents')
@click.option('--mean-hsi-signal-multiplicity', default=1, help='Fake HSI only: rate of individual HSI signals in emulation mode 1')
@click.option('--hsi-signal-emulation-mode', default=0, help='Fake HSI only: HSI signal emulation mode')
@click.option('--enabled-hsi-signals', default=0b00000001, help='Fake HSI only: bit mask of enabled fake HSI signals')
# trigger options
@click.option('--ttcm-s1', default=1, help="Timing trigger candidate maker accepted HSI signal ID 1")
@click.option('--ttcm-s2', default=2, help="Timing trigger candidate maker accepted HSI signal ID 2")
@click.option('--trigger-activity-plugin', default='TriggerActivityMakerPrescalePlugin', help="Trigger activity algorithm plugin")
@click.option('--trigger-activity-config', default='dict(prescale=100)', help="Trigger activity algorithm config (string containing python dictionary)")
@click.option('--trigger-candidate-plugin', default='TriggerCandidateMakerPrescalePlugin', help="Trigger candidate algorithm plugin")
@click.option('--trigger-candidate-config', default='dict(prescale=100)', help="Trigger candidate algorithm config (string containing python dictionary)")

@click.option('--enable-raw-recording', is_flag=True, help="Add queues and modules necessary for the record command")
@click.option('--raw-recording-output-dir', type=click.Path(), default='.', help="Output directory where recorded data is written to. Data for each link is written to a separate file")
@click.option('--frontend-type', type=click.Choice(['wib', 'wib2', 'pds_queue', 'pds_list']), default='wib', help="Frontend type (wib, wib2 or pds) and latency buffer implementation in case of pds (folly queue or skip list)")
@click.option('--enable-dqm', is_flag=True, help="Enable Data Quality Monitoring")
@click.option('--opmon-impl', type=click.Choice(['json','cern','pocket'], case_sensitive=False),default='json', help="Info collector service implementation to use")
@click.option('--ers-impl', type=click.Choice(['local','cern','pocket'], case_sensitive=False), default='local', help="ERS destination (Kafka used for cern and pocket)")
@click.option('--dqm-impl', type=click.Choice(['local','cern','pocket'], case_sensitive=False), default='local', help="DQM destination (Kafka used for cern and pocket)")
@click.option('--pocket-url', default='127.0.0.1', help="URL for connecting to Pocket services")
@click.option('--enable-software-tpg', is_flag=True, default=False, help="Enable software TPG")
@click.option('--enable-tpset-writing', is_flag=True, default=False, help="Enable the writing of TPSets to disk (only works with --enable-software-tpg")
@click.option('--use-fake-data-producers', is_flag=True, default=False, help="Use fake data producers that respond with empty fragments immediately instead of (fake) cards and DLHs")
@click.argument('json_dir', type=click.Path())

def cli(partition_name, number_of_data_producers, emulator_mode, data_rate_slowdown_factor, run_number, trigger_rate_hz, trigger_window_before_ticks, trigger_window_after_ticks,
        token_count, data_file, output_path, disable_trace, use_felix, host_df, host_ru, host_trigger, host_hsi, host_timing_hw, control_timing_hw,
        hsi_device_name, hsi_readout_period, hsi_endpoint_address, hsi_endpoint_partition, hsi_re_mask, hsi_fe_mask, hsi_inv_mask, hsi_source,
        use_hsi_hw, hsi_device_id, mean_hsi_signal_multiplicity, hsi_signal_emulation_mode, enabled_hsi_signals,
        ttcm_s1, ttcm_s2, trigger_activity_plugin, trigger_activity_config, trigger_candidate_plugin, trigger_candidate_config,
        enable_raw_recording, raw_recording_output_dir, frontend_type, opmon_impl, enable_dqm, ers_impl, dqm_impl, pocket_url, enable_software_tpg, enable_tpset_writing, use_fake_data_producers, json_dir):
    """
      JSON_DIR: Json file output folder
    """


    ####################################################################
    # Prologue
    ####################################################################
    
    console.log("Loading dataflow config generator")
    from . import dataflow_gen
    console.log("Loading readout config generator")
    from . import readout_gen
    console.log("Loading trigger config generator")
    from . import trigger_gen
    console.log("Loading hsi config generator")
    from . import hsi_gen
    console.log("Loading fake hsi config generator")
    from . import fake_hsi_gen
    console.log("Loading timing hardware config generator")
    from . import thi_gen
    console.log(f"Generating configs for hosts trigger={host_trigger} dataflow={host_df} readout={host_ru} hsi={host_hsi}")

    total_number_of_data_producers = 0
    if number_of_data_producers > 10:
        total_old = number_of_data_producers
        number_of_data_producers = math.floor(number_of_data_producers / len(host_ru))
        total_number_of_data_producers = number_of_data_producers * len(host_ru)
        console.log(f"More than 10 data producers were requested ({total_old}): Will setup {number_of_data_producers} per host, for a total of {total_number_of_data_producers}")
    else:
        total_number_of_data_producers = number_of_data_producers * len(host_ru)
        console.log(f"10 or fewer data producers were requested: Will setup {number_of_data_producers} per host, for a total of {total_number_of_data_producers}")

    if enable_software_tpg and frontend_type != 'wib':
        raise Exception("Software TPG is only available for the wib at the moment!")

    if enable_software_tpg and use_fake_data_producers:
        raise Exception("Fake data producers don't support software tpg")

    if use_fake_data_producers and enable_dqm:
        raise Exception("DQM can't be used with fake data producers")

    if enable_tpset_writing and not enable_software_tpg:
        raise Exception("TPSet writing can only be used when software TPG is enabled")

    if token_count > 0:
        df_token_count = 0
        trigemu_token_count = token_count
    else:
        df_token_count = -1 * token_count
        trigemu_token_count = 0

    ru_app_names=[f"ruflx{idx}" if use_felix else f"ruemu{idx}" for idx in range(len(host_ru))]
    
    add_endpoint("hsievent", "hsi")
    add_endpoint("trigdec",  "trigger")
    add_endpoint("triginh",  "dataflow")
    add_endpoint("hsicmds",  "hsi")

    if frontend_type == 'wib' or frontend_type == 'wib2':
        system_type = 'TPC'
    else:
        system_type = 'PDS'

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

    dqm_kafka_address = "dqmbroadcast:9092" if dqm_impl == 'cern' else pocket_url + ":30092" if dqm_impl == 'pocket' else ''

    ####################################################################
    # Assigning network endpoints
    ####################################################################

    for idx in range(total_number_of_data_producers):
        add_endpoint(f"datareq_{idx}", "dataflow")
        if enable_software_tpg:
            add_endpoint(f"tp_datareq_{idx}", "dataflow")
            add_endpoint(f'frags_tpset_ds_{idx}', "trigger")
            add_endpoint(f"ds_tp_datareq_{idx}", "dataflow")

    cardid = {}
    host_id_dict = {}

    for hostidx in range(len(host_ru)):
        add_endpoint(f"timesync_{hostidx}",  ru_app_names[hostidx])
        add_endpoint(f"frags_{hostidx}",  ru_app_names[hostidx])

        if enable_software_tpg:
            add_endpoint(f"tp_frags_{hostidx}", ru_app_names[hostidx])
            for idx in range(number_of_data_producers):
                add_endpoint(f"tpsets_{hostidx*number_of_data_producers+idx}", ru_app_names[hostidx])

        if host_ru[hostidx] in host_id_dict:
            host_id_dict[host_ru[hostidx]] = host_id_dict[host_ru[hostidx]] + 1
            cardid[hostidx] = host_id_dict[host_ru[hostidx]]
        else:
            cardid[hostidx] = 0
            host_id_dict[host_ru[hostidx]] = 0
        hostidx = hostidx + 1


    network_endpoints = assign_endpoints()

    console.log(network_endpoints)
    
    ####################################################################
    # Application command data generation
    ####################################################################

    if control_timing_hw:
        timing_cmd_network_endpoints=set()
        if use_hsi_hw:
            timing_cmd_network_endpoints.add('hsicmds')
        cmd_data_thi = thi_gen.generate(
            RUN_NUMBER = run_number,
            NETWORK_ENDPOINTS=network_endpoints,
            TIMING_CMD_NETWORK_ENDPOINTS=timing_cmd_network_endpoints,
            HSI_DEVICE_NAME=hsi_device_name,
        )
        console.log("thi cmd data:", cmd_data_thi)

    if use_hsi_hw:
        cmd_data_hsi = hsi_gen.generate(network_endpoints,
            RUN_NUMBER = run_number,
            CONTROL_HSI_HARDWARE=control_timing_hw,
            READOUT_PERIOD_US = hsi_readout_period,
            HSI_DEVICE_NAME = hsi_device_name,
            HSI_ENDPOINT_ADDRESS = hsi_endpoint_address,
            HSI_ENDPOINT_PARTITION = hsi_endpoint_partition,
            HSI_RE_MASK=hsi_re_mask,
            HSI_FE_MASK=hsi_fe_mask,
            HSI_INV_MASK=hsi_inv_mask,
            HSI_SOURCE=hsi_source,)
    else:
        cmd_data_hsi = fake_hsi_gen.generate(
            network_endpoints,
            RUN_NUMBER = run_number,
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
            TRIGGER_RATE_HZ = trigger_rate_hz,
            HSI_DEVICE_ID = hsi_device_id,
            MEAN_SIGNAL_MULTIPLICITY = mean_hsi_signal_multiplicity,
            SIGNAL_EMULATION_MODE = hsi_signal_emulation_mode,
            ENABLED_SIGNALS =  enabled_hsi_signals,)

    console.log("hsi cmd data:", cmd_data_hsi)

    cmd_data_trigger = trigger_gen.generate(network_endpoints,
        NUMBER_OF_RAWDATA_PRODUCERS = total_number_of_data_producers,
        NUMBER_OF_TPSET_PRODUCERS = total_number_of_data_producers if enable_software_tpg else 0,
        ACTIVITY_PLUGIN = trigger_activity_plugin,
        ACTIVITY_CONFIG = eval(trigger_activity_config),
        CANDIDATE_PLUGIN = trigger_candidate_plugin,
        CANDIDATE_CONFIG = eval(trigger_candidate_config),
        TOKEN_COUNT = trigemu_token_count,
        SYSTEM_TYPE = system_type,
        TTCM_S1=ttcm_s1,
        TTCM_S2=ttcm_s2,
        TRIGGER_WINDOW_BEFORE_TICKS = trigger_window_before_ticks,
        TRIGGER_WINDOW_AFTER_TICKS = trigger_window_after_ticks)


    console.log("trigger cmd data:", cmd_data_trigger)

    cmd_data_dataflow = dataflow_gen.generate(network_endpoints,
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        RUN_NUMBER = run_number,
        OUTPUT_PATH = output_path,
        TOKEN_COUNT = df_token_count,
        SYSTEM_TYPE = system_type,
        SOFTWARE_TPG_ENABLED = enable_software_tpg,
        TPSET_WRITING_ENABLED = enable_tpset_writing)
    console.log("dataflow cmd data:", cmd_data_dataflow)

    cmd_data_readout = [ readout_gen.generate(network_endpoints,
            NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
            TOTAL_NUMBER_OF_DATA_PRODUCERS=total_number_of_data_producers,
            EMULATOR_MODE = emulator_mode,
            DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
            RUN_NUMBER = run_number,
            DATA_FILE = data_file,
            FLX_INPUT = use_felix,
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            HOSTIDX = hostidx,
            CARDID = cardid[hostidx],
            RAW_RECORDING_ENABLED = enable_raw_recording,
            RAW_RECORDING_OUTPUT_DIR = raw_recording_output_dir,
            FRONTEND_TYPE = frontend_type,
            SYSTEM_TYPE = system_type,
            DQM_ENABLED=enable_dqm,
            DQM_KAFKA_ADDRESS=dqm_kafka_address,
            SOFTWARE_TPG_ENABLED = enable_software_tpg,
            USE_FAKE_DATA_PRODUCERS = use_fake_data_producers
            ) for hostidx in range(len(host_ru))]
    console.log("readout cmd data:", cmd_data_readout)

    ####################################################################
    # System generation
    ####################################################################

    # Make dummy system data
    
    from . import util
    apps = {
        "hsi":      util.App(host=host_hsi),
        "trigger":  util.App(host=host_trigger),
        "dataflow": util.App(host=host_df),
    }

    apps.update({ru_name: util.App(host=host_ru[i]) for i,ru_name in enumerate(ru_app_names)})
    
    if control_timing_hw:
        apps["thi"] = util.App()
        
    the_system = util.System(apps, app_connections=None,
                             app_start_order=["dataflow"]+ru_app_names+["trigger", "hsi"])

    # Arrange per-app command data into the format used by util.write_json_files()
    app_command_datas = {
        "hsi": cmd_data_hsi,
        "trigger": cmd_data_trigger,
        "dataflow": cmd_data_dataflow,
    }
    for name, cmd_data in zip(ru_app_names, cmd_data_readout):
        app_command_datas[name]=cmd_data
    
    if control_timing_hw:
        app_command_datas["thi"] = cmd_data_thi

    ##################################################################################

    # Make boot.json config

    system_command_datas = util.make_system_command_datas(the_system)
    # Override the default boot.json with the one from minidaqapp
    boot = util.generate_boot(the_system.apps, partition_name=partition_name, ers_settings=ers_settings, info_svc_uri=info_svc_uri)
    
    if disable_trace:
        del boot["exec"]["daq_application"]["env"]["TRACE_FILE"]
        del boot["exec"]["daq_application_ups"]["env"]["TRACE_FILE"]

    if use_kafka:
        boot["env"]["DUNEDAQ_ERS_STREAM_LIBS"] = "erskafka"

    # PAR 2021-09-29 HACK
    #
    # Not all apps give us a cmd_data with all of the commands, so to
    # be consistent, we have to remove the relevant app/command pair
    # from the system command data, otherwise nanorc barfs. We have to
    # find a way to modify the commands received by an app in util
    for c in ('resume', 'pause'):
        cmd_apps=system_command_datas[c]['apps']
        for_removal=["dataflow", "thi", "hsi"] + ru_app_names
        for app in for_removal:
            if app in cmd_apps:
                del cmd_apps[app]
    
    system_command_datas['boot'] = boot
    
    util.write_json_files(app_command_datas, system_command_datas, json_dir)
    
    console.log(f"MDAapp config generated in {json_dir}")


if __name__ == '__main__':
    try:
        cli(show_default=True, standalone_mode=True)
    except Exception as e:
        console.print_exception()
