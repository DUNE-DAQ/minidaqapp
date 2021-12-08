import json
import os
import math
import rich.traceback
from rich.console import Console
from os.path import exists, join
from collections import defaultdict
from copy import deepcopy

CLOCK_SPEED_HZ = 50000000

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

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
@click.option('--region-id', default=0)
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
        token_count, data_file, output_path, disable_trace, use_felix, host_df, host_ru, host_trigger, host_hsi, host_timing_hw, control_timing_hw, region_id,
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

    if opmon_impl == 'cern':
        info_svc_uri = "influx://188.185.88.195:80/write?db=db1"
    elif opmon_impl == 'pocket':
        info_svc_uri = "influx://" + pocket_url + ":31002/write?db=influxdb"
    else:
        info_svc_uri = "file://info_${APP_NAME}_${APP_PORT}.json"

    if frontend_type == 'wib' or frontend_type == 'wib2':
        system_type = 'TPC'
    else:
        system_type = 'PDS'

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
    # Generating apps
    ####################################################################

    from . import util
    
    the_system = util.System()

    #-------------------------------------------------------------------
    # Trigger app
    
    mgraph_trigger = trigger_gen.generate(
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

    the_system.apps["trigger"] = util.App(modulegraph=mgraph_trigger, host=host_trigger)
    
    console.log("trigger module graph:", mgraph_trigger)

    #-------------------------------------------------------------------
    # Readout apps
    
    cardid = {}
    host_id_dict = {}

    ru_app_names=[f"ruflx{idx}" if use_felix else f"ruemu{idx}" for idx in range(len(host_ru))]

    for hostidx in range(len(host_ru)):
        if host_ru[hostidx] in host_id_dict:
            host_id_dict[host_ru[hostidx]] = host_id_dict[host_ru[hostidx]] + 1
            cardid[hostidx] = host_id_dict[host_ru[hostidx]]
        else:
            cardid[hostidx] = 0
            host_id_dict[host_ru[hostidx]] = 0
        hostidx = hostidx + 1

    mgraphs_readout = []
    for hostidx in range(len(host_ru)):
        this_readout_mgraph = readout_gen.generate(NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
                                                    TOTAL_NUMBER_OF_DATA_PRODUCERS=total_number_of_data_producers,
                                                    EMULATOR_MODE = emulator_mode,
                                                    DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                                                    DATA_FILE = data_file,
                                                    FLX_INPUT = use_felix,
                                                    CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
                                                    HOSTIDX = hostidx,
                                                    CARDID = cardid[hostidx],
                                                    RAW_RECORDING_ENABLED = enable_raw_recording,
                                                    RAW_RECORDING_OUTPUT_DIR = raw_recording_output_dir,
                                                    FRONTEND_TYPE = frontend_type,
                                                    SYSTEM_TYPE = system_type,
                                                    REGION_ID = region_id + hostidx,
                                                    DQM_ENABLED=enable_dqm,
                                                    DQM_KAFKA_ADDRESS=dqm_kafka_address,
                                                    SOFTWARE_TPG_ENABLED = enable_software_tpg,
                                                    USE_FAKE_DATA_PRODUCERS = use_fake_data_producers)
        console.log("readout mgraph:", this_readout_mgraph)
        mgraphs_readout.append(this_readout_mgraph)

    for i,ru_name in enumerate(ru_app_names):
        the_system.apps[ru_name] = util.App(modulegraph=mgraphs_readout[i], host=host_ru[i])

    #-------------------------------------------------------------------
    # (Fake) HSI app
    
    if use_hsi_hw:
        mgraph_hsi = hsi_gen.generate(
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
        mgraph_hsi = fake_hsi_gen.generate(
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
            TRIGGER_RATE_HZ = trigger_rate_hz,
            HSI_DEVICE_ID = hsi_device_id,
            MEAN_SIGNAL_MULTIPLICITY = mean_hsi_signal_multiplicity,
            SIGNAL_EMULATION_MODE = hsi_signal_emulation_mode,
            ENABLED_SIGNALS =  enabled_hsi_signals,)

    the_system.apps["hsi"] = util.App(modulegraph=mgraph_hsi, host=host_hsi)

    # TODO: Actually deal with "thi" properly
    if control_timing_hw:
        the_system.apps["thi"] = util.App()
    
    #-------------------------------------------------------------------
    # Dataflow app
    #
    # This has to be last, because it needs to know all of the
    # fragment producers that exist in the system, which are defined
    # by the other apps' generate functions
    
    mgraph_dataflow = dataflow_gen.generate(
        FRAGMENT_PRODUCERS = the_system.get_fragment_producers(),
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        OUTPUT_PATH = output_path,
        TOKEN_COUNT = df_token_count,
        SYSTEM_TYPE = system_type,
        SOFTWARE_TPG_ENABLED = enable_software_tpg,
        TPSET_WRITING_ENABLED = enable_tpset_writing)

    the_system.apps["dataflow"] = util.App(modulegraph=mgraph_dataflow, host=host_df)
    
    console.log("dataflow module graph:", mgraph_dataflow)

    app_connections = {
        "trigger.trigger_decisions": util.Sender(msg_type="dunedaq::dfmessages::TriggerDecision",
                                                 msg_module_name="TriggerDecisionNQ",
                                                 receiver="dataflow.trigger_decisions"),
        "hsi.hsievent":              util.Sender(msg_type="dunedaq::dfmessages::HSIEvent",
                                                 msg_module_name="HSIEventNQ",
                                                 receiver="trigger.hsievents_in"),
        "dataflow.tokens":           util.Sender(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                                 msg_module_name="TriggerDecisionTokenNQ",
                                                 receiver="trigger.tokens"),
    }

    app_connections.update({ f"ruemu0.tpsets_{idx}": util.Publisher(msg_type="dunedaq::trigger::TPSet",
                                                                    msg_module_name="TPSetNQ",
                                                                    subscribers=[f"trigger.tpsets_into_buffer_link{idx}",
                                                                                 f"trigger.tpsets_into_chain_link{idx}"])
                             for idx in range(total_number_of_data_producers) })
    
    app_connections.update({ f"ruemu0.timesync_{idx}": util.Publisher(msg_type="dunedaq::dfmessages::TimeSync",
                                                                      msg_module_name="TimeSyncNQ",
                                                                      subscribers=["hsi.time_sync"])
                             for idx in range(total_number_of_data_producers) })

    app_connections.update({ f"ruemu0.timesync_{total_number_of_data_producers+idx}": util.Publisher(msg_type="dunedaq::dfmessages::TimeSync",
                                                                      msg_module_name="TimeSyncNQ",
                                                                      subscribers=["hsi.time_sync"])
                             for idx in range(total_number_of_data_producers) })

    the_system.app_connections=app_connections
    the_system.app_start_order=["dataflow", "trigger"]+ru_app_names+["hsi"]

    util.connect_all_fragment_producers(the_system, verbose=True)

    console.log("After connecting fragment producers, trigger mgraph:", the_system.apps['trigger'].modulegraph)
    console.log("After connecting fragment producers, the_system.app_connections:", the_system.app_connections)

    util.set_mlt_links(the_system, "trigger", verbose=True)
    
    util.add_network("trigger", the_system, verbose=True)
    console.log("After adding network, trigger mgraph:", the_system.apps['trigger'].modulegraph)
    util.add_network("hsi", the_system, verbose=True)
    for ru_app_name in ru_app_names:
        util.add_network(ru_app_name, the_system, verbose=True)

    util.add_network("dataflow", the_system, verbose=True)
    

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
    
    # Arrange per-app command data into the format used by util.write_json_files()
    app_command_datas = {
        name : util.make_app_command_data(app)
        for name,app in the_system.apps.items()
    }

    if control_timing_hw:
        app_command_datas["thi"] = cmd_data_thi

    ##################################################################################

    # Make boot.json config

    system_command_datas = util.make_system_command_datas(the_system)
    # Override the default boot.json with the one from minidaqapp
    boot = util.generate_boot(the_system.apps, partition_name=partition_name, ers_settings=ers_settings, info_svc_uri=info_svc_uri,
                              disable_trace=disable_trace, use_kafka=use_kafka)

    system_command_datas['boot'] = boot

    util.write_json_files(app_command_datas, system_command_datas, json_dir)

    console.log(f"MDAapp config generated in {json_dir}")


if __name__ == '__main__':
    try:
        cli(show_default=True, standalone_mode=True)
    except Exception as e:
        console.print_exception()
