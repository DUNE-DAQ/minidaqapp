import json
import os
import math
import sys
import glob
import rich.traceback
from rich.console import Console
from os.path import exists, join
from appfwk.system import System
from appfwk.conf_utils import AppConnection

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
@click.option('-g', '--global-partition-name', default="global", help="Name of the global partition to use, for ERS and OPMON and timing commands")
@click.option('--host-global', default='np04-srv-012.cern.ch', help='Host to run the (global) timing hardware interface app on')
@click.option('--port-global', default=12345, help='Port to host running the (global) timing hardware interface app on')
@click.option('-p', '--partition-name', default="${USER}_test", help="Name of the partition to use, for ERS and OPMON")
@click.option('-n', '--number-of-data-producers', default=2, help="Number of links to use for each readout application")
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
@click.option('--use-ssp', is_flag=True, help="Use real SSPs instead of fake sources")
@click.option('--host-df', multiple=True, default=['localhost'], help="This option is repeatable, with each repetition adding another dataflow app.")
@click.option('--host-dfo', default='localhost', help="Sets the host for the DFO app")
@click.option('--host-ru', multiple=True, default=['localhost'], help="This option is repeatable, with each repetition adding an additional ru process.")
@click.option('--host-trigger', default='localhost', help='Host to run the trigger app on')
@click.option('--host-hsi', default='localhost', help='Host to run the HSI app on')
@click.option('--host-tpw', default='localhost', help='Host to run the TPWriter app on')
@click.option('--host-tprtc', default='localhost', help='Host to run the timing partition controller app on')
@click.option('--region-id', multiple=True, default=[0], help="Define the Region IDs for the RUs. If only specified once, will apply to all RUs.")
@click.option('--latency-buffer-size', default=499968, help="Size of the latency buffers (in number of elements)")
# hsi readout options
@click.option('--hsi-hw-connections-file', default="${TIMING_SHARE}/config/etc/connections.xml", help='Real timing hardware only: path to hardware connections file')
@click.option('--hsi-device-name', default="", help='Real HSI hardware only: device name of HSI hw')
@click.option('--hsi-readout-period', default=1e3, help='Real HSI hardware only: Period between HSI hardware polling [us]')
# hw hsi options
@click.option('--control-hsi-hw', is_flag=True, default=False, help='Flag to control whether we are controlling hsi hardware')
@click.option('--hsi-endpoint-address', default=1, help='Timing address of HSI endpoint')
@click.option('--hsi-endpoint-partition', default=0, help='Timing partition of HSI endpoint')
@click.option('--hsi-re-mask', default=0x0, help='Rising-edge trigger mask')
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
# timing hw partition options
@click.option('--control-timing-partition', is_flag=True, default=False, help='Flag to control whether we are controlling timing partition in master hardware')
@click.option('--timing-partition-master-device-name', default="", help='Timing partition master hardware device name')
@click.option('--timing-partition-id', default=0, help='Timing partition id')
@click.option('--timing-partition-trigger-mask', default=0xff, help='Timing partition trigger mask')
@click.option('--timing-partition-rate-control-enabled', default=False, help='Timing partition rate control enabled')
@click.option('--timing-partition-spill-gate-enabled', default=False, help='Timing partition spill gate enabled')

@click.option('--enable-raw-recording', is_flag=True, help="Add queues and modules necessary for the record command")
@click.option('--raw-recording-output-dir', type=click.Path(), default='.', help="Output directory where recorded data is written to. Data for each link is written to a separate file")
@click.option('--frontend-type', type=click.Choice(['wib', 'wib2', 'pds_queue', 'pds_list', 'pacman', 'ssp']), default='wib', help="Frontend type (wib, wib2 or pds) and latency buffer implementation in case of pds (folly queue or skip list)")
@click.option('--enable-dqm', is_flag=True, help="Enable Data Quality Monitoring")
@click.option('--opmon-impl', type=click.Choice(['json','cern','pocket'], case_sensitive=False),default='json', help="Info collector service implementation to use")
@click.option('--ers-impl', type=click.Choice(['local','cern','pocket'], case_sensitive=False), default='local', help="ERS destination (Kafka used for cern and pocket)")
@click.option('--dqm-impl', type=click.Choice(['local','cern','pocket'], case_sensitive=False), default='local', help="DQM destination (Kafka used for cern and pocket)")
@click.option('--pocket-url', default='127.0.0.1', help="URL for connecting to Pocket services")
@click.option('--enable-software-tpg', is_flag=True, default=False, help="Enable software TPG")
@click.option('--enable-tpset-writing', is_flag=True, default=False, help="Enable the writing of TPs to disk (only works with --enable-software-tpg")
@click.option('--use-fake-data-producers', is_flag=True, default=False, help="Use fake data producers that respond with empty fragments immediately instead of (fake) cards and DLHs")
@click.option('--dqm-cmap', type=click.Choice(['HD', 'VD']), default='HD', help="Which channel map to use for DQM")
@click.option('--dqm-rawdisplay-params', nargs=3, default=[60, 10, 50], help="Parameters that control the data sent for the raw display plot")
@click.option('--dqm-meanrms-params', nargs=3, default=[10, 1, 100], help="Parameters that control the data sent for the mean/rms plot")
@click.option('--dqm-fourier-params', nargs=3, default=[600, 60, 100], help="Parameters that control the data sent for the fourier transform plot")
@click.option('--dqm-fouriersum-params', nargs=3, default=[600, 60, 1000], help="Parameters that control the data sent for the summed fourier transform plot")
@click.option('--op-env', default='swtest', help="Operational environment - used for raw data filename prefix and HDF5 Attribute inside the files")
@click.option('--tpc-region-name-prefix', default='APA', help="Prefix to be used for the 'Region' Group name inside the HDF5 file")
@click.option('--max-file-size', default=4*1024*1024*1024, help="The size threshold when raw data files are closed (in bytes)")
@click.option('--debug', default=False, is_flag=True, help="Switch to get a lot of printout and dot files")
@click.argument('json_dir', type=click.Path())

def cli(global_partition_name, host_global, port_global, partition_name, number_of_data_producers, emulator_mode, data_rate_slowdown_factor, run_number, trigger_rate_hz, trigger_window_before_ticks, trigger_window_after_ticks,
        token_count, data_file, output_path, disable_trace, use_felix, use_ssp, host_df, host_dfo, host_ru, host_trigger, host_hsi, host_tpw, host_tprtc, region_id, latency_buffer_size,
        hsi_hw_connections_file, hsi_device_name, hsi_readout_period, control_hsi_hw, hsi_endpoint_address, hsi_endpoint_partition, hsi_re_mask, hsi_fe_mask, hsi_inv_mask, hsi_source,
        use_hsi_hw, hsi_device_id, mean_hsi_signal_multiplicity, hsi_signal_emulation_mode, enabled_hsi_signals,
        ttcm_s1, ttcm_s2, trigger_activity_plugin, trigger_activity_config, trigger_candidate_plugin, trigger_candidate_config,
        control_timing_partition, timing_partition_master_device_name, timing_partition_id, timing_partition_trigger_mask, timing_partition_rate_control_enabled, timing_partition_spill_gate_enabled,
        enable_raw_recording, raw_recording_output_dir, frontend_type, opmon_impl, enable_dqm, ers_impl, dqm_impl, pocket_url, enable_software_tpg, enable_tpset_writing, use_fake_data_producers, dqm_cmap,
        dqm_rawdisplay_params, dqm_meanrms_params, dqm_fourier_params, dqm_fouriersum_params,
        op_env, tpc_region_name_prefix, max_file_size, debug, json_dir):


    if exists(json_dir):
        raise RuntimeError(f"Directory {json_dir} already exists")

    console.log("Loading dataflow config generator")
    from .dataflow_gen import DataFlowApp
    if enable_dqm:
        console.log("Loading dqm config generator")
        from .dqm_gen import DQMApp
    console.log("Loading readout config generator")
    from .readout_gen import ReadoutApp
    console.log("Loading trigger config generator")
    from .trigger_gen import TriggerApp
    console.log("Loading DFO config generator")
    from .dfo_gen import DFOApp
    console.log("Loading hsi config generator")
    from .hsi_gen import HSIApp
    console.log("Loading fake hsi config generator")
    from .fake_hsi_gen import FakeHSIApp
    console.log("Loading timing partition controller config generator")
    from .tprtc_gen import TPRTCApp
    if enable_tpset_writing:
        console.log("Loading TPWriter config generator")
        from .tpwriter_gen import TPWriterApp

    console.log(f"Generating configs for hosts trigger={host_trigger} DFO={host_dfo} dataflow={host_df} readout={host_ru} hsi={host_hsi} dqm={host_ru}")

    the_system = System(partition_name, first_port=port_global)
   
    total_number_of_data_producers = 0

    if use_ssp:
        total_number_of_data_producers = number_of_data_producers * len(host_ru)
        console.log(f"Will setup {number_of_data_producers} SSP channels per host, for a total of {total_number_of_data_producers}")
    else:
        total_number_of_data_producers = number_of_data_producers * len(host_ru)
        console.log(f"Will setup {number_of_data_producers} TPC channels per host, for a total of {total_number_of_data_producers}")

    if enable_software_tpg and frontend_type != 'wib':
        raise Exception("Software TPG is only available for the wib at the moment!")

    if enable_software_tpg and use_fake_data_producers:
        raise Exception("Fake data producers don't support software tpg")

    if use_fake_data_producers and enable_dqm:
        raise Exception("DQM can't be used with fake data producers")

    if enable_tpset_writing and not enable_software_tpg:
        raise Exception("TP writing can only be used when software TPG is enabled")

    if token_count > 0:
        trigemu_token_count = token_count
    else:
        trigemu_token_count = 0

    if (len(region_id) != len(host_ru)) and (len(region_id) != 1):
        raise Exception("--region-id should be specified either once only or once for each --host-ru!")

    if frontend_type == 'wib' or frontend_type == 'wib2':
        system_type = 'TPC'
    elif frontend_type == 'pacman':
        system_type = 'NDLArTPC'
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

    if control_hsi_hw or control_timing_partition:
        the_system.network_endpoints.append(nwmgr.Connection(name=f"{global_partition_name}.timing_cmds",  topics=[], address="tcp://{"+host_global+"}:"+f"{port_global}"))

    host_id_dict = {}
    ru_configs = []
    ru_channel_counts = {}
    for region in region_id: ru_channel_counts[region] = 0
    regionidx = 0

    ru_app_names=[f"ruflx{idx}" if use_felix else f"ruemu{idx}" for idx in range(len(host_ru))]
    dqm_app_names = [f"dqm{idx}" for idx in range(len(host_ru))]
    
    for hostidx,ru_host in enumerate(ru_app_names):
        if enable_dqm:
            the_system.network_endpoints.append(nwmgr.Connection(name=f"{partition_name}.fragx_dqm_{hostidx}", topics=[], address=f"tcp://{{host_{ru_host}}}:{the_system.next_unassigned_port()}"))

        the_system.network_endpoints.append(nwmgr.Connection(name=f"{partition_name}.datareq_{hostidx}", topics=[], address=f"tcp://{{host_{ru_host}}}:{the_system.next_unassigned_port()}"))

        # Should end up something like 'network_endpoints[timesync_0]:
        # "tcp://{host_ru0}:12347"'
        the_system.network_endpoints.append(nwmgr.Connection(name=f"{partition_name}.timesync_{hostidx}", topics=["Timesync"], address=f"tcp://{{host_{ru_host}}}:{the_system.next_unassigned_port()}"))
        
        cardid = 0
        if host_ru[hostidx] in host_id_dict:
            host_id_dict[host_ru[hostidx]] = host_id_dict[host_ru[hostidx]] + 1
            cardid = host_id_dict[host_ru[hostidx]]
        else:
            host_id_dict[host_ru[hostidx]] = 0
        ru_configs.append( {"host": host_ru[hostidx], "card_id": cardid, "region_id": region_id[regionidx], "start_channel": ru_channel_counts[region_id[regionidx]], "channel_count": number_of_data_producers} )
        ru_channel_counts[region_id[regionidx]] += number_of_data_producers
        if len(region_id) != 1: regionidx = regionidx + 1

    if use_hsi_hw:
        the_system.apps["hsi"] = HSIApp(
            RUN_NUMBER = run_number,
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            TRIGGER_RATE_HZ = trigger_rate_hz,
            CONTROL_HSI_HARDWARE=control_hsi_hw,
            CONNECTIONS_FILE=hsi_hw_connections_file,
            READOUT_PERIOD_US = hsi_readout_period,
            HSI_DEVICE_NAME = hsi_device_name,
            HSI_ENDPOINT_ADDRESS = hsi_endpoint_address,
            HSI_ENDPOINT_PARTITION = hsi_endpoint_partition,
            HSI_RE_MASK=hsi_re_mask,
            HSI_FE_MASK=hsi_fe_mask,
            HSI_INV_MASK=hsi_inv_mask,
            HSI_SOURCE=hsi_source,
            PARTITION=partition_name,
            GLOBAL_PARTITION=global_partition_name,
            HOST=host_hsi,
            DEBUG=debug)
    else:
        the_system.apps["hsi"] = FakeHSIApp(
            RUN_NUMBER = run_number,
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
            TRIGGER_RATE_HZ = trigger_rate_hz,
            HSI_DEVICE_ID = hsi_device_id,
            MEAN_SIGNAL_MULTIPLICITY = mean_hsi_signal_multiplicity,
            SIGNAL_EMULATION_MODE = hsi_signal_emulation_mode,
            ENABLED_SIGNALS =  enabled_hsi_signals,
            PARTITION=partition_name,
            HOST=host_hsi,
            DEBUG=debug)
    
    if control_hsi_hw and use_hsi_hw:
        the_system.app_connections[f"hsi.timing_cmds"] = AppConnection(nwmgr_connection=f"{global_partition_name}.timing_cmds",
                                                                            msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                                                            msg_module_name="TimingHwCmdNQ",
                                                                            topics=[],
                                                                            receivers=[])

        # the_system.apps["hsi"] = util.App(modulegraph=mgraph_hsi, host=host_hsi)
    if debug: console.log("hsi cmd data:", the_system.apps["hsi"])

    if control_timing_partition:
        the_system.apps["tprtc"] = TPRTCApp(
            MASTER_DEVICE_NAME=timing_partition_master_device_name,
            TIMING_PARTITION=timing_partition_id,
            TRIGGER_MASK=timing_partition_trigger_mask,
            RATE_CONTROL_ENABLED=timing_partition_rate_control_enabled,
            SPILL_GATE_ENABLED=timing_partition_spill_gate_enabled,
            PARTITION=partition_name,
            GLOBAL_PARTITION=global_partition_name,
            HOST=host_tprtc,
            DEBUG=debug)
        the_system.app_connections[f"tprtc.timing_cmds"] = AppConnection(nwmgr_connection=f"{global_partition_name}.timing_cmds",
                                                                            msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                                                            msg_module_name="TimingHwCmdNQ",
                                                                            topics=[],
                                                                            receivers=[])

    the_system.apps['trigger'] = TriggerApp(
        SOFTWARE_TPG_ENABLED = enable_software_tpg,
        RU_CONFIG = ru_configs,
        ACTIVITY_PLUGIN = trigger_activity_plugin,
        ACTIVITY_CONFIG = eval(trigger_activity_config),
        CANDIDATE_PLUGIN = trigger_candidate_plugin,
        CANDIDATE_CONFIG = eval(trigger_candidate_config),
        SYSTEM_TYPE = system_type,
        TTCM_S1=ttcm_s1,
        TTCM_S2=ttcm_s2,
        TRIGGER_WINDOW_BEFORE_TICKS = trigger_window_before_ticks,
        TRIGGER_WINDOW_AFTER_TICKS = trigger_window_after_ticks,
        PARTITION=partition_name,
        HOST=host_trigger,
        DEBUG=debug)

    the_system.apps['dfo'] = DFOApp(
        DF_COUNT = len(host_df),
        TOKEN_COUNT = trigemu_token_count,
        PARTITION=partition_name,
        HOST=host_trigger,
        DEBUG=debug)

    # console.log("trigger cmd data:", cmd_data_trigger)

    #-------------------------------------------------------------------
    # Readout apps
    
    cardid = {}
    host_id_dict = {}

    for hostidx in range(len(host_ru)):
        if host_ru[hostidx] in host_id_dict:
            host_id_dict[host_ru[hostidx]] = host_id_dict[host_ru[hostidx]] + 1
            cardid[hostidx] = host_id_dict[host_ru[hostidx]]
        else:
            cardid[hostidx] = 0
            host_id_dict[host_ru[hostidx]] = 0
        hostidx = hostidx + 1


    # Set up the nwmgr endpoints for TPSets. Need to wait till now to do this so that ru_configs is filled
    if enable_software_tpg:
        for apa_idx,ru_app_name in enumerate(ru_app_names):
            ru_config=ru_configs[apa_idx]
            min_link=ru_config["start_channel"]
            max_link=min_link+ru_config["channel_count"]
            for link in range(min_link, max_link):
                the_system.network_endpoints.append(nwmgr.Connection(name=f"{partition_name}.tpsets_apa{apa_idx}_link{link}", topics=["TPSets"], address = f"tcp://{{host_{ru_app_name}}}:{the_system.next_unassigned_port()}"))



    mgraphs_readout = []
    for i,host in enumerate(host_ru):
        ru_name = ru_app_names[i]
        the_system.apps[ru_name] = ReadoutApp(PARTITION=partition_name,
                                              RU_CONFIG=ru_configs,
                                              EMULATOR_MODE = emulator_mode,
                                              DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                                              DATA_FILE = data_file,
                                              FLX_INPUT = use_felix,
                                              SSP_INPUT = use_ssp,
                                              CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
                                              RUIDX = i,
                                              RAW_RECORDING_ENABLED = enable_raw_recording,
                                              RAW_RECORDING_OUTPUT_DIR = raw_recording_output_dir,
                                              FRONTEND_TYPE = frontend_type,
                                              SYSTEM_TYPE = system_type,
                                              SOFTWARE_TPG_ENABLED = enable_software_tpg,
                                              USE_FAKE_DATA_PRODUCERS = use_fake_data_producers,
                                              HOST=host,
                                              LATENCY_BUFFER_SIZE=latency_buffer_size,
                                              DEBUG=debug)
        if debug: console.log(f"{ru_name} app: {the_system.apps[ru_name]}")

        if enable_dqm:
            dqm_name = dqm_app_names[i]
            the_system.apps[dqm_name] = DQMApp(
                RU_CONFIG = ru_configs,
                RU_NAME=ru_name,
                EMULATOR_MODE = emulator_mode,
                DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                RUN_NUMBER = run_number,
                DATA_FILE = data_file,
                CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
                RUIDX = i,
                SYSTEM_TYPE = system_type,
                DQM_ENABLED=enable_dqm,
                DQM_KAFKA_ADDRESS=dqm_kafka_address,
                DQM_CMAP=dqm_cmap,
                DQM_RAWDISPLAY_PARAMS=dqm_rawdisplay_params,
                DQM_MEANRMS_PARAMS=dqm_meanrms_params,
                DQM_FOURIER_PARAMS=dqm_fourier_params,
                DQM_FOURIERSUM_PARAMS=dqm_fouriersum_params,
                PARTITION=partition_name,
                HOST=host,
                DEBUG=debug)
            if debug: console.log(f"{dqm_name} app: {the_system.apps[dqm_name]}")

    if enable_tpset_writing:
        tpw_name=f'tpwriter'
        the_system.apps[tpw_name] = TPWriterApp(
            RU_CONFIG = ru_configs, 
            HOST=host_tpw, 
            DEBUG=debug)
        if debug: console.log(f"{tpw_name} app: {the_system.apps[tpw_name]}")

    df_app_names = []
    for i,host in enumerate(host_df):
        app_name = f'dataflow{i}'
        df_app_names.append(app_name)
        the_system.apps[app_name] = DataFlowApp(
            HOSTIDX = i,
            OUTPUT_PATH = output_path,
            PARTITION=partition_name,
            OPERATIONAL_ENVIRONMENT = op_env,
            TPC_REGION_NAME_PREFIX = tpc_region_name_prefix,
            MAX_FILE_SIZE = max_file_size,
            HOST=host,
            DEBUG=debug
        )

    for name,app in the_system.apps.items():
        if app.name=="__app":
            app.name=name

    # TODO PAR 2021-12-11 Fix up the indexing here. There's one output
    # endpoint per link in the ru apps (maybe there should just be one
    # per app?), and all the TPSets from one RU go to the same TA
    # input in the trigger app
    if enable_software_tpg:
        for ruidx,ru_app_name in enumerate(ru_app_names):
            ru_config = ru_configs[ruidx]
            apa_idx = ru_config['region_id']
            for link in range(ru_config["channel_count"]):
                # PL 2022-02-02: global_link is needed here to have non-overlapping app connections if len(ru)>1 with the same region_id
                # Adding the ru number here too, in case we have many region_ids
                global_link = link+ru_config["start_channel"]
                the_system.app_connections.update(
                    {
                        f"{ru_app_name}.tpsets_ru{ruidx}_link{global_link}":
                        AppConnection(nwmgr_connection=f"{partition_name}.tpsets_apa{apa_idx}_link{global_link}",
                                      msg_type="dunedaq::trigger::TPSet",
                                      msg_module_name="TPSetNQ",
                                      topics=["TPSets"],
                                      receivers=([f"trigger.tpsets_into_buffer_ru{ruidx}_link{link}",
                                                  f"trigger.tpsets_into_chain_apa{apa_idx}"] +
                                                 (["tpwriter.tpsets_into_writer"] if enable_tpset_writing else [])))
                    })

    for i,df_app_name in enumerate(df_app_names):
        the_system.app_connections[f"dfo.trigger_decisions{i}"] = AppConnection(nwmgr_connection=f"{partition_name}.trigdec_{i}",
                                                                                    msg_type="dunedaq::dfmessages::TriggerDecision",
                                                                                    msg_module_name="TriggerDecisionNQ",
                                                                                    topics=[],
                                                                                    receivers=[f"{df_app_name}.trigger_decisions"])
    
    the_system.app_connections["hsi.hsievents"] = AppConnection(nwmgr_connection=f"{partition_name}.hsievents",
                                                                topics=[],
                                                                use_nwqa=False,
                                                                receivers=["trigger.hsievents"])

    the_system.app_connections["trigger.td_to_dfo"] = AppConnection(nwmgr_connection=f"{partition_name}.td_mlt_to_dfo",
                                                                topics=[],
                                                                use_nwqa=False,
                                                                receivers=["dfo.td_to_dfo"])

    the_system.app_connections["dfo.df_busy_signal"] = AppConnection(nwmgr_connection=f"{partition_name}.df_busy_signal",
                                                                  topics=[],
                                                                  use_nwqa=False,
                                                                  receivers=["trigger.df_busy_signal"])
 
    # TODO: How to do this more automatically?
    the_system.network_endpoints.append(nwmgr.Connection(name=f"{the_system.partition_name}.triginh",
                                                         topics=[],
                                                         address=f"tcp://{{host_dfo}}:{the_system.next_unassigned_port()}"))
                                                                            
    
    #     console.log(f"MDAapp config generated in {json_dir}")
    from appfwk.conf_utils import connect_all_fragment_producers, add_network, make_app_command_data, set_mlt_links
    if debug:
        the_system.export("system_no_frag_prod_connection.dot")
    connect_all_fragment_producers(the_system, verbose=debug)
    
    # console.log("After connecting fragment producers, trigger mgraph:", the_system.apps['trigger'].modulegraph)
    # console.log("After connecting fragment producers, the_system.app_connections:", the_system.app_connections)

    set_mlt_links(the_system, "trigger", verbose=debug)
    mlt_links=the_system.apps["trigger"].modulegraph.get_module("mlt").conf.links
    if debug:
        console.log(f"After set_mlt_links, mlt_links is {mlt_links}")
    add_network("trigger", the_system, verbose=debug)
    add_network("dfo", the_system, verbose=debug)

    # # console.log("After adding network, trigger mgraph:", the_system.apps['trigger'].modulegraph)
    add_network("hsi", the_system, verbose=debug)
    if enable_tpset_writing:
        add_network("tpwriter", the_system, verbose=debug)
    if control_timing_partition:
        add_network("tprtc", the_system, verbose=debug)
    for ru_app_name in ru_app_names:
        add_network(ru_app_name, the_system, verbose=debug)

    for df_app_name in df_app_names:
        add_network(df_app_name, the_system, verbose=debug)
    if debug:
        the_system.export("system.dot")

    ####################################################################
    # Application command data generation
    ####################################################################
    
    # Arrange per-app command data into the format used by util.write_json_files()
    app_command_datas = {
        name : make_app_command_data(the_system, app, verbose=debug)
        for name,app in the_system.apps.items()
    }

    ##################################################################################

    # Make boot.json config
    from appfwk.conf_utils import make_system_command_datas,generate_boot, write_json_files
    system_command_datas = make_system_command_datas(the_system)
    # Override the default boot.json with the one from minidaqapp
    boot = generate_boot(the_system.apps, partition_name=partition_name, ers_settings=ers_settings, info_svc_uri=info_svc_uri,
                              disable_trace=disable_trace, use_kafka=use_kafka)

    system_command_datas['boot'] = boot

    write_json_files(app_command_datas, system_command_datas, json_dir, verbose=debug)

    console.log(f"MDAapp config generated in {json_dir}")


if __name__ == '__main__':
    try:
        cli(show_default=True, standalone_mode=True)
    except Exception as e:
        console.print_exception()
