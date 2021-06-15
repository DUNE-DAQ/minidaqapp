import json
import os
import math
import rich.traceback
from rich.console import Console
from os.path import exists, join


CLOCK_SPEED_HZ = 50000000;

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()


import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-n', '--number-of-data-producers', default=2, help="Number of links to use, either per ru (<10) or total. If total is given, will be adjusted to the closest multiple of the number of rus")
@click.option('-e', '--emulator-mode', is_flag=True, help="If active, timestamps of data frames are overwritten when processed by the readout. This is necessary if the felix card does not set correct timestamps.")
@click.option('-s', '--data-rate-slowdown-factor', default=1)
@click.option('-r', '--run-number', default=333)
@click.option('-t', '--trigger-rate-hz', default=1.0)
@click.option('-c', '--token-count', default=10)
@click.option('-d', '--data-file', type=click.Path(), default='./frames.bin', help="File containing data frames to be replayed by the fake cards")
@click.option('-o', '--output-path', type=click.Path(), default='.')
@click.option('--enable-trace', is_flag=True)
@click.option('-f', '--use-felix', is_flag=True, help="Use real felix cards instead of fake ones")
@click.option('--host-df', default='localhost')
@click.option('--host-ru', multiple=True, default=['localhost'], help="This option is repeatable, with each repetition adding an additional ru process.")
@click.option('--host-trigger', default='localhost', help='Host to run the trigger app on')
@click.option('--host-hsi', default='localhost', help='Host to run the HSI app on')
# hsi readout options
@click.option('--hsi-device-name', default="BOREAS_TLU", help='Real HSI hardware only: device name of HSI hw')
@click.option('--hsi-readout-period', default=1e3, help='Real HSI hardware only: Period between HSI hardware polling [us]')
# fake hsi options
@click.option('--use-hsi-hw', is_flag=True, default=False, help='Flag to control whether fake or real hardware HSI config is generated. Default is fake')
@click.option('--hsi-event-period', default=1e9, help='Fake HSI only: how often are fake HSIEvents sent (given valid generated signal)')
@click.option('--hsi-device-id', default=0, help='Fake HSI only: device ID of fake HSIEvents')
@click.option('--mean-hsi-signal-multiplicity', default=1, help='Fake HSI only: rate of individual HSI signals in emulation mode 1')
@click.option('--hsi-signal-emulation-mode', default=0, help='Fake HSI only: HSI signal emulation mode')
@click.option('--enabled-hsi-signals', default=0b00000001, help='Fake HSI only: bit mask of enabled fake HSI signals')
# trigger options
@click.option('--ttcm-s1', default=1, help="Timing trigger candidate maker accepted HSI signal ID 1")
@click.option('--ttcm-s2', default=2, help="Timing trigger candidate maker accepted HSI signal ID 2")
@click.option('--tr-timeout', default=0, help="Timeout for trigger records to be considered completed, in timestamp units. 0 means no timeout.")
@click.option('--enable-raw-recording', is_flag=True, help="Add queues and modules necessary for the record command")
@click.option('--raw-recording-output-dir', type=click.Path(), default='.', help="Output directory where recorded data is written to. Data for each link is written to a separate file")
@click.option('--frontend-type', type=click.Choice(['wib', 'wib2', 'pds_queue', 'pds_list']), default='wib', help="Frontend type (wib, wib2 or pds) and latency buffer implementation in case of pds (folly queue or skip list)")
@click.argument('json_dir', type=click.Path())
def cli(number_of_data_producers, emulator_mode, data_rate_slowdown_factor, run_number, trigger_rate_hz, token_count, data_file, output_path, enable_trace, use_felix, host_df, host_ru, host_trigger, host_hsi, 
        hsi_device_name, hsi_readout_period, use_hsi_hw, hsi_event_period, hsi_device_id, mean_hsi_signal_multiplicity, hsi_signal_emulation_mode, enabled_hsi_signals,
        ttcm_s1, ttcm_s2, tr_timeout,
        enable_raw_recording, raw_recording_output_dir, frontend_type, json_dir):
    """
      JSON_DIR: Json file output folder
    """
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
        

    if token_count > 0:
        df_token_count = 0
        trigemu_token_count = token_count
    else:
        df_token_count = -1 * token_count
        trigemu_token_count = 0

    network_endpoints={
        "hsievent" : "tcp://{host_hsi}:12344",
        "trigdec" : "tcp://{host_trigger}:12345",
        "triginh" : "tcp://{host_df}:12346",
    }

    if frontend_type == 'wib' or frontend_type == 'wib2':
        system_type = 'TPC'
    else:
        system_type = 'PDS'

    port=12347
    for idx in range(total_number_of_data_producers):
        network_endpoints[f"datareq_{idx}"] = "tcp://{host_df}:"+f"{port}"
        port = port + 1

    cardid = {}
    host_id_dict = {}
    for hostidx in range(len(host_ru)):
        # Should end up something like 'network_endpoints[timesync_0]: "tcp://{host_ru0}:12347"'
        network_endpoints[f"timesync_{hostidx}"] = "tcp://{host_ru" + f"{hostidx}" + "}:" + f"{port}"
        port = port + 1
        network_endpoints[f"frags_{hostidx}"] = "tcp://{host_ru"+ f"{hostidx}" + "}:" + f"{port}"
        port = port + 1
        if host_ru[hostidx] in host_id_dict:
            host_id_dict[host_ru[hostidx]] = host_id_dict[host_ru[hostidx]] + 1
            cardid[hostidx] = host_id_dict[host_ru[hostidx]]
        else:
            cardid[hostidx] = 0
            host_id_dict[host_ru[hostidx]] = 0
        hostidx = hostidx + 1
    
    if use_hsi_hw:
        cmd_data_hsi = hsi_gen.generate(
            network_endpoints,
            RUN_NUMBER = run_number,
            READOUT_PERIOD_US = hsi_readout_period,
            HSI_DEVICE_NAME = hsi_device_name,
        )
    else:
        cmd_data_hsi = fake_hsi_gen.generate(
            network_endpoints,
            RUN_NUMBER = run_number,
            CLOCK_SPEED_HZ = CLOCK_SPEED_HZ,
            DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
            HSI_EVENT_PERIOD_NS = hsi_event_period,
            HSI_DEVICE_ID = hsi_device_id,
            MEAN_SIGNAL_MULTIPLICITY = mean_hsi_signal_multiplicity,
            SIGNAL_EMULATION_MODE = hsi_signal_emulation_mode,
            ENABLED_SIGNALS =  enabled_hsi_signals,
        )
    
    console.log("hsi cmd data:", cmd_data_hsi)

    cmd_data_trigger = trigger_gen.generate(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        TOKEN_COUNT = trigemu_token_count,
        SYSTEM_TYPE = system_type,
        TTCM_S1=ttcm_s1,
        TTCM_S2=ttcm_s2
    )

    console.log("trigger cmd data:", cmd_data_trigger)

    cmd_data_dataflow = dataflow_gen.generate(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        RUN_NUMBER = run_number, 
        OUTPUT_PATH = output_path,
        TOKEN_COUNT = df_token_count,
        TR_TIMEOUT = tr_timeout,
        SYSTEM_TYPE = system_type
    )
    console.log("dataflow cmd data:", cmd_data_dataflow)

    cmd_data_readout = [ readout_gen.generate(
            network_endpoints,
            NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
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
            SYSTEM_TYPE = system_type
            ) for hostidx in range(len(host_ru))]
    console.log("readout cmd data:", cmd_data_readout)

    if exists(json_dir):
        raise RuntimeError(f"Directory {json_dir} already exists")

    data_dir = join(json_dir, 'data')
    os.makedirs(data_dir)

    app_hsi="hsi"
    app_trigger="trigger"
    app_df="dataflow"
    app_ru=[f"ruflx{idx}" if use_felix else f"ruemu{idx}" for idx in range(len(host_ru))]

    jf_hsi = join(data_dir, app_hsi)
    jf_trigemu = join(data_dir, app_trigger)
    jf_df = join(data_dir, app_df)
    jf_ru = [join(data_dir, app_ru[idx]) for idx in range(len(host_ru))]

    cmd_set = ["init", "conf", "start", "stop", "pause", "resume", "scrap", "record"]
    for app,data in [(app_hsi, cmd_data_hsi), (app_trigger, cmd_data_trigger), (app_df, cmd_data_dataflow)] + list(zip(app_ru, cmd_data_readout)):
        console.log(f"Generating {app} command data json files")
        for c in cmd_set:
            with open(f'{join(data_dir, app)}_{c}.json', 'w') as f:
                json.dump(data[c].pod(), f, indent=4, sort_keys=True)


    console.log(f"Generating top-level command json files")
    start_order = [app_df] + app_ru + [app_trigger] + [app_hsi]
    for c in cmd_set:
        with open(join(json_dir,f'{c}.json'), 'w') as f:
            cfg = {
                "apps": { app: f'data/{app}_{c}' for app in [app_trigger, app_df, app_hsi] + app_ru }
            }
            if c == 'start':
                cfg['order'] = start_order
            elif c == 'stop':
                cfg['order'] = start_order[::-1]
            elif c in ('resume', 'pause'):
                del cfg['apps'][app_df]
                del cfg['apps'][app_hsi]
                for ruapp in app_ru:
                    del cfg['apps'][ruapp]

            json.dump(cfg, f, indent=4, sort_keys=True)


    console.log(f"Generating boot json file")
    with open(join(json_dir,'boot.json'), 'w') as f:
        daq_app_specs = {
            "daq_application_ups" : {
                "comment": "Application profile based on a full dbt runtime environment",
                "env": {
                "DBT_AREA_ROOT": "getenv"
                },
                "cmd": [
                    "CMD_FAC=rest://localhost:${APP_PORT}",
                    "INFO_SVC=file://info_${APP_NAME}_${APP_PORT}.json",
                    "cd ${DBT_AREA_ROOT}",
                    "source dbt-env.sh",
                    "dbt-workarea-env",
                    "cd ${APP_WD}",
                    "daq_application --name ${APP_NAME} -c ${CMD_FAC} -i ${INFO_SVC}"
                ]
            },
            "daq_application" : {
                "comment": "Application profile using  PATH variables (lower start time)",
                "env":{
                    "CET_PLUGIN_PATH": "getenv",
                    "DUNEDAQ_SHARE_PATH": "getenv",
                    "TIMING_SHARE": "getenv",
                    "LD_LIBRARY_PATH": "getenv",
                    "PATH": "getenv"
                },
                "cmd": [
                    "CMD_FAC=rest://localhost:${APP_PORT}",
                    "INFO_SVC=file://info_${APP_NAME}_${APP_PORT}.json",
                    "cd ${APP_WD}",
                    "daq_application --name ${APP_NAME} -c ${CMD_FAC} -i ${INFO_SVC}"
                ]
            }
        }

        if enable_trace:
            daq_app_specs["daq_application"]["env"]["TRACE_FILE"] = "getenv"
            daq_app_specs["daq_application_ups"]["env"]["TRACE_FILE"] = "getenv"

        cfg = {
            "env" : {
                "DUNEDAQ_ERS_VERBOSITY_LEVEL": 1
            },
            "hosts": {
                "host_df": host_df,
                "host_trigger": host_trigger,
                "host_hsi": host_hsi
            },
            "apps" : {
                app_hsi: {
                    "exec": "daq_application",
                    "host": "host_hsi",
                    "port": 3332
                },
                app_trigger : {
                    "exec": "daq_application",
                    "host": "host_trigger",
                    "port": 3333
                },
                app_df: {
                    "exec": "daq_application",
                    "host": "host_df",
                    "port": 3334
                },
            },
            "response_listener": {
                "port": 56789
            },
            "exec": daq_app_specs
        }
        appport=3335
        for hostidx in range(len(host_ru)):
            cfg["hosts"][f"host_ru{hostidx}"] = host_ru[hostidx]
            cfg["apps"][app_ru[hostidx]] = {
                    "exec": "daq_application",
                    "host": f"host_ru{hostidx}",
                    "port": appport }
            appport = appport + 1
        json.dump(cfg, f, indent=4, sort_keys=True)
    console.log(f"MDAapp config generated in {json_dir}")


if __name__ == '__main__':
    try:
        cli(show_default=True, standalone_mode=True)
    except Exception as e:
        console.print_exception()
