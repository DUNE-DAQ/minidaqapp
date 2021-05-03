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
@click.option('-e', '--emulator-mode', is_flag=True)
@click.option('-s', '--data-rate-slowdown-factor', default=1)
@click.option('-r', '--run-number', default=333)
@click.option('-t', '--trigger-rate-hz', default=1.0)
@click.option('-c', '--token-count', default=10)
@click.option('-d', '--data-file', type=click.Path(), default='./frames.bin')
@click.option('-o', '--output-path', type=click.Path(), default='.')
@click.option('--disable-data-storage', is_flag=True)
@click.option('--disable-trace', is_flag=True, help="Do not export the TRACE_FILE environment variable to processes started by nanorc")
@click.option('-f', '--use-felix', is_flag=True)
@click.option('--host-df', default='localhost')
@click.option('--host-ru', multiple=True, default=['localhost'], help="This option is repeatable, with each repetition adding an additional ru process.")
@click.option('--host-trg', default='localhost')
@click.argument('json_dir', type=click.Path())
def cli(number_of_data_producers, emulator_mode, data_rate_slowdown_factor, run_number, trigger_rate_hz, token_count, data_file, output_path, disable_data_storage, disable_trace, use_felix, host_df, host_ru, host_trg, json_dir):
    """
      JSON_DIR: Json file output folder
    """
    console.log("Loading dataflow config generator")
    from . import dataflow_gen
    console.log("Loading readout config generator")
    from . import readout_gen
    console.log("Loading trg config generator")
    from . import trg_gen
    console.log(f"Generating configs for hosts trg={host_trg} dataflow={host_df} readout={host_ru}")

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
        "trigdec" : "tcp://{host_trg}:12345",
        "triginh" : "tcp://{host_df}:12346",
    }

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


    cmd_data_trg = trg_gen.generate(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
        RUN_NUMBER = run_number, 
        TRIGGER_RATE_HZ = trigger_rate_hz,
        TOKEN_COUNT = trigemu_token_count,
        CLOCK_SPEED_HZ = CLOCK_SPEED_HZ
    )

    console.log("trg cmd data:", cmd_data_trg)

    cmd_data_dataflow = dataflow_gen.generate(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS = total_number_of_data_producers,
        RUN_NUMBER = run_number, 
        OUTPUT_PATH = output_path,
        DISABLE_OUTPUT = disable_data_storage,
        TOKEN_COUNT = df_token_count
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
            CARDID = cardid[hostidx]
            ) for hostidx in range(len(host_ru))]
    console.log("readout cmd data:", cmd_data_readout)

    if exists(json_dir):
        raise RuntimeError(f"Directory {json_dir} already exists")

    data_dir = join(json_dir, 'data')
    os.makedirs(data_dir)

    app_trgemu="trgemu"
    app_df="dataflow"
    app_ru=[f"ruflx{idx}" if use_felix else f"ruemu{idx}" for idx in range(len(host_ru))]

    jf_trigemu = join(data_dir, app_trgemu)
    jf_df = join(data_dir, app_df)
    jf_ru = [join(data_dir, app_ru[idx]) for idx in range(len(host_ru))]

    cmd_set = ["init", "conf", "start", "stop", "pause", "resume", "scrap"]
    for app,data in [(app_trgemu, cmd_data_trg), (app_df, cmd_data_dataflow)] + list(zip(app_ru, cmd_data_readout)):
        console.log(f"Generating {app} command data json files")
        for c in cmd_set:
            with open(f'{join(data_dir, app)}_{c}.json', 'w') as f:
                json.dump(data[c].pod(), f, indent=4, sort_keys=True)


    console.log(f"Generating top-level command json files")
    start_order = app_ru + [app_df, app_trgemu]
    for c in cmd_set:
        with open(join(json_dir,f'{c}.json'), 'w') as f:
            cfg = {
                "apps": { app: f'data/{app}_{c}' for app in [app_trgemu, app_df] + app_ru }
            }
            if c == 'start':
                cfg['order'] = start_order
            elif c == 'stop':
                cfg['order'] = start_order[::-1]
            elif c in ('resume', 'pause'):
                del cfg['apps'][app_df]
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
                    "INFO_SVC=file://info_${APP_ID}_${APP_PORT}.json",
                    "cd ${DBT_AREA_ROOT}",
                    "source dbt-setup-env.sh",
                    "dbt-setup-runtime-environment",
                    "cd ${APP_WD}",
                    "daq_application --name ${APP_ID} -c ${CMD_FAC} -i ${INFO_SVC}"
                ]
            },
            "daq_application" : {
                "comment": "Application profile using  PATH variables (lower start time)",
                "env":{
                    "CET_PLUGIN_PATH": "getenv",
                    "DUNEDAQ_SHARE_PATH": "getenv",
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

        if not disable_trace:
            daq_app_specs["daq_application"]["env"]["TRACE_FILE"] = "getenv"

        cfg = {
            "env" : {
                "DUNEDAQ_ERS_VERBOSITY_LEVEL": 1
            },
            "hosts": {
                "host_df": host_df,
                "host_trg": host_trg
            },
            "apps" : {
                app_trgemu : {
                    "exec": "daq_application",
                    "host": "host_trg",
                    "port": 3333
                },
                app_df: {
                    "exec": "daq_application",
                    "host": "host_df",
                    "port": 3334
                }
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
