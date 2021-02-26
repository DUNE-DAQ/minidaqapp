# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('trigemu/TriggerDecisionEmulator.jsonnet')
moo.otypes.load_types('dfmodules/requestgenerator.jsonnet')
moo.otypes.load_types('dfmodules/fragmentreceiver.jsonnet')
moo.otypes.load_types('dfmodules/datawriter.jsonnet')
moo.otypes.load_types('dfmodules/hdf5datastore.jsonnet')
moo.otypes.load_types('readout/fakecardreader.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')

# Import new types
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.trigemu.triggerdecisionemulator as tde
import dunedaq.dfmodules.requestgenerator as rqg
import dunedaq.dfmodules.fragmentreceiver as ffr
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
import dunedaq.readout.fakecardreader as fcr
import dunedaq.readout.datalinkhandler as dlh

from appfwk.utils import mcmd, mspec

import json
import math
# Time to waait on pop()
QUEUE_POP_WAIT_MS=100;
# local clock speed Hz
CLOCK_SPEED_HZ = 50000000;

def generate(
        NUMBER_OF_DATA_PRODUCERS=2,          
        DATA_RATE_SLOWDOWN_FACTOR = 10,
        RUN_NUMBER = 333, 
        TRIGGER_RATE_HZ = 1.0,
        DATA_FILE="./frames.bin",
        OUTPUT_PATH=".",
        DISABLE_OUTPUT=False
    ):
    
    trigger_interval_ticks = math.floor((1/TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR)

    # Define modules and queues
    queue_bare_specs = [
            cmd.QueueSpec(inst="time_sync_q", kind='FollyMPMCQueue', capacity=100),
            cmd.QueueSpec(inst="trigger_inhibit_q", kind='FollySPSCQueue', capacity=20),
            cmd.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=20),
            cmd.QueueSpec(inst="trigger_decision_copy_for_bookkeeping", kind='FollySPSCQueue', capacity=20),
            cmd.QueueSpec(inst="trigger_decision_copy_for_inhibit", kind='FollySPSCQueue', capacity=20),
            cmd.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=20),
            cmd.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=100),
        ] + [
            cmd.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=20)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            cmd.QueueSpec(inst=f"wib_fake_link_{idx}", kind='FollySPSCQueue', capacity=100000)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = cmd.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("tde", "TriggerDecisionEmulator", [
                        cmd.QueueInfo(name="time_sync_source", inst="time_sync_q", dir="input"),
                        cmd.QueueInfo(name="trigger_inhibit_source", inst="trigger_inhibit_q", dir="input"),
                        cmd.QueueInfo(name="trigger_decision_sink", inst="trigger_decision_q", dir="output"),
                    ]),

        mspec("rqg", "RequestGenerator", [
                        cmd.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_q", dir="input"),
                        cmd.QueueInfo(name="trigger_decision_for_event_building", inst="trigger_decision_copy_for_bookkeeping", dir="output"),
                        cmd.QueueInfo(name="trigger_decision_for_inhibit", inst="trigger_decision_copy_for_inhibit", dir="output"),
                    ] + [
                        cmd.QueueInfo(name=f"data_request_{idx}_output_queue", inst=f"data_requests_{idx}", dir="output")
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                    ]),

        mspec("ffr", "FragmentReceiver", [
                        cmd.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_copy_for_bookkeeping", dir="input"),
                        cmd.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q", dir="output"),
                        cmd.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input"),
                    ]),

        mspec("datawriter", "DataWriter", [
                        cmd.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input"),
                        cmd.QueueInfo(name="trigger_decision_for_inhibit", inst="trigger_decision_copy_for_inhibit", dir="input"),
                        cmd.QueueInfo(name="trigger_inhibit_output_queue", inst="trigger_inhibit_q", dir="output"),
                    ]),

        mspec("fake_source", "FakeCardReader", [
                        cmd.QueueInfo(name=f"output_{idx}", inst=f"wib_fake_link_{idx}", dir="output")
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                        ]),

        ] + [
                mspec(f"datahandler_{idx}", "DataLinkHandler", [
                            cmd.QueueInfo(name="raw_input", inst=f"wib_fake_link_{idx}", dir="input"),
                            cmd.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                            cmd.QueueInfo(name="requests", inst=f"data_requests_{idx}", dir="input"),
                            cmd.QueueInfo(name="fragments", inst="data_fragments_q", dir="output"),
                            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    init_specs = cmd.Init(queues=queue_specs, modules=mod_specs)

    jstr = json.dumps(init_specs.pod(), indent=4, sort_keys=True)
    print(jstr)

    initcmd = cmd.Command(
        id=cmd.CmdId("init"),
        data=init_specs
    )


    confcmd = mcmd("conf", [
                ("tde", tde.ConfParams(
                        links=[idx for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                        min_links_in_request=NUMBER_OF_DATA_PRODUCERS,
                        max_links_in_request=NUMBER_OF_DATA_PRODUCERS,
                        min_readout_window_ticks=1200,
                        max_readout_window_ticks=1200,
                        trigger_window_offset=1000,
                        # The delay is set to put the trigger well within the latency buff
                        trigger_delay_ticks=math.floor( 2* CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR),
                        # We divide the trigger interval by
                        # DATA_RATE_SLOWDOWN_FACTOR so the triggers are still
                        # emitted per (wall-clock) second, rather than being
                        # spaced out further
                        trigger_interval_ticks=trigger_interval_ticks,
                        clock_frequency_hz=CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR                    
                        )),
                ("rqg", rqg.ConfParams(
                        map=rqg.mapgeoidqueue([
                                rqg.geoidinst(apa=0, link=idx, queueinstance=f"data_requests_{idx}") for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ])  
                        )),
                ("ffr", ffr.ConfParams(
                            general_queue_timeout=QUEUE_POP_WAIT_MS
                        )),
                ("datawriter", dw.ConfParams(
                            data_store_parameters=hdf5ds.ConfParams(
                                name="data_store",
                                # type = "HDF5DataStore", # default
                                directory_path = OUTPUT_PATH, # default
                                # mode = "all-per-file", # default
                                max_file_size_bytes = 1073741834,
                                filename_parameters = hdf5ds.HDF5DataStoreFileNameParams(
                                    overall_prefix = "fake_minidaqapp",
                                    # digits_for_run_number = 6, #default
                                    file_index_prefix = "file"
                                ),
                                file_layout_parameters = hdf5ds.HDF5DataStoreFileLayoutParams(
                                    trigger_record_name_prefix= "TriggerRecord",
                                    digits_for_trigger_number = 5,
                                )
                            )
                        )),
                ("fake_source",fcr.Conf(
                            link_ids=list(range(NUMBER_OF_DATA_PRODUCERS)),
                            # input_limit=10485100, # default
                            rate_khz = CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR*1000),
                            raw_type = "wib",
                            data_filename = DATA_FILE,
                            queue_timeout_ms = QUEUE_POP_WAIT_MS
                        )),
            ] + [
                (f"datahandler_{idx}", dlh.Conf(
                        raw_type = "wib",
                        # fake_trigger_flag=0, # default
                        source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                        latency_buffer_size = 3*CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR),
                        pop_limit_pct = 0.8,
                        pop_size_pct = 0.1,
                        apa_number = 0,
                        link_number = idx
                        )) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ])
    
    jstr = json.dumps(confcmd.pod(), indent=4, sort_keys=True)
    print(jstr)

    startpars = cmd.StartParams(run=RUN_NUMBER)
    startcmd = mcmd("start", [
            ("datawriter", dw.StartParams(
                run=RUN_NUMBER,
                disable_data_storage=DISABLE_OUTPUT,
                data_storage_prescale=1
              )),
            ("ffr", startpars),
            ("datahandler_.*", startpars),
            ("fake_source", startpars),
            ("rqg", startpars),
            ("tde", startpars),
        ])

    jstr = json.dumps(startcmd.pod(), indent=4, sort_keys=True)
    print("="*80+"\nStart\n\n", jstr)

    emptypars = cmd.EmptyParams()

    stopcmd = mcmd("stop", [
            ("tde", emptypars),
            ("rqg", emptypars),
            ("fake_source", emptypars),
            ("datahandler_.*", emptypars),
            ("ffr", emptypars),
            ("datawriter", emptypars),
        ])

    jstr = json.dumps(stopcmd.pod(), indent=4, sort_keys=True)
    print("="*80+"\nStop\n\n", jstr)

    pausecmd = mcmd("pause", [
            ("", emptypars)
        ])

    jstr = json.dumps(pausecmd.pod(), indent=4, sort_keys=True)
    print("="*80+"\nPause\n\n", jstr)

    resumecmd = mcmd("resume", [
            ("tde", tde.ResumeParams(
                            trigger_interval_ticks=trigger_interval_ticks
                        ))
        ])

    jstr = json.dumps(resumecmd.pod(), indent=4, sort_keys=True)
    print("="*80+"\nResume\n\n", jstr)

    scrapcmd = mcmd("scrap", [
            ("", emptypars)
        ])

    jstr = json.dumps(scrapcmd.pod(), indent=4, sort_keys=True)
    print("="*80+"\nScrap\n\n", jstr)

    # Create a list of commands
    cmd_seq = [initcmd, confcmd, startcmd, stopcmd, pausecmd, resumecmd, scrapcmd]

    # Print them as json (to be improved/moved out)
    jstr = json.dumps([c.pod() for c in cmd_seq], indent=4, sort_keys=True)
    return jstr
        
if __name__ == '__main__':
    # Add -h as default help option
    CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

    import click

    @click.command(context_settings=CONTEXT_SETTINGS)
    @click.option('-n', '--number-of-data-producers', default=2)
    @click.option('-s', '--data-rate-slowdown-factor', default=10)
    @click.option('-r', '--run-number', default=333)
    @click.option('-t', '--trigger-rate-hz', default=1.0)
    @click.option('-d', '--data-file', type=click.Path(), default='./frames.bin')
    @click.option('-o', '--output-path', type=click.Path(), default='.')
    @click.option('--disable-data-storage', is_flag=True)
    @click.argument('json_file', type=click.Path(), default='minidaq-app-fake-readout.json')
    def cli(number_of_data_producers, data_rate_slowdown_factor, run_number, trigger_rate_hz, data_file, output_path, disable_data_storage, json_file):
        """
          JSON_FILE: Input raw data file.
          JSON_FILE: Output json configuration file.
        """

        with open(json_file, 'w') as f:
            f.write(generate(
                    NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
                    DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                    RUN_NUMBER = run_number, 
                    TRIGGER_RATE_HZ = trigger_rate_hz,
                    DATA_FILE = data_file,
                    OUTPUT_PATH = output_path,
                    DISABLE_OUTPUT = disable_data_storage
                ))

        print(f"'{json_file}' generation completed.")

    cli()
    