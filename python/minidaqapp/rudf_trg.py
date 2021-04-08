# testapp_noreadout_two_process.py

# This python configuration produces *two* json configuration files
# that together form a MiniDAQApp with the same functionality as
# MiniDAQApp v1, but in two processes. One process contains the
# TriggerDecisionEmulator, while the other process contains everything
# else. The network communication is done with the QueueToNetwork and
# NetworkToQueue modules from the nwqueueadapters package.
#
# As with testapp_noreadout_confgen.py
# in this directory, no modules from the readout package are used: the
# fragments are provided by the FakeDataProd module from dfmodules


# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('trigemu/triggerdecisionemulator.jsonnet')
#moo.otypes.load_types('trigemu/faketimesyncsource.jsonnet')
moo.otypes.load_types('dfmodules/requestgenerator.jsonnet')
moo.otypes.load_types('dfmodules/fragmentreceiver.jsonnet')
moo.otypes.load_types('dfmodules/datawriter.jsonnet')
moo.otypes.load_types('dfmodules/hdf5datastore.jsonnet')
#moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('serialization/networkobjectreceiver.jsonnet')
moo.otypes.load_types('serialization/networkobjectsender.jsonnet')
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')
moo.otypes.load_types('readout/fakecardreader.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')


# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd, 
import dunedaq.rcif.cmd as rccmd # AddressedCmd, 
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.trigemu.triggerdecisionemulator as tde
#import dunedaq.trigemu.faketimesyncsource as ftss
import dunedaq.dfmodules.requestgenerator as rqg
import dunedaq.dfmodules.fragmentreceiver as ffr
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
#import dunedaq.dfmodules.fakedataprod as fdp
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.serialization.networkobjectreceiver as nor
import dunedaq.serialization.networkobjectsender as nos
import dunedaq.readout.fakecardreader as fakecr
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh



from appfwk.utils import mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS=100;
# local clock speed Hz
CLOCK_SPEED_HZ = 50000000;


def generate_df(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR = 1,
        RUN_NUMBER = 333, 
        TRIGGER_RATE_HZ = 1.0,
        DATA_FILE="./frames.bin",
        OUTPUT_PATH=".",
        DISABLE_OUTPUT=False,
        FLX_INPUT=True,
        TOKEN_COUNT=0
    ):
    """Generate the json configuration for the readout and DF process"""
   
    trg_interval_ticks = math.floor((1/TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR)

    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_q", kind='FollyMPMCQueue', capacity=100),
            app.QueueSpec(inst="token_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_from_netq", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_copy_for_bookkeeping", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_to_netq", kind='FollyMPMCQueue', capacity=1000),
            app.QueueSpec(inst="data_fragments_from_netq", kind='FollyMPMCQueue', capacity=1000),
        ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [

            app.QueueSpec(inst=f"wib_link_{idx}", kind='FollySPSCQueue', capacity=100000)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("ntoq_fragments", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="data_fragments_from_netq", dir="output")
                    ]),

        mspec("qton_fragments", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="data_fragments_to_netq", dir="input")
                    ]),

        mspec("ntoq_trigdec", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="trigger_decision_from_netq", dir="output")
                    ]),

        mspec("qton_token", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="token_q", dir="input")
                    ]),

        mspec("qton_timesync", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="time_sync_q", dir="input")
                    ]),

        mspec("rqg", "RequestGenerator", [
                        app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_from_netq", dir="input"),
                        app.QueueInfo(name="trigger_decision_for_event_building", inst="trigger_decision_copy_for_bookkeeping", dir="output"),
                    ] + [
                        app.QueueInfo(name=f"data_request_{idx}_output_queue", inst=f"data_requests_{idx}", dir="output")
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                    ]),

        mspec("ffr", "FragmentReceiver", [
                        app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_copy_for_bookkeeping", dir="input"),
                        app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q", dir="output"),
                        app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_from_netq", dir="input"),
                    ]),

        mspec("datawriter", "DataWriter", [
                        app.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input"),
                        app.QueueInfo(name="token_output_queue", inst="token_q", dir="output"),
                    ]),

        ] + [
                mspec(f"datahandler_{idx}", "DataLinkHandler", [

                            app.QueueInfo(name="raw_input", inst=f"wib_link_{idx}", dir="input"),
                            app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                            app.QueueInfo(name="requests", inst=f"data_requests_{idx}", dir="input"),
                            app.QueueInfo(name="fragments", inst="data_fragments_to_netq", dir="output"),
                            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if FLX_INPUT:
        mod_specs.append(mspec("flxcard_0", "FelixCardReader", [
                        app.QueueInfo(name=f"output_{idx}", inst=f"wib_link_{idx}", dir="output")
                            for idx in range(0,min(5, NUMBER_OF_DATA_PRODUCERS))
                        ]))
        if NUMBER_OF_DATA_PRODUCERS>5 :
            mod_specs.append(mspec("flxcard_1", "FelixCardReader", [
                            app.QueueInfo(name=f"output_{idx}", inst=f"wib_link_{idx}", dir="output")
                                for idx in range(5, NUMBER_OF_DATA_PRODUCERS)
                            ]))
    else:
        mod_specs.append(mspec("fake_source", "FakeCardReader", [
                        app.QueueInfo(name=f"output_{idx}", inst=f"wib_link_{idx}", dir="output")
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                        ]))

    


    init_specs = app.Init(queues=queue_specs, modules=mod_specs)

    initcmd = rccmd.RCCommand(
        id=basecmd.CmdId("init"),
        entry_state="NONE",
        exit_state="INITIAL",
        data=init_specs
    )

    confcmd = mrccmd("conf", "INITIAL", "CONFIGURED",[
                ("ntoq_fragments", ntoq.Conf(msg_type="std::unique_ptr<dunedaq::dataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address="tcp://127.0.0.1:12333")
                                           )
                 ),

                ("qton_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::dataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address="tcp://127.0.0.1:12333",
                                                                  stype="msgpack")
                                           )
                 ),

        
                ("ntoq_trigdec", ntoq.Conf(msg_type="dunedaq::dfmessages::TriggerDecision",
                                           msg_module_name="TriggerDecisionNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=network_endpoints["trigdec"])
                                           )
                 ),

                ("qton_token", qton.Conf(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                           msg_module_name="TriggerDecisionTokenNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=network_endpoints["triginh"],
                                                                  stype="msgpack")
                                           )
                 ),

                ("qton_timesync", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                            msg_module_name="TimeSyncNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                   address=network_endpoints["timesync"],
                                                                   stype="msgpack")
                                           )
                ),
        
                ("rqg", rqg.ConfParams(
                        map=rqg.mapgeoidqueue([
                                rqg.geoidinst(apa=0, link=idx, queueinstance=f"data_requests_{idx}") for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ])  
                        )),
                ("ffr", ffr.ConfParams(
                            general_queue_timeout=QUEUE_POP_WAIT_MS
                        )),
                ("datawriter", dw.ConfParams(
                            initial_token_count=TOKEN_COUNT,
                            data_store_parameters=hdf5ds.ConfParams(
                                name="data_store",
                                # type = "HDF5DataStore", # default
                                directory_path = OUTPUT_PATH, # default
                                # mode = "all-per-file", # default
                                max_file_size_bytes = 1073741824,
                                disable_unique_filename_suffix = False,
                                filename_parameters = hdf5ds.HDF5DataStoreFileNameParams(
                                    overall_prefix = "swtest",
                                    digits_for_run_number = 6,
                                    file_index_prefix = "",
                                    digits_for_file_index = 4,
                                ),
                                file_layout_parameters = hdf5ds.HDF5DataStoreFileLayoutParams(
                                    trigger_record_name_prefix= "TriggerRecord",
                                    digits_for_trigger_number = 5,
                                    digits_for_apa_number = 3,
                                    digits_for_link_number = 2,
                                )
                            )
                        )),
                ("fake_source",fakecr.Conf(
                            link_ids=list(range(NUMBER_OF_DATA_PRODUCERS)),
                            # input_limit=10485100, # default
                            rate_khz = CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR*1000),
                            raw_type = "wib",
                            data_filename = DATA_FILE,
                            queue_timeout_ms = QUEUE_POP_WAIT_MS
                        )),
                ("flxcard_0",flxcr.Conf(
                            card_id=0,
                            logical_unit=0,
                            dma_id=0,
                            chunk_trailer_size= 32,
                            dma_block_size_kb= 4,
                            dma_memory_size_gb= 4,
                            numa_id=0,
                            num_links=min(5,NUMBER_OF_DATA_PRODUCERS)
                        )),
                ("flxcard_1",flxcr.Conf(
                            card_id=0,
                            logical_unit=1,
                            dma_id=0,
                            chunk_trailer_size= 32,
                            dma_block_size_kb= 4,
                            dma_memory_size_gb= 4,
                            numa_id=0,
                            num_links=max(0, NUMBER_OF_DATA_PRODUCERS-5)
                        )),
            ] + [
                (f"datahandler_{idx}", dlh.Conf(
                        raw_type = "wib",
                        emulator_mode = EMULATOR_MODE,
                        # fake_trigger_flag=0, # default
                        source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                        latency_buffer_size = 3*CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR),
                        pop_limit_pct = 0.8,
                        pop_size_pct = 0.1,
                        apa_number = 0,
                        link_number = idx
                        )) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ])

    startpars = rccmd.StartParams(run=RUN_NUMBER, trigger_interval_ticks=trg_interval_ticks, disable_data_storage=DISABLE_OUTPUT)
    startcmd = mrccmd("start", "CONFIGURED", "RUNNING", [
            ("qton_token", startpars),
            ("datawriter", startpars),
            ("ffr", startpars),
            ("qton_timesync", startpars),
            ("datahandler_.*", startpars),
            ("fake_source", startpars),
            ("flxcard.*", startpars),
            ("rqg", startpars),
            ("ntoq_trigdec", startpars),
        ])

    stopcmd = mrccmd("stop", "RUNNING", "CONFIGURED", [
            ("ntoq_trigdec", None),
            ("rqg", None),
            ("flxcard.*", None),
            ("fake_source", None),
            ("datahandler_.*", None),
            ("qton_timesync", None),
            ("ffr", None),
            ("datawriter", None),
            ("qton_token", None),
        ])

    pausecmd = mrccmd("pause", "RUNNING", "RUNNING", [
            ("", None)
        ])

    resumecmd = mrccmd("resume", "RUNNING", "RUNNING", [
            ("tde", tde.ResumeParams(
                            trigger_interval_ticks=trg_interval_ticks
                        ))
        ])

    scrapcmd = mrccmd("scrap", "CONFIGURED", "INITIAL", [
            ("", None)
        ])

    # Create a list of commands
    cmd_seq = [initcmd, confcmd, startcmd, stopcmd, pausecmd, resumecmd, scrapcmd]

    # Print them as json (to be improved/moved out)
    jstr = json.dumps([c.pod() for c in cmd_seq], indent=4, sort_keys=True)
    return jstr

#===============================================================================
def generate_trigemu(
        network_endpoints,
        NUMBER_OF_DATA_PRODUCERS=2,          
        DATA_RATE_SLOWDOWN_FACTOR = 1,
        RUN_NUMBER = 333, 
        TRIGGER_RATE_HZ = 1.0,
        DATA_FILE="./frames.bin",
        OUTPUT_PATH=".",
        TOKEN_COUNT=10
    ):
    """Generate the json config for the TriggerDecisionEmulator process"""
    
    trg_interval_ticks = math.floor((1/TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR)

    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_from_netq", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="token_from_netq", kind='FollySPSCQueue', capacity=20),
            app.QueueSpec(inst="trigger_decision_to_netq", kind='FollySPSCQueue', capacity=20),
        ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("qton_trigdec", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="trigger_decision_to_netq", dir="input")
                    ]),

        mspec("ntoq_token", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="token_from_netq", dir="output")
                    ]),

        mspec("ntoq_timesync", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="time_sync_from_netq", dir="output")
                    ]),

        mspec("tde", "TriggerDecisionEmulator", [
                        app.QueueInfo(name="time_sync_source", inst="time_sync_from_netq", dir="input"),
                        app.QueueInfo(name="token_source", inst="token_from_netq", dir="input"),
                        app.QueueInfo(name="trigger_decision_sink", inst="trigger_decision_to_netq", dir="output"),
                    ]),
        ]

    init_specs = app.Init(queues=queue_specs, modules=mod_specs)

    initcmd = rccmd.RCCommand(
        id=basecmd.CmdId("init"),
        entry_state="NONE",
        exit_state="INITIAL",
        data=init_specs
    )

    confcmd = mrccmd("conf", "INITIAL", "CONFIGURED",[
                ("qton_trigdec", qton.Conf(msg_type="dunedaq::dfmessages::TriggerDecision",
                                           msg_module_name="TriggerDecisionNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=network_endpoints["trigdec"],
                                                                  stype="msgpack")
                                           )
                 ),

                 ("ntoq_token", ntoq.Conf(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                            msg_module_name="TriggerDecisionTokenNQ",
                                            receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                     address=network_endpoints["triginh"])
                                            )
                 ),

                ("ntoq_timesync", ntoq.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                           msg_module_name="TimeSyncNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=network_endpoints["timesync"])
                                           )
                ),

                ("tde", tde.ConfParams(
                        links=[idx for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                        min_links_in_request=NUMBER_OF_DATA_PRODUCERS,
                        max_links_in_request=NUMBER_OF_DATA_PRODUCERS,
                        min_readout_window_ticks=math.floor(CLOCK_SPEED_HZ/(DATA_RATE_SLOWDOWN_FACTOR*1000)),
                        max_readout_window_ticks=math.floor(CLOCK_SPEED_HZ/(DATA_RATE_SLOWDOWN_FACTOR*1000)),
                        trigger_window_offset=math.floor(CLOCK_SPEED_HZ/(DATA_RATE_SLOWDOWN_FACTOR*2000)),
                        # The delay is set to put the trigger well within the latency buff
                        trigger_delay_ticks=math.floor(CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR),
                        # We divide the trigger interval by
                        # DATA_RATE_SLOWDOWN_FACTOR so the triggers are still
                        # emitted per (wall-clock) second, rather than being
                        # spaced out further
                        trigger_interval_ticks=trg_interval_ticks,
                        clock_frequency_hz=CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR,
                        initial_token_count=TOKEN_COUNT                    
                        )),
            ])

    startpars = rccmd.StartParams(run=RUN_NUMBER, disable_data_storage=False)
    startcmd = mrccmd("start", "CONFIGURED", "RUNNING", [
            ("qton_trigdec", startpars),
            ("ntoq_token", startpars),
            ("ntoq_timesync", startpars),
            ("tde", startpars),
        ])

    stopcmd = mrccmd("stop", "RUNNING", "CONFIGURED", [
            ("qton_trigdec", None),
            ("ntoq_timesync", None),
            ("ntoq_token", None),
            ("tde", None),
        ])

    pausecmd = mrccmd("pause", "RUNNING", "RUNNING", [
            ("", None)
        ])

    resumecmd = mrccmd("resume", "RUNNING", "RUNNING", [
            ("tde", tde.ResumeParams(
                            trigger_interval_ticks=trg_interval_ticks
                        ))
        ])

    scrapcmd = mrccmd("scrap", "CONFIGURED", "INITIAL", [
            ("", None)
        ])

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
    @click.option('-e', '--emulator-mode', is_flag=True)
    @click.option('-s', '--data-rate-slowdown-factor', default=1)
    @click.option('-r', '--run-number', default=333)
    @click.option('-t', '--trigger-rate-hz', default=1.0)
    @click.option('-c', '--token-count', default=10)
    @click.option('-d', '--data-file', type=click.Path(), default='./frames.bin')
    @click.option('-o', '--output-path', type=click.Path(), default='.')
    @click.option('--disable-data-storage', is_flag=True)
    @click.option('-f', '--use-felix', is_flag=True)
    @click.option('--host-ip-df', default='127.0.0.1')
    @click.option('--host-ip-trigemu', default='127.0.0.1')
    @click.argument('json_file_base', type=click.Path(), default='minidaqapp')
    def cli(number_of_data_producers, emulator_mode, data_rate_slowdown_factor, run_number, trigger_rate_hz, token_count, data_file, output_path, disable_data_storage, use_felix, host_ip_df, host_ip_trigemu, json_file_base):
        """
          JSON_FILE: Input raw data file.
          JSON_FILE: Output json configuration file.
        """

        json_file_trigemu=json_file_base+"-trgemu.json"
        if use_felix:
            json_file_df=json_file_base+"-ruflx_df.json"
        else:
            json_file_df=json_file_base+"-ruemu_df.json"

        network_endpoints={
            "trigdec" : f"tcp://{host_ip_trigemu}:12345",
            "triginh" : f"tcp://{host_ip_df}:12346",
            "timesync": f"tcp://{host_ip_df}:12347"
        }

        if token_count > 0:
            df_token_count = 0
            trigemu_token_count = token_count
        else:
            df_token_count = -1 * token_count
            trigemu_token_count = 0

        with open(json_file_trigemu, 'w') as f:
            f.write(generate_trigemu(
                    network_endpoints,
                    NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
                    DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                    RUN_NUMBER = run_number, 
                    TRIGGER_RATE_HZ = trigger_rate_hz,
                    DATA_FILE = data_file,
                    OUTPUT_PATH = output_path,
                    TOKEN_COUNT = trigemu_token_count
                ))

        with open(json_file_df, 'w') as f:
            f.write(generate_df(
                    network_endpoints,
                    NUMBER_OF_DATA_PRODUCERS = number_of_data_producers,
                    EMULATOR_MODE = emulator_mode,
                    DATA_RATE_SLOWDOWN_FACTOR = data_rate_slowdown_factor,
                    RUN_NUMBER = run_number, 
                    TRIGGER_RATE_HZ = trigger_rate_hz,
                    DATA_FILE = data_file,
                    OUTPUT_PATH = output_path,
                    DISABLE_OUTPUT = disable_data_storage,
                    FLX_INPUT = use_felix,
                    TOKEN_COUNT = df_token_count
                ))

    cli()
    
