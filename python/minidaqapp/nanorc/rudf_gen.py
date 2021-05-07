
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

from os.path import join

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

#moo.otypes.load_types('trigemu/faketimesyncsource.jsonnet')
moo.otypes.load_types('dfmodules/requestgenerator.jsonnet')
moo.otypes.load_types('dfmodules/fragmentreceiver.jsonnet')
moo.otypes.load_types('dfmodules/datawriter.jsonnet')
moo.otypes.load_types('dfmodules/hdf5datastore.jsonnet')
#moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')
moo.otypes.load_types('readout/fakecardreader.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')
moo.otypes.load_types('readout/datarecorder.jsonnet')


# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd, 
import dunedaq.rcif.cmd as rccmd # AddressedCmd, 
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.dfmodules.requestgenerator as rqg
import dunedaq.dfmodules.fragmentreceiver as ffr
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
#import dunedaq.dfmodules.fakedataprod as fdp
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.readout.fakecardreader as fakecr
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh
import dunedaq.readout.datarecorder as dr

from appfwk.utils import mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS=100;
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def acmd(mods: list):
    """ 
    Helper function to create appfwk's Commands addressed to modules.
        
    :param      cmdid:  The coommand id
    :type       cmdid:  str
    :param      mods:   List of module name/data structures 
    :type       mods:   list
    
    :returns:   A constructed Command object
    :rtype:     dunedaq.appfwk.cmd.Command
    """
    return cmd.CmdObj(
        modules=cmd.AddressedCmds(
            cmd.AddressedCmd(match=m, data=o)
            for m,o in mods
        )
    )


def generate(
        NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR = 1,
        RUN_NUMBER = 333, 
        DATA_FILE="./frames.bin",
        OUTPUT_PATH=".",
        FLX_INPUT=True,
        TOKEN_COUNT=0,
        CLOCK_SPEED_HZ = 50000000,
        RAW_RECORDING_ENABLED=False,
        RAW_RECORDING_OUTPUT_DIR="."

    ):
    """Generate the json configuration for the readout and DF process"""

    cmd_data = {}

    required_eps = {'trigdec', 'triginh', 'timesync'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")



    LATENCY_BUFFER_SIZE=3*CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR)
    RATE_KHZ=CLOCK_SPEED_HZ/(25*12*DATA_RATE_SLOWDOWN_FACTOR*1000)

    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_q", kind='FollyMPMCQueue', capacity=100),
            app.QueueSpec(inst="token_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_from_netq", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_copy_for_bookkeeping", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=100*NUMBER_OF_DATA_PRODUCERS),
        ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [

            app.QueueSpec(inst=f"wib_link_{idx}", kind='FollySPSCQueue', capacity=100000)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if RAW_RECORDING_ENABLED:
        queue_bare_specs = queue_bare_specs + [
            app.QueueSpec(inst=f"raw_recording_link_{idx}", kind='FollySPSCQueue', capacity=100000)
            for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
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
                        app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input"),
                    ]),

        mspec("datawriter", "DataWriter", [
                        app.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input"),
                        app.QueueInfo(name="token_output_queue", inst="token_q", dir="output"),
                    ]),

        ]

    if RAW_RECORDING_ENABLED:
        mod_specs = mod_specs + [
            mspec(f"datahandler_{idx}", "DataLinkHandler", [
                app.QueueInfo(name="raw_input", inst=f"wib_link_{idx}", dir="input"),
                app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                app.QueueInfo(name="requests", inst=f"data_requests_{idx}", dir="input"),
                app.QueueInfo(name="fragments", inst="data_fragments_q", dir="output"),
                app.QueueInfo(name="snb", inst=f"raw_recording_link_{idx}", dir="output")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            mspec(f"data_recorder_{idx}", "DataRecorder", [
                app.QueueInfo(name="snb", inst=f"raw_recording_link_{idx}", dir="input")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    else:
        mod_specs = mod_specs + [
            mspec(f"datahandler_{idx}", "DataLinkHandler", [
                app.QueueInfo(name="raw_input", inst=f"wib_link_{idx}", dir="input"),
                app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                app.QueueInfo(name="requests", inst=f"data_requests_{idx}", dir="input"),
                app.QueueInfo(name="fragments", inst="data_fragments_q", dir="output")
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

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)


    cmd_data['conf'] = acmd([
                ("ntoq_trigdec", ntoq.Conf(msg_type="dunedaq::dfmessages::TriggerDecision",
                                           msg_module_name="TriggerDecisionNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS["trigdec"])
                                           )
                 ),

                ("qton_token", qton.Conf(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                           msg_module_name="TriggerDecisionTokenNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS["triginh"],
                                                                  stype="msgpack")
                                           )
                 ),

                ("qton_timesync", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                            msg_module_name="TimeSyncNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                   address=NETWORK_ENDPOINTS["timesync"],
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
                            rate_khz = RATE_KHZ,
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
                        latency_buffer_size = LATENCY_BUFFER_SIZE,
                        pop_limit_pct = 0.8,
                        pop_size_pct = 0.1,
                        apa_number = 0,
                        link_number = idx
                        )) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
                (f"data_recorder_{idx}", dr.Conf(
                        output_file = join(RAW_RECORDING_OUTPUT_DIR, f"output_{idx}.out"),
                        compression_algorithm = "None",
                        stream_buffer_size = 8388608
                        )) for idx in (range(NUMBER_OF_DATA_PRODUCERS) if RAW_RECORDING_ENABLED else [])
            ])


    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([
            ("qton_token", startpars),
            ("datawriter", startpars),
            ("ffr", startpars),
            ("qton_timesync", startpars),
            ("datahandler_.*", startpars),
            ("data_recorder_.*", startpars),
            ("fake_source", startpars),
            ("flxcard.*", startpars),
            ("rqg", startpars),
            ("ntoq_trigdec", startpars),
        ])

    cmd_data['stop'] = acmd([
            ("ntoq_trigdec", None),
            ("rqg", None),
            ("flxcard.*", None),
            ("fake_source", None),
            ("datahandler_.*", None),
            ("data_recorder_.*", None),
            ("qton_timesync", None),
            ("ffr", None),
            ("datawriter", None),
            ("qton_token", None),
        ])

    cmd_data['pause'] = acmd([
            ("", None)
        ])

    cmd_data['resume'] = acmd([
            ("", None)
        ])

    cmd_data['scrap'] = acmd([
            ("", None)
        ])

    cmd_data['record'] = acmd([
            ("", None)
        ])

    return cmd_data
