
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')
moo.otypes.load_types('readout/fakecardreader.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')
moo.otypes.load_types('readout/datarecorder.jsonnet')
moo.otypes.load_types('dqm/dqmprocessor.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.readout.fakecardreader as fakecr
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh
import dunedaq.readout.datarecorder as dr
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dqm.dqmprocessor as dqmprocessor

from appfwk.utils import acmd, mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR=1,
        RUN_NUMBER=333, 
        DATA_FILE="./frames.bin",
        FLX_INPUT=True,
        CLOCK_SPEED_HZ=50000000,
        HOSTIDX=0, CARDID=0,
        RAW_RECORDING_ENABLED=False,
        RAW_RECORDING_OUTPUT_DIR=".",
        FRONTEND_TYPE='wib',
        SYSTEM_TYPE='TPC',
        DQM_ENABLED=False):
    """Generate the json configuration for the readout and DF process"""

    cmd_data = {}

    required_eps = {f'timesync_{HOSTIDX}'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")



    LATENCY_BUFFER_SIZE = 3 * CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR)
    RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)


    MIN_LINK = HOSTIDX*NUMBER_OF_DATA_PRODUCERS
    MAX_LINK = MIN_LINK + NUMBER_OF_DATA_PRODUCERS
    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_q", kind='FollyMPMCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
        ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(MIN_LINK,MAX_LINK)
        ] + [
            app.QueueSpec(inst=f"{FRONTEND_TYPE}_link_{idx}", kind='FollySPSCQueue', capacity=100000)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if RAW_RECORDING_ENABLED:
        queue_bare_specs = queue_bare_specs + [
            app.QueueSpec(inst=f"{FRONTEND_TYPE}_recording_link_{idx}", kind='FollySPSCQueue', capacity=100000)
            for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if DQM_ENABLED:
        queue_bare_specs += [
            app.QueueSpec(inst=f"time_sync_dqm_q", kind='FollyMPMCQueue', capacity=1000),
            app.QueueSpec(inst="data_fragments_q_dqm", kind='FollyMPMCQueue', capacity=1000),
            app.QueueSpec(inst="trigger_decision_q_dqm", kind='FollySPSCQueue', capacity=20),
            app.QueueSpec(inst="trigger_record_q_dqm", kind='FollySPSCQueue', capacity=20),
        ] + [
            app.QueueSpec(inst=f"data_requests_dqm_{idx+MIN_LINK}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [
        # mspec("qton_timesync", "QueueToNetwork", [app.QueueInfo(name="input", inst="time_sync_q", dir="input")]),
        mspec("qton_fragments", "QueueToNetwork", [app.QueueInfo(name="input", inst="data_fragments_q", dir="input")]),
    ] + [
        mspec(f"ntoq_datareq_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst=f"data_requests_{idx}", dir="output")]) for idx in range(MIN_LINK,MAX_LINK)
    ]


    # There are two flags to be checked so I think a for loop
    # is the closest way to the blocks that are being used here

    for idx in range(NUMBER_OF_DATA_PRODUCERS):
        ls = [
                app.QueueInfo(name="raw_input", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="input"),
                app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                app.QueueInfo(name="data_requests_0", inst=f"data_requests_{idx + MIN_LINK}", dir="input"),
                app.QueueInfo(name="data_response_0", inst="data_fragments_q", dir="output"),
            ]

        if RAW_RECORDING_ENABLED:
            ls.append(app.QueueInfo(name="raw_recording", inst=f"{FRONTEND_TYPE}_recording_link_{idx}", dir="output"))
        else:
            # This else can be removed once readout doesn't always check for the raw_recording queue
            # At the moment it is needed or it won't find the queue and crash
            ls.append(app.QueueInfo(name="raw_recording", inst=f"{FRONTEND_TYPE}_recording_link_{idx}", dir="output"))

        if DQM_ENABLED:
            ls.extend([
            app.QueueInfo(name="data_requests_1", inst=f"data_requests_dqm_{idx+MIN_LINK}", dir="input"),
            app.QueueInfo(name="data_response_1", inst="data_fragments_q_dqm", dir="output")])
        mod_specs += [mspec(f"datahandler_{idx + MIN_LINK}", "DataLinkHandler", ls)]

    if RAW_RECORDING_ENABLED:
        mod_specs += [
            mspec(f"data_recorder_{idx}", "DataRecorder", [
                app.QueueInfo(name="raw_recording", inst=f"{FRONTEND_TYPE}_recording_link_{idx}", dir="input")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    mod_specs += [mspec("timesync_to_network", "QueueToNetwork",
              [app.QueueInfo(name="input", inst="time_sync_q", dir="input")]
              )]

    if DQM_ENABLED:
        mod_specs += [mspec("trb_dqm", "TriggerRecordBuilder", [
                        app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_q_dqm", dir="input"),
                        app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q_dqm", dir="output"),
                        app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q_dqm", dir="input")
                    ] + [
                        app.QueueInfo(name=f"data_request_{idx}_output_queue", inst=f"data_requests_dqm_{idx+MIN_LINK}", dir="output")
                            # for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                    ]),
        ]
        mod_specs += [mspec("dqmprocessor", "DQMProcessor", [
                        app.QueueInfo(name="trigger_record_dqm_processor", inst="trigger_record_q_dqm", dir="input"),
                        app.QueueInfo(name="trigger_decision_dqm_processor", inst="trigger_decision_q_dqm", dir="output"),
                        # app.QueueInfo(name="timesync_dqm_processor", inst="time_sync_q", dir="input"),
                        app.QueueInfo(name="timesync_dqm_processor", inst="time_sync_dqm_q", dir="input"),
                    ]),

        ]

        mod_specs += [mspec("dqm_subscriber", "NetworkToQueue",
                [app.QueueInfo(name="output", inst="time_sync_dqm_q", dir="output")]
                )]
    if FLX_INPUT:
        mod_specs.append(mspec("flxcard_0", "FelixCardReader", [
                        app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                            for idx in range(min(5, NUMBER_OF_DATA_PRODUCERS))
                        ]))
        if NUMBER_OF_DATA_PRODUCERS > 5 :
            mod_specs.append(mspec("flxcard_1", "FelixCardReader", [
                            app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                                for idx in range(5, NUMBER_OF_DATA_PRODUCERS)
                            ]))
    else:
        mod_specs.append(mspec("fake_source", "FakeCardReader", [
                        app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                            for idx in range(NUMBER_OF_DATA_PRODUCERS)
                        ]))

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)


    cmd_data['conf'] = acmd([("qton_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::dataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"frags_{HOSTIDX}"],
                                                                  stype="msgpack"))),


                ("qton_timesync", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                            msg_module_name="TimeSyncNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                   address=NETWORK_ENDPOINTS[f"timesync_{HOSTIDX}"],
                                                                   stype="msgpack"))),
        
                ("fake_source",fakecr.Conf(
                            link_confs=[fakecr.LinkConfiguration(
                            geoid=fakecr.GeoID(system=SYSTEM_TYPE, region=0, element=idx),
                                slowdown=DATA_RATE_SLOWDOWN_FACTOR,
                                queue_name=f"output_{idx-MIN_LINK}",
                                data_filename = DATA_FILE
                                ) for idx in range(MIN_LINK,MAX_LINK)],
                            # input_limit=10485100, # default
                            queue_timeout_ms = QUEUE_POP_WAIT_MS)),
                ("flxcard_0",flxcr.Conf(card_id=CARDID,
                            logical_unit=0,
                            dma_id=0,
                            chunk_trailer_size= 32,
                            dma_block_size_kb= 4,
                            dma_memory_size_gb= 4,
                            numa_id=0,
                            num_links=min(5,NUMBER_OF_DATA_PRODUCERS))),
                ("flxcard_1",flxcr.Conf(card_id=CARDID,
                            logical_unit=1,
                            dma_id=0,
                            chunk_trailer_size= 32,
                            dma_block_size_kb= 4,
                            dma_memory_size_gb= 4,
                            numa_id=0,
                            num_links=max(0, NUMBER_OF_DATA_PRODUCERS - 5))),
            ] + [
                (f"ntoq_datareq_{idx}", ntoq.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                           msg_module_name="DataRequestNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS[f"datareq_{idx}"]))) for idx in range(MIN_LINK,MAX_LINK)
            ] + [
                (f"datahandler_{idx}", dlh.Conf(
                        emulator_mode = EMULATOR_MODE,
                        # fake_trigger_flag=0, # default
                        source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                        latency_buffer_size = LATENCY_BUFFER_SIZE,
                        pop_limit_pct = 0.8,
                        pop_size_pct = 0.1,
                        apa_number = 0,
                        link_number = idx)) for idx in range(MIN_LINK,MAX_LINK)
            ] + [
                (f"data_recorder_{idx}", dr.Conf(
                        output_file = f"output_{idx + MIN_LINK}.out",
                        compression_algorithm = "None",
                        stream_buffer_size = 8388608)) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
                ("trb_dqm", trb.ConfParams(
                        general_queue_timeout=QUEUE_POP_WAIT_MS,
                        map=trb.mapgeoidqueue([
                                trb.geoidinst(region=0, element=idx, system="TPC", queueinstance=f"data_requests_dqm_{idx}") for idx in range(MIN_LINK, MAX_LINK)
                            ]),
                        ))
            ] + [
                ('dqmprocessor', dqmprocessor.Conf(
                        mode='normal', # normal or debug
                        sdqm=[1, 1, 1],
                        kafka_address="dqmbroadcast:9092",
                        link_idx=list(range(MIN_LINK, MAX_LINK)),
                        ))
            ] + [
                ("timesync_to_network", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                msg_module_name="TimeSyncNQ",
                                sender_config=nos.Conf(ipm_plugin_type="ZmqPublisher",
                                                        address=NETWORK_ENDPOINTS[f"timesync_{HOSTIDX}"],
                                                        topic="Timesync",
                                                        stype="msgpack")
                                )
                )
            ] + [
                ("dqm_subscriber", ntoq.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                msg_module_name="TimeSyncNQ",
                                receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                                        address=NETWORK_ENDPOINTS[f"timesync_{HOSTIDX}"],
                                                        subscriptions=["Timesync"],
                                                        # stype="msgpack")
                                                         )
                                )
                )

    ])


    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([("qton_fragments", startpars),
            ("qton_timesync", startpars),
            ("datahandler_.*", startpars),
            ("data_recorder_.*", startpars),
            ("fake_source", startpars),
            ("flxcard.*", startpars),
            ("ntoq_datareq_.*", startpars),
            ("ntoq_trigdec", startpars),
            ("trb_dqm", startpars),
            ("dqmprocessor", startpars),])

    cmd_data['stop'] = acmd([("ntoq_trigdec", None),
            ("ntoq_datareq_.*", None),
            ("flxcard.*", None),
            ("fake_source", None),
            ("datahandler_.*", None),
            ("data_recorder_.*", None),
            ("qton_timesync", None),
            ("qton_fragments", None),
            ("trb_dqm", None),
            ("dqmprocessor", None),])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
