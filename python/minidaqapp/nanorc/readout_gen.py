
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
moo.otypes.load_types('readout/sourceemulatorconfig.jsonnet')
moo.otypes.load_types('readout/readoutconfig.jsonnet')
moo.otypes.load_types('lbrulibs/pacmancardreader.jsonnet')
moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')
moo.otypes.load_types('dfmodules/requestreceiver.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.readout.sourceemulatorconfig as sec
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.readoutconfig as rconf
import dunedaq.lbrulibs.pacmancardreader as pcr
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.fakedataprod as fdp
import dunedaq.dfmodules.requestreceiver as rrcv
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from os import path

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(
        NW_SPECS,
        NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR=1,
        RUN_NUMBER=333, 
        DATA_FILE="./frames.bin",
        FLX_INPUT=False,
        SSP_INPUT=True,
        CLOCK_SPEED_HZ=50000000,
        HOSTIDX=0, CARDID=0,
        RAW_RECORDING_ENABLED=False,
        RAW_RECORDING_OUTPUT_DIR=".",
        FRONTEND_TYPE='wib',
        SYSTEM_TYPE='TPC',
        REGION_ID=0,
        DQM_ENABLED=False,
        SOFTWARE_TPG_ENABLED=False,
        USE_FAKE_DATA_PRODUCERS=False,
        PARTITION="UNKNOWN"):
    """Generate the json configuration for the readout and DF process"""

    cmd_data = {}

    required_eps = {f'{PARTITION}.timesync_{HOSTIDX}'}
    if not required_eps.issubset([nw.name for nw in NW_SPECS]):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")

    LATENCY_BUFFER_SIZE = 3 * CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR)
    RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)

    MIN_LINK = HOSTIDX*NUMBER_OF_DATA_PRODUCERS
    MAX_LINK = MIN_LINK + NUMBER_OF_DATA_PRODUCERS
    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if not USE_FAKE_DATA_PRODUCERS:
        queue_bare_specs += [
            app.QueueSpec(inst=f"{FRONTEND_TYPE}_link_{idx}", kind='FollySPSCQueue', capacity=100000)
            for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    if SOFTWARE_TPG_ENABLED:
        queue_bare_specs += [
            app.QueueSpec(inst=f"tp_link_{idx}", kind='FollySPSCQueue', capacity=100000)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            app.QueueSpec(inst=f"tp_fragments_q", kind='FollyMPMCQueue', capacity=100)
        ] + [
            app.QueueSpec(inst=f"tpset_queue", kind='FollyMPMCQueue', capacity=10000)
        ] + [
            app.QueueSpec(inst=f"tp_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    if DQM_ENABLED:
        queue_bare_specs += [
            app.QueueSpec(inst="data_fragments_q_dqm", kind='FollyMPMCQueue', capacity=1000),
        ] + [
            app.QueueSpec(inst=f"data_requests_dqm_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(MIN_LINK,MAX_LINK)
        ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [
        mspec(f"request_receiver", "RequestReceiver", [app.QueueInfo(name="output", inst=f"data_requests_{idx}", dir="output")]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
    ]

    if DQM_ENABLED:
        mod_specs += [
            mspec("qton_fragments_dqm", "QueueToNetwork",
                  [app.QueueInfo(name="input", inst="data_fragments_q_dqm", dir="input")])
        ] + [
            mspec(f"ntoq_datareq_dqm_{idx}", "NetworkToQueue",
                  [app.QueueInfo(name="output", inst=f"data_requests_dqm_{idx}", dir="output")])
            for idx in range(MIN_LINK,MAX_LINK)
        ]

    if SOFTWARE_TPG_ENABLED:
        mod_specs += [
            mspec("qton_tp_fragments", "QueueToNetwork", [app.QueueInfo(name="input", inst="tp_fragments_q", dir="input")])
        ] + [
            mspec(f"tp_request_receiver", "RequestReceiver", [app.QueueInfo(name="output", inst=f"tp_requests_{idx}", dir="output")]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            mspec(f"tp_datahandler_{idx}", "DataLinkHandler", [
                app.QueueInfo(name="raw_input", inst=f"tp_link_{idx}", dir="input"),
                app.QueueInfo(name="data_requests_0", inst=f"tp_requests_{idx}", dir="input"),
                app.QueueInfo(name="data_response_0", inst="tp_fragments_q", dir="output")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            mspec(f"tpset_publisher", "QueueToNetwork", [
                app.QueueInfo(name="input", inst=f"tpset_queue", dir="input")
            ])
        ]

    # There are two flags to be checked so I think a for loop
    # is the closest way to the blocks that are being used here

    for idx in range(NUMBER_OF_DATA_PRODUCERS):
        if USE_FAKE_DATA_PRODUCERS:
            mod_specs = mod_specs + [
                mspec(f"fakedataprod_{idx}", "FakeDataProd", [
                    app.QueueInfo(name="data_request_input_queue", inst=f"data_requests_{idx}", dir="input"),
                ])
            ]
        else:
            ls = [
                    app.QueueInfo(name="raw_input", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="input"),
                    app.QueueInfo(name="data_requests_0", inst=f"data_requests_{idx}", dir="input"),
                ]
            if DQM_ENABLED:
                ls.extend([
                    app.QueueInfo(name="data_requests_1", inst=f"data_requests_dqm_{idx+MIN_LINK}", dir="input"),
                    app.QueueInfo(name="data_response_1", inst="data_fragments_q_dqm", dir="output")])

            if SOFTWARE_TPG_ENABLED:
                ls.extend([
                    app.QueueInfo(name="tp_out", inst=f"tp_link_{idx}", dir="output"),
                    app.QueueInfo(name="tpset_out", inst=f"tpset_queue", dir="output")
                ])

            mod_specs += [mspec(f"datahandler_{idx}", "DataLinkHandler", ls)]

    if not USE_FAKE_DATA_PRODUCERS:
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
        elif SSP_INPUT:
            mod_specs.append(mspec("ssp_0", "SSPCardReader", [
                            app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                                for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ]))

        else:
            fake_source = "fake_source"
            card_reader = "FakeCardReader"
            if FRONTEND_TYPE=='pacman':
              fake_source = "pacman_source"
              card_reader = "PacmanCardReader"

            mod_specs.append(mspec(fake_source, card_reader, [
                            app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                                for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ]))

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)

    conf_list = [("fake_source",sec.Conf(
                            link_confs=[sec.LinkConfiguration(
                            geoid=sec.GeoID(system=SYSTEM_TYPE, region=REGION_ID, element=idx),
                                slowdown=DATA_RATE_SLOWDOWN_FACTOR,
                                queue_name=f"output_{idx}",
                                data_filename = DATA_FILE
                                ) for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                            # input_limit=10485100, # default
                            queue_timeout_ms = QUEUE_POP_WAIT_MS)),
                ("pacman_source",pcr.Conf(
                                          link_confs=[pcr.LinkConfiguration(
                                           geoid=pcr.GeoID(system=SYSTEM_TYPE, region=REGION_ID, element=idx),
                                           ) for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                                           zmq_receiver_timeout = 10000)),
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
                ("ssp_0",flxcr.Conf(card_id=CARDID,
                            logical_unit=0,
                            dma_id=0,
                            chunk_trailer_size= 32,
                            dma_block_size_kb= 4,
                            dma_memory_size_gb= 4,
                            numa_id=0,
                            num_links=NUMBER_OF_DATA_PRODUCERS)),
            ] + [
                 ("request_receiver", rrcv.ConfParams(
                            map = [rrcv.geoidinst(region=HOSTIDX , element=idx , system=SYSTEM_TYPE , queueinstance=f"data_requests_{idx}") for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                            general_queue_timeout = QUEUE_POP_WAIT_MS,
                            connection_name = f"{PARTITION}.datareq_{HOSTIDX}"
                 )) 
            ] + [
                (f"datahandler_{idx}", rconf.Conf(
                        readoutmodelconf= rconf.ReadoutModelConf(
                            source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                            # fake_trigger_flag=0, # default
                            region_id = REGION_ID,
                            element_id = idx,
                            timesync_connection_name = f"{PARTITION}.timesync_{HOSTIDX}",
                            timesync_topic_name = "Timesync",
                        ),
                        latencybufferconf= rconf.LatencyBufferConf(
                            latency_buffer_size = LATENCY_BUFFER_SIZE,
                            region_id = REGION_ID,
                            element_id = idx,
                        ),
                        rawdataprocessorconf= rconf.RawDataProcessorConf(
                            region_id = REGION_ID,
                            element_id = idx,
                            enable_software_tpg = SOFTWARE_TPG_ENABLED,
                            emulator_mode = EMULATOR_MODE
                        ),
                        requesthandlerconf= rconf.RequestHandlerConf(
                            latency_buffer_size = LATENCY_BUFFER_SIZE,
                            pop_limit_pct = 0.8,
                            pop_size_pct = 0.1,
                            region_id = REGION_ID,
                            element_id = idx,
                            output_file = f"output_{HOSTIDX}_{idx}.out",
                            stream_buffer_size = 8388608,
                            enable_raw_recording = RAW_RECORDING_ENABLED,
                        )
                        )) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
                (f"tp_datahandler_{idx}", rconf.Conf(
                        readoutmodelconf= rconf.ReadoutModelConf(
                            source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                            # fake_trigger_flag=0, default
                            region_id = REGION_ID,
                            element_id = TOTAL_NUMBER_OF_DATA_PRODUCERS + idx,
                        ),
                        latencybufferconf= rconf.LatencyBufferConf(
                            latency_buffer_size = LATENCY_BUFFER_SIZE,
                            region_id = REGION_ID,
                            element_id = TOTAL_NUMBER_OF_DATA_PRODUCERS + idx,
                        ),
                        rawdataprocessorconf= rconf.RawDataProcessorConf(
                            region_id = REGION_ID,
                            element_id = TOTAL_NUMBER_OF_DATA_PRODUCERS + idx,
                            enable_software_tpg = False,
                        ),
                        requesthandlerconf= rconf.RequestHandlerConf(
                            latency_buffer_size = LATENCY_BUFFER_SIZE,
                            pop_limit_pct = 0.8,
                            pop_size_pct = 0.1,
                            region_id = REGION_ID,
                            element_id = TOTAL_NUMBER_OF_DATA_PRODUCERS + idx,
                            # output_file = f"output_{idx + MIN_LINK}.out",
                            stream_buffer_size = 100 if FRONTEND_TYPE=='pacman' else 8388608,
                            enable_raw_recording = False,
                        )
                        )) for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ]

    if DQM_ENABLED:
        conf_list.append( ("qton_fragments_dqm", qton.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"fragx_dqm_{HOSTIDX}"],
                                                                  stype="msgpack"))) )
        conf_list.extend( (f"ntoq_datareq_dqm_{idx}", ntoq.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                        msg_module_name="DataRequestNQ",
                                        receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                 address=NETWORK_ENDPOINTS[f"datareq_dqm_{HOSTIDX}_{idx}"])))
                          for idx in range(NUMBER_OF_DATA_PRODUCERS) )

    if SOFTWARE_TPG_ENABLED:
        conf_list.extend([
                            ("qton_tp_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                                            msg_module_name="FragmentNQ",
                                                            sender_config=nos.Conf(name=f"{PARTITION}.tp_frags_0",
                                                                                   stype="msgpack")))
                        ] + [
                            ("tp_request_receiver", rrcv.ConfParams(
                                        map = [rrcv.geoidinst(region=HOSTIDX , element=NUMBER_OF_DATA_PRODUCERS+idx , system=SYSTEM_TYPE , queueinstance=f"tp_requests_{idx}") for idx in range(NUMBER_OF_DATA_PRODUCERS)],
                                        general_queue_timeout = QUEUE_POP_WAIT_MS,
                                        connection_name = f"{PARTITION}.tp_datareq_{HOSTIDX}")) 
                        ] + [
                            (f"tpset_publisher", qton.Conf(msg_type="dunedaq::trigger::TPSet",
                                                                 msg_module_name="TPSetNQ",
                                                                 sender_config=nos.Conf(name=f"{PARTITION}.tpsets_{HOSTIDX}",
                                                                                        topic="TPSets",
                                                                                        stype="msgpack")))
                        ])

    if USE_FAKE_DATA_PRODUCERS:
        conf_list.extend([
            (f"fakedataprod_{idx}", fdp.ConfParams(
                system_type = SYSTEM_TYPE,
                apa_number = HOSTIDX,
                link_number = idx,
                time_tick_diff = 25,
                frame_size = 464,
                response_delay = 0,
                timesync_connection_name = f"{PARTITION}.timesync_{HOSTIDX}",
                timesync_topic_name = "Timesync",
                fragment_type = "FakeData")) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ])

    cmd_data['conf'] = acmd(conf_list)


    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([
            ("qton_fragments_dqm", startpars),
            ("datahandler_.*", startpars),
            ("fake_source", startpars),
            ("pacman_source", startpars),
            ("flxcard.*", startpars),
            ("request_receiver", startpars),
            ("tp_request_receiver", startpars),
            ("ssp.*", startpars),
            ("ntoq_trigdec", startpars),
            ("qton_tp_fragments", startpars),
            (f"tp_datahandler_.*", startpars),
            (f"tpset_publisher", startpars),
            ("fakedataprod_.*", startpars)])

    cmd_data['stop'] = acmd([
            ("request_receiver", None),
            ("flxcard.*", None),
            ("ssp.*", None),
            ("fake_source", None),
            ("pacman_source", None),
            ("datahandler_.*", None),
            ("qton_fragments_dqm", None),
            ("qton_tp_fragments", None),
            (f"tp_datahandler_.*", None),
            (f"tpset_publisher", None),
            ("tp_request_receiver", None),
            ("fakedataprod_.*", None)])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("request_receiver", None),
            ("tp_request_receiver", None),
            ("qton_tp_fragments", None),
            (f"tpset_publisher", None),
            ("timesync_to_network", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
