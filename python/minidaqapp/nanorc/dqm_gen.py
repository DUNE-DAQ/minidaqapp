
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
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dqm.dqmprocessor as dqmprocessor

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from os import path

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        TOTAL_NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        RUN_NUMBER=333,
        DATA_FILE="./frames.bin",
        CLOCK_SPEED_HZ=50000000,
        HOSTIDX=0, CARDID=0,
        SYSTEM_TYPE='TPC',
        REGION_ID=0,
        DQM_ENABLED=False,
        DQM_KAFKA_ADDRESS='',
        DQM_CMAP='HD',
        DQM_RAWDISPLAY_PARAMS=[60, 10, 50],
        DQM_MEANRMS_PARAMS=[10, 1, 100],
        DQM_FOURIER_PARAMS=[600, 60, 100],):
    """Generate the json configuration for the dqm process"""

    cmd_data = {}

    required_eps = {f'timesync_{HOSTIDX}'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")

    MIN_LINK = HOSTIDX*NUMBER_OF_DATA_PRODUCERS
    MAX_LINK = MIN_LINK + NUMBER_OF_DATA_PRODUCERS
    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
        ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(MIN_LINK,MAX_LINK)
        ]

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

    for idx in range(NUMBER_OF_DATA_PRODUCERS):
        if USE_FAKE_DATA_PRODUCERS:
            mod_specs = mod_specs + [
                mspec(f"fakedataprod_{idx + MIN_LINK}", "FakeDataProd", [
                    app.QueueInfo(name="timesync_output_queue", inst="time_sync_q", dir="output"),
                    app.QueueInfo(name="data_request_input_queue", inst=f"data_requests_{idx + MIN_LINK}", dir="input"),
                    app.QueueInfo(name="data_fragment_output_queue", inst="data_fragments_q", dir="output")
                ])
            ]
        else:
            ls = [
                    app.QueueInfo(name="raw_input", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="input"),
                    app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                    app.QueueInfo(name="data_requests_0", inst=f"data_requests_{idx + MIN_LINK}", dir="input"),
                    app.QueueInfo(name="data_response_0", inst="data_fragments_q", dir="output"),
                ]

            ls.extend([
            app.QueueInfo(name="data_requests_1", inst=f"data_requests_dqm_{idx+MIN_LINK}", dir="input"),
            app.QueueInfo(name="data_response_1", inst="data_fragments_q_dqm", dir="output")])

            mod_specs += [mspec(f"datahandler_{idx + MIN_LINK}", "DataLinkHandler", ls)]

    mod_specs += [mspec("timesync_to_network", "QueueToNetwork",
              [app.QueueInfo(name="input", inst="time_sync_q", dir="input")]
              )]

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

            mod_specs.append(mspec(fake_source, card_reader, [
                            app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                                for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ]))

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)


    conf_list = [("qton_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"frags_{HOSTIDX}"],
                                                                  stype="msgpack"))),


                ("qton_timesync", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                            msg_module_name="TimeSyncNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                   address=NETWORK_ENDPOINTS[f"timesync_{HOSTIDX}"],
                                                                   stype="msgpack"))),

                ("fake_source",sec.Conf(
                            link_confs=[sec.LinkConfiguration(
                            geoid=sec.GeoID(system=SYSTEM_TYPE, region=REGION_ID, element=idx),
                                slowdown=DATA_RATE_SLOWDOWN_FACTOR,
                                queue_name=f"output_{idx-MIN_LINK}",
                                data_filename = DATA_FILE
                                ) for idx in range(MIN_LINK,MAX_LINK)],
                            # input_limit=10485100, # default
                            queue_timeout_ms = QUEUE_POP_WAIT_MS)),
            ] + [
                (f"ntoq_datareq_{idx}", ntoq.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                           msg_module_name="DataRequestNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS[f"datareq_{idx}"]))) for idx in range(MIN_LINK,MAX_LINK)
            ] + [
                ("trb_dqm", trb.ConfParams(
                        general_queue_timeout=QUEUE_POP_WAIT_MS,
                        map=trb.mapgeoidqueue([
                                trb.geoidinst(region=REGION_ID, element=idx, system=SYSTEM_TYPE, queueinstance=f"data_requests_dqm_{idx}") for idx in range(MIN_LINK, MAX_LINK)
                            ]),
                        ))
            ] + [
                ('dqmprocessor', dqmprocessor.Conf(
                        region=REGION_ID,
                        channel_map=DQM_CMAP, # 'HD' for horizontal drift or 'VD' for vertical drift
                        sdqm_hist=dqmprocessor.StandardDQM(**{'how_often' : DQM_RAWDISPLAY_PARAMS[0], 'unavailable_time' : DQM_RAWDISPLAY_PARAMS[1], 'num_frames' : DQM_RAWDISPLAY_PARAMS[2]}),
                        sdqm_mean_rms=dqmprocessor.StandardDQM(**{'how_often' : DQM_MEANRMS_PARAMS[0], 'unavailable_time' : DQM_MEANRMS_PARAMS[1], 'num_frames' : DQM_MEANRMS_PARAMS[2]}),
                        sdqm_fourier=dqmprocessor.StandardDQM(**{'how_often' : DQM_FOURIER_PARAMS[0], 'unavailable_time' : DQM_FOURIER_PARAMS[1], 'num_frames' : DQM_FOURIER_PARAMS[2]}),
                        kafka_address=DQM_KAFKA_ADDRESS,
                        link_idx=list(range(MIN_LINK, MAX_LINK)),
                        clock_frequency=CLOCK_SPEED_HZ,
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
            ]

    cmd_data['conf'] = acmd(conf_list)

    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([("qton_fragments", startpars),
            ("qton_timesync", startpars),
            ("datahandler_.*", startpars),
            ("fake_source", startpars),
            ("ntoq_datareq_.*", startpars),
            ("ntoq_trigdec", startpars),
            ("trb_dqm", startpars),
            ("dqmprocessor", startpars),
            ("fakedataprod_.*", startpars)])

    cmd_data['stop'] = acmd([("ntoq_trigdec", None),
            ("ntoq_datareq_.*", None),
            ("fake_source", None),
            ("datahandler_.*", None),
            ("qton_timesync", None),
            ("qton_fragments", None),
            ("trb_dqm", None),
            ("dqmprocessor", None),
            ("fakedataprod_.*", None)])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
