
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('dfmodules/triggerrecordbuilder.jsonnet')
moo.otypes.load_types('dfmodules/datawriter.jsonnet')
moo.otypes.load_types('dfmodules/hdf5datastore.jsonnet')
moo.otypes.load_types('dfmodules/tpsetwriter.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')


# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
import dunedaq.dfmodules.tpsetwriter as tpsw
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos

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
        RUN_NUMBER=333,
        OUTPUT_PATH=".",
        TOKEN_COUNT=0,
        SYSTEM_TYPE="TPC",
        REGION_ID=0,
        SOFTWARE_TPG_ENABLED=False,
        TPSET_WRITING_ENABLED=False,
        OPERATIONAL_ENVIRONMENT="swtest"):
    """Generate the json configuration for the readout and DF process"""

    if SOFTWARE_TPG_ENABLED:
        NUMBER_OF_RAW_TP_PRODUCERS = NUMBER_OF_DATA_PRODUCERS
        NUMBER_OF_DS_TP_PRODUCERS = NUMBER_OF_DATA_PRODUCERS
    else:
        NUMBER_OF_RAW_TP_PRODUCERS = 0
        NUMBER_OF_DS_TP_PRODUCERS = 0

    if TPSET_WRITING_ENABLED:
        NUMBER_OF_TP_SUBSCRIBERS = NUMBER_OF_DATA_PRODUCERS
    else:
        NUMBER_OF_TP_SUBSCRIBERS = 0

    cmd_data = {}

    required_eps = {'trigdec', 'triginh'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")



    # Define modules and queues
    queue_bare_specs = [app.QueueSpec(inst="token_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_from_netq", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
            ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
            app.QueueSpec(inst=f"tp_data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)
            ] + [
                app.QueueSpec(inst=f"ds_tp_data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DS_TP_PRODUCERS)
            ] + ([
            app.QueueSpec(inst="tpsets_from_netq", kind='FollyMPMCQueue', capacity=1000),
            ] if TPSET_WRITING_ENABLED else [])

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("ntoq_trigdec", "NetworkToQueue", [app.QueueInfo(name="output", inst="trigger_decision_from_netq", dir="output")]),

        mspec("qton_token", "QueueToNetwork", [app.QueueInfo(name="input", inst="token_q", dir="input")]),

        mspec("trb", "TriggerRecordBuilder", [  app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_from_netq", dir="input"),
                                                app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q", dir="output"),
                                                app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input")
                                             ] + [
                                                app.QueueInfo(name=f"data_request_{idx}_output_queue", inst=f"data_requests_{idx}", dir="output")
                                                    for idx in range(NUMBER_OF_DATA_PRODUCERS)
                                             ] + [
                                                app.QueueInfo(name=f"data_request_{NUMBER_OF_DATA_PRODUCERS + idx}_output_queue", inst=f"tp_data_requests_{idx}", dir="output")
                                                    for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)
                                             ] + [
                                                app.QueueInfo(name=f"data_request_{NUMBER_OF_DATA_PRODUCERS + NUMBER_OF_RAW_TP_PRODUCERS + idx}_output_queue", inst=f"ds_tp_data_requests_{idx}", dir="output")
                                                    for idx in range(NUMBER_OF_DS_TP_PRODUCERS)
                                             ]),

        mspec("datawriter", "DataWriter", [ app.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input"),
                                            app.QueueInfo(name="token_output_queue", inst="token_q", dir="output"),]),

    ] + [
        mspec(f"ntoq_fragments_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")]) for idx, inst in enumerate(NETWORK_ENDPOINTS) if "frags" in inst
    ] + [
        mspec(f"qton_datareq_{idx}", "QueueToNetwork", [app.QueueInfo(name="input", inst=f"data_requests_{idx}", dir="input")])  for idx in range(NUMBER_OF_DATA_PRODUCERS)
    ] + [
        mspec(f"qton_tp_datareq_{idx}", "QueueToNetwork", [app.QueueInfo(name="input", inst=f"tp_data_requests_{idx}", dir="input")])  for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)
    ] + [
        mspec(f"qton_ds_tp_datareq_{idx}", "QueueToNetwork", [app.QueueInfo(name="input", inst=f"ds_tp_data_requests_{idx}", dir="input")])  for idx in range(NUMBER_OF_DS_TP_PRODUCERS)
    ] + ([        
        mspec(f"tpset_subscriber_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst=f"tpsets_from_netq", dir="output")])  for idx in range(NUMBER_OF_TP_SUBSCRIBERS)
    ]) + ([
        mspec("tpswriter", "TPSetWriter", [app.QueueInfo(name="tpset_source", inst="tpsets_from_netq", dir="input")])
    ] if TPSET_WRITING_ENABLED else [])

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)


    cmd_data['conf'] = acmd([
                ("ntoq_trigdec", ntoq.Conf(msg_type="dunedaq::dfmessages::TriggerDecision",
                                           msg_module_name="TriggerDecisionNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS["trigdec"]))),


                ("qton_token", qton.Conf(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                           msg_module_name="TriggerDecisionTokenNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS["triginh"],
                                                                  stype="msgpack"))),

                ("trb", trb.ConfParams( general_queue_timeout=QUEUE_POP_WAIT_MS,
                                        map=trb.mapgeoidqueue([
                                                trb.geoidinst(region=REGION_ID, element=idx, system=SYSTEM_TYPE, queueinstance=f"data_requests_{idx}")  for idx in range(NUMBER_OF_DATA_PRODUCERS)
                                        ] + [
                                            trb.geoidinst(region=REGION_ID, element=NUMBER_OF_DATA_PRODUCERS + idx, system=SYSTEM_TYPE, queueinstance=f"tp_data_requests_{idx}")  for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)
                                        ] + [
                                            trb.geoidinst(region=REGION_ID, element=idx, system="DataSelection", queueinstance=f"ds_tp_data_requests_{idx}")  for idx in range(NUMBER_OF_DS_TP_PRODUCERS)
                                        ]
                                                              ))),
                ("datawriter", dw.ConfParams(initial_token_count=TOKEN_COUNT,
                                data_store_parameters=hdf5ds.ConfParams(name="data_store",
                                version = 3,
                                operational_environment = OPERATIONAL_ENVIRONMENT,
                                directory_path = OUTPUT_PATH,
                                max_file_size_bytes = 4294967296,
                                disable_unique_filename_suffix = False,
                                filename_parameters = hdf5ds.FileNameParams(overall_prefix = OPERATIONAL_ENVIRONMENT,
                                    digits_for_run_number = 6,
                                    file_index_prefix = "",
                                    digits_for_file_index = 4,),
                                file_layout_parameters = hdf5ds.FileLayoutParams(trigger_record_name_prefix= "TriggerRecord",
                                    digits_for_trigger_number = 5,
                                    path_param_list = hdf5ds.PathParamList([hdf5ds.PathParams(detector_group_type="TPC",
                                                                                              detector_group_name="TPC",
                                                                                              region_name_prefix="APA",
                                                                                              element_name_prefix="Link"),
                                                                            hdf5ds.PathParams(detector_group_type="PDS",
                                                                                              detector_group_name="PDS"),
                                                                            hdf5ds.PathParams(detector_group_type="NDLArTPC",
                                                                                              detector_group_name="NDLArTPC"),
                                                                            hdf5ds.PathParams(detector_group_type="Trigger",
                                                                                              detector_group_name="Trigger"),
                                                                            hdf5ds.PathParams(detector_group_type="TPC_TP",
                                                                                              detector_group_name="TPC",
                                                                                              region_name_prefix="TP_APA",
                                                                                              element_name_prefix="Link")])
                                )))),
            ] + [
                (f"qton_datareq_{idx}", qton.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                           msg_module_name="DataRequestNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"datareq_{idx}"],
                                                                  stype="msgpack")))
                 for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
                (f"qton_tp_datareq_{idx}", qton.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                            msg_module_name="DataRequestNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                    address=NETWORK_ENDPOINTS[f"tp_datareq_{idx}"],
                                                                    stype="msgpack")))
                for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)

            ] + [
                (f"qton_ds_tp_datareq_{idx}", qton.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                                        msg_module_name="DataRequestNQ",
                                                        sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                               address=NETWORK_ENDPOINTS[f"ds_tp_datareq_{idx}"],
                                                                               stype="msgpack")))
                for idx in range(NUMBER_OF_RAW_TP_PRODUCERS)

            ] + [
                (f"ntoq_fragments_{idx}", ntoq.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS[inst])))
                for idx, inst in enumerate(NETWORK_ENDPOINTS) if "frags" in inst

            ] + [
                (f"tpset_subscriber_{idx}", ntoq.Conf(
                    msg_type="dunedaq::trigger::TPSet",
                    msg_module_name="TPSetNQ",
                    receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                             address=NETWORK_ENDPOINTS[f'tpsets_{idx}'],
                                             subscriptions=["TPSets"])
                ))
                for idx in range(NUMBER_OF_TP_SUBSCRIBERS)
            ] + ([
                ("tpswriter", tpsw.ConfParams(
                    max_file_size_bytes=1000000000,
                ))] if TPSET_WRITING_ENABLED else [])
            )

    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([] +
            ([("tpswriter", startpars),
              ("tpset_subscriber_.*", startpars)
            ] if TPSET_WRITING_ENABLED else [])
            + [
            ("qton_token", startpars),
            ("datawriter", startpars),
            ("ntoq_fragments_.*", startpars),
            ("qton_datareq_.*", startpars),
            ("trb", startpars),
            ("ntoq_trigdec", startpars),
            ("qton_tp_datareq_.*", startpars),
            ("qton_ds_tp_datareq_.*", startpars)])

    cmd_data['stop'] = acmd([("ntoq_trigdec", None),
            ("trb", None),
            ("qton_datareq_.*", None),
            ("ntoq_fragments_.*", None),
            ("datawriter", None),
            ("qton_token", None),
            ("qton_tp_datareq_.*", None),
            ("qton_ds_tp_datareq_.*", None),
            ] + ([
              ("tpset_subscriber_.*", None),
              ("tpswriter", None)
              ] if TPSET_WRITING_ENABLED else [])
            )

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
