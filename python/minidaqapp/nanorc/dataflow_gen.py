
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
moo.otypes.load_types('dfmodules/fragmentreceiver.jsonnet')
moo.otypes.load_types('dfmodules/triggerdecisionreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')


# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
import dunedaq.dfmodules.tpsetwriter as tpsw
import dunedaq.dfmodules.fragmentreceiver as frcv
import dunedaq.dfmodules.triggerdecisionreceiver as tdrcv
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(NW_SPECS,
        NUMBER_OF_DATA_PRODUCERS=2,
        NUMBER_OF_RU_HOSTS=1,
        RUN_NUMBER=333,
        OUTPUT_PATH=".",
        TOKEN_COUNT=0,
        SYSTEM_TYPE="TPC",
        REGION_ID=0,
        SOFTWARE_TPG_ENABLED=False,
        TPSET_WRITING_ENABLED=False,
        PARTITION="UNKNOWN",
        OPERATIONAL_ENVIRONMENT="swtest",
        TPC_REGION_NAME_PREFIX="APA"):
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

    required_eps = {PARTITION+'.trigdec', PARTITION+'.triginh'}
    if not required_eps.issubset([nw.name for nw in NW_SPECS]):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")



    # Define modules and queues
    queue_bare_specs = [app.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
            ] + ([
            app.QueueSpec(inst="tpsets_from_netq", kind='FollyMPMCQueue', capacity=1000),
            ] if TPSET_WRITING_ENABLED else [])

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("trigdec_receiver", "TriggerDecisionReceiver", [app.QueueInfo(name="output", inst="trigger_decision_q", dir="output")]),

        mspec("fragment_receiver", "FragmentReceiver", [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")]),

        mspec("trb", "TriggerRecordBuilder", [  app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_q", dir="input"),
                                                app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q", dir="output"),
                                                app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input")
                                             ]),
        
        mspec("datawriter", "DataWriter", [ app.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input")]),

    ] + ([        
        mspec(f"tpset_subscriber_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst=f"tpsets_from_netq", dir="output")])  for idx in range(NUMBER_OF_TP_SUBSCRIBERS)
    ]) + ([
        mspec("tpswriter", "TPSetWriter", [app.QueueInfo(name="tpset_source", inst="tpsets_from_netq", dir="input")])
    ] if TPSET_WRITING_ENABLED else []) + ([
        mspec("tp_fragment_receiver", "FragmentReceiver", [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")]),
        mspec("ds_tpset_fragment_receiver", "FragmentReceiver", [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")]),
    ] if SOFTWARE_TPG_ENABLED else []) 


    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)


    cmd_data['conf'] = acmd([
                ("trigdec_receiver", tdrcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                      connection_name=PARTITION+".trigdec")),

                ("trb", trb.ConfParams( general_queue_timeout=QUEUE_POP_WAIT_MS,
                                        reply_connection_name = PARTITION+".frags_0",
                                        map=trb.mapgeoidconnections([
                                                trb.geoidinst(region=idx, element=idy, system=SYSTEM_TYPE, connection_name=f"{PARTITION}.datareq_{idx}")  for idx in range(NUMBER_OF_RU_HOSTS) for idy in range(NUMBER_OF_DATA_PRODUCERS)
                                        ] + [
                                            trb.geoidinst(region=idx, element=NUMBER_OF_DATA_PRODUCERS + idy, system=SYSTEM_TYPE, connection_name=f"{PARTITION}.tp_datareq_{idx}") for idx in range(NUMBER_OF_RU_HOSTS) for idy in range(NUMBER_OF_RAW_TP_PRODUCERS)
                                        ] + [
                                            trb.geoidinst(region=idx, element=idy, system="DataSelection", connection_name=f"{PARTITION}.ds_tp_datareq_0")  for idx in range(NUMBER_OF_RU_HOSTS) for idy in range(NUMBER_OF_DATA_PRODUCERS)

                                        ]
                                                              ) )),
                ("datawriter", dw.ConfParams(initial_token_count=TOKEN_COUNT,
                                             token_connection=PARTITION+".triginh",
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
                                                                                              region_name_prefix=TPC_REGION_NAME_PREFIX,
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
                ("fragment_receiver", frcv.ConfParams(
                    general_queue_timeout=QUEUE_POP_WAIT_MS,
                    connection_name=PARTITION+".frags_0"
                )), 
                ("tp_fragment_receiver", frcv.ConfParams(
                    general_queue_timeout=QUEUE_POP_WAIT_MS,
                    connection_name=PARTITION+".tp_frags_0"
                )), 
                ("ds_tpset_fragment_receiver", frcv.ConfParams(
                    general_queue_timeout=QUEUE_POP_WAIT_MS,
                    connection_name=PARTITION+".frags_tpset_ds_0"
                )), 
            ] + [
                (f"tpset_subscriber_{idx}", ntoq.Conf(
                    msg_type="dunedaq::trigger::TPSet",
                    msg_module_name="TPSetNQ",
                    receiver_config=nor.Conf(name=f'{PARTITION}.tpsets_{idx}',
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
            ("datawriter", startpars),
            ("fragment_receiver", startpars),
            ("tp_fragment_receiver",startpars),
            ("ds_tpset_fragment_receiver",startpars),
            ("trb", startpars),
            ("trigdec_receiver", startpars)])

    cmd_data['stop'] = acmd([("trigdec_receiver", None),
            ("trb", None),
            ("fragment_receiver", None),
              ("tp_fragment_receiver",None),
              ("ds_tpset_fragment_receiver",None),
            ("datawriter", None),
            ] + ([
              ("tpset_subscriber_.*", None),
              ("tpswriter", None)
              ] if TPSET_WRITING_ENABLED else [])
            )

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([
            ("fragment_receiver", None),
            ("trigdec_receiver", None),
            ("tp_fragment_receiver",None),
            ("ds_tpset_fragment_receiver",None),
            ("qton_token", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
