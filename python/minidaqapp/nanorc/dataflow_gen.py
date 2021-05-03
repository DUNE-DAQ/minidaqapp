
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

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

from appfwk.utils import mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
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
    return cmd.CmdObj(modules=cmd.AddressedCmds(cmd.AddressedCmd(match=m, data=o)
            for m,o in mods))



def generate(NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        RUN_NUMBER=333, 
        OUTPUT_PATH=".",
        TOKEN_COUNT=0,):
    """Generate the json configuration for the readout and DF process"""

    cmd_data = {}

    required_eps = {'trigdec', 'triginh'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")



    # Define modules and queues
    queue_bare_specs = [app.QueueSpec(inst="token_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_from_netq", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_decision_copy_for_bookkeeping", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="trigger_record_q", kind='FollySPSCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [
        mspec("ntoq_trigdec", "NetworkToQueue", [app.QueueInfo(name="output", inst="trigger_decision_from_netq", dir="output")]),

        mspec("qton_token", "QueueToNetwork", [app.QueueInfo(name="input", inst="token_q", dir="input")]),

        mspec("rqg", "RequestGenerator", [  app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_from_netq", dir="input"),
                                            app.QueueInfo(name="trigger_decision_for_event_building", inst="trigger_decision_copy_for_bookkeeping", dir="output"),
                                         ] + [
                                            app.QueueInfo(name=f"data_request_{idx}_output_queue", inst=f"data_requests_{idx}", dir="output")
                                                for idx in range(NUMBER_OF_DATA_PRODUCERS)
                                         ]),

        mspec("ffr", "FragmentReceiver", [  app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_copy_for_bookkeeping", dir="input"),
                                            app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q", dir="output"),
                                            app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input"),]),

        mspec("datawriter", "DataWriter", [ app.QueueInfo(name="trigger_record_input_queue", inst="trigger_record_q", dir="input"),
                                            app.QueueInfo(name="token_output_queue", inst="token_q", dir="output"),]),

    ] + [
        mspec(f"ntoq_fragments_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")]) for idx, inst in enumerate(NETWORK_ENDPOINTS) if "frags" in inst
    ] + [
        mspec(f"qton_datareq_{idx}", "QueueToNetwork", [app.QueueInfo(name="input", inst=f"data_requests_{idx}", dir="input")])  for idx in range(NUMBER_OF_DATA_PRODUCERS)
    ]

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
        
                ("rqg", rqg.ConfParams(map=rqg.mapgeoidqueue([
                                rqg.geoidinst(apa=0, link=idx, queueinstance=f"data_requests_{idx}")  for idx in range(NUMBER_OF_DATA_PRODUCERS)
                            ]))),
                ("ffr", ffr.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS)),
                ("datawriter", dw.ConfParams(initial_token_count=TOKEN_COUNT,
                            data_store_parameters=hdf5ds.ConfParams(name="data_store",
                                # type = "HDF5DataStore", # default
                                directory_path = OUTPUT_PATH, # default
                                # mode = "all-per-file", # default
                                max_file_size_bytes = 1073741824,
                                disable_unique_filename_suffix = False,
                                filename_parameters = hdf5ds.HDF5DataStoreFileNameParams(overall_prefix = "swtest",
                                    digits_for_run_number = 6,
                                    file_index_prefix = "",
                                    digits_for_file_index = 4,),
                                file_layout_parameters = hdf5ds.HDF5DataStoreFileLayoutParams(trigger_record_name_prefix= "TriggerRecord",
                                    digits_for_trigger_number = 5,
                                    digits_for_apa_number = 3,
                                    digits_for_link_number = 2,)))),
            ] + [
                (f"qton_datareq_{idx}", qton.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                           msg_module_name="DataRequestNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"datareq_{idx}"],
                                                                  stype="msgpack"))) 
                 for idx in range(NUMBER_OF_DATA_PRODUCERS)
            ] + [
                (f"ntoq_fragments_{idx}", ntoq.Conf(msg_type="std::unique_ptr<dunedaq::dataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS[inst])))
                for idx, inst in enumerate(NETWORK_ENDPOINTS) if "frags" in inst
            ])






    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([("qton_token", startpars),
            ("datawriter", startpars),
            ("ffr", startpars),
            ("ntoq_fragments_.*", startpars),
            ("qton_datareq_.*", startpars),
            ("rqg", startpars),
            ("ntoq_trigdec", startpars),])

    cmd_data['stop'] = acmd([("ntoq_trigdec", None),
            ("rqg", None),
            ("qton_datareq_.*", None),
            ("ntoq_fragments_.*", None),
            ("ffr", None),
            ("datawriter", None),
            ("qton_token", None),])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    return cmd_data
