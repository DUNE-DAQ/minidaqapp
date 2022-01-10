
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
from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction, Connection, data_request_endpoint_name

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100

class DataFlowApp(App):
    def __init__(self,
                 # NW_SPECS,
                 FRAGMENT_PRODUCERS,
                 RU_CONFIG=[],
                 RUN_NUMBER=333,
                 OUTPUT_PATH=".",
                 SYSTEM_TYPE="TPC",
                 SOFTWARE_TPG_ENABLED=False,
                 TPSET_WRITING_ENABLED=False,
                 PARTITION="UNKNOWN",
                 OPERATIONAL_ENVIRONMENT="swtest",
                 TPC_REGION_NAME_PREFIX="APA",
                 HOST="localhost",
                 MAX_FILE_SIZE=4*1024*1024*1024):
        
        """Generate the json configuration for the readout and DF process"""

        required_eps = {PARTITION+'.trigdec', PARTITION+'.triginh'}
        # if not required_eps.issubset([nw.name for nw in NW_SPECS]):
        #     raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")

        modules = []
        total_link_count = 0
        for ru in range(len(RU_CONFIG)):
            total_link_count += RU_CONFIG[ru]["channel_count"]
        
        modules += [DAQModule(name = 'trigdec_receiver',
                           plugin = 'TriggerDecisionReceiver',
                           connections = {'output': Connection('trb.trigger_decision_input_queue')},
                           conf = tdrcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                   connection_name=PARTITION+".trigdec")),
        
                    DAQModule(name = 'fragment_receiver',
                           plugin = 'FragmentReceiver',
                           connections = {'output': Connection('trb.data_fragment_input_queue')},
                           conf = frcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                  connection_name=PARTITION+".frags_0")),
                    
                    DAQModule(name = 'trb',
                           plugin = 'TriggerRecordBuilder',
                           connections = {'trigger_record_output_queue': Connection('datawriter.trigger_record_input_queue')},
                           conf = trb.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                 reply_connection_name = PARTITION+".frags_0",
                                                 map=trb.mapgeoidconnections([
                                                     trb.geoidinst(region=RU_CONFIG[ru]["region_id"],
                                                                   element=idx+RU_CONFIG[ru]["start_channel"],
                                                                   system=SYSTEM_TYPE,
                                                                   connection_name=f"{PARTITION}.datareq_{ru}")
                                                     for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
                                                 ] + ([
                                                     trb.geoidinst(region=RU_CONFIG[ru]["region_id"],
                                                                   element=idx+RU_CONFIG[ru]["start_channel"]+total_link_count,
                                                                   system=SYSTEM_TYPE,
                                                                   connection_name=f"{PARTITION}.datareq_{ru}")
                                                     for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
                                                 ] if SOFTWARE_TPG_ENABLED else []) + ([
                                                     trb.geoidinst(region=RU_CONFIG[ru]["region_id"],
                                                                   element=idx+RU_CONFIG[ru]["start_channel"],
                                                                   system="DataSelection",
                                                                   connection_name=f"{PARTITION}.ds_tp_datareq_0")
                                                     for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
                                                 ] if SOFTWARE_TPG_ENABLED else [])))),
                    DAQModule(name = 'datawriter',
                           plugin = 'DataWriter',
                           connections = {}, # {'trigger_record_input_queue': Connection('datawriter.trigger_record_q')},
                           conf = dw.ConfParams(
                               token_connection=PARTITION+".triginh",
                               data_store_parameters=hdf5ds.ConfParams(
                                   name="data_store",
                                   version = 3,
                                   operational_environment = OPERATIONAL_ENVIRONMENT,
                                   directory_path = OUTPUT_PATH,
                                   max_file_size_bytes = MAX_FILE_SIZE,
                                   disable_unique_filename_suffix = False,
                                   filename_parameters = hdf5ds.FileNameParams(
                                       overall_prefix = OPERATIONAL_ENVIRONMENT,
                                       digits_for_run_number = 6,
                                       file_index_prefix = "",
                                       digits_for_file_index = 4),
                                   file_layout_parameters = hdf5ds.FileLayoutParams(
                                       trigger_record_name_prefix= "TriggerRecord",
                                       digits_for_trigger_number = 5,
                                       path_param_list = hdf5ds.PathParamList(
                                           [hdf5ds.PathParams(detector_group_type="TPC",
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
                                                              element_name_prefix="Link")])))))]
            
        if TPSET_WRITING_ENABLED:
            for idx in range(len(RU_CONFIG)):
                modules += [DAQModule(name = f'tpset_subscriber_{idx}',
                                   plugin = "NetworkToQueue",
                                   connections = {'output':Connection(f"tpswriter.tpsets_from_netq")},
                                   conf = nor.Conf(name=f'{PARTITION}.tpsets_{idx}',
                                                   subscriptions=["TPSets"]))]

            modules += [DAQModule(name = 'tpswriter',
                               plugin = "TPSetWriter",
                               connections = {'tpset_source': Connection("tpsets_from_netq")},
                               conf = tpsw.ConfParams(max_file_size_bytes=1000000000))]

        if SOFTWARE_TPG_ENABLED:
            modules += [DAQModule(name = 'tp_fragment_receiver',
                               plugin = "FragmentReceiver",
                               connections = {'output': Connection("trb.data_fragments_q")},
                               conf = frcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                      connection_name=PARTITION+".tp_frags_0")),
                        
                        DAQModule(name = 'ds_tpset_fragment_receiver',
                               plugin = "FragmentReceiver",
                               connections = {"output": Connection("trb.data_fragments_q")},
                               conf = frcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                      connection_name=PARTITION+".frags_tpset_ds_0"))]
                        
        mgraph=ModuleGraph(modules)
        # PAR 2021-12-10 All of the dataflow app's sending and
        # receiving is done via NetworkManager, so there are no
        # endpoints for the moment
        
        # mgraph.add_endpoint("fragments",         "trb.data_fragment_input_queue",    Direction.IN)
        # mgraph.add_endpoint("trigger_decisions", "trb.trigger_decision_input_queue", Direction.IN)
        # mgraph.add_endpoint("tokens",            "datawriter.token_output_queue",    Direction.OUT)

        # for i, producer in enumerate(FRAGMENT_PRODUCERS):
        #     queue_name=f"data_request_{i}_output_queue"
        #     mgraph.add_endpoint(data_request_endpoint_name(producer), f"trb.{queue_name}", Direction.OUT)
        
        super().__init__(modulegraph=mgraph, host=HOST)
        self.export("dataflow_app.dot")

