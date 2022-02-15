
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
                 HOSTIDX=0,
                 OUTPUT_PATH=".",
                 PARTITION="UNKNOWN",
                 OPERATIONAL_ENVIRONMENT="swtest",
                 TPC_REGION_NAME_PREFIX="APA",
                 HOST="localhost",
                 MAX_FILE_SIZE=4*1024*1024*1024,
                 DEBUG=False):
        
        """Generate the json configuration for the readout and DF process"""

        modules = []
        
        modules += [DAQModule(name = 'trb',
                              plugin = 'TriggerRecordBuilder',
                              connections = {'trigger_record_output_queue': Connection('datawriter.trigger_record_input_queue')},
                              conf = trb.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                    reply_connection_name = "",
                                                    map=trb.mapgeoidconnections([]))), # We patch this up in connect_fragment_producers
                    DAQModule(name = 'datawriter',
                           plugin = 'DataWriter',
                           connections = {},
                           conf = dw.ConfParams(decision_connection=f"{PARTITION}.trigdec_{HOSTIDX}",
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

        mgraph=ModuleGraph(modules)

        mgraph.add_endpoint("trigger_decisions", "trb.trigger_decision_input_queue", Direction.IN)
        
        super().__init__(modulegraph=mgraph, host=HOST)
        if DEBUG:
            self.export("dataflow_app.dot")
