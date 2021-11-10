# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

moo.otypes.load_types('dfmodules/triggerrecordbuilder.jsonnet')
moo.otypes.load_types('dfmodules/datawriter.jsonnet')
moo.otypes.load_types('dfmodules/hdf5datastore.jsonnet')
moo.otypes.load_types('dfmodules/tpsetwriter.jsonnet')
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')

moo.otypes.load_types('readout/datalinkhandler.jsonnet')

import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.datawriter as dw
import dunedaq.dfmodules.hdf5datastore as hdf5ds
import dunedaq.dfmodules.tpsetwriter as tpsw

import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh

from collections import namedtuple

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(FRAGMENT_PRODUCERS,
             NUMBER_OF_DATA_PRODUCERS=2,
             # RUN_NUMBER=333,
             OUTPUT_PATH=".",
             TOKEN_COUNT=0,
             SYSTEM_TYPE="TPC",
             SOFTWARE_TPG_ENABLED=False,
             TPSET_WRITING_ENABLED=False):
    """Generate the json configuration for the readout and DF process"""

    if TPSET_WRITING_ENABLED:
        NUMBER_OF_TP_SUBSCRIBERS = NUMBER_OF_DATA_PRODUCERS
    else:
        NUMBER_OF_TP_SUBSCRIBERS = 0

    from .util import Module, ModuleGraph, Direction
    from .util import Connection as Conn
    from . import util
    
    modules = {}

    trb_geoid_list = []

    for producer in FRAGMENT_PRODUCERS.values():
        trb_geoid_list.append(trb.geoidinst(region = producer.geoid.region,
                                            element = producer.geoid.element,
                                            system = producer.geoid.system,
                                            queuename = producer.queue_name))
        
    # trb_geoid_list += [ trb.geoidinst(region=0, element=NUMBER_OF_DATA_PRODUCERS + idx, system=SYSTEM_TYPE, queueinstance=f"tp_data_requests_{idx}")  for idx in range(NUMBER_OF_RAW_TP_PRODUCERS) ]
    # trb_geoid_list += [ trb.geoidinst(region=0, element=idx, system="DataSelection", queueinstance=f"ds_tp_data_requests_{idx}")  for idx in range(NUMBER_OF_DS_TP_PRODUCERS) ]
    
    trb_geoid_map = trb.mapgeoidqueue(trb_geoid_list)
    
    modules["trb"] = Module(plugin = "TriggerRecordBuilder",
                            connections = {"trigger_record_output_queue" : Conn("datawriter.trigger_record_input_queue")},
                            conf = trb.ConfParams(general_queue_timeout = QUEUE_POP_WAIT_MS,
                                                  map = trb_geoid_map))

    
    modules["datawriter"] = Module(plugin = "DataWriter",
                                   connections = {},
                                   conf = dw.ConfParams(initial_token_count = TOKEN_COUNT,
                                                        data_store_parameters = hdf5ds.ConfParams(name = "data_store",
                                                                                                  # type = "HDF5DataStore", # default
                                                                                                  directory_path = OUTPUT_PATH, # default
                                                                                                  # mode = "all-per-file", # default
                                                                                                  max_file_size_bytes = 1073741824,
                                                                                                  disable_unique_filename_suffix = False,
                                                                                                  filename_parameters = hdf5ds.FileNameParams(overall_prefix = "swtest",
                                                                                                                                                         digits_for_run_number = 6,
                                                                                                                                                         file_index_prefix = "",
                                                                                                                                                         digits_for_file_index = 4,),
                                                                                                  file_layout_parameters = hdf5ds.FileLayoutParams(trigger_record_name_prefix = "TriggerRecord",
                                                                                                                                                              digits_for_trigger_number = 5,))))

    if TPSET_WRITING_ENABLED:
        modules["tpswriter"] = Module(plugin = "TPSetWriter",
                                      conf =  tpsw.ConfParams(max_file_size_bytes=1000000000,))
    
    mgraph = ModuleGraph(modules)

    mgraph.add_endpoint("fragments",         "trb.data_fragment_input_queue",    Direction.IN)
    mgraph.add_endpoint("trigger_decisions", "trb.trigger_decision_input_queue", Direction.IN)
    mgraph.add_endpoint("tokens",            "datawriter.token_output_queue",    Direction.OUT)

    for producer in FRAGMENT_PRODUCERS.values():
        mgraph.add_endpoint(producer.queue_name, f"trb.{producer.queue_name}", Direction.OUT)
        
    return mgraph
