# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')
moo.otypes.load_types('readout/sourceemulatorconfig.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')
moo.otypes.load_types('readout/datarecorder.jsonnet')
moo.otypes.load_types('dqm/dqmprocessor.jsonnet')
moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')

# Import new types

import dunedaq.readout.sourceemulatorconfig as sec
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh
import dunedaq.readout.datarecorder as dr
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dqm.dqmprocessor as dqmprocessor
import dunedaq.dfmodules.fakedataprod as fdp

import math

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100

def generate(# NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        TOTAL_NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR=1,
        # RUN_NUMBER=333, 
        DATA_FILE="./frames.bin",
        FLX_INPUT=True,
        CLOCK_SPEED_HZ=50000000,
        HOSTIDX=0, CARDID=0,
        RAW_RECORDING_ENABLED=False,
        RAW_RECORDING_OUTPUT_DIR=".",
        FRONTEND_TYPE='wib',
        SYSTEM_TYPE='TPC',
        DQM_ENABLED=False,
        DQM_KAFKA_ADDRESS='',
        SOFTWARE_TPG_ENABLED=False,
        USE_FAKE_DATA_PRODUCERS=False):
    """Generate the json configuration for the readout and DF process"""

    LATENCY_BUFFER_SIZE = 3 * CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR)
    RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)

    MIN_LINK = HOSTIDX*NUMBER_OF_DATA_PRODUCERS
    MAX_LINK = MIN_LINK + NUMBER_OF_DATA_PRODUCERS

    from .util import Module, ModuleGraph, Direction
    from .util import Connection as Conn
    from . import util

    modules = {}

    if not USE_FAKE_DATA_PRODUCERS and not FLX_INPUT:
        modules["fake_source"] = Module(plugin = "FakeCardReader",
                                        connections = { f"output_{idx-MIN_LINK}" : Conn(f"datahandler_{idx}.raw_input")
                                                        for idx in range(MIN_LINK,MAX_LINK) },
                                        conf = sec.Conf(link_confs = [ sec.LinkConfiguration(geoid=sec.GeoID(system = SYSTEM_TYPE, region = 0, element = idx),
                                                                                                   slowdown=DATA_RATE_SLOWDOWN_FACTOR,
                                                                                                   queue_name=f"output_{idx-MIN_LINK}",
                                                                                                   data_filename = DATA_FILE)
                                                                          for idx in range(MIN_LINK,MAX_LINK) ],
                                                           # input_limit=10485100, # default
                                                           queue_timeout_ms = QUEUE_POP_WAIT_MS))

    if not USE_FAKE_DATA_PRODUCERS and FLX_INPUT:
        modules["flxcard_0"] = Module(plugin = "FelixCardReader",
                                      connections = {},
                                      conf = flxcr.Conf(card_id = CARDID,
                                                        logical_unit = 0,
                                                        dma_id = 0,
                                                        chunk_trailer_size = 32,
                                                        dma_block_size_kb = 4,
                                                        dma_memory_size_gb = 4,
                                                        numa_id = 0,
                                                        num_links = min(5,NUMBER_OF_DATA_PRODUCERS)))

        modules["flxcard_1"] = Module(plugin = "FelixCardReader",
                                      connections = {},
                                      conf = flxcr.Conf(card_id = CARDID,
                                                        logical_unit = 1,
                                                        dma_id = 0,
                                                        chunk_trailer_size = 32,
                                                        dma_block_size_kb = 4,
                                                        dma_memory_size_gb = 4,
                                                        numa_id = 0,
                                                        num_links = max(0, NUMBER_OF_DATA_PRODUCERS - 5)))

    if SOFTWARE_TPG_ENABLED:
        for idx in range(MIN_LINK, MAX_LINK):
            modules[f"tp_datahandler_{TOTAL_NUMBER_OF_DATA_PRODUCERS + idx}"] = Module(plugin="DataLinkHandler",
                                                                                       connections = {},
                                                                                       conf = dlh.Conf(emulator_mode = False,
                                                                                                       enable_software_tpg = False,
                                                                                                       # fake_trigger_flag=0, # default
                                                                                                       source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                                                                                                       latency_buffer_size = LATENCY_BUFFER_SIZE,
                                                                                                       pop_limit_pct = 0.8,
                                                                                                       pop_size_pct = 0.1,
                                                                                                       apa_number = 0,
                                                                                                       link_number = TOTAL_NUMBER_OF_DATA_PRODUCERS + idx))


    if not USE_FAKE_DATA_PRODUCERS:
        for idx in range(NUMBER_OF_DATA_PRODUCERS):
            modules[f"datahandler_{idx}"] = Module(plugin = "DataLinkHandler",
                                                   connections = {"raw_recording": Conn(f"data_recorder_{idx}.raw_recording"),
                                                                  "tp_out": Conn(f"tp_datahandler_{idx}.raw_input")},
                                                   conf = dlh.Conf(emulator_mode = EMULATOR_MODE,
                                                                   enable_software_tpg = SOFTWARE_TPG_ENABLED,
                                                                   # fake_trigger_flag=0, # default
                                                                   source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                                                                   latency_buffer_size = LATENCY_BUFFER_SIZE,
                                                                   pop_limit_pct = 0.8,
                                                                   pop_size_pct = 0.1,
                                                                   apa_number = 0,
                                                                   link_number = idx))
    if RAW_RECORDING_ENABLED:
        for idx in range(NUMBER_OF_DATA_PRODUCERS):
            modules[f"data_recorder_{idx}"] = Module(plugin = "DataRecorder",
                                                     connections = {}, # No outgoing connections
                                                     conf = dr.Conf(output_file = f"output_{idx + MIN_LINK}.out",
                                                                    compression_algorithm = "None",
                                                                    stream_buffer_size = 8388608))
    if DQM_ENABLED:
        modules["trb_dqm"] = Module(plugin = "TriggerRecordBuilder",
                                    connections = {},
                                    conf = trb.ConfParams(general_queue_timeout = QUEUE_POP_WAIT_MS,
                                                          map=trb.mapgeoidqueue([trb.geoidinst(region = 0,
                                                                                               element = idx,
                                                                                               system = "TPC",
                                                                                               queuename = f"data_requests_dqm_{idx}")
                                                                                 for idx in range(MIN_LINK, MAX_LINK)])))
        modules["dqmprocessor"] = Module(plugin = "DQMProcessor",
                                         connections = {},
                                         conf = dqmprocessor.Conf(mode='normal', # normal or debug
                                                                  sdqm=[1, 1, 1],
                                                                  kafka_address=DQM_KAFKA_ADDRESS,
                                                                  link_idx=list(range(MIN_LINK, MAX_LINK)),
                                                                  clock_frequency=CLOCK_SPEED_HZ))

    if USE_FAKE_DATA_PRODUCERS:
        for idx in range(MIN_LINK,MAX_LINK):
            modules[f"fakedataprod_{idx}"] = Module(plugin = "FakeDataProducer",
                                                    connections = {},
                                                    conf = fdp.Conf(system_type = SYSTEM_TYPE,
                                                                    apa_number = 0,
                                                                    link_number = idx,
                                                                    time_tick_diff = 25,
                                                                    frame_size = 464,
                                                                    response_delay = 0,
                                                                    fragment_type = "FakeData"))

    mgraph = ModuleGraph(modules)

    for idx in range(NUMBER_OF_DATA_PRODUCERS):
        mgraph.add_endpoint(f"tpsets_{idx}", f"datahandler_{idx}.tpset_out",    Direction.OUT)
        # TODO: Should we just have one timesync outgoing endpoint?
        mgraph.add_endpoint(f"timesync_{idx}", f"datahandler_{idx}.timesync",    Direction.OUT)
        
        mgraph.add_fragment_producer(region = 0, element = idx, system = SYSTEM_TYPE,
                                     requests_in   = f"datahandler_{idx}.data_requests_0",
                                     fragments_out = f"datahandler_{idx}.data_response_0")
        mgraph.add_fragment_producer(region = 1, element = idx, system = SYSTEM_TYPE,
                                     requests_in   = f"tp_datahandler_{idx}.data_requests_0",
                                     fragments_out = f"tp_datahandler_{idx}.data_response_0")
        
    return mgraph
