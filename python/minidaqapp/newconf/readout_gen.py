
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
moo.otypes.load_types('readoutlibs/sourceemulatorconfig.jsonnet')
moo.otypes.load_types('readoutlibs/readoutconfig.jsonnet')
moo.otypes.load_types('lbrulibs/pacmancardreader.jsonnet')
moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')
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
import dunedaq.readoutlibs.sourceemulatorconfig as sec
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readoutlibs.readoutconfig as rconf
import dunedaq.lbrulibs.pacmancardreader as pcr
# import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.fakedataprod as fdp
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from os import path

import json
from appfwk.conf_utils import Direction, Connection
from appfwk.daqmodule import DAQModule
from appfwk.app import App,ModuleGraph
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def get_readout_app(RU_CONFIG=[],
                    EMULATOR_MODE=False,
                    DATA_RATE_SLOWDOWN_FACTOR=1,
                    RUN_NUMBER=333, 
                    DATA_FILE="./frames.bin",
                    FLX_INPUT=False,
                    SSP_INPUT=True,
                    CLOCK_SPEED_HZ=50000000,
                    RUIDX=0,
                    RAW_RECORDING_ENABLED=False,
                    RAW_RECORDING_OUTPUT_DIR=".",
                    FRONTEND_TYPE='wib',
                    SYSTEM_TYPE='TPC',
                    SOFTWARE_TPG_ENABLED=False,
                    USE_FAKE_DATA_PRODUCERS=False,
                    PARTITION="UNKNOWN",
                    LATENCY_BUFFER_SIZE=499968,
                    HOST="localhost",
                    DEBUG=False):
    """Generate the json configuration for the readout and DF process"""
    NUMBER_OF_DATA_PRODUCERS = len(RU_CONFIG)
    cmd_data = {}
    
    required_eps = {f'{PARTITION}.timesync_{RUIDX}'}
    # if not required_eps.issubset([nw.name for nw in NW_SPECS]):
    #     raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")
    
    RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)
    
    MIN_LINK = RU_CONFIG[RUIDX]["start_channel"]
    MAX_LINK = MIN_LINK + RU_CONFIG[RUIDX]["channel_count"]
    
    if DEBUG: print(f"ReadoutApp.__init__ with RUIDX={RUIDX}, MIN_LINK={MIN_LINK}, MAX_LINK={MAX_LINK}")
    modules = []

    total_link_count = 0
    for ru in range(len(RU_CONFIG)):
        if RU_CONFIG[ru]['region_id'] == RU_CONFIG[RUIDX]['region_id']:
            total_link_count += RU_CONFIG[ru]["channel_count"]

    if SOFTWARE_TPG_ENABLED:
        connections = {}

        for idx in range(MIN_LINK, MAX_LINK):
            queue_inst = f"data_requests_{idx}"
            connections[f'output_{idx}'] = Connection(f"datahandler_{idx}.data_requests_0",
                                                      queue_name = queue_inst)
            
            queue_inst = f"tp_requests_{idx}"
            connections[f'tp_output_{idx}'] = Connection(f"tp_datahandler_{idx}.data_requests_0",
                                                             queue_name = queue_inst)
            modules += [DAQModule(name = f"tp_datahandler_{idx}",
                               plugin = "DataLinkHandler",
                               connections =  {}, #{'fragment_queue': Connection('fragment_sender.input_queue')},
                               conf = rconf.Conf(readoutmodelconf = rconf.ReadoutModelConf(source_queue_timeout_ms = QUEUE_POP_WAIT_MS,
                                                                                         region_id = RU_CONFIG[RUIDX]["region_id"],
                                                                                         element_id = total_link_count+idx),
                                                 latencybufferconf = rconf.LatencyBufferConf(latency_buffer_size = LATENCY_BUFFER_SIZE,
                                                                                            region_id = RU_CONFIG[RUIDX]["region_id"],
                                                                                            element_id = total_link_count + idx),
                                                 rawdataprocessorconf = rconf.RawDataProcessorConf(region_id = RU_CONFIG[RUIDX]["region_id"],
                                                                                                  element_id = total_link_count + idx,
                                                                                                  enable_software_tpg = False),
                                                 requesthandlerconf= rconf.RequestHandlerConf(latency_buffer_size = LATENCY_BUFFER_SIZE,
                                                                                              pop_limit_pct = 0.8,
                                                                                              pop_size_pct = 0.1,
                                                                                              region_id = RU_CONFIG[RUIDX]["region_id"],
                                                                                              element_id =total_link_count + idx,
                                                                                              # output_file = f"output_{idx + MIN_LINK}.out",
                                                                                              stream_buffer_size = 100 if FRONTEND_TYPE=='pacman' else 8388608,
                                                                                              enable_raw_recording = False)))]
        # modules += [DAQModule(name = f"tpset_publisher",
        #                    plugin = "QueueToNetwork",
        #                    # connections = {'input': Connection('tpset_queue', Direction.IN)},
        #                    conf = qton.Conf(msg_type="dunedaq::trigger::TPSet",
        #                                     msg_module_name="TPSetNQ",
        #                                     sender_config=nos.Conf(name=f"{PARTITION}.tpsets_{RUIDX}",
        #                                                            topic="TPSets",
        #                                                            stype="msgpack")))]
    if FRONTEND_TYPE == 'wib':
        modules += [DAQModule(name = "errored_frame_consumer",
                           plugin = "ErroredFrameConsumer",
                           connections={})]

    # There are two flags to be checked so I think a for loop
    # is the closest way to the blocks that are being used here
    
    for idx in range(MIN_LINK,MAX_LINK):
        if USE_FAKE_DATA_PRODUCERS:
            modules += [DAQModule(name = f"fakedataprod_{idx}",
                               plugin='FakeDataProd',
                               connections={'input': Connection(f'data_request_{idx}')})]
        else:
            connections = {}
            # connections['raw_input']      = Connection(f"{FRONTEND_TYPE}_link_{idx}", Direction.IN)
            # connections['data_request_0'] = Connection(f'data_requests_{idx}',        Direction.IN)
            # connections['fragment_queue'] = Connection('fragment_sender.input_queue')
            if SOFTWARE_TPG_ENABLED:
                connections['tp_out']    = Connection(f"tp_datahandler_{idx}.raw_input",
                                                      queue_name = f"sw_tp_link_{idx}",
                                                      queue_kind = "FollySPSCQueue",
                                                      queue_capacity = 100000)
                # connections['tpset_out'] = Connection('tpset_queue',       Direction.OUT)
                
            if FRONTEND_TYPE == 'wib':
                connections['errored_frames'] = Connection('errored_frame_consumer.input_queue')

            modules += [DAQModule(name = f"datahandler_{idx}",
                                  plugin = "DataLinkHandler", 
                                  connections = connections,
                                  conf = rconf.Conf(
                                      readoutmodelconf= rconf.ReadoutModelConf(
                                          source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
                                          # fake_trigger_flag=0, # default
                                          region_id = RU_CONFIG[RUIDX]["region_id"],
                                          element_id = idx,
                                          timesync_connection_name = f"{PARTITION}.timesync_{RUIDX}",
                                          timesync_topic_name = "Timesync",
                                      ),
                                      latencybufferconf= rconf.LatencyBufferConf(
                                          latency_buffer_alignment_size = 4096,
                                          latency_buffer_size = LATENCY_BUFFER_SIZE,
                                          region_id = RU_CONFIG[RUIDX]["region_id"],
                                          element_id = idx,
                                      ),
                                      rawdataprocessorconf= rconf.RawDataProcessorConf(
                                          region_id = RU_CONFIG[RUIDX]["region_id"],
                                          element_id = idx,
                                          enable_software_tpg = SOFTWARE_TPG_ENABLED,
                                          emulator_mode = EMULATOR_MODE,
                                          error_counter_threshold=100,
                                          error_reset_freq=10000
                                      ),
                                      requesthandlerconf= rconf.RequestHandlerConf(
                                          latency_buffer_size = LATENCY_BUFFER_SIZE,
                                          pop_limit_pct = 0.8,
                                          pop_size_pct = 0.1,
                                          region_id = RU_CONFIG[RUIDX]["region_id"],
                                          element_id = idx,
                                          output_file = path.join(RAW_RECORDING_OUTPUT_DIR, f"output_{RUIDX}_{idx}.out"),
                                          stream_buffer_size = 8388608,
                                          enable_raw_recording = RAW_RECORDING_ENABLED,
                                      )))]
                    
                    
    if not USE_FAKE_DATA_PRODUCERS:
        if FLX_INPUT:
            connections = {}
            for idx in range(MIN_LINK, MIN_LINK + min(5, RU_CONFIG[RUIDX]["channel_count"])):
                connections[f'output_{idx}'] = Connection(f"datahandler_{idx}.raw_input",
                                                          queue_name = f'{FRONTEND_TYPE}_link_{idx}',
                                                          queue_kind = "FollySPSCQueue",
                                                          queue_capacity = 100000)
            
            modules += [DAQModule(name = 'flxcard_0',
                               plugin = 'FelixCardReader',
                               connections = connections,
                               conf = flxcr.Conf(card_id = RU_CONFIG[RUIDX]["card_id"],
                                                 logical_unit = 0,
                                                 dma_id = 0,
                                                 chunk_trailer_size = 32,
                                                 dma_block_size_kb = 4,
                                                 dma_memory_size_gb = 4,
                                                 numa_id = 0,
                                                 num_links = min(5,RU_CONFIG[RUIDX]["channel_count"])))]
            
            if RU_CONFIG[RUIDX]["channel_count"] > 5 :
                connections = {}
                for idx in range(MIN_LINK+5, MAX_LINK):
                    connections[f'output_{idx}'] = Connection(f"datahandler_{idx}.raw_input",
                                                              queue_name = f'{FRONTEND_TYPE}_link_{idx}',
                                                              queue_kind = "FollySPSCQueue",
                                                              queue_capacity = 100000)
                    
                modules += [DAQModule(name = "flxcard_1",
                                   plugin = "FelixCardReader",
                                   connections = connections,
                                   conf = flxcr.Conf(card_id = RU_CONFIG[RUIDX]["card_id"],
                                                     logical_unit = 1,
                                                     dma_id = 0,
                                                     chunk_trailer_size = 32,
                                                     dma_block_size_kb = 4,
                                                     dma_memory_size_gb = 4,
                                                     numa_id = 0,
                                                     num_links = max(0, RU_CONFIG[RUIDX]["channel_count"] - 5)))]
                
        elif SSP_INPUT:
            modules += [DAQModule(name = "ssp_0",
                               plugin = "SSPCardReader",
                               connections = {f'output_{idx}': Connection(f"datahandler_{idx}.raw_input",
                                                                          queue_name = f'{FRONTEND_TYPE}_link_{idx}',
                                                                          queue_kind = "FollySPSCQueue",
                                                                          queue_capacity = 100000)},
                               conf = flxcr.Conf(card_id = RU_CONFIG[RUIDX]["card_id"],
                                                 logical_unit = 0,
                                                 dma_id = 0,
                                                 chunk_trailer_size = 32,
                                                 dma_block_size_kb = 4,
                                                 dma_memory_size_gb = 4,
                                                 numa_id = 0,
                                                 num_links = RU_CONFIG[RUIDX]["channel_count"]))]
    
        else:
            fake_source = "fake_source"
            card_reader = "FakeCardReader"
            conf = sec.Conf(link_confs = [sec.LinkConfiguration(geoid=sec.GeoID(system=SYSTEM_TYPE,
                                                                                region=RU_CONFIG[RUIDX]["region_id"],
                                                                                element=idx),
                                                                slowdown=DATA_RATE_SLOWDOWN_FACTOR,
                                                                queue_name=f"output_{idx}",
                                                                data_filename = DATA_FILE,
                                                                emu_frame_error_rate=0) for idx in range(MIN_LINK,MAX_LINK)],
                            # input_limit=10485100, # default
                            queue_timeout_ms = QUEUE_POP_WAIT_MS)
            
            if FRONTEND_TYPE=='pacman':
                fake_source = "pacman_source"
                card_reader = "PacmanCardReader"
                conf = pcr.Conf(link_confs = [pcr.LinkConfiguration(geoid = pcr.GeoID(system = SYSTEM_TYPE,
                                                                                      region = RU_CONFIG[RUIDX]["region_id"],
                                                                                      element = idx))
                                              for idx in range(MIN_LINK,MAX_LINK)],
                                zmq_receiver_timeout = 10000)
            modules += [DAQModule(name = fake_source,
                               plugin = card_reader,
                               connections = {f'output_{idx}': Connection(f"datahandler_{idx}.raw_input",
                                                                          queue_name = f'{FRONTEND_TYPE}_link_{idx}',
                                                                          queue_kind = "FollySPSCQueue",
                                                                          queue_capacity = 100000)
                                              for idx in range(MIN_LINK, MAX_LINK)},
                               conf = conf)]

    # modules += [
    #     DAQModule(name = "fragment_sender",
    #                    plugin = "FragmentSender",
    #                    conf = None)]
                        
    mgraph = ModuleGraph(modules)

    for idx in range(MIN_LINK, MAX_LINK):
        # P. Rodrigues 2022-02-15 We don't make endpoints for the
        # timesync connection because they are handled by some
        # special-case magic in NetworkManager, which holds a map
        # of topics to connections, and looks up all the
        # connections for a given topic.
        #
        # mgraph.add_endpoint(f"timesync_{idx}", f"datahandler_{idx}.timesync",    Direction.OUT)
        if SOFTWARE_TPG_ENABLED:
            mgraph.add_endpoint(f"tpsets_ru{RUIDX}_link{idx}", f"datahandler_{idx}.tpset_out",    Direction.OUT)
            mgraph.add_endpoint(f"timesync_{idx+RU_CONFIG[RUIDX]['channel_count']}", f"tp_datahandler_{idx}.timesync",    Direction.OUT)

        # Add fragment producers for raw data
        mgraph.add_fragment_producer(region = RU_CONFIG[RUIDX]["region_id"], element = idx, system = SYSTEM_TYPE,
                                     requests_in   = f"datahandler_{idx}.data_requests_0",
                                     fragments_out = f"datahandler_{idx}.fragment_queue")

        # Add fragment producers for TPC TPs. Make sure the element index doesn't overlap with the ones for raw data
        if SOFTWARE_TPG_ENABLED:
            mgraph.add_fragment_producer(region = RU_CONFIG[RUIDX]["region_id"], element = idx + total_link_count, system = SYSTEM_TYPE,
                                         requests_in   = f"tp_datahandler_{idx}.data_requests_0",
                                         fragments_out = f"tp_datahandler_{idx}.fragment_queue")

    readout_app = App(mgraph, host=HOST)
    if DEBUG:
        readout_app.export("readout_app.dot")

    return readout_app
