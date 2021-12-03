
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
import dunedaq.readoutlibs.sourceemulatorconfig as sec
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readoutlibs.readoutconfig as rconf
import dunedaq.lbrulibs.pacmancardreader as pcr
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.fakedataprod as fdp
import dunedaq.dfmodules.requestreceiver as rrcv
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from appfwk.conf_utils import ModuleGraph, App
from os import path

import json
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

class ReadoutApp(App):
    def __init__(self,
                 NW_SPECS,
                 RU_CONFIG=[],
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
                 HOST="localhost"):
        """Generate the json configuration for the readout and DF process"""
    
        cmd_data = {}
    
        required_eps = {f'{PARTITION}.timesync_{RUIDX}'}
        if not required_eps.issubset([nw.name for nw in NW_SPECS]):
            raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")
    
        RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)
    
        MIN_LINK = RU_CONFIG[RUIDX]["start_channel"]
        MAX_LINK = MIN_LINK + RU_CONFIG[RUIDX]["channel_count"]
        # # Define modules and queues
        # queue_bare_specs = [
        #         app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
        #             for idx in range(MIN_LINK,MAX_LINK)
        #     ] + [
        #         app.QueueSpec(inst="fragment_q", kind="FollyMPMCQueue", capacity=100)
        #     ]
    
        # if not USE_FAKE_DATA_PRODUCERS:
        #     queue_bare_specs += [
        #         app.QueueSpec(inst=f"{FRONTEND_TYPE}_link_{idx}", kind='FollySPSCQueue', capacity=100000)
        #         for idx in range(MIN_LINK,MAX_LINK)
        #     ]
        # if SOFTWARE_TPG_ENABLED:
        #     queue_bare_specs += [
        #         app.QueueSpec(inst=f"sw_tp_link_{idx}", kind='FollySPSCQueue', capacity=100000)
        #             for idx in range(MIN_LINK, MAX_LINK)
        #     ] + [
        #         app.QueueSpec(inst=f"tpset_queue", kind='FollyMPMCQueue', capacity=10000)
        #     ] + [
        #         app.QueueSpec(inst=f"tp_requests_{idx}", kind='FollySPSCQueue', capacity=100)
        #             for idx in range(MIN_LINK,MAX_LINK)
        #     ]
    
        # if FRONTEND_TYPE == 'wib':
        #     queue_bare_specs += [
        #         app.QueueSpec(inst="errored_frames_q", kind="FollyMPMCQueue", capacity=10000)
        #     ]
    
        # # Only needed to reproduce the same order as when using jsonnet
        # queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))
        from appfwk.conf_utils import Module, ModuleGraph, Direction
        from appfwk.conf_utils import Connection as Conn
        modules = {}
        
        if SOFTWARE_TPG_ENABLED:
            modules["request_receiver"] = Module(plugin="RequestReceiver",
                                                 #conf, [app.QueueInfo(name="output", inst=f"data_requests_{idx}", dir="output") for idx in range(MIN_LINK,MAX_LINK)] +
                                                 #[app.QueueInfo(name="output", inst=f"tp_requests_{idx}", dir="output") for idx in range(MIN_LINK,MAX_LINK)])
                                                 )
            for idx in range(MIN_LINK,MAX_LINK):
                module[f"tp_datahandler_{idx}"] = Module(plugin="DataLinkHandler",
                                                         connections = {},
                                                         conf = rconf.Conf(
                                                             readoutmodelconf=rconf.ReadoutModelConf(
                                                                 source_queue_timeout_ms = QUEUE_POP_WAIT_MS,
                                                                 region_id = REGION_ID,
                                                                 element_id = TOTAL_NUMBER_OF_DATA_PRODUCERS+idx
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
                                                         ))

        #     modules[f"tpset_publisher"] = Module(plugin="QueueToNetwork",
        #                                          #conf[app.QueueInfo(name="input", inst=f"tpset_queue", dir="input")
        #                                          )
        # else:
        #     modules[f"request_receiver"] = Module("RequestReceiver",
        #                                           #conf [app.QueueInfo(name="output", inst=f"data_requests_{idx}", dir="output") 
        #                                           )

        if FRONTEND_TYPE == 'wib':
            modules["errored_frame_consumer"] = Module("ErroredFrameConsumer",
                                                       connections={})
        #     mod_specs += [
        # #         mspec("errored_frame_consumer", "ErroredFrameConsumer", [app.QueueInfo(name="input_queue", inst="errored_frames_q", dir="input")])
        # #     ]
    
    
        # There are two flags to be checked so I think a for loop
        # is the closest way to the blocks that are being used here
    
        for idx in range(MIN_LINK,MAX_LINK):
            if USE_FAKE_DATA_PRODUCERS:
                modules[f"fakedataprod_{idx}"] = Module("FakeDataProd")
                # app.QueueInfo(name="data_request_input_queue", inst=f"data_requests_{idx}", dir="input"),
            else:
                #ls = [
                #    app.QueueInfo(name="raw_input", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="input"),
                #    app.QueueInfo(name="data_requests_0", inst=f"data_requests_{idx}", dir="input"),
                #    app.QueueInfo(name="fragment_queue", inst="fragment_q", dir="output")
                #]
                if SOFTWARE_TPG_ENABLED:
                    #ls.extend([
                    #    app.QueueInfo(name="tp_out", inst=f"sw_tp_link_{idx}", dir="output"),
                    #    app.QueueInfo(name="tpset_out", inst=f"tpset_queue", dir="output")
                    #])
    
                if FRONTEND_TYPE == 'wib':
                    #ls.extend([
                    #    app.QueueInfo(name="errored_frames", inst="errored_frames_q", dir="output")
                    # ])

                modules[f"datahandler_{idx}"] = Module("DataLinkHandler", ls)
    
        if not USE_FAKE_DATA_PRODUCERS:
            if FLX_INPUT:
                modules["flxcard_0"] = Module("FelixCardReader")
                # app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                # for idx in range(MIN_LINK, MIN_LINK + min(5, RU_CONFIG[RUIDX]["channel_count"]))
                
                if RU_CONFIG[RUIDX]["channel_count"] > 5 :
                    modules["flxcard_1"] = Module("FelixCardReader")
                    #app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                    #for idx in range(MIN_LINK + 5, MAX_LINK)
                    
            elif SSP_INPUT:
                modules["ssp_0"] = Module("SSPCardReader")
                # app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                # for idx in range(MIN_LINK,MAX_LINK)
    
            else:
                fake_source = "fake_source"
                card_reader = "FakeCardReader"
                if FRONTEND_TYPE=='pacman':
                    fake_source = "pacman_source"
                    card_reader = "PacmanCardReader"
    
                modules[fake_source] = Module(card_reader)
                #app.QueueInfo(name=f"output_{idx}", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="output")
                #for idx in range(MIN_LINK,MAX_LINK)
        mgraph = ModuleGraph(modules)

        for idx in range(NUMBER_OF_DATA_PRODUCERS):
            mgraph.add_endpoint(f"tpsets_{idx}", f"datahandler_{idx}.tpset_out",    Direction.OUT)
            # TODO: Should we just have one timesync outgoing endpoint?
            mgraph.add_endpoint(f"timesync_{idx}", f"datahandler_{idx}.timesync",    Direction.OUT)
            mgraph.add_endpoint(f"timesync_{idx+NUMBER_OF_DATA_PRODUCERS}", f"tp_datahandler_{idx}.timesync",    Direction.OUT)

            # Add fragment producers for raw data
            mgraph.add_fragment_producer(region = REGION_ID, element = idx, system = SYSTEM_TYPE,
                                         requests_in   = f"datahandler_{idx}.data_requests_0",
                                         fragments_out = f"datahandler_{idx}.data_response_0")

            # Add fragment producers for TPC TPs. Make sure the element index doesn't overlap with the ones for raw data
            mgraph.add_fragment_producer(region = REGION_ID, element = idx + NUMBER_OF_DATA_PRODUCERS, system = SYSTEM_TYPE,
                                         requests_in   = f"tp_datahandler_{idx}.data_requests_0",
                                         fragments_out = f"tp_datahandler_{idx}.data_response_0")

        super().__init__(mgraph, host=HOST)
        # cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)
    
        # total_link_count = 0
        # for ru in range(len(RU_CONFIG)):
        #     total_link_count += RU_CONFIG[ru]["channel_count"]
    
        # conf_list = [("fake_source",sec.Conf(
        #                         link_confs=[sec.LinkConfiguration(
        #                         geoid=sec.GeoID(system=SYSTEM_TYPE, region=RU_CONFIG[RUIDX]["region_id"], element=idx),
        #                             slowdown=DATA_RATE_SLOWDOWN_FACTOR,
        #                             queue_name=f"output_{idx}",
        #                             data_filename = DATA_FILE,
        #                             emu_frame_error_rate=0,
        #                             ) for idx in range(MIN_LINK,MAX_LINK)],
        #                         # input_limit=10485100, # default
        #                         queue_timeout_ms = QUEUE_POP_WAIT_MS)),
        #             ("pacman_source",pcr.Conf(
        #                                       link_confs=[pcr.LinkConfiguration(
        #                                        geoid=pcr.GeoID(system=SYSTEM_TYPE, region=RU_CONFIG[RUIDX]["region_id"], element=idx),
        #                                        ) for idx in range(MIN_LINK,MAX_LINK)],
        #                                        zmq_receiver_timeout = 10000)),
        #             ("flxcard_0",flxcr.Conf(card_id=RU_CONFIG[RUIDX]["card_id"],
        #                         logical_unit=0,
        #                         dma_id=0,
        #                         chunk_trailer_size= 32,
        #                         dma_block_size_kb= 4,
        #                         dma_memory_size_gb= 4,
        #                         numa_id=0,
        #                         num_links=min(5,RU_CONFIG[RUIDX]["channel_count"]))),
        #             ("flxcard_1",flxcr.Conf(card_id=RU_CONFIG[RUIDX]["card_id"],
        #                         logical_unit=1,
        #                         dma_id=0,
        #                         chunk_trailer_size= 32,
        #                         dma_block_size_kb= 4,
        #                         dma_memory_size_gb= 4,
        #                         numa_id=0,
        #                         num_links=max(0, RU_CONFIG[RUIDX]["channel_count"] - 5))),
        #             ("ssp_0",flxcr.Conf(card_id=RU_CONFIG[RUIDX]["card_id"],
        #                         logical_unit=0,
        #                         dma_id=0,
        #                         chunk_trailer_size= 32,
        #                         dma_block_size_kb= 4,
        #                         dma_memory_size_gb= 4,
        #                         numa_id=0,
        #                         num_links=RU_CONFIG[RUIDX]["channel_count"])),
        #         ] + [
        #              ("request_receiver", rrcv.ConfParams(
        #                         map = [rrcv.geoidinst(region=RU_CONFIG[RUIDX]["region_id"] , element=idx , system=SYSTEM_TYPE , queueinstance=f"data_requests_{idx}") for idx in range(MIN_LINK,MAX_LINK)] +
        #                             [rrcv.geoidinst(region=RU_CONFIG[RUIDX]["region_id"] , element=idx + total_link_count, system=SYSTEM_TYPE , queueinstance=f"tp_requests_{idx}") for idx in range(MIN_LINK,MAX_LINK) if SOFTWARE_TPG_ENABLED],
        #                         general_queue_timeout = QUEUE_POP_WAIT_MS,
        #                         connection_name = f"{PARTITION}.datareq_{RUIDX}"
        #              )) 
        #         ] + [
        #             (f"datahandler_{idx}", rconf.Conf(
        #                     readoutmodelconf= rconf.ReadoutModelConf(
        #                         source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
        #                         # fake_trigger_flag=0, # default
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = idx,
        #                         timesync_connection_name = f"{PARTITION}.timesync_{RUIDX}",
        #                         timesync_topic_name = "Timesync",
        #                     ),
        #                     latencybufferconf= rconf.LatencyBufferConf(
        #                         latency_buffer_alignment_size = 4096,
        #                         latency_buffer_size = LATENCY_BUFFER_SIZE,
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = idx,
        #                     ),
        #                     rawdataprocessorconf= rconf.RawDataProcessorConf(
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = idx,
        #                         enable_software_tpg = SOFTWARE_TPG_ENABLED,
        #                         emulator_mode = EMULATOR_MODE,
        #                         error_counter_threshold=100,
        #                         error_reset_freq=10000
        #                     ),
        #                     requesthandlerconf= rconf.RequestHandlerConf(
        #                         latency_buffer_size = LATENCY_BUFFER_SIZE,
        #                         pop_limit_pct = 0.8,
        #                         pop_size_pct = 0.1,
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = idx,
        #                         output_file = path.join(RAW_RECORDING_OUTPUT_DIR, f"output_{RUIDX}_{idx}.out"),
        #                         stream_buffer_size = 8388608,
        #                         enable_raw_recording = RAW_RECORDING_ENABLED,
        #                     )
        #                     )) for idx in range(MIN_LINK,MAX_LINK)
        #         ] + [
        #             (f"tp_datahandler_{idx}", rconf.Conf(
        #                     readoutmodelconf= rconf.ReadoutModelConf(
        #                         source_queue_timeout_ms= QUEUE_POP_WAIT_MS,
        #                         # fake_trigger_flag=0, default
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = total_link_count+idx,
        #                     ),
        #                     latencybufferconf= rconf.LatencyBufferConf(
        #                         latency_buffer_size = LATENCY_BUFFER_SIZE,
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id =  total_link_count+idx,
        #                     ),
        #                     rawdataprocessorconf= rconf.RawDataProcessorConf(
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id =  total_link_count+idx,
        #                         enable_software_tpg = False,
        #                     ),
        #                     requesthandlerconf= rconf.RequestHandlerConf(
        #                         latency_buffer_size = LATENCY_BUFFER_SIZE,
        #                         pop_limit_pct = 0.8,
        #                         pop_size_pct = 0.1,
        #                         region_id = RU_CONFIG[RUIDX]["region_id"],
        #                         element_id = total_link_count+idx,
        #                         # output_file = f"output_{idx + MIN_LINK}.out",
        #                         stream_buffer_size = 100 if FRONTEND_TYPE=='pacman' else 8388608,
        #                         enable_raw_recording = False,
        #                     )
        #                     )) for idx in range(MIN_LINK,MAX_LINK)
        #         ]
    
        # if SOFTWARE_TPG_ENABLED:
    
        #     conf_list.extend([
        #                         (f"tpset_publisher", qton.Conf(msg_type="dunedaq::trigger::TPSet",
        #                                                              msg_module_name="TPSetNQ",
        #                                                              sender_config=nos.Conf(name=f"{PARTITION}.tpsets_{RUIDX}",
        #                                                                                     topic="TPSets",
        #                                                                                     stype="msgpack")))
        #                     ])
    
        # if USE_FAKE_DATA_PRODUCERS:
        #     conf_list.extend([
        #         (f"fakedataprod_{idx}", fdp.ConfParams(
        #             system_type = SYSTEM_TYPE,
        #             apa_number = RU_CONFIG[RUIDX]["region_id"],
        #             link_number = idx,
        #             time_tick_diff = 25,
        #             frame_size = 464,
        #             response_delay = 0,
        #             timesync_connection_name = f"{PARTITION}.timesync_{RUIDX}",
        #             timesync_topic_name = "Timesync",
        #             fragment_type = "FakeData")) for idx in range(MIN_LINK,MAX_LINK)
        #     ])
    
        # conf_list.extend([
        #     ("fragment_sender", None)
        # ])
    
        # cmd_data['conf'] = acmd(conf_list)
    
    
        # startpars = rccmd.StartParams(run=RUN_NUMBER)
        # cmd_data['start'] = acmd([
        #         ("datahandler_.*", startpars),
        #         ("fake_source", startpars),
        #         ("pacman_source", startpars),
        #         ("flxcard.*", startpars),
        #         ("request_receiver", startpars),
        #         ("ssp.*", startpars),
        #         ("ntoq_trigdec", startpars),
        #         (f"tp_datahandler_.*", startpars),
        #         (f"tpset_publisher", startpars),
        #         ("fakedataprod_.*", startpars),
        #         ("fragment_sender", startpars),
        #         ("errored_frame_consumer", startpars)])
    
        # cmd_data['stop'] = acmd([
        #         ("request_receiver", None),
        #         ("flxcard.*", None),
        #         ("ssp.*", None),
        #         ("fake_source", None),
        #         ("pacman_source", None),
        #         ("datahandler_.*", None),
        #         (f"tp_datahandler_.*", None),
        #         (f"tpset_publisher", None),
        #         ("fakedataprod_.*", None),
        #         ("fragment_sender", None),
        #         ("errored_frame_consumer", None)])
    
        # cmd_data['pause'] = acmd([("", None)])
    
        # cmd_data['resume'] = acmd([("", None)])
    
        # cmd_data['scrap'] = acmd([("", None)])
    
        # cmd_data['record'] = acmd([("", None)])
    
        # return cmd_data
