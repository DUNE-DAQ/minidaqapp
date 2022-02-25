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
moo.otypes.load_types('dfmodules/fragmentreceiver.jsonnet')
moo.otypes.load_types('dqm/dqmprocessor.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.dfmodules.triggerrecordbuilder as trb
import dunedaq.dfmodules.fragmentreceiver as frcv
import dunedaq.dqm.dqmprocessor as dqmprocessor

from appfwk.utils import acmd, mcmd, mrccmd, mspec

from appfwk.conf_utils import Direction, Connection
from appfwk.daqmodule import DAQModule
from appfwk.app import App,ModuleGraph

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def get_dqm_app(RU_CONFIG=[],
                 RU_NAME='',
                 EMULATOR_MODE=False,
                 DATA_RATE_SLOWDOWN_FACTOR=1,
                 RUN_NUMBER=333,
                 DATA_FILE="./frames.bin",
                 CLOCK_SPEED_HZ=50000000,
                 RUIDX=0,
                 SYSTEM_TYPE='TPC',
                 DQM_KAFKA_ADDRESS='',
                 DQM_CMAP='HD',
                 DQM_RAWDISPLAY_PARAMS=[60, 10, 50],
                 DQM_MEANRMS_PARAMS=[10, 1, 100],
                 DQM_FOURIER_PARAMS=[600, 60, 100],
                 DQM_FOURIERSUM_PARAMS=[10, 1, 8192],
                 PARTITION="UNKNOWN",
                 HOST="localhost",
                 NUM_DF_APPS=1,
                 MODE="readout",
                 DEBUG=False):

        cmd_data = {}

        MIN_LINK = RU_CONFIG[RUIDX]["start_channel"]
        MAX_LINK = MIN_LINK + RU_CONFIG[RUIDX]["channel_count"]

        modules = []

        connections = {}

        if MODE == 'readout':

            connections['output'] = Connection(f'trb_dqm.data_fragment_input_queue',
                                            queue_name='data_fragments_q',
                                            queue_kind='FollyMPMCQueue',
                                            queue_capacity=1000)

            modules += [DAQModule(name='fragment_receiver_dqm',
                                plugin='FragmentReceiver',
                                connections=connections,
                                conf=frcv.ConfParams(general_queue_timeout=QUEUE_POP_WAIT_MS,
                                                    connection_name=f"{PARTITION}.fragx_dqm_{RUIDX}")
                                )
                                ]

            connections = {}

            connections['trigger_record_output_queue'] = Connection('dqmprocessor.trigger_record_dqm_processor',
                                            queue_name='trigger_record_q_dqm',
                                            queue_kind="FollySPSCQueue",
                                            queue_capacity=100,
                                            toposort=False)

            modules += [DAQModule(name='trb_dqm',
                                plugin='TriggerRecordBuilder',
                                connections=connections,
                                conf=trb.ConfParams(# This needs to be done in connect_fragment_producers
                                    general_queue_timeout=QUEUE_POP_WAIT_MS,
                                    reply_connection_name=f"{PARTITION}.fragx_dqm_{RUIDX}",
                                    mon_connection_name=f"",
                                    map=trb.mapgeoidconnections([
                                        trb.geoidinst(region=RU_CONFIG[RUIDX]["region_id"],
                                                        element=idx,
                                                        system=SYSTEM_TYPE,
                                                        connection_name=f"{PARTITION}.data_requests_for_{RU_NAME}") for idx in range(MIN_LINK, MAX_LINK)
                                    ]),
                                ))
                        ]

            connections = {}

            connections['trigger_decision_input_queue'] = Connection(f'trb_dqm.trigger_decision_input_queue',
                                                    queue_name='trigger_decision_q_dqm',
                                                    queue_kind="FollySPSCQueue",
                                                    queue_capacity=100)

        modules += [DAQModule(name='dqmprocessor',
                              plugin='DQMProcessor',
                              connections=connections,
                              conf=dqmprocessor.Conf(
                                  region=RU_CONFIG[RUIDX]["region_id"],
                                  channel_map=DQM_CMAP, # 'HD' for horizontal drift or 'VD' for vertical drift
                                  mode=MODE,
                                  sdqm_hist=dqmprocessor.StandardDQM(**{'how_often' : DQM_RAWDISPLAY_PARAMS[0], 'unavailable_time' : DQM_RAWDISPLAY_PARAMS[1], 'num_frames' : DQM_RAWDISPLAY_PARAMS[2]}),
                                  sdqm_mean_rms=dqmprocessor.StandardDQM(**{'how_often' : DQM_MEANRMS_PARAMS[0], 'unavailable_time' : DQM_MEANRMS_PARAMS[1], 'num_frames' : DQM_MEANRMS_PARAMS[2]}),
                                  sdqm_fourier=dqmprocessor.StandardDQM(**{'how_often' : DQM_FOURIER_PARAMS[0], 'unavailable_time' : DQM_FOURIER_PARAMS[1], 'num_frames' : DQM_FOURIER_PARAMS[2]}),
                                  sdqm_fourier_sum=dqmprocessor.StandardDQM(**{'how_often' : DQM_FOURIERSUM_PARAMS[0], 'unavailable_time' : DQM_FOURIERSUM_PARAMS[1], 'num_frames' : DQM_FOURIERSUM_PARAMS[2]}),
                                  kafka_address=DQM_KAFKA_ADDRESS,
                                  link_idx=list(range(MIN_LINK, MAX_LINK)),
                                  clock_frequency=CLOCK_SPEED_HZ,
                                  timesync_connection_name = f"{PARTITION}.timesync_{RUIDX}",
                                  df2dqm_connection_name=f"{PARTITION}.tr_df2dqm_{RUIDX}" if RUIDX < NUM_DF_APPS else '',
                                  dqm2df_connection_name=f"{PARTITION}.trmon_dqm2df_{RUIDX}" if RUIDX < NUM_DF_APPS else '',
                                  readout_window_offset=10**7 / DATA_RATE_SLOWDOWN_FACTOR, # 10^7 works fine for WIBs with no slowdown
                                   )
                              )
                              ]

        mgraph = ModuleGraph(modules)

    dqm_app = App(mgraph, host=HOST)

        if DEBUG:
        dqm_app.export("dqm_app.dot")

    return dqm_app
