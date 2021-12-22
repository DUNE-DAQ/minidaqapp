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

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(NW_SPECS,
        RU_CONFIG=[],
        EMULATOR_MODE=False,
        RUN_NUMBER=333,
        DATA_FILE="./frames.bin",
        CLOCK_SPEED_HZ=50000000,
        RUIDX=0,
        SYSTEM_TYPE='TPC',
        DQM_ENABLED=False,
        DQM_KAFKA_ADDRESS='',
        DQM_CMAP='HD',
        DQM_RAWDISPLAY_PARAMS=[60, 10, 50],
        DQM_MEANRMS_PARAMS=[10, 1, 100],
        DQM_FOURIER_PARAMS=[600, 60, 100],
        DQM_FOURIERSUM_PARAMS=[10, 1, 8192],
        PARTITION="UNKNOWN"):
    """Generate the json configuration for the dqm process"""

    cmd_data = {}

    required_eps = {f'{PARTITION}.timesync_{RUIDX}'}
    if not required_eps.issubset([nw.name for nw in NW_SPECS]):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")

    MIN_LINK = RU_CONFIG[RUIDX]["start_channel"]
    MAX_LINK = MIN_LINK + RU_CONFIG[RUIDX]["channel_count"]
    # Define modules and queues
    queue_bare_specs =  [
        app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
        app.QueueSpec(inst="trigger_decision_q_dqm", kind='FollySPSCQueue', capacity=20),
        app.QueueSpec(inst="trigger_record_q_dqm", kind='FollySPSCQueue', capacity=20)
    ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [mspec("trb_dqm", "TriggerRecordBuilder", [
                    app.QueueInfo(name="trigger_decision_input_queue", inst="trigger_decision_q_dqm", dir="input"),
                    app.QueueInfo(name="trigger_record_output_queue", inst="trigger_record_q_dqm", dir="output"),
                    app.QueueInfo(name="data_fragment_input_queue", inst="data_fragments_q", dir="input")
                ]),
    ]
    mod_specs += [mspec("dqmprocessor", "DQMProcessor", [
                    app.QueueInfo(name="trigger_record_dqm_processor", inst="trigger_record_q_dqm", dir="input"),
                    app.QueueInfo(name="trigger_decision_dqm_processor", inst="trigger_decision_q_dqm", dir="output"),
                ]),
    ]

    mod_specs += [
        mspec(f"fragment_receiver_dqm", "FragmentReceiver",
              [app.QueueInfo(name="output", inst="data_fragments_q", dir="output")
               ])]

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)

    conf_list = [("fragment_receiver_dqm", frcv.ConfParams(
                    general_queue_timeout=QUEUE_POP_WAIT_MS,
                    connection_name=f"{PARTITION}.fragx_dqm_{RUIDX}"))
            ] + [
                ("trb_dqm", trb.ConfParams(
                        general_queue_timeout=QUEUE_POP_WAIT_MS,
                        reply_connection_name = f"{PARTITION}.fragx_dqm_{RUIDX}",
                        map=trb.mapgeoidconnections([
                                trb.geoidinst(region=RU_CONFIG[RUIDX]["region_id"], element=idx, system=SYSTEM_TYPE, connection_name=f"{PARTITION}.datareq_{RUIDX}") for idx in range(MIN_LINK, MAX_LINK)
                            ]),
                        ))
            ] + [
                ('dqmprocessor', dqmprocessor.Conf(
                        region=RU_CONFIG[RUIDX]["region_id"],
                        channel_map=DQM_CMAP, # 'HD' for horizontal drift or 'VD' for vertical drift
                        sdqm_hist=dqmprocessor.StandardDQM(**{'how_often' : DQM_RAWDISPLAY_PARAMS[0], 'unavailable_time' : DQM_RAWDISPLAY_PARAMS[1], 'num_frames' : DQM_RAWDISPLAY_PARAMS[2]}),
                        sdqm_mean_rms=dqmprocessor.StandardDQM(**{'how_often' : DQM_MEANRMS_PARAMS[0], 'unavailable_time' : DQM_MEANRMS_PARAMS[1], 'num_frames' : DQM_MEANRMS_PARAMS[2]}),
                        sdqm_fourier=dqmprocessor.StandardDQM(**{'how_often' : DQM_FOURIER_PARAMS[0], 'unavailable_time' : DQM_FOURIER_PARAMS[1], 'num_frames' : DQM_FOURIER_PARAMS[2]}),
                        sdqm_fourier_sum=dqmprocessor.StandardDQM(**{'how_often' : DQM_FOURIERSUM_PARAMS[0], 'unavailable_time' : DQM_FOURIERSUM_PARAMS[1], 'num_frames' : DQM_FOURIERSUM_PARAMS[2]}),
                        kafka_address=DQM_KAFKA_ADDRESS,
                        link_idx=list(range(MIN_LINK, MAX_LINK)),
                        clock_frequency=CLOCK_SPEED_HZ,
                        timesync_connection_name = f"{PARTITION}.timesync_{RUIDX}",
                        ))
            ]

    cmd_data['conf'] = acmd(conf_list)

    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([
            ("fragment_receiver_dqm", startpars),
            ("dqmprocessor", startpars),
            ("trb_dqm", startpars),
            ])

    cmd_data['stop'] = acmd([
            ("trb_dqm", None), 
            ("dqmprocessor", None),
            ("fragment_receiver_dqm", None),
            ])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
