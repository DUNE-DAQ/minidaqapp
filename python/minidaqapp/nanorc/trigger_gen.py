# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/intervaltccreator.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')
moo.otypes.load_types('trigger/fakedataflow.jsonnet')
moo.otypes.load_types('trigger/timingtriggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/tpsetbuffercreator.jsonnet')
moo.otypes.load_types('trigger/tpsetreceiver.jsonnet')

moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('dfmodules/requestreceiver.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.trigger.intervaltccreator as itcc
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggercandidatemaker as tcm
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.moduleleveltrigger as mlt
import dunedaq.trigger.fakedataflow as fdf
import dunedaq.trigger.timingtriggercandidatemaker as ttcm
import dunedaq.trigger.tpsetbuffercreator as buf
import dunedaq.trigger.tpsetreceiver as tpsrcv

import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.dfmodules.requestreceiver as rrcv
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from appfwk.conf_utils import ModuleGraph, Module

import json
import math
from pprint import pprint


#FIXME maybe one day, triggeralgs will define schemas... for now allow a dictionary of 4byte int, 4byte floats, and strings
moo.otypes.make_type(schema='number', dtype='i4', name='temp_integer', path='temptypes')
moo.otypes.make_type(schema='number', dtype='f4', name='temp_float', path='temptypes')
moo.otypes.make_type(schema='string', name='temp_string', path='temptypes')
def make_moo_record(conf_dict,name,path='temptypes'):
    fields = []
    for pname,pvalue in conf_dict.items():
        typename = None
        if type(pvalue) == int:
            typename = 'temptypes.temp_integer'
        elif type(pvalue) == float:
            typename = 'temptypes.temp_float'
        elif type(pvalue) == str:
            typename = 'temptypes.temp_string'
        else:
            raise Exception(f'Invalid config argument type: {type(value)}')
        fields.append(dict(name=pname,item=typename))
    moo.otypes.make_type(schema='record', fields=fields, name=name, path=path)

#===============================================================================
def generate(
        NW_SPECS: list,
        
        SOFTWARE_TPG_ENABLED: bool = False,
        RU_CONFIG: list = [],

        ACTIVITY_PLUGIN: str = 'TriggerActivityMakerPrescalePlugin',
        ACTIVITY_CONFIG: dict = dict(prescale=10000),

        CANDIDATE_PLUGIN: str = 'TriggerCandidateMakerPrescalePlugin',
        CANDIDATE_CONFIG: int = dict(prescale=10),

        TOKEN_COUNT: int = 10,
        SYSTEM_TYPE = 'wib',
        TTCM_S1: int = 1,
        TTCM_S2: int = 2,
        TRIGGER_WINDOW_BEFORE_TICKS: int = 1000,
        TRIGGER_WINDOW_AFTER_TICKS: int = 1000,
        PARTITION="UNKNOWN"
):
    """
    { item_description }
    """
    cmd_data = {}

    # Define modules and queues
    queue_bare_specs = [
        app.QueueSpec(inst='trigger_candidate_q', kind='FollyMPMCQueue', capacity=1000),
        app.QueueSpec(inst="hsievent_from_netq", kind='FollyMPMCQueue', capacity=1000),
    ]

    if SOFTWARE_TPG_ENABLED:
        queue_bare_specs.extend([
                app.QueueSpec(inst=f"fragment_q", kind='FollyMPMCQueue', capacity=1000),
                app.QueueSpec(inst=f'taset_q', kind='FollyMPMCQueue', capacity=1000),
        ])
        for ru in range(len(RU_CONFIG)):
            queue_bare_specs.extend([
                app.QueueSpec(inst=f"tpsets_from_netq_{ru}", kind='FollySPSCQueue', capacity=1000),
                app.QueueSpec(inst=f'zipped_tpset_q_{ru}', kind='FollySPSCQueue', capacity=1000),
            ])
            for idx in range(RU_CONFIG[ru]["channel_count"]):
                queue_bare_specs.extend([
                    app.QueueSpec(inst=f"tpset_q_for_buf{ru}_{idx}", kind='FollySPSCQueue', capacity=1000),
                    app.QueueSpec(inst=f"data_request_q{ru}_{idx}", kind='FollySPSCQueue', capacity=1000),
                ])

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = []

    if SOFTWARE_TPG_ENABLED:
        mod_specs.extend([
            mspec(f"request_receiver", "RequestReceiver", [app.QueueInfo(name="output", inst=f"data_request_q{ru}_{idy}", dir="output") for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])])
        ] + [
            mspec(f"tpset_receiver", "TPSetReceiver", [app.QueueInfo(name="output", inst=f"tpset_q_for_buf{ru}_{idy}", dir="output") for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])])
        ] + [
            mspec(f"qton_fragments", "QueueToNetwork", [app.QueueInfo(name="input", inst=f"fragment_q", dir="input")]),
                mspec(f'tcm', 'TriggerCandidateMaker', [ # TASet -> TC
                    app.QueueInfo(name='input', inst=f'taset_q', dir='input'),
                    app.QueueInfo(name='output', inst=f'trigger_candidate_q', dir='output'),
                ])
        ])
        for ru in range(len(RU_CONFIG)):
            mod_specs.extend([
                mspec(f"tpset_subscriber_{ru}", "NetworkToQueue", [
                    app.QueueInfo(name="output", inst=f"tpsets_from_netq_{ru}", dir="output")
                ]),
                mspec(f"zip_{ru}", "TPZipper", [
                    app.QueueInfo(name="input", inst=f"tpsets_from_netq_{ru}", dir="input"),
                    app.QueueInfo(name="output", inst=f"zipped_tpset_q_{ru}", dir="output"), #FIXME need to fanout this zipped_tpset_q if using multiple algorithms
                ]),

                ### Algorithm(s)

                mspec(f'tam_{ru}', 'TriggerActivityMaker', [ # TPSet -> TASet
                    app.QueueInfo(name='input', inst=f'zipped_tpset_q_{ru}', dir='input'),
                    app.QueueInfo(name='output', inst=f'taset_q', dir='output'),
                ]),

            ])
            for idy in range(RU_CONFIG[ru]["channel_count"]):
                mod_specs.extend([
                mspec(f"buf{ru}_{idy}", "TPSetBufferCreator", [
                    app.QueueInfo(name="tpset_source", inst=f"tpset_q_for_buf{ru}_{idy}", dir="input"),
                    app.QueueInfo(name="data_request_source", inst=f"data_request_q{ru}_{idy}", dir="input"),
                    app.QueueInfo(name="fragment_sink", inst=f"fragment_q", dir="output"),
                ])
            ])

    mod_specs += ([

        ### Timing TCs
        mspec("ntoq_hsievent", "NetworkToQueue", [
            app.QueueInfo(name="output", inst="hsievent_from_netq", dir="output")
        ]),

        mspec("ttcm", "TimingTriggerCandidateMaker", [
            app.QueueInfo(name="input", inst="hsievent_from_netq", dir="input"),
            app.QueueInfo(name="output", inst="trigger_candidate_q", dir="output"),
        ]),

        ### Module level trigger

        mspec("mlt", "ModuleLevelTrigger", [
            app.QueueInfo(name="trigger_candidate_source", inst="trigger_candidate_q", dir="input"),
        ]),

    ])

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)

    # Generate schema for the maker plugins on the fly in the temptypes module
    make_moo_record(ACTIVITY_CONFIG,'ActivityConf','temptypes')
    make_moo_record(CANDIDATE_CONFIG,'CandidateConf','temptypes')
    import temptypes

    tp_confs = []

    if SOFTWARE_TPG_ENABLED:
        tp_confs.extend([
            ("request_receiver", rrcv.ConfParams(
                                                 map = [rrcv.geoidinst(region=RU_CONFIG[ru]["region_id"], element=idy + RU_CONFIG[ru]["start_channel"], system="DataSelection" , queueinstance=f"data_request_q{ru}_{idy}") for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])],
                                                 general_queue_timeout = 100,
                                                 connection_name = f"{PARTITION}.ds_tp_datareq_0")),
            ("tpset_receiver", tpsrcv.ConfParams(
                                                 map = [tpsrcv.geoidinst(region=RU_CONFIG[ru]["region_id"] , element=idy + RU_CONFIG[ru]["start_channel"], system=SYSTEM_TYPE , queueinstance=f"tpset_q_for_buf{ru}_{idy}") for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])],
                                                 general_queue_timeout = 100,
                                                 topic = f"TPSets")),
            (f"qton_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                          msg_module_name="FragmentNQ",
                                          sender_config=nos.Conf(name=f"{PARTITION}.frags_tpset_ds_0",
                                                                 stype="msgpack"))),
                (f'tcm', tcm.Conf(
                    candidate_maker=CANDIDATE_PLUGIN,
                    candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG)
                )),
        ])
        for idx in range(len(RU_CONFIG)):
            tp_confs.extend([
                (f"tpset_subscriber_{idx}", ntoq.Conf(
                    msg_type="dunedaq::trigger::TPSet",
                    msg_module_name="TPSetNQ",
                    receiver_config=nor.Conf(name=f'{PARTITION}.tpsets_{idx}',
                                             subscriptions=["TPSets"])
                )),
                (f"zip_{idx}", tzip.ConfParams(
                    cardinality=RU_CONFIG[idx]["channel_count"],
                    max_latency_ms=1000,
                    region_id=0, # Fake placeholder
                    element_id=0 # Fake placeholder
                )),

                ### Algorithms

                (f'tam_{idx}', tam.Conf(
                    activity_maker=ACTIVITY_PLUGIN,
                    geoid_region=0, # Fake placeholder
                    geoid_element=0, # Fake placeholder
                    window_time=10000, # should match whatever makes TPSets, in principle
                    buffer_time=625000, # 10ms in 62.5 MHz ticks
                    activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)
                )),

            ])
            for idy in range(RU_CONFIG[idx]["channel_count"]):
                tp_confs.extend([
                    (f"buf{idx}_{idy}", buf.Conf(tpset_buffer_size=10000, region=RU_CONFIG[idx]["region_id"], element=idy + RU_CONFIG[idx]["start_channel"])) 
                ])


    total_link_count = 0
    for ru in range(len(RU_CONFIG)):
        total_link_count += RU_CONFIG[ru]["channel_count"]

    cmd_data['conf'] = acmd(tp_confs + [


        ### Timing TCs

        ("ntoq_hsievent", ntoq.Conf(
            msg_type="dunedaq::dfmessages::HSIEvent",
            msg_module_name="HSIEventNQ",
            receiver_config=nor.Conf(name=PARTITION+".hsievent")
        )),

        ("ttcm", ttcm.Conf(
            s1=ttcm.map_t(signal_type=TTCM_S1,
                          time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                          time_after=TRIGGER_WINDOW_AFTER_TICKS),
            s2=ttcm.map_t(signal_type=TTCM_S2,
                          time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                          time_after=TRIGGER_WINDOW_AFTER_TICKS)
            )
        ),

        # Module level trigger
        ("mlt", mlt.ConfParams(
            # This line requests the raw data from upstream DAQ _and_ the raw TPs from upstream DAQ
            td_connection_name=PARTITION+".trigdec",
            token_connection_name=PARTITION+".triginh",
            links=[
                mlt.GeoID(system=SYSTEM_TYPE, region=RU_CONFIG[ru]["region_id"], element=RU_CONFIG[ru]["start_channel"] + idx)
                    for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
            ] + ([
                mlt.GeoID(system="DataSelection", region=RU_CONFIG[ru]["region_id"], element=RU_CONFIG[ru]["start_channel"] + idx) 
                    for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
            ] if SOFTWARE_TPG_ENABLED else []) + ([
                mlt.GeoID(system=SYSTEM_TYPE, region=RU_CONFIG[ru]["region_id"], element=RU_CONFIG[ru]["start_channel"] + idx + total_link_count)
                    for ru in range(len(RU_CONFIG)) for idx in range(RU_CONFIG[ru]["channel_count"])
            ] if SOFTWARE_TPG_ENABLED else []),
            initial_token_count=TOKEN_COUNT
        )),
    ])

    # We start modules in "downstream-to-upstream" order, so that each
    # module is ready before its input starts sending data. The stop
    # order is the reverse (upstream-to-downstream), so each module
    # can process all of its input then stop, ensuring all data gets
    # processed
    start_order = [
        "buf.*",
        "mlt",
        "ttcm",
        "ntoq_hsievent",
        "ntoq_token"
    ]

    if SOFTWARE_TPG_ENABLED:
        start_order += [
            "qton_fragments",
            "tcm",
            "tam_.*",
            "zip_.*",
            "tpset_subscriber_.*",
            "tpset_receiver",
            "request_receiver"
        ]

    stop_order = start_order[::-1]

    startpars = rccmd.StartParams(run=1)
    cmd_data['start'] = acmd([ (m, startpars) for m in start_order ])
    cmd_data['stop'] = acmd([ (m, None) for m in stop_order ])

    cmd_data['pause'] = acmd([
        ("mlt", None)
    ])

    resumepars = rccmd.ResumeParams(trigger_interval_ticks=50000000)
    cmd_data['resume'] = acmd([
        ("mlt", resumepars)
    ])

    cmd_data['scrap'] = acmd([
        ("", None)
    ])

    cmd_data['record'] = acmd([
        ("", None)
    ])

    return cmd_data
