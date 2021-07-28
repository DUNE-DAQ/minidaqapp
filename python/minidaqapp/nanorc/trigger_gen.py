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

moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')

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

import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos

from appfwk.utils import acmd, mcmd, mrccmd, mspec

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
        NETWORK_ENDPOINTS: list,
        
        TOTAL_NUMBER_OF_DATA_PRODUCERS: int = 2,
        SUBSCRIBE_TO_TPSETS: bool = True,
        
        ACTIVITY_PLUGIN: str = 'TriggerActivityMakerPrescalePlugin',
        ACTIVITY_CONFIG: dict = dict(prescale=1000),
        
        CANDIDATE_PLUGIN: str = 'TriggerCandidateMakerPrescalePlugin',
        CANDIDATE_CONFIG: int = dict(prescale=1000),
        
        TOKEN_COUNT: int = 10,
        SYSTEM_TYPE = 'wib',
        TTCM_S1: int = 1,
        TTCM_S2: int = 2,
        TRIGGER_WINDOW_BEFORE_TICKS: int = 1000,
        TRIGGER_WINDOW_AFTER_TICKS: int = 1000
):
    """
    { item_description }
    """
    cmd_data = {}

    # Derived parameters
    # TRIGGER_INTERVAL_NS = math.floor((1e9/TRIGGER_RATE_HZ))

    # Define modules and queues
    queue_bare_specs = [
        ] + ([
            app.QueueSpec(inst="tpsets_from_netq", kind='FollyMPMCQueue', capacity=1000),
            app.QueueSpec(inst='zipped_tpset_q', kind='FollySPSCQueue', capacity=1000),
            app.QueueSpec(inst='taset_q', kind='FollySPSCQueue', capacity=1000),
        ] if SUBSCRIBE_TO_TPSETS else []) + [
        app.QueueSpec(inst='trigger_candidate_q', kind='FollyMPMCQueue', capacity=1000),
        app.QueueSpec(inst="hsievent_from_netq", kind='FollyMPMCQueue', capacity=1000),
        app.QueueSpec(inst="token_from_netq", kind='FollySPSCQueue', capacity=1000),        
        app.QueueSpec(inst="trigger_decision_to_netq", kind='FollySPSCQueue', capacity=1000),
    ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [
        ] + ([        
            
            ### TPSet input
        
            ] + [
                mspec(f"tpset_subscriber_{idx}", "NetworkToQueue", [ 
                    app.QueueInfo(name="output", inst=f"tpsets_from_netq", dir="output")
                ]) for idx in range(TOTAL_NUMBER_OF_DATA_PRODUCERS)
            ] + [
            
            mspec("zip", "TPZipper", [
                app.QueueInfo(name="input", inst="tpsets_from_netq", dir="input"),
                app.QueueInfo(name="output", inst="zipped_tpset_q", dir="output"), #FIXME need to fanout this zipped_tpset_q if using multiple algorithms
            ]),
            
            ### Algorithm(s)
            
            mspec('tam', 'TriggerActivityMaker', [ # TPSet -> TASet
                app.QueueInfo(name='input', inst='zipped_tpset_q', dir='input'),
                app.QueueInfo(name='output', inst='taset_q', dir='output'),
            ]),
            
            mspec('tcm', 'TriggerCandidateMaker', [ # TASet -> TC
                app.QueueInfo(name='input', inst='taset_q', dir='input'),
                app.QueueInfo(name='output', inst='trigger_candidate_q', dir='output'),
            ])
            
        ] if SUBSCRIBE_TO_TPSETS else []) + [
        
        ### Timing TCs
        
        mspec("ntoq_hsievent", "NetworkToQueue", [
            app.QueueInfo(name="output", inst="hsievent_from_netq", dir="output")
        ]),

        mspec("ttcm", "TimingTriggerCandidateMaker", [
            app.QueueInfo(name="input", inst="hsievent_from_netq", dir="input"),
            app.QueueInfo(name="output", inst="trigger_candidate_q", dir="output"),
        ]),
        
        ### Module level trigger

        mspec("ntoq_token", "NetworkToQueue", [
            app.QueueInfo(name="output", inst="token_from_netq", dir="output")
        ]),

        mspec("qton_trigdec", "QueueToNetwork", [
            app.QueueInfo(name="input", inst="trigger_decision_to_netq", dir="input")
        ]),

        mspec("mlt", "ModuleLevelTrigger", [
            app.QueueInfo(name="token_source", inst="token_from_netq", dir="input"),
            app.QueueInfo(name="trigger_decision_sink", inst="trigger_decision_to_netq", dir="output"),
            app.QueueInfo(name="trigger_candidate_source", inst="trigger_candidate_q", dir="input"),
        ]),


    ]

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)
    
    # Generate schema for the maker plugins on the fly in the temptypes module
    make_moo_record(ACTIVITY_CONFIG,'ActivityConf','temptypes')
    make_moo_record(CANDIDATE_CONFIG,'CandidateConf','temptypes')
    import temptypes
    
    cmd_data['conf'] = acmd([
        
        ### TPSet input
        ] + ([
            (f"tpset_subscriber_{idx}", ntoq.Conf(
                msg_type="dunedaq::trigger::TPSet",
                msg_module_name="TPSetNQ",
                receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                         address=NETWORK_ENDPOINTS[f'tpsets_{idx}'],
                                         subscriptions=["TPSets"])
            ))
            for idx in range(TOTAL_NUMBER_OF_DATA_PRODUCERS)
        ] if SUBSCRIBE_TO_TPSETS else [])  + [
        
        ("zip", tzip.ConfParams(
             cardinality=TOTAL_NUMBER_OF_DATA_PRODUCERS,
             max_latency_ms=1000,
             region_id=0, # Fake placeholder
             element_id=0 # Fake placeholder
        )),
        
        ### Algorithms
        
        ('tam', tam.Conf(
            activity_maker=ACTIVITY_PLUGIN,
            geoid_region=0, # Fake placeholder
            geoid_element=0, # Fake placeholder
            window_time=10000, # should match whatever makes TPSets, in principle
            buffer_time=625000, # 10ms in 62.5 MHz ticks
            activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)
        )),
        
        ('tcm', tcm.Conf(
            candidate_maker=CANDIDATE_PLUGIN,
            candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG)
        )),
        
        ### Timing TCs
        
        ("ntoq_hsievent", ntoq.Conf(
            msg_type="dunedaq::dfmessages::HSIEvent",
            msg_module_name="HSIEventNQ",
            receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                     address=NETWORK_ENDPOINTS["hsievent"])
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
        
        ("ntoq_token", ntoq.Conf(
            msg_type="dunedaq::dfmessages::TriggerDecisionToken",
            msg_module_name="TriggerDecisionTokenNQ",
            receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                     address=NETWORK_ENDPOINTS["triginh"])
        )),
        
        ("qton_trigdec", qton.Conf(
            msg_type="dunedaq::dfmessages::TriggerDecision",
            msg_module_name="TriggerDecisionNQ",
            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                   address=NETWORK_ENDPOINTS["trigdec"])
        )),
        
        ("mlt", mlt.ConfParams(
            links=[mlt.GeoID(system=SYSTEM_TYPE, region=0, element=idx) for idx in range(TOTAL_NUMBER_OF_DATA_PRODUCERS)],
            initial_token_count=TOKEN_COUNT                    
        )),
    ])

    startpars = rccmd.StartParams(run=1)
    cmd_data['start'] = acmd([
        ] + ([
            ("tpset_subscriber", startpars),
            ("zip", startpars),
            ("tam", startpars),
            ("tcm", startpars),
        ] if SUBSCRIBE_TO_TPSETS else []) + [
        ("mlt", startpars),
        ("ttcm", startpars),
        ("ntoq_hsievent", startpars),
        ("ntoq_token", startpars),
        ("qton_trigdec", startpars),
    ])

    cmd_data['stop'] = acmd([
        ] + ([
            ("tpset_subscriber", None),
            ("zip", None),
            ("tam", None),
            ("tcm", None),
        ] if SUBSCRIBE_TO_TPSETS else []) + [
        ("mlt", None),
        ("ttcm", None),
        ("ntoq_hsievent", None),
        ("ntoq_token", None),
        ("qton_trigdec", None),
    ])

    cmd_data['pause'] = acmd([
        ("", None)
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
