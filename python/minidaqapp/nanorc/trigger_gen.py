# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

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


#===============================================================================
def generate(
        NETWORK_ENDPOINTS: list,
        NUMBER_OF_DATA_PRODUCERS: int = 2,
        TOKEN_COUNT: int = 10,
        SYSTEM_TYPE = 'wib',
        TTCM_S1: int = 1,
        TTCM_S2: int = 2,
):
    """
    { item_description }
    """
    cmd_data = {}

    # Derived parameters
    # TRIGGER_INTERVAL_NS = math.floor((1e9/TRIGGER_RATE_HZ))

    # Define modules and queues
    queue_bare_specs = [
        app.QueueSpec(inst="hsievent_from_netq", kind='FollyMPMCQueue', capacity=1000),
        app.QueueSpec(inst="token_from_netq", kind='FollySPSCQueue', capacity=2000),        
        app.QueueSpec(inst="trigger_decision_to_netq", kind='FollySPSCQueue', capacity=2000),
        app.QueueSpec(inst="trigger_candidate_q", kind='FollySPSCQueue', capacity=2000),
    ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [

        mspec("ntoq_hsievent", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="hsievent_from_netq", dir="output")
                    ]),

        mspec("ntoq_token", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="token_from_netq", dir="output")
                    ]),

        mspec("qton_trigdec", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="trigger_decision_to_netq", dir="input")
                    ]),

        mspec("mlt", "ModuleLevelTrigger", [
            app.QueueInfo(name="token_source", inst="token_from_netq", dir="input"),
            app.QueueInfo(name="trigger_decision_sink", inst="trigger_decision_to_netq", dir="output"),
            app.QueueInfo(name="trigger_candidate_source", inst="trigger_candidate_q", dir="output"),
        ]),

        mspec("ttcm", "TimingTriggerCandidateMaker", [
            app.QueueInfo(name="input", inst="hsievent_from_netq", dir="input"),
            app.QueueInfo(name="output", inst="trigger_candidate_q", dir="output"),
        ]),


    ]

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)

    cmd_data['conf'] = acmd([
        ("mlt", mlt.ConfParams(
            links=[mlt.GeoID(system=SYSTEM_TYPE, region=0, element=idx) for idx in range(NUMBER_OF_DATA_PRODUCERS)],
            initial_token_count=TOKEN_COUNT                    
        )),
        
        ("ttcm", ttcm.Conf(
                        s1=ttcm.map_t(signal_type=TTCM_S1,
                                      time_before=100000,
                                      time_after=200000),
                        s2=ttcm.map_t(signal_type=TTCM_S2,
                                      time_before=100000,
                                      time_after=200000)
                        )
        ),

        ("ntoq_hsievent", ntoq.Conf(msg_type="dunedaq::dfmessages::HSIEvent",
                                           msg_module_name="HSIEventNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS["hsievent"])
                                           )
                ),
        ("ntoq_token", ntoq.Conf(msg_type="dunedaq::dfmessages::TriggerDecisionToken",
                                           msg_module_name="TriggerDecisionTokenNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS["triginh"])
                                           )
                ),
        ("qton_trigdec", qton.Conf(msg_type="dunedaq::dfmessages::TriggerDecision",
                                           msg_module_name="TriggerDecisionNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                    address=NETWORK_ENDPOINTS["trigdec"])
                                           )
                ),
    ])

    startpars = rccmd.StartParams(run=1, disable_data_storage=False)
    cmd_data['start'] = acmd([
        ("mlt", startpars),
        ("ttcm", startpars),
        ("ntoq_hsievent", startpars),
        ("ntoq_token", startpars),
        ("qton_trigdec", startpars),
    ])

    cmd_data['stop'] = acmd([
        ("mlt", None),
        ("ttcm", None),
        ("ntoq_hsievent", None),
        ("ntoq_token", startpars),
        ("qton_trigdec", startpars),
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
