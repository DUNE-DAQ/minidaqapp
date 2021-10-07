# testapp_noreadout_two_process.py

# This python configuration produces *two* json configuration files
# that together form a MiniDAQApp with the same functionality as
# MiniDAQApp v1, but in two processes. One process contains the
# TriggerDecisionEmulator, while the other process contains everything
# else. The network communication is done with the QueueToNetwork and
# NetworkToQueue modules from the nwqueueadapters package.
#
# As with testapp_noreadout_confgen.py
# in this directory, no modules from the readout package are used: the
# fragments are provided by the FakeDataProd module from dfmodules


# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('timinglibs/fakehsieventgenerator.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd, 
import dunedaq.rcif.cmd as rccmd # AddressedCmd, 
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.timinglibs.fakehsieventgenerator as fhsig
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
        RUN_NUMBER = 333,
        CLOCK_SPEED_HZ: int = 50000000,
        DATA_RATE_SLOWDOWN_FACTOR: int = 1,
        TRIGGER_RATE_HZ: int = 1,
        HSI_DEVICE_ID: int = 0,
        MEAN_SIGNAL_MULTIPLICITY: int = 0,
        SIGNAL_EMULATION_MODE: int = 0,
        ENABLED_SIGNALS: int = 0b00000001,
    ):
    """
    { item_description }
    """
    cmd_data = {}

    required_eps = {'hsi.hsievent'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")

    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_from_netq", kind='FollyMPMCQueue', capacity=100),
            app.QueueSpec(inst="hsievent_q_to_net", kind='FollySPSCQueue', capacity=100),
        ]

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))


    mod_specs = [

        mspec("fhsig", "FakeHSIEventGenerator", [
                        app.QueueInfo(name="time_sync_source", inst="time_sync_from_netq", dir="input"),
                        app.QueueInfo(name="hsievent_sink", inst="hsievent_q_to_net", dir="output"),
                    ]),
        mspec("qton_hsievent", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="hsievent_q_to_net", dir="input")
                    ]),
        ] + [

           mspec(f"ntoq_timesync_{idx}", "NetworkToQueue", [
                        app.QueueInfo(name="output", inst="time_sync_from_netq", dir="output")
                    ]) for idx, inst in enumerate(NETWORK_ENDPOINTS) if "timesync" in inst
        ]

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)

    trigger_interval_ticks=0
    if TRIGGER_RATE_HZ > 0:
        trigger_interval_ticks=math.floor((1/TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR)
    
    cmd_data['conf'] = acmd([

                ("fhsig", fhsig.Conf(
                        clock_frequency=CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR,
                        trigger_interval_ticks=trigger_interval_ticks,
                        mean_signal_multiplicity=MEAN_SIGNAL_MULTIPLICITY,
                        signal_emulation_mode=SIGNAL_EMULATION_MODE,
                        enabled_signals=ENABLED_SIGNALS,
                        )),

                ("qton_hsievent", qton.Conf(msg_type="dunedaq::dfmessages::HSIEvent",
                                           msg_module_name="HSIEventNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS["hsi.hsievent"],
                                                                  stype="msgpack")
                                           )
                 ),
    ] + [

                (f"ntoq_timesync_{idx}", ntoq.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                           msg_module_name="TimeSyncNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
								    subscriptions=["Timesync"],
                                                                    address=NETWORK_ENDPOINTS[inst])
                                           )
                ) for idx, inst in enumerate(NETWORK_ENDPOINTS) if "timesync" in inst
    ])
 

    startpars = rccmd.StartParams(run=RUN_NUMBER, trigger_interval_ticks = trigger_interval_ticks)
    resumepars = rccmd.ResumeParams(trigger_interval_ticks = trigger_interval_ticks)

    cmd_data['start'] = acmd([
            ("ntoq_timesync_.*", startpars),
            ("fhsig", startpars),
            ("qton_hsievent", startpars)
        ])

    cmd_data['stop'] = acmd([
            ("ntoq_timesync_.*", None),
            ("fhsig", None),
            ("qton_hsievent", None)
        ])

    cmd_data['pause'] = acmd([
            ("", None)
        ])

    cmd_data['resume'] = acmd([
        ("fhsig", resumepars)
    ])

    cmd_data['scrap'] = acmd([
            ("", None)
        ])

    cmd_data['record'] = acmd([
            ("", None)
    ])

    return cmd_data
