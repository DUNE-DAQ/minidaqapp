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

moo.otypes.load_types('timinglibs/hsireadout.jsonnet')
moo.otypes.load_types('timinglibs/hsicontroller.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd, 
import dunedaq.rcif.cmd as rccmd # AddressedCmd, 
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.timinglibs.hsireadout as hsi
import dunedaq.timinglibs.hsicontroller as hsic
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
        CONTROL_HSI_HARDWARE = False,
        READOUT_PERIOD_US: int = 1e3,
        HSI_ENDPOINT_ADDRESS = 1,
        HSI_ENDPOINT_PARTITION = 0,
        HSI_RE_MASK = 0x20000,
        HSI_FE_MASK = 0,
        HSI_INV_MASK = 0,
        HSI_SOURCE = 1,
        CONNECTIONS_FILE="${TIMING_SHARE}/config/etc/connections.xml",
        HSI_DEVICE_NAME="BOREAS_TLU",
        UHAL_LOG_LEVEL="notice",
    ):
    """
    { item_description }
    """
    cmd_data = {}

    required_eps = {'hsievent'}
    if CONTROL_HSI_HARDWARE:
        required_eps.add('hsicmds')

    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")

    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="hsievent_q_to_net", kind='FollySPSCQueue', capacity=100),
    ]
    
    if CONTROL_HSI_HARDWARE:
        queue_bare_specs.extend([app.QueueSpec(inst="hw_cmds_q_to_net", kind='FollySPSCQueue', capacity=100)])

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    hsi_controller_init_data = hsic.InitParams(
                                                  qinfos=app.QueueInfos([app.QueueInfo(name="hardware_commands_out", inst="hw_cmds_q_to_net", dir="output")]),
                                                  device=HSI_DEVICE_NAME,
                                                 )
    mod_specs = [
        mspec("qton_hsievent", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="hsievent_q_to_net", dir="input")
                    ]),

        mspec("hsir", "HSIReadout", [
                                    app.QueueInfo(name="hsievent_sink", inst="hsievent_q_to_net", dir="output"),
                                ]),
        ]
    
    if CONTROL_HSI_HARDWARE:
        hsi_controller_init_data = hsic.InitParams(
                                                  qinfos=app.QueueInfos([app.QueueInfo(name="hardware_commands_out", inst="hw_cmds_q_to_net", dir="output")]),
                                                  device=HSI_DEVICE_NAME,
                                                 )
        mod_specs.extend ( [
            mspec("qton_hw_cmds", "QueueToNetwork", [
                        app.QueueInfo(name="input", inst="hw_cmds_q_to_net", dir="input")
                    ]),
            app.ModSpec(inst="hsic", plugin="HSIController", data=hsi_controller_init_data)
        ])

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)
    
    conf_cmds = [
        ("qton_hsievent", qton.Conf(msg_type="dunedaq::dfmessages::HSIEvent",
                                           msg_module_name="HSIEventNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS["hsievent"],
                                                                  stype="msgpack")
                                           )
                ),
                        ("hsir", hsi.ConfParams(
                        connections_file=CONNECTIONS_FILE,
                        readout_period=READOUT_PERIOD_US,
                        hsi_device_name=HSI_DEVICE_NAME,
                        uhal_log_level=UHAL_LOG_LEVEL
                        )),
    ]
    
    if CONTROL_HSI_HARDWARE:
        conf_cmds.extend([
            ("qton_hw_cmds", qton.Conf(msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                           msg_module_name="TimingHwCmdNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS["hsicmds"],
                                                                  stype="msgpack")
                                           )),
            ("hsic", hsic.ConfParams(
                                address=HSI_ENDPOINT_ADDRESS,
                                partition=HSI_ENDPOINT_PARTITION,
                                rising_edge_mask=HSI_RE_MASK,
                                falling_edge_mask=HSI_FE_MASK,
                                invert_edge_mask=HSI_INV_MASK,
                                data_source=HSI_SOURCE,
                                )),
        ])
    cmd_data['conf'] = acmd(conf_cmds)

    startpars = rccmd.StartParams(run=RUN_NUMBER)

    cmd_data['start'] = acmd([
            ("hsi.*", startpars),
            ("qton_.*", startpars)
        ])

    cmd_data['stop'] = acmd([
            ("hsi.*", None),
            ("qton.*", None)
        ])

    cmd_data['pause'] = acmd([
            ("", None)
        ])

    cmd_data['resume'] = acmd([
            ("", None)
        ])

    cmd_data['scrap'] = acmd([
            ("", None)
        ])

    cmd_data['record'] = acmd([
            ("", None)
    ])

    return cmd_data
