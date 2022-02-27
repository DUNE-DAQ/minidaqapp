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

import math
from rich.console import Console
console = Console()

# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('timinglibs/timingpartitioncontroller.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd, 
import dunedaq.rcif.cmd as rccmd # AddressedCmd, 
import dunedaq.appfwk.cmd as cmd # AddressedCmd, 
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.timinglibs.timingpartitioncontroller as tprtc
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction, Connection

#===============================================================================
def get_tprtc_app(MASTER_DEVICE_NAME="",
                  TIMING_PARTITION=0,
                  TRIGGER_MASK=0xff,
                  RATE_CONTROL_ENABLED=True,
                  SPILL_GATE_ENABLED=False,
                  PARTITION="UNKNOWN",
                  GLOBAL_PARTITION="UNKNOWN",
                  HOST="localhost",
                  DEBUG=False):
    
    modules = {}

    modules = [DAQModule(name = "tprtc",
                         plugin = "TimingPartitionController",
                         conf = tprtc.PartitionConfParams(
                                             device=MASTER_DEVICE_NAME,
                                             partition_id=TIMING_PARTITION,
                                             trigger_mask=TRIGGER_MASK,
                                             spill_gate_enabled=SPILL_GATE_ENABLED,
                                             rate_control_enabled=RATE_CONTROL_ENABLED,
                                             ))]

    mgraph = ModuleGraph(modules)
     
    mgraph.add_endpoint("timing_cmds", "tprtc.hardware_commands_out", Direction.OUT)
     
    tprtc_app = App(modulegraph=mgraph, host=HOST, name="TPRTCApp")
     
    if DEBUG:
        tprtc_app.export("tprtc_app.dot")

    return tprtc_app
