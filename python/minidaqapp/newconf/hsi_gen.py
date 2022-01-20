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

moo.otypes.load_types('timinglibs/hsireadout.jsonnet')
moo.otypes.load_types('timinglibs/hsicontroller.jsonnet')
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
import dunedaq.timinglibs.hsireadout as hsi
import dunedaq.timinglibs.hsicontroller as hsic
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction

#===============================================================================
class HSIApp(App):
    def __init__(self,
                 NW_SPECS: list,
                 RUN_NUMBER = 333,
                 CLOCK_SPEED_HZ: int = 50000000,
                 TRIGGER_RATE_HZ: int = 1,
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
                 PARTITION="UNKNOWN",
                 HOST="localhost"## CHANGE ME TO WHAT IT SHOULD BE
                 ):
        """
        { item_description }
        """
    
        required_eps = {PARTITION+'.hsievent'}
        if CONTROL_HSI_HARDWARE:
            required_eps.add('hsicmds')
    
        if not required_eps.issubset([nw.name for nw in NW_SPECS]):
            raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")
    
        
        hsi_controller_init_data = hsic.InitParams(qinfos=app.QueueInfos([app.QueueInfo(name="hardware_commands_out", inst="hw_cmds_q_to_net", dir="output")]),
                                                   device=HSI_DEVICE_NAME,
                                                   )
        modules = {}

        ## TODO all the connections...
        modules["hsir"] = Module(plugin = "HSIReadout",
                                 conf = hsi.ConfParams(connections_file=CONNECTIONS_FILE,
                                                       readout_period=READOUT_PERIOD_US,
                                                       hsi_device_name=HSI_DEVICE_NAME,
                                                       uhal_log_level=UHAL_LOG_LEVEL,
                                                       hsievent_connection_name = f"{PARTITION}.hsievent"))
        
        if CONTROL_HSI_HARDWARE:
            modules["hsic"] = Module(plugin = "HSIController",
                                     conf = hsic.ConfParams(clock_frequency=CLOCK_SPEED_HZ,
                                                            trigger_interval_ticks=trigger_interval_ticks,
                                                            address=HSI_ENDPOINT_ADDRESS,
                                                            partition=HSI_ENDPOINT_PARTITION,
                                                            rising_edge_mask=HSI_RE_MASK,
                                                            falling_edge_mask=HSI_FE_MASK,
                                                            invert_edge_mask=HSI_INV_MASK,
                                                            data_source=HSI_SOURCE))
        mgraph = ModuleGraph(modules)
        super().__init__(modulegraph=mgraph, host=HOST)