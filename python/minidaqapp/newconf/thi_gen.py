# testapp_noreadout_two_process.py

# This python configuration produces *two* json configuration files
# that together form a MiniDAQApp with the same functionality as
# MiniDAQApp v1, but in two processes.  One process contains the
# TriggerDecisionEmulator, while the other process contains everything
# else.  The network communication is done with the QueueToNetwork and
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

moo.otypes.load_types('timinglibs/timinghardwaremanagerpdi.jsonnet')
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
import dunedaq.timinglibs.timinghardwaremanagerpdi as thi
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
class THIApp(App):
    def __init__(self,
                 GATHER_INTERVAL=1e6,
                 GATHER_INTERVAL_DEBUG=10e6,
                 MASTER_DEVICE_NAME="",
                 HSI_DEVICE_NAME="",
                 CONNECTIONS_FILE="${TIMING_SHARE}/config/etc/connections.xml",
                 UHAL_LOG_LEVEL="notice",
                 HOST="localhost",
                 DEBUG=False):
        """
        { item_description }
        """
        modules = {}
        modules = [ 
                    DAQModule( name="thi",
                                    plugin="TimingHardwareManagerPDI",
                                    conf= thi.ConfParams(connections_file=CONNECTIONS_FILE,
                                                           gather_interval=GATHER_INTERVAL,
                                                           gather_interval_debug=GATHER_INTERVAL_DEBUG,
                                                           monitored_device_name_master=MASTER_DEVICE_NAME,
                                                           monitored_device_names_fanout=[],
                                                           monitored_device_name_endpoint="",
                                                           monitored_device_name_hsi=HSI_DEVICE_NAME,
                                                           uhal_log_level=UHAL_LOG_LEVEL)),
                    ]                
            

        mgraph = ModuleGraph(modules)
        mgraph.add_endpoint("timing_cmds", "thi.timing_cmds_queue", Direction.IN)
        super().__init__(modulegraph=mgraph, host=HOST, name="THIApp")
        if DEBUG:
            self.export("thi_app.dot")
