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

#===============================================================================
class THIApp(App):
    def __init__(self,
                 RUN_NUMBER: int,
                 NW_SPECS: list,
                 TIMING_CMD_NETWORK_ENDPOINTS: set,
                 GATHER_INTERVAL=1e6,
                 GATHER_INTERVAL_DEBUG=10e6,
                 HSI_DEVICE_NAME="",
                 CONNECTIONS_FILE="${TIMING_SHARE}/config/etc/connections.xml",
                 UHAL_LOG_LEVEL="notice",
                 PARTITION="UNKNOWN"):
        """
        { item_description }
        """
        cmd_data = {}
        ## TODO: Everything?
        required_eps = TIMING_CMD_NETWORK_ENDPOINTS
        if not required_eps.issubset([nw.name for nw in NW_SPECS]):
            raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")
    
        # # Define modules and queues
        # queue_bare_specs = [app.QueueSpec(inst="ntoq_timing_cmds", kind='FollyMPMCQueue', capacity=100),]
    
        # # Only needed to reproduce the same order as when using jsonnet
        # queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))
        thi_init_data = thi.InitParams(qinfos=app.QueueInfos([app.QueueInfo(name="hardware_commands_in", inst="ntoq_timing_cmds", dir="input")]),
                                       connections_file=CONNECTIONS_FILE,
                                       gather_interval=GATHER_INTERVAL,
                                       gather_interval_debug=GATHER_INTERVAL_DEBUG,
                                       monitored_device_name_master="",
                                       monitored_device_names_fanout=[],
                                       monitored_device_name_endpoint="",
                                       monitored_device_name_hsi=HSI_DEVICE_NAME,
                                       uhal_log_level=UHAL_LOG_LEVEL)

        modules = {}
        modules["thi"] = Module("TimingHardwareManagerPDI")
        # mod_specs = [app.ModSpec(inst="thi", plugin="TimingHardwareManagerPDI", data=thi_init_data),]
        for cmd_nw_endpoint in TIMING_CMD_NETWORK_ENDPOINTS:
            mod_specs.extend([mspec(f'ntoq_{cmd_nw_endpoint}', "NetworkToQueue", [app.QueueInfo(name="output", inst="ntoq_timing_cmds", dir="output")]),])
                
        cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)
        
    
        conf_cmds = []
        for cmd_nw_endpoint in TIMING_CMD_NETWORK_ENDPOINTS:
            conf_cmds.extend([(f'ntoq_{cmd_nw_endpoint}', ntoq.Conf(msg_type="dunedaq::timinglibs::timingcmd::TimingHwCmd",
                                                   msg_module_name="TimingHwCmdNQ",
                                                   receiver_config=nor.Conf(name=cmd_nw_endpoint))),])
        mgraph = ModuleGraph(modules)
        super().__init__(modulegraph=mgraph, host=HOST)
