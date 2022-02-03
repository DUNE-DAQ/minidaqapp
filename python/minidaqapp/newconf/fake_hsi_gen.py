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

moo.otypes.load_types('timinglibs/fakehsieventgenerator.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.timinglibs.fakehsieventgenerator as fhsig

from appfwk.utils import acmd, mcmd, mrccmd, mspec
    
from appfwk.daqmodule import DAQModule
from appfwk.app import ModuleGraph, App
from appfwk.conf_utils import Direction
        
import math

#===============================================================================
class FakeHSIApp(App):
    def __init__(self,
                 RUN_NUMBER=333,
                 CLOCK_SPEED_HZ: int=50000000,
                 DATA_RATE_SLOWDOWN_FACTOR: int=1,
                 TRIGGER_RATE_HZ: int=1,
                 HSI_DEVICE_ID: int=0,
                 MEAN_SIGNAL_MULTIPLICITY: int=0,
                 SIGNAL_EMULATION_MODE: int=0,
                 ENABLED_SIGNALS: int=0b00000001,
                 PARTITION="UNKNOWN",
                 HOST="localhost"):
        
        trigger_interval_ticks = 0
        if TRIGGER_RATE_HZ > 0:
            trigger_interval_ticks = math.floor((1 / TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ / DATA_RATE_SLOWDOWN_FACTOR)

        startpars = rccmd.StartParams(run=RUN_NUMBER, trigger_interval_ticks = trigger_interval_ticks)
        resumepars = rccmd.ResumeParams(trigger_interval_ticks = trigger_interval_ticks)

        modules = [DAQModule(name   = 'fhsig',
                             plugin = "FakeHSIEventGenerator",
                             conf   =  fhsig.Conf(clock_frequency=CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR,
                                                  trigger_interval_ticks=trigger_interval_ticks,
                                                  mean_signal_multiplicity=MEAN_SIGNAL_MULTIPLICITY,
                                                  signal_emulation_mode=SIGNAL_EMULATION_MODE,
                                                  enabled_signals=ENABLED_SIGNALS,
                                                  hsievent_connection_name=PARTITION+".hsievents"),
                             extra_commands = {"start": startpars,
                                               "resume": resumepars})]
    
        mgraph = ModuleGraph(modules)
        mgraph.add_endpoint("time_sync", "fhsig.time_sync_source", Direction.IN)
        mgraph.add_endpoint("hsievents", "fhsig.hsievent_sink",    Direction.OUT)
        super().__init__(modulegraph=mgraph, host=HOST, name="FakeHSIApp")
        self.export("fake_hsi_app.dot")
        

