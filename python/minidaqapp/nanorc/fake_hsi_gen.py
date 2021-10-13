# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('timinglibs/fakehsieventgenerator.jsonnet')

import dunedaq.timinglibs.fakehsieventgenerator as fhsig

import math

#===============================================================================
def generate(
        #NETWORK_ENDPOINTS: list,
        #RUN_NUMBER = 333,
        CLOCK_SPEED_HZ: int = 50000000,
        DATA_RATE_SLOWDOWN_FACTOR: int = 1,
        TRIGGER_RATE_HZ: int = 1,
        HSI_DEVICE_ID: int = 0,
        MEAN_SIGNAL_MULTIPLICITY: int = 0,
        SIGNAL_EMULATION_MODE: int = 0,
        ENABLED_SIGNALS: int = 0b00000001,
    ):

    trigger_interval_ticks=0
    if TRIGGER_RATE_HZ > 0:
        trigger_interval_ticks=math.floor((1/TRIGGER_RATE_HZ) * CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR)

    from .util import Module, ModuleGraph, Direction
    from .util import Connection as Conn
    from . import util
    
    modules = {}

    modules["fhsig"] = Module(plugin = "FakeHSIEventGenerator",
                              conf =  fhsig.Conf(clock_frequency=CLOCK_SPEED_HZ/DATA_RATE_SLOWDOWN_FACTOR,
                                                 trigger_interval_ticks=trigger_interval_ticks,
                                                 mean_signal_multiplicity=MEAN_SIGNAL_MULTIPLICITY,
                                                 signal_emulation_mode=SIGNAL_EMULATION_MODE,
                                                 enabled_signals=ENABLED_SIGNALS)
                              )

    mgraph = ModuleGraph(modules)
    mgraph.add_endpoint("time_sync", "fhsig.time_sync_source", Direction.IN)
    mgraph.add_endpoint("hsievent",  "fhsig.hsievent_sink",    Direction.OUT)

    return mgraph
