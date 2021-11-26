# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()
import matplotlib.pyplot as plt

from os.path import exists, join
from rich.console import Console
from collections import namedtuple, defaultdict
import json
import os
from graphviz import Digraph
import networkx as nx
import moo.otypes

moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')

from appfwk.utils import acmd, mcmd, mspec
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.appfwk.app as appfwk  # AddressedCmd,
import dunedaq.rcif.cmd as rccmd  # AddressedCmd,
import dunedaq.trigger.moduleleveltrigger as mlt

# from .system import System
# from .app import App
# from .module import Module, ModuleGraph

console = Console()

########################################################################
#
# Classes
#
########################################################################

# TODO: Connections between modules are held in the module object, but
# connections between applications are held in their own
# structure. Not clear yet which is better, but should probably be
# consistent either way

# TODO: Understand whether extra_commands is actually needed. Seems like "resume" is already being sent to everyone?

# TODO: Make these all dataclasses




########################################################################
#
# Functions
#
########################################################################




def make_unique_name(base, dictionary):
    suffix=0
    while f"{base}{suffix}" in dictionary:
        suffix+=1
    assert f"{base}{suffix}" not in dictionary

    return f"{base}{suffix}"





