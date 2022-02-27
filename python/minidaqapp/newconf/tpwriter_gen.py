
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('dfmodules/tpsetwriter.jsonnet')

# Import new types
import dunedaq.dfmodules.tpsetwriter as tpsw

from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction

# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100

def get_tpwriter_app(RU_CONFIG=[],
                     HOST="localhost",
                     DEBUG=False):

    """Generate the json configuration for the readout and DF process"""

    modules = []

    modules += [DAQModule(name = 'tpswriter',
                          plugin = "TPStreamWriter",
                          connections = {}, #'tpset_source': Connection("tpsets_from_netq")},
                          conf = tpsw.ConfParams(max_file_size_bytes=1000000000))]

    mgraph=ModuleGraph(modules)

    mgraph.add_endpoint("tpsets_into_writer", "tpswriter.tpset_source", Direction.IN)

    tpw_app = App(modulegraph=mgraph, host=HOST)

    if DEBUG:
        tpw_app.export("tpwriter_app.dot")

    return tpw_app
