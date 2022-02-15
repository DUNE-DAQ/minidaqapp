
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

class TPSetWriterApp(App):
    def __init__(self,
                 RU_CONFIG=[],
                 TPSET_WRITING_ENABLED=False,
                 HOST="localhost",
                 DEBUG=False):
        
        """Generate the json configuration for the readout and DF process"""

        modules = []
        
        if TPSET_WRITING_ENABLED:
            for ruidx in range(len(RU_CONFIG)):
                    modules += [DAQModule(name = f'tpswriter_ru{ruidx}',
                                          plugin = "TPSetWriter",
                                          connections = {}, #'tpset_source': Connection("tpsets_from_netq")},
                                          conf = tpsw.ConfParams(max_file_size_bytes=1000000000))]

        mgraph=ModuleGraph(modules)

        if TPSET_WRITING_ENABLED:
            for ruidx in range(len(RU_CONFIG)):
                    mgraph.add_endpoint(f"tpsets_into_writer_ru{ruidx}", f"tpswriter_ru{ruidx}.tpset_source", Direction.IN)

        super().__init__(modulegraph=mgraph, host=HOST)
        if DEBUG:
            self.export("tpsetwriter_app.dot")
