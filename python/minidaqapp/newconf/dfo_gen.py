# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('dfmodules/datafloworchestrator.jsonnet')

# Import new types
import dunedaq.dfmodules.datafloworchestrator as dfo

from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction, Connection


#FIXME maybe one day, triggeralgs will define schemas... for now allow a dictionary of 4byte int, 4byte floats, and strings
moo.otypes.make_type(schema='number', dtype='i4', name='temp_integer', path='temptypes')
moo.otypes.make_type(schema='number', dtype='f4', name='temp_float', path='temptypes')
moo.otypes.make_type(schema='string', name='temp_string', path='temptypes')
def make_moo_record(conf_dict,name,path='temptypes'):
    fields = []
    for pname,pvalue in conf_dict.items():
        typename = None
        if type(pvalue) == int:
            typename = 'temptypes.temp_integer'
        elif type(pvalue) == float:
            typename = 'temptypes.temp_float'
        elif type(pvalue) == str:
            typename = 'temptypes.temp_string'
        else:
            raise Exception(f'Invalid config argument type: {type(value)}')
        fields.append(dict(name=pname,item=typename))
    moo.otypes.make_type(schema='record', fields=fields, name=name, path=path)

#===============================================================================
def get_dfo_app(TOKEN_COUNT: int = 10,
                PARTITION="UNKNOWN",
                DF_COUNT: int = 1,
                HOST="localhost",
                DEBUG=False):
    
    modules = []
    
    df_app_configs = [dfo.app_config(decision_connection=f"{PARTITION}.trigdec_{dfidx}", 
                                     thresholds=dfo.busy_thresholds(free=max(1, int(TOKEN_COUNT/2)),
                                                                             busy=TOKEN_COUNT)) for dfidx in range(DF_COUNT)]
    modules += [DAQModule(name = "dfo",
                          plugin = "DataFlowOrchestrator",
                          conf = dfo.ConfParams(token_connection = PARTITION+".triginh",
                                                busy_connection = PARTITION+".df_busy_signal",
                                                td_connection = PARTITION+".td_mlt_to_dfo",
                                                dataflow_applications=df_app_configs))]
    
    mgraph = ModuleGraph(modules)
    mgraph.add_endpoint("td_to_dfo", None, Direction.IN)
    mgraph.add_endpoint("df_busy_signal", None, Direction.OUT)
    for i in range(DF_COUNT):
        # We have an outgoing endpoint for trigger decisions, but the
        # TDs come directly from the DFO to a nwmgr connection, so the
        # queue we connect to is None
        mgraph.add_endpoint(f"trigger_decisions{i}", None, Direction.OUT)
        # mgraph.add_endpoint("tokens", "mlt.token_source", Direction.IN)

    dfo_app = App(modulegraph=mgraph, host=HOST, name='DFOApp')
    
    if DEBUG:
        dfo_app.export("dfo_app.dot")
    
    return dfo_app
