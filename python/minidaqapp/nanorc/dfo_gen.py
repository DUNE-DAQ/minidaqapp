# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('dfmodules/datafloworchestrator.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,

import dunedaq.dfmodules.datafloworchestrator as dfo
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec

import json
import math
from pprint import pprint


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
def generate(
        NW_SPECS: list,
        
        TOKEN_COUNT: int = 10,
        DF_COUNT: int = 1,
        PARTITION="UNKNOWN"
):
    """
    { item_description }
    """
    cmd_data = {}
    
    required_eps = {PARTITION + '.hsievent'}
    if not required_eps.issubset([nw.name for nw in NW_SPECS]):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join([nw.name for nw in NW_SPECS])}")

    # Define modules and queues
    queue_bare_specs = []

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = []

    mod_specs += ([

        ### DFO
        mspec("dfo", "DataFlowOrchestrator", []),

    ])

    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs, nwconnections=NW_SPECS)

    # Generate schema for the maker plugins on the fly in the temptypes module
    #make_moo_record(ACTIVITY_CONFIG,'ActivityConf','temptypes')
    #make_moo_record(CANDIDATE_CONFIG,'CandidateConf','temptypes')
    import temptypes

    cmd_data['conf'] = acmd([

        ("dfo", dfo.ConfParams(
            token_connection=PARTITION+".triginh",
            busy_connection=PARTITION+".df_busy_signal",
            td_connection=PARTITION+".td_mlt_to_dfo",
            dataflow_applications=[dfo.app_config(decision_connection=f"{PARTITION}.trigdec_{dfidx}", 
                                                  thresholds=dfo.busy_thresholds( free=min(1, int(TOKEN_COUNT/2)),
                                                                                           busy=TOKEN_COUNT)) for dfidx in range(DF_COUNT)]
        )),
    ])

    # We start modules in "downstream-to-upstream" order, so that each
    # module is ready before its input starts sending data. The stop
    # order is the reverse (upstream-to-downstream), so each module
    # can process all of its input then stop, ensuring all data gets
    # processed
    start_order = [
        "dfo"
    ]

    stop_order = start_order[::-1]

    startpars = rccmd.StartParams(run=1)
    cmd_data['start'] = acmd([ (m, startpars) for m in start_order ])
    cmd_data['stop'] = acmd([ (m, None) for m in stop_order ])

    cmd_data['pause'] = acmd([
        ("", None)
    ])
    cmd_data['resume'] = acmd([
        ("", None)
    ])
    cmd_data['scrap'] = acmd([
        ("dfo", None)
    ])

    cmd_data['record'] = acmd([
        ("", None)
    ])

    return cmd_data
