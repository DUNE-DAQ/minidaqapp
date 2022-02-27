# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')
moo.otypes.load_types('trigger/fakedataflow.jsonnet')
moo.otypes.load_types('trigger/timingtriggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/tpsetbuffercreator.jsonnet')

# Import new types
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggercandidatemaker as tcm
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.moduleleveltrigger as mlt
import dunedaq.trigger.fakedataflow as fdf
import dunedaq.trigger.timingtriggercandidatemaker as ttcm
import dunedaq.trigger.tpsetbuffercreator as buf

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
def get_trigger_app(SOFTWARE_TPG_ENABLED: bool = False,
                    RU_CONFIG: list = [],

                    ACTIVITY_PLUGIN: str = 'TriggerActivityMakerPrescalePlugin',
                    ACTIVITY_CONFIG: dict = dict(prescale=10000),

                    CANDIDATE_PLUGIN: str = 'TriggerCandidateMakerPrescalePlugin',
                    CANDIDATE_CONFIG: int = dict(prescale=10),

                    SYSTEM_TYPE = 'wib',
                    TTCM_S1: int = 1,
                    TTCM_S2: int = 2,
                    TRIGGER_WINDOW_BEFORE_TICKS: int = 1000,
                    TRIGGER_WINDOW_AFTER_TICKS: int = 1000,
                    PARTITION="UNKNOWN",

                    HOST="localhost",
                    DEBUG=False):
    
    # Generate schema for the maker plugins on the fly in the temptypes module
    make_moo_record(ACTIVITY_CONFIG , 'ActivityConf' , 'temptypes')
    make_moo_record(CANDIDATE_CONFIG, 'CandidateConf', 'temptypes')
    import temptypes

    modules = []
    
    if SOFTWARE_TPG_ENABLED:
        config_tcm =  tcm.Conf(candidate_maker=CANDIDATE_PLUGIN,
                               candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG))
        
        modules += [DAQModule(name = 'tcm',
                              plugin = 'TriggerCandidateMaker',
                              connections = {#'input' : Connection(f'tcm.taset_q'),
                                  'output': Connection(f'mlt.trigger_candidate_source')},
                              conf = config_tcm)]
        
        region_ids = set()
        for ru in range(len(RU_CONFIG)):
            ## 1 zipper/TAM per region id
            region_id = RU_CONFIG[ru]["region_id"]
            skip=False
            if region_id in region_ids: skip=True

            if not skip: # we only add Zipper/TAM is that region_id wasn't seen before (in a very clunky way)
                region_ids.add(region_id)
                cardinality = 0
                for RU in RU_CONFIG:
                    if RU['region_id'] == region_id:
                        cardinality += RU['channel_count']
                modules += [DAQModule(name = f'zip_{region_id}',
                                      plugin = 'TPZipper',
                                              connections = {# 'input' are App.network_endpoints, from RU
                                                  'output': Connection(f'tam_{region_id}.input')},
                                              conf = tzip.ConfParams(cardinality=cardinality,
                                                                     max_latency_ms=1000,
                                                                     region_id=region_id,
                                                                     # 2022-02-02 PL: Not sure what element_id should be,
                                                                     # since zipper is merging the stream for the whole region_id
                                                             element_id=0)),
                                    
                            DAQModule(name = f'tam_{region_id}',
                                      plugin = 'TriggerActivityMaker',
                                      connections = {'output': Connection('tcm.input')},
                                      conf = tam.Conf(activity_maker=ACTIVITY_PLUGIN,
                                                      geoid_region=region_id,
                                                      geoid_element=0,  # 2022-02-02 PL: Same comment as above
                                                      window_time=10000,  # should match whatever makes TPSets, in principle
                                                      buffer_time=625000,  # 10ms in 62.5 MHz ticks
                                                      activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)))]

            for idy in range(RU_CONFIG[ru]["channel_count"]):
                # 1 buffer per TPG channel
                modules += [DAQModule(name = f'buf_ru{ru}_link{idy}',
                                      plugin = 'TPSetBufferCreator',
                                      connections = {},#'tpset_source': Connection(f"tpset_q_for_buf{ru}_{idy}"),#already in request_receiver
                                      #'data_request_source': Connection(f"data_request_q{ru}_{idy}"), #ditto
                                      # 'fragment_sink': Connection('qton_fragments.fragment_q')},
                                   conf = buf.Conf(tpset_buffer_size=10000, region=RU_CONFIG[ru]["region_id"], element=idy + RU_CONFIG[ru]["start_channel"]))]

    modules += [DAQModule(name = 'ttcm',
                          plugin = 'TimingTriggerCandidateMaker',
                          connections={"output": Connection("mlt.trigger_candidate_source")},
                          conf=ttcm.Conf(s1=ttcm.map_t(signal_type=TTCM_S1,
                                                       time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                       time_after=TRIGGER_WINDOW_AFTER_TICKS),
                                         s2=ttcm.map_t(signal_type=TTCM_S2,
                                                       time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                       time_after=TRIGGER_WINDOW_AFTER_TICKS),
                                         hsievent_connection_name = PARTITION+".hsievents"))]
    
    # We need to populate the list of links based on the fragment
    # producers available in the system. This is a bit of a
    # chicken-and-egg problem, because the trigger app itself creates
    # fragment producers (see below). Eventually when the MLT is its
    # own process, this problem will probably go away, but for now, we
    # leave the list of links here blank, and replace it in
    # util.connect_fragment_producers
    modules += [DAQModule(name = 'mlt',
                          plugin = 'ModuleLevelTrigger',
                          #connections = { #"trigger_decision_sink": Connection("dfo.trigger_decision_queue")
                          #             },
                          conf=mlt.ConfParams(links=[],  # To be updated later - see comment above
                                              dfo_connection=f"{PARTITION}.td_mlt_to_dfo",
                                              dfo_busy_connection=f"{PARTITION}.df_busy_signal"))]

    mgraph = ModuleGraph(modules)
    mgraph.add_endpoint("hsievents", None, Direction.IN)
    mgraph.add_endpoint("td_to_dfo", None, Direction.OUT)
    mgraph.add_endpoint("df_busy_signal", None, Direction.IN)
    if SOFTWARE_TPG_ENABLED:
        for ruidx, ru_config in enumerate(RU_CONFIG):
            # 1 zipper input per region_id
            # PL 2022-02-02: Maybe need to check that we don't create twice the same endpoint?
            mgraph.add_endpoint(f"tpsets_into_chain_apa{ru_config['region_id']}", f"zip_{ru_config['region_id']}.input", Direction.IN)

            for link_idx in range(ru_config["channel_count"]):
                # 1 buffer per link
                buf_name=f'buf_ru{ruidx}_link{link_idx}'
                global_link = link_idx+ru_config['start_channel'] # for the benefit of correct fragment geoid
                mgraph.add_endpoint(f"tpsets_into_buffer_ru{ruidx}_link{link_idx}", f"{buf_name}.tpset_source", Direction.IN)
                mgraph.add_fragment_producer(region=ru_config['region_id'], element=global_link, system="DataSelection",
                                             requests_in=f"{buf_name}.data_request_source",
                                             fragments_out=f"{buf_name}.fragment_sink")


    trigger_app = App(modulegraph=mgraph, host=HOST, name='TriggerApp')
    
    if DEBUG:
        trigger_app.export("trigger_app.dot")

    return trigger_app

