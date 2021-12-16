# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/intervaltccreator.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')
moo.otypes.load_types('trigger/fakedataflow.jsonnet')
moo.otypes.load_types('trigger/timingtriggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/tpsetbuffercreator.jsonnet')
moo.otypes.load_types('trigger/tpsetreceiver.jsonnet')

moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('dfmodules/requestreceiver.jsonnet')
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')

# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.trigger.intervaltccreator as itcc
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggercandidatemaker as tcm
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.moduleleveltrigger as mlt
import dunedaq.trigger.fakedataflow as fdf
import dunedaq.trigger.timingtriggercandidatemaker as ttcm
import dunedaq.trigger.tpsetbuffercreator as buf
import dunedaq.trigger.tpsetreceiver as tpsrcv

import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.dfmodules.requestreceiver as rrcv
import dunedaq.networkmanager.nwmgr as nwmgr

from appfwk.utils import acmd, mcmd, mrccmd, mspec
from appfwk.conf_utils import App, ModuleGraph, Module, Direction, Connection


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
class TriggerApp(App):
    def __init__(self,
                 # NW_SPECS: list,
                 
                 SOFTWARE_TPG_ENABLED: bool = False,
                 RU_CONFIG: list = [],

                 ACTIVITY_PLUGIN: str = 'TriggerActivityMakerPrescalePlugin',
                 ACTIVITY_CONFIG: dict = dict(prescale=10000),

                 CANDIDATE_PLUGIN: str = 'TriggerCandidateMakerPrescalePlugin',
                 CANDIDATE_CONFIG: int = dict(prescale=10),

                 TOKEN_COUNT: int = 10,
                 SYSTEM_TYPE = 'wib',
                 TTCM_S1: int = 1,
                 TTCM_S2: int = 2,
                 TRIGGER_WINDOW_BEFORE_TICKS: int = 1000,
                 TRIGGER_WINDOW_AFTER_TICKS: int = 1000,
                 PARTITION="UNKNOWN",
                 HOST="localhost"
                 ):
        """
        { item_description }
        """
        
        # Generate schema for the maker plugins on the fly in the temptypes module
        make_moo_record(ACTIVITY_CONFIG , 'ActivityConf' , 'temptypes')
        make_moo_record(CANDIDATE_CONFIG, 'CandidateConf', 'temptypes')
        import temptypes

        modules = []
    
        if SOFTWARE_TPG_ENABLED:
            connections_request_receiver = {}
            connections_tpset_receiver = {}
            for ru in range(len(RU_CONFIG)):
                for idy in range(RU_CONFIG[ru]["channel_count"]):
                    connections_request_receiver[f'output_{ru}_{idy}'] = Connection(f'buf{ru}_{idy}.data_request_q{ru}_{idy}')
                    connections_tpset_receiver  [f'output_{ru}_{idy}'] = Connection(f'buf{ru}_{idy}.tpset_q_for_buf{ru}_{idy}')

            config_request_receiver = rrcv.ConfParams(map = [rrcv.geoidinst(region=RU_CONFIG[ru]["region_id"],
                                                                            element=idy+RU_CONFIG[ru]["start_channel"],
                                                                            system="DataSelection",
                                                                            queueinstance=f"data_request_q{ru}_{idy}")
                                                             for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])],
                                                      general_queue_timeout = 100,
                                                      connection_name = f"{PARTITION}.ds_tp_datareq_0")
            
            config_tpset_receiver = tpsrcv.ConfParams(map = [tpsrcv.geoidinst(region=RU_CONFIG[ru]["region_id"],
                                                                              element=idy+RU_CONFIG[ru]["start_channel"],
                                                                              system=SYSTEM_TYPE,
                                                                              queueinstance=f"tpset_q_for_buf{ru}_{idy}")
                                                             for ru in range(len(RU_CONFIG)) for idy in range(RU_CONFIG[ru]["channel_count"])],
                                                      general_queue_timeout = 100,
                                                      topic = f"TPSets")
    
            config_qton_fragment = qton.Conf(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                             msg_module_name="FragmentNQ",
                                             sender_config=nos.Conf(name=f"{PARTITION}.frags_tpset_ds_0",stype="msgpack"))

            config_tcm =  tcm.Conf(candidate_maker=CANDIDATE_PLUGIN,
                                   candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG))
            
            modules += [Module(name = 'request_receiver',
                               plugin = 'RequestReceiver',
                               connections = connections_request_receiver,
                               conf = config_request_receiver),
                        
                        Module(name = 'tpset_receiver',
                               plugin = 'TPSetReceiver',
                               connections = connections_tpset_receiver,
                               conf = config_tpset_receiver),
                        
                        Module(name = 'qton_fragments',
                               plugin = 'QueueToNetwork',
                               connections = {}, # all the incoming links in TPSetBufferCreators
                               conf = config_qton_fragment),
                        
                        Module(name = 'tcm',
                               plugin = 'TriggerCandidateMaker',
                               connections = {#'input' : Connection(f'tcm.taset_q'),
                                   'output': Connection(f'mlt.trigger_candidate_q')},
                               conf = config_tcm)]
            
            for ru in range(len(RU_CONFIG)):
                
                modules += [Module(name = f'tpset_subscriber_{ru}',
                                   plugin = 'NetworkToQueue',
                                   connections = {'output': Connection(f'zip_{ru}.tpsets_from_netq_{ru}')},
                                   conf = ntoq.Conf(msg_type="dunedaq::trigger::TPSet",
                                                    msg_module_name="TPSetNQ",
                                                    receiver_config=nor.Conf(name=f'{PARTITION}.tpsets_{ru}',
                                                                             subscriptions=["TPSets"]))),
                            
                            Module(name = f'zip_{ru}',
                                   plugin = 'TPZipper',
                                   connections = {# 'input' are App.network_endpoints, from RU
                                       'output': Connection(f'tam_{ru}.input')},
                                   conf = tzip.ConfParams(cardinality=RU_CONFIG[ru]['channel_count'],
                                                          max_latency_ms=1000,
                                                          region_id=0,
                                                          element_id=0)),
                            
                            Module(name = f'tam_{ru}',
                                   plugin = 'TriggerActivityMaker',
                                   connections = {'output': Connection('tcm.taset_q')},
                                   conf = tam.Conf(activity_maker=ACTIVITY_PLUGIN,
                                                   geoid_region=0,  # Fake placeholder
                                                   geoid_element=0,  # Fake placeholder
                                                   window_time=10000,  # should match whatever makes TPSets, in principle
                                                   buffer_time=625000,  # 10ms in 62.5 MHz ticks
                                                   activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)))]

                for idy in range(RU_CONFIG[ru]["channel_count"]):
                    modules += [Module(name = f'buf{ru}_{idy}',
                                       plugin = 'TPSetBufferCreator',
                                       connections = {#'tpset_source': Connection(f"tpset_q_for_buf{ru}_{idy}"),#already in request_receiver
                                                      #'data_request_source': Connection(f"data_request_q{ru}_{idy}"), #ditto
                                                      'fragment_sink': Connection('qton_fragments.fragment_q')},
                                       conf = buf.Conf(tpset_buffer_size=10000, region=RU_CONFIG[ru]["region_id"], element=idy + RU_CONFIG[ru]["start_channel"]))]

        modules += [Module(name = 'ttcm',
                           plugin = 'TimingTriggerCandidateMaker',
                           connections={"output": Connection("mlt.trigger_candidate_q")},
                           conf=ttcm.Conf(s1=ttcm.map_t(signal_type=TTCM_S1,
                                                        time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                        time_after=TRIGGER_WINDOW_AFTER_TICKS),
                                          s2=ttcm.map_t(signal_type=TTCM_S2,
                                                        time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                        time_after=TRIGGER_WINDOW_AFTER_TICKS),
                                          hsievent_connection_name = PARTITION+".hsievent"))]
                    
        # We need to populate the list of links based on the fragment
        # producers available in the system. This is a bit of a
        # chicken-and-egg problem, because the trigger app itself creates
        # fragment producers (see below). Eventually when the MLT is its
        # own process, this problem will probably go away, but for now, we
        # leave the list of links here blank, and replace it in
        # util.connect_fragment_producers
        modules += [Module(name = 'mlt',
                           plugin = 'ModuleLevelTrigger',
                           conf=mlt.ConfParams(links=[], # To be updated later - see comment above
                                               td_connection_name=PARTITION+".trigdec",
                                               token_connection_name=PARTITION+".triginh"))]
        
        mgraph = ModuleGraph(modules)
        mgraph.add_endpoint("hsievents",  "ttcm.input", Direction.IN)
        if SOFTWARE_TPG_ENABLED:
            for idx in range(len(RU_CONFIG)):
                mgraph.add_endpoint(f"tpsets_into_chain_link{idx}", f"tpset_receiver.input", Direction.IN)
                mgraph.add_endpoint(f"tpsets_into_buffer_link{idx}", f"tpset_subscriber_{idx}.tpset_source", Direction.IN)

                mgraph.add_fragment_producer(region=0, element=idx, system="DataSelection",
                                             requests_in=f"request_receiver.data_request_source",
                                             fragments_out=f"qton_fragments.fragment_sink")


        mgraph.add_endpoint("trigger_decisions", "mlt.trigger_decision_sink", Direction.OUT)
        mgraph.add_endpoint("tokens", "mlt.token_source", Direction.IN)

        super().__init__(modulegraph=mgraph, host=HOST, name='TriggerApp')
        self.export("trigger_app.dot")

