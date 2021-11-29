# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/intervaltccreator.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')
moo.otypes.load_types('trigger/timingtriggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/tpsetbuffercreator.jsonnet')

# Import new types
import dunedaq.trigger.intervaltccreator as itcc
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggercandidatemaker as tcm
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.moduleleveltrigger as mlt
import dunedaq.trigger.timingtriggercandidatemaker as ttcm
import dunedaq.trigger.tpsetbuffercreator as buf

from .module import Module, ModuleGraph
from .app import App
from .connection import Direction
from .connection import Connection as Conn

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
class Trigger(App):
    def __init__(# NETWORK_ENDPOINTS: list,
            NUMBER_OF_RAWDATA_PRODUCERS: int = 2,
            NUMBER_OF_TPSET_PRODUCERS: int = 2,
            
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
            HOST: str = "localhost"):

        # Generate schema for the maker plugins on the fly in the temptypes module
        make_moo_record(ACTIVITY_CONFIG ,'ActivityConf' ,'temptypes')
        make_moo_record(CANDIDATE_CONFIG,'CandidateConf','temptypes')
        import temptypes

        modules = {}

        for idx in range(NUMBER_OF_TPSET_PRODUCERS):
            modules[f"buf{idx}"] = Module(plugin="TPSetBufferCreator",
                                          conf=buf.Conf(tpset_buffer_size=10000, region=0, element=idx),
                                          connections={})
        if NUMBER_OF_TPSET_PRODUCERS>0:
            print(f"NUMBER_OF_TPSET_PRODUCERS {NUMBER_OF_TPSET_PRODUCERS}")
            modules["zip"] = Module(plugin="TPZipper",
                                    connections={"output": Conn("tam.input")},
                                    conf=tzip.ConfParams(cardinality=NUMBER_OF_TPSET_PRODUCERS,
                                                         max_latency_ms=1000,
                                                         region_id=0,
                                                         element_id=0))
    
            modules["tam"] = Module(plugin="TriggerActivityMaker",
                                    connections={"output": Conn("tcm.input")},
                                    conf=tam.Conf(activity_maker=ACTIVITY_PLUGIN,
                                                  geoid_region=0,  # Fake placeholder
                                                  geoid_element=0,  # Fake placeholder
                                                  window_time=10000,  # should match whatever makes TPSets, in principle
                                                  buffer_time=625000,  # 10ms in 62.5 MHz ticks
                                                  activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)))
    
            modules["tcm"] = Module(plugin="TriggerCandidateMaker",
                                    connections={"output": Conn("mlt.trigger_candidate_source")},
                                    conf=tcm.Conf(candidate_maker=CANDIDATE_PLUGIN,
                                                  candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG)))
    
    
            modules["ttcm"] = Module(plugin="TimingTriggerCandidateMaker",
                                     conf=ttcm.Conf(s1=ttcm.map_t(signal_type=TTCM_S1,
                                                                  time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                                  time_after=TRIGGER_WINDOW_AFTER_TICKS),
                                                    s2=ttcm.map_t(signal_type=TTCM_S2,
                                                                  time_before=TRIGGER_WINDOW_BEFORE_TICKS,
                                                                  time_after=TRIGGER_WINDOW_AFTER_TICKS)
                                                    ),
                                     connections={"output": Conn("mlt.trigger_candidate_source")}
                                     )
    
        # The full list of MLT links is the raw data from upstream DAQ _and_ the raw TPs from upstream DAQ
        # all_mlt_links = [ mlt.GeoID(system=SYSTEM_TYPE, region=0, element=idx)
        #                   for idx in range(NUMBER_OF_RAWDATA_PRODUCERS + NUMBER_OF_TPSET_PRODUCERS) ]
    
        # all_mlt_links += [ mlt.GeoID(system="DataSelection", region=0, element=idx)
        #                    for idx in range(NUMBER_OF_TPSET_PRODUCERS) ]
    
        # We need to populate the list of links based on the fragment
        # producers available in the system. This is a bit of a
        # chicken-and-egg problem, because the trigger app itself creates
        # fragment producers (see below). Eventually when the MLT is its
        # own process, this problem will probably go away, but for now, we
        # leave the list of links here blank, and replace it in
        # util.connect_fragment_producers
        modules["mlt"] = Module(plugin="ModuleLevelTrigger",
                                conf=mlt.ConfParams(links=[], # To be updated later - see comment above
                                                    initial_token_count=TOKEN_COUNT),
                                connections={})
    
        mgraph = ModuleGraph(modules)
    
        mgraph.add_endpoint("hsievents_in",  "ttcm.input", Direction.IN)
    
        for idx in range(NUMBER_OF_TPSET_PRODUCERS):
            mgraph.add_endpoint(f"tpsets_into_buffer_link{idx}", f"buf{idx}.tpset_source", Direction.IN)
            mgraph.add_endpoint(f"tpsets_into_chain_link{idx}",   "zip.input",             Direction.IN)
    
            mgraph.add_fragment_producer(region=0, element=idx, system="DataSelection",
                                         requests_in=f"buf{idx}.data_request_source",
                                         fragments_out=f"buf{idx}.fragment_sink")
    
    
        mgraph.add_endpoint("trigger_decisions", "mlt.trigger_decision_sink", Direction.OUT)
        mgraph.add_endpoint("tokens", "mlt.token_source", Direction.IN)
    
        self.super().__init(modulegraph=mgraph, host=HOST)

    def finalise(self, system):
        self.connect_all_fragment_producers(system)
        self.set_mlt_links(system)
    

    def set_mlt_links(self, system):
        """
        The MLT needs to know the full list of fragment producers in the
        system so it can populate the TriggerDecisions it creates. This
        function gets all the fragment producers in the system and adds their
        GeoIDs to the MLT's config. It assumes that the ModuleLevelTrigger
        lives in an application with name `mlt_app_name` and has the name
        "mlt".
        """
        mlt_links = []
        for producer in system.get_fragment_producers():
            geoid = producer.geoid
            mlt_links.append(mlt.GeoID(system=geoid.system, region=geoid.region, element=geoid.element) )

        # Now we add the full set of links to the MLT plugin conf. It
        # would be nice to just modify the `links` attribute of the
        # mlt.ConfParams object, but moo-derived objects work in a funny
        # way (returning a copy of the attribute, not returning a
        # reference to it), which means we have to copy and replace the
        # whole thing
        if self.verbose:
            self.console.log(f"Adding {len(mlt_links)} links to mlt.links: {mlt_links}")
        old_mlt = deepcopy(self.apps["trigger"].modulegraph.modules["mlt"])
        # from .module import Module # ARG!
        self.modulegraph.modules["mlt"] = Module(plugin=old_mlt.plugin,
                                                 conf=mlt.ConfParams(links=mlt_links,
                                                                     initial_token_count=old_mlt.conf.initial_token_count),
                                                 connections=old_mlt.connections)

    def connect_fragment_producers(self, system, app_name):
        """
        Connect the data request and fragment sending queues from all of
        the fragment producers in the app with name `app_name` to the
        appropriate endpoints of the dataflow app.
        """
        if self.verbose:
            self.console.log(f"Connecting fragment producers in {app_name}")

        app = system.apps[app_name]
        producers = app.modulegraph.fragment_producers

        for producer in producers.values():
            request_endpoint = producer.data_request_endpoint_name()
            if self.verbose:
                self.console.log(f"Creating request endpoint {request_endpoint}")

            app.modulegraph.add_endpoint(request_endpoint, producer.requests_in, Direction.IN)
            system.app_connections[f"dataflow.{request_endpoint}"] = Sender(msg_type="dunedaq::dfmessages::DataRequest",
                                                                            msg_module_name="DataRequestNQ",
                                                                            receiver=f"{app_name}.{request_endpoint}")

            frag_endpoint = f"fragments_{producer.geoid.raw_str()}"
            if self.verbose:
                self.console.log(f"Creating fragment endpoint {frag_endpoint}")
            app.modulegraph.add_endpoint(frag_endpoint, producer.fragments_out, Direction.OUT)

            system.app_connections[f"{app_name}.{frag_endpoint}"] = Sender(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                                                           msg_module_name="FragmentNQ",
                                                                           receiver=f"dataflow.fragments")


    def connect_all_fragment_producers(self, system, dataflow_name="dataflow"):
        """
        Connect all fragment producers in the system to the appropriate
        queues in the dataflow app.
        """
        for name, app in system.apps.items():
            if name==dataflow_name:
                continue
            self.connect_fragment_producers(system, name)

