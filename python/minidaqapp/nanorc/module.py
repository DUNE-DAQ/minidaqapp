from .connection import *

class GeoID:
    def __init__(self, system, region, element):
        self.system = system
        self.region = region
        self.element = element
        
    def raw_str(self):
        """Get a string representation of a GeoID suitable for using in queue names"""
        return f"geoid{self.system}_{self.region}_{self.element}"

class FragmentProducer:
    def __init__(self, geoid, requests_in, fragments_out, queue_name):
        self.geoid = geoid
        self.requests_in = requests_in
        self.fragments_out = fragments_out
        self.queue_name = queue_name
        
    def data_request_endpoint_name(self):
        return f"data_request_{self.geoid.raw_str()}"
    
class Module:
    """An individual DAQModule within an application, along with its
       configuration object and list of outgoing connections to other
       modules
    """

    def __init__(self, plugin, conf=None, connections=None):
        self.plugin=plugin
        self.conf=conf
        self.connections=connections if connections else dict()

    def __repr__(self):
        return f"module(plugin={self.plugin}, conf={self.conf}, connections={self.connections})"

    def __rich_repr__(self):
        yield "plugin", self.plugin
        yield "conf", self.conf
        yield "connections", self.connections




class ModuleGraph:
    """A set of modules and connections between them.

    modulegraph holds a dictionary of modules, with each module
    knowing its (outgoing) connections to other modules in the
    modulegraph.

    Connections to other modulegraphs are represented by
    `endpoints`. The endpoint's `external_name` is the "public" name
    of the connection, which other modulegraphs should use. The
    endpoint's `internal_name` specifies the particular module and
    sink/source name which the endpoint is connected to, which may be
    changed without affecting other applications.

    """
    def __init__(self, modules=None, endpoints=None, fragment_producers=None):
        self.modules=modules if modules else dict()
        self.endpoints=endpoints if endpoints else dict()
        self.fragment_producers = fragment_producers if  fragment_producers else dict()

    def __repr__(self):
        return f"modulegraph(modules={self.modules}, endpoints={self.endpoints}, fragment_producers={self.fragment_producers})"

    def __rich_repr__(self):
        yield "modules", self.modules
        yield "endpoints", self.endpoints
        yield "fragment_producers", self.fragment_producers

    def set_from_dict(self, module_dict):
        self.modules=module_dict

    def module_names(self):
        return self.modules.keys()

    def module_list(self):
        return self.modules.values()

    def add_module(self, name, **kwargs):
        mod=module(**kwargs)
        self.modules[name]=mod
        return mod

    def add_connection(self, from_endpoint, to_endpoint):
        from_mod, from_name=from_endpoint.split(".")
        self.modules[from_mod].connections[from_name]=Connection(to_endpoint)

    def add_endpoint(self, external_name, internal_name, inout):
        self.endpoints[external_name]=Endpoint(external_name, internal_name, inout)

    def endpoint_names(self, inout=None):
        if inout is not None:
            return [ e[0] for e in self.endpoints.items() if e[1].inout==inout ]
        return self.endpoints.keys()

    def add_fragment_producer(self, system, region, element, requests_in, fragments_out):
        geoid = GeoID(system, region, element)
        if geoid in self.fragment_producers:
            raise ValueError(f"There is already a fragment producer for GeoID {geoid}")
        # Can't set queue_name here, because the queue names need to be unique system-wide, but we're inside a particular app here. Instead, we create the queue names in readout_gen.generate, where all of the fragment producers are known
        queue_name = None
        self.fragment_producers[geoid] = FragmentProducer(geoid, requests_in, fragments_out, queue_name)
