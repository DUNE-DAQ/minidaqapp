from collections import namedtuple
from enum import Enum
from .util import make_unique_name


Publisher = namedtuple("Publisher", ['msg_type', 'msg_module_name', 'subscribers'])
Sender = namedtuple("Sender", ['msg_type', 'msg_module_name', 'receiver'])
Connection = namedtuple("Connection", ['to', 'queue_kind', 'queue_capacity', 'queue_name', 'toposort'],
                        defaults=("FollyMPMCQueue", 1000, None, True))

class Direction(Enum):
    IN = 1
    OUT = 2

Endpoint = namedtuple("Endpoint", [ 'external_name', 'internal_name', 'direction' ])

def resolve_endpoint(app, external_name, inout, verbose=False):
    """Resolve an `external` endpoint name to the corresponding internal "module.sinksource"
    """
    if external_name in app.modulegraph.endpoints:
        e=app.modulegraph.endpoints[external_name]
        if e.direction==inout:
            if verbose:
                console.log(f"Endpoint {external_name} resolves to {e.internal_name}")
            return e.internal_name
        else:
            raise KeyError(f"Endpoint {external_name} has direction {e.direction}, but requested direction was {inout}")
    else:
        raise KeyError(f"Endpoint {external_name} not found")


def assign_network_endpoints(the_system, verbose=False):
    """Given a set of applications and connections between them, come up
    with a list of suitable zeromq endpoints. Return value is a mapping
    from name of upstream end of connection to endpoint name.

    Algorithm is to make an endpoint name like tcp://host:port, where
    host is the hostname for the app at the upstream end of the
    connection, port starts at some fixed value, and increases by 1
    for each new endpoint.

    You might think that we could reuse port numbers for different
    apps, but that's not possible since multiple applications may run
    on the same host, and we don't know the _actual_ host here, just,
    eg "{host_dataflow}", which is later interpreted by nanorc.

    """
    endpoints = {}
    #host_ports = defaultdict(int)
    port = 12345
    for conn in the_system.app_connections.keys():
        app = conn.split(".")[0]
        #host = the_system.apps[app].host
        # if host == "localhost":
        #     host = "127.0.0.1"
        #port = first_port + host_ports[host]
        #host_ports[host] += 1
        endpoints[conn] = f"tcp://{{host_{app}}}:{port}"
        if verbose:
            console.log(f"Assigned endpoint {endpoints[conn]} for connection {conn}")
        port+=1
    return endpoints
