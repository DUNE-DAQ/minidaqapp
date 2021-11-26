from .connection import *
from .app import App
import dunedaq.trigger.moduleleveltrigger as mlt
from copy import deepcopy
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networktoqueue as ntoq

class System:
    """A full DAQ system consisting of multiple applications and the
    connections between them. The `apps` member is a dictionary from
    application name to app object, and the app_connections member is
    a dictionary from upstream endpoint to publisher or sender object
    representing the downstream endpoint(s). Endpoints are specified
    as strings like app_name.endpoint_name.

    An explicit mapping from upstream endpoint name to zeromq
    connection string may be specified, but typical usage is to not
    specify this, and leave the mapping to be automatically generated.

    The same is true for application start order.

    """
    def __init__(self, apps=None, app_connections=None, network_endpoints=None, app_start_order=None, console=None, verbose=True):
        self.apps=apps if apps else dict()
        self.app_connections=app_connections if app_connections else dict()
        self.network_endpoints=network_endpoints
        self.app_start_order=app_start_order
        self.console = console
        self.verbose = verbose

    def __rich_repr__(self):
        yield "apps", self.apps
        yield "app_connections", self.app_connections
        yield "network_endpoints", self.network_endpoints
        yield "app_start_order", self.app_start_order

    def get_fragment_producers(self):
        """Get a list of all the fragment producers in the system"""
        all_producers = []
        all_geoids = set()
        for app in self.apps.values():
            self.console.log(app)
            producers = app.modulegraph.fragment_producers
            for producer in producers.values():
                if producer.geoid in all_geoids:
                    raise ValueError(f"GeoID {producer.geoid} has multiple fragment producers")
                all_geoids.add(producer.geoid)
                all_producers.append(producer)
        return all_producers


    def set_mlt_links(self, mlt_app_name="trigger"):
        """
        The MLT needs to know the full list of fragment producers in the
        system so it can populate the TriggerDecisions it creates. This
        function gets all the fragment producers in the system and adds their
        GeoIDs to the MLT's config. It assumes that the ModuleLevelTrigger
        lives in an application with name `mlt_app_name` and has the name
        "mlt".
        """
        mlt_links = []
        for producer in self.get_fragment_producers():
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
        from .module import Module # ARG!
        self.apps[mlt_app_name].modulegraph.modules["mlt"] = Module(plugin=old_mlt.plugin,
                                                                    conf=mlt.ConfParams(links=mlt_links,
                                                                                        initial_token_count=old_mlt.conf.initial_token_count),
                                                                    connections=old_mlt.connections)

    def connect_fragment_producers(self, app_name):
        """
        Connect the data request and fragment sending queues from all of
        the fragment producers in the app with name `app_name` to the
        appropriate endpoints of the dataflow app.
        """
        if self.verbose:
            self.console.log(f"Connecting fragment producers in {app_name}")

        app = self.apps[app_name]
        producers = app.modulegraph.fragment_producers

        for producer in producers.values():
            request_endpoint = producer.data_request_endpoint_name()
            if self.verbose:
                self.console.log(f"Creating request endpoint {request_endpoint}")

            app.modulegraph.add_endpoint(request_endpoint, producer.requests_in, Direction.IN)
            self.app_connections[f"dataflow.{request_endpoint}"] = Sender(msg_type="dunedaq::dfmessages::DataRequest",
                                                                          msg_module_name="DataRequestNQ",
                                                                          receiver=f"{app_name}.{request_endpoint}")

            frag_endpoint = f"fragments_{producer.geoid.raw_str()}"
            if self.verbose:
                self.console.log(f"Creating fragment endpoint {frag_endpoint}")
            app.modulegraph.add_endpoint(frag_endpoint, producer.fragments_out, Direction.OUT)

            self.app_connections[f"{app_name}.{frag_endpoint}"] = Sender(msg_type="std::unique_ptr<dunedaq::daqdataformats::Fragment>",
                                                                         msg_module_name="FragmentNQ",
                                                                         receiver=f"dataflow.fragments")


    def connect_all_fragment_producers(self, dataflow_name="dataflow"):
        """
        Connect all fragment producers in the system to the appropriate
        queues in the dataflow app.
        """
        for name, app in self.apps.items():
            if name==dataflow_name:
                continue
            self.connect_fragment_producers(name)

    def add_network(self, app_name):
        """
        Add the necessary QueueToNetwork and NetworkToQueue objects to the
        application named `app_name`, based on the inter-application
        connections specified in `the_system`. NB `the_system` is modified
        in-place.
        """

        if self.network_endpoints is None:
            self.network_endpoints=assign_network_endpoints(self)

        app = self.apps[app_name]

        modules_with_network = deepcopy(app.modulegraph.modules)

        unconnected_endpoints = set(app.modulegraph.endpoints.keys())

        if self.verbose:
            self.console.log(f"Endpoints to connect are: {unconnected_endpoints}")

        for conn_name, conn in self.app_connections.items():
            from_app, from_endpoint = conn_name.split(".", maxsplit=1)

            if from_app == app_name:
                unconnected_endpoints.remove(from_endpoint)
                from_endpoint = resolve_endpoint(app, from_endpoint, Direction.OUT)
                from_endpoint_module, from_endpoint_sink = from_endpoint.split(".")
                # We're a publisher or sender. Make the queue to network
                qton_name = conn_name.replace(".", "_")
                qton_name = make_unique_name(qton_name, modules_with_network)
                
                if self.verbose:
                    self.console.log(f"Adding QueueToNetwork named {qton_name} connected to {from_endpoint} in app {app_name}")

                from .module import Module
                modules_with_network[qton_name] = Module(plugin="QueueToNetwork",
                                                         connections={}, # No outgoing connections
                                                         conf=qton.Conf(msg_type=conn.msg_type,
                                                                        msg_module_name=conn.msg_module_name,
                                                                        sender_config=nos.Conf(ipm_plugin_type="ZmqPublisher" if type(conn) == Publisher else "ZmqSender",
                                                                                               address=self.network_endpoints[conn_name],
                                                                                               topic="foo",
                                                                                               stype="msgpack")))
                # Connect the module to the QueueToNetwork
                mod_connections = modules_with_network[from_endpoint_module].connections
                mod_connections[from_endpoint_sink] = Connection(f"{qton_name}.input")

            if hasattr(conn, "subscribers"):
                for to_conn in conn.subscribers:
                    to_app, to_endpoint = to_conn.split(".", maxsplit=1)

                    if app_name == to_app:
                        if self.verbose:
                            self.console.log(f"App {app_name} endpoint {to_endpoint} is being connected")

                        # For pub/sub connections, we might connect
                        # multiple times to the same endpoint, so it might
                        # already have been removed from the list
                        if to_endpoint in unconnected_endpoints:
                            unconnected_endpoints.remove(to_endpoint)
                        to_endpoint = resolve_endpoint(app, to_endpoint, Direction.IN)
                        ntoq_name = to_conn.replace(".", "_")
                        ntoq_name = make_unique_name(ntoq_name, modules_with_network)

                        if self.verbose:
                            self.console.log(f"Adding NetworkToQueue named {ntoq_name} connected to {to_endpoint} in app {app_name}")

                        from .module import Module
                        modules_with_network[ntoq_name] = Module(plugin="NetworkToQueue",
                                                                 connections={"output": Connection(to_endpoint)},
                                                                 conf=ntoq.Conf(msg_type=conn.msg_type,
                                                                                msg_module_name=conn.msg_module_name,
                                                                                receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                                                                                         address=self.network_endpoints[conn_name],subscriptions=["foo"])))

            if hasattr(conn, "receiver") and app_name == conn.receiver.split(".")[0]:
                # We're a receiver. Add a NetworkToQueue of receiver type
                #
                # TODO: DRY
                to_app, to_endpoint = conn.receiver.split(".", maxsplit=1)
                if to_endpoint in unconnected_endpoints:
                    unconnected_endpoints.remove(to_endpoint)
                to_endpoint = resolve_endpoint(app, to_endpoint, Direction.IN)

                ntoq_name = conn.receiver.replace(".", "_")
                ntoq_name = make_unique_name(ntoq_name, modules_with_network)
                
                if self.verbose:
                    self.console.log(f"Adding NetworkToQueue named {ntoq_name} connected to {to_endpoint} in app {app_name}")
                    from .module import Module
                    modules_with_network[ntoq_name] = Module(plugin="NetworkToQueue",
                                                             connections={"output": Connection(to_endpoint)},
                                                             conf=ntoq.Conf(msg_type=conn.msg_type,
                                                                            msg_module_name=conn.msg_module_name,
                                                                            receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                                                     address=self.network_endpoints[conn_name])))

        if unconnected_endpoints:
            # TODO: Use proper logging
            self.console.log(f"Warning: the following endpoints of {app_name} were not connected to anything: {unconnected_endpoints}")

        app.modulegraph.modules = modules_with_network
