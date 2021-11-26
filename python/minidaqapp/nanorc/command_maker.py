import matplotlib.pyplot as plt
import networkx as nx
from collections import defaultdict

from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()
import moo.otypes

moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

import dunedaq.appfwk.app as appfwk  # AddressedCmd,
import dunedaq.rcif.cmd as rccmd  # AddressedCmd,
from appfwk.utils import acmd, mcmd, mspec


cmd_set = ["init", "conf", "start", "stop", "pause", "resume", "scrap"]

class CommandMaker:
    def __init__(self, verbose=True, console=None):
        self.verbose = verbose
        self.console = console

    def make_module_deps(self, module_dict):
        """Given a dictionary of `module` objects, produce a dictionary giving
        the dependencies between them. A dependency is any connection between
        modules (implemented using an appfwk queue). Connections whose
        upstream ends begin with a '!' are not considered dependencies, to
        allow us to break cycles in the DAG.
    
        Returns a networkx DiGraph object where nodes are module names
    
        """
    
        deps = nx.DiGraph()
        for mod_name in module_dict.keys():
            deps.add_node(mod_name)
        for name, mod in module_dict.items():
            for upstream_name, downstream_connection in mod.connections.items():
                if downstream_connection.toposort:
                    other_mod = downstream_connection.to.split(".")[0]
                    deps.add_edge(name, other_mod)
        plt.clf()
        nx.draw(deps, with_labels=True, font_weight='bold')
        plt.savefig(list(module_dict.keys())[0]+'.png')
        return deps
    
    
    def make_app_deps(self, the_system):
        """
        Produce a dictionary giving
        the dependencies between a set of applications, given their connections.
    
        Returns a networkx DiGraph object where nodes are app names
    
        """
    
        deps = nx.DiGraph()
    
        for app in the_system.apps.keys():
            deps.add_node(app)
    
        for from_endpoint, conn in the_system.app_connections.items():
            from_app = from_endpoint.split(".")[0]
            if hasattr(conn, "subscribers"):
                for to_app in [ds.split(".")[0] for ds in conn.subscribers]:
                    deps.add_edge(from_app, to_app)
            elif hasattr(conn, "receiver"):
                to_app = conn.receiver.split(".")[0]
                deps.add_edge(from_app, to_app)
    
        plt.clf()
        nx.draw(deps, with_labels=True, font_weight='bold')
        plt.savefig(list(the_system_dict.keys())[0]+'.png')
        return deps

    def generate_boot(self, apps: list, partition_name="${USER}_test",
                      ers_settings=None, info_svc_uri="file://info_${APP_ID}_${APP_PORT}.json",
                      disable_trace=False, use_kafka=False) -> dict:
        """Generate the dictionary that will become the boot.json file"""
    
        if ers_settings is None:
            ers_settings={
                "INFO":    "erstrace,throttle,lstdout",
                "WARNING": "erstrace,throttle,lstdout",
                "ERROR":   "erstrace,throttle,lstdout",
                "FATAL":   "erstrace,lstdout",
            }
    
        daq_app_specs = {
            "daq_application_ups" : {
                "comment": "Application profile based on a full dbt runtime environment",
                "env": {
                    "DBT_AREA_ROOT": "getenv",
                    "TRACE_FILE": "getenv:/tmp/trace_buffer_${HOSTNAME}_${USER}",
                },
                "cmd": ["CMD_FAC=rest://localhost:${APP_PORT}",
                        "INFO_SVC=" + info_svc_uri,
                        "cd ${DBT_AREA_ROOT}",
                        "source dbt-env.sh",
                        "dbt-workarea-env",
                        "cd ${APP_WD}",
                        "daq_application --name ${APP_NAME} -c ${CMD_FAC} -i ${INFO_SVC}"]
            },
            "daq_application" : {
                "comment": "Application profile using  PATH variables (lower start time)",
                "env":{
                    "CET_PLUGIN_PATH": "getenv",
                    "DUNEDAQ_SHARE_PATH": "getenv",
                    "TIMING_SHARE": "getenv",
                    "LD_LIBRARY_PATH": "getenv",
                    "PATH": "getenv",
                    "READOUT_SHARE": "getenv",
                    "TRACE_FILE": "getenv:/tmp/trace_buffer_${HOSTNAME}_${USER}",
                },
                "cmd": ["CMD_FAC=rest://localhost:${APP_PORT}",
                        "INFO_SVC=" + info_svc_uri,
                        "cd ${APP_WD}",
                        "daq_application --name ${APP_NAME} -c ${CMD_FAC} -i ${INFO_SVC}"]
            }
        }
    
        first_port = 3333
        ports = {}
        for i, name in enumerate(apps.keys()):
            ports[name] = first_port + i
    
        boot = {
            "env": {
                "DUNEDAQ_ERS_VERBOSITY_LEVEL": "getenv:1",
                "DUNEDAQ_PARTITION": partition_name,
                "DUNEDAQ_ERS_INFO": ers_settings["INFO"],
                "DUNEDAQ_ERS_WARNING": ers_settings["WARNING"],
                "DUNEDAQ_ERS_ERROR": ers_settings["ERROR"],
                "DUNEDAQ_ERS_FATAL": ers_settings["FATAL"],
                "DUNEDAQ_ERS_DEBUG_LEVEL": "getenv:-1",
            },
            "apps": {
                name: {
                    "exec": "daq_application",
                    "host": f"host_{name}",
                    "port": ports[name]
                }
                for name, app in apps.items()
            },
            "hosts": {
                f"host_{name}": app.host
                for name, app in apps.items()
            },
            "response_listener": {
                "port": 56789
            },
            "exec": daq_app_specs
        }
    
        if disable_trace:
            del boot["exec"]["daq_application"]["env"]["TRACE_FILE"]
            del boot["exec"]["daq_application_ups"]["env"]["TRACE_FILE"]
    
        if use_kafka:
            boot["env"]["DUNEDAQ_ERS_STREAM_LIBS"] = "erskafka"
    
        if self.verbose:
            self.console.log("Boot data")
            self.console.log(boot)
    
        return boot

    def make_app_command_data(self, app):
        """Given an App instance, create the 'command data' suitable for
        feeding to nanorc. The needed queues are inferred from from
        connections between modules, as are the start and stop order of the
        modules
    
        TODO: This should probably be split up into separate stages of
        inferring/creating the queues (which can be part of validation)
        and actually making the command data objects for nanorc.
    
        """
    
        
        modules = app.modulegraph.modules
    
        module_deps = self.make_module_deps(modules)
        if self.verbose:
            self.console.log(f"inter-module dependencies are: {module_deps}")
    
        start_order = list(nx.algorithms.dag.topological_sort(module_deps))
        stop_order = start_order[::-1]
    
        if self.verbose:
            self.console.log(f"Inferred module start order is {start_order}")
            self.console.log(f"Inferred module stop order is {stop_order}")
    
        command_data = {}
    
        queue_specs = []
    
        app_qinfos = defaultdict(list)
    
        # Infer the queues we need based on the connections between modules
    
        # Terminology: an "endpoint" is "module.name"
        for name, mod in modules.items():
            for from_name, downstream_connection in mod.connections.items():
                # The name might be prefixed with a "!" to indicate that it doesn't participate in dependencies. Remove that here because "!" is illegal in actual queue names
                from_name = from_name.replace("!", "")
                from_endpoint = ".".join([name, from_name])
                to_endpoint=downstream_connection.to
                to_mod, to_name = to_endpoint.split(".")
                queue_inst = f"{from_endpoint}_to_{to_endpoint}".replace(".", "")
                # Is there already a queue connecting either endpoint? If so, we reuse it
    
                # TODO: This is a bit complicated. Might be nicer to find
                # the list of necessary queues in a first step, and then
                # actually make the QueueSpec/QueueInfo objects
                found_from = False
                found_to = False
                for k, v in app_qinfos.items():
                    for qi in v:
                        test_endpoint = ".".join([k, qi.name])
                        if test_endpoint == from_endpoint:
                            found_from = True
                            queue_inst = qi.inst
                        if test_endpoint == to_endpoint:
                            found_to = True
                            queue_inst = qi.inst
    
                if not (found_from or found_to):
                    queue_inst = queue_inst if downstream_connection.queue_name is None else downstream_connection.queue_name
                    if self.verbose:
                        self.console.log(f"Creating {downstream_connection.queue_kind}({downstream_connection.queue_capacity}) queue with name {queue_inst} connecting {from_endpoint} to {to_endpoint}")
                    queue_specs.append(appfwk.QueueSpec(
                        inst=queue_inst, kind=downstream_connection.queue_kind, capacity=downstream_connection.queue_capacity))
    
                if not found_from:
                    if self.verbose:
                        self.console.log(f"Adding output queue to module {name}: inst={queue_inst}, name={from_name}")
                    app_qinfos[name].append(appfwk.QueueInfo(
                        name=from_name, inst=queue_inst, dir="output"))
                if not found_to:
                    if self.verbose:
                        self.console.log(f"Adding input queue to module {to_mod}: inst={queue_inst}, name={to_name}")
                    app_qinfos[to_mod].append(appfwk.QueueInfo(
                        name=to_name, inst=queue_inst, dir="input"))
    
        mod_specs = [mspec(name, mod.plugin, app_qinfos[name])
                     for name, mod in modules.items()]
    
        # Fill in the "standard" command entries in the command_data structure
    
        command_data['init'] = appfwk.Init(queues=queue_specs, modules=mod_specs)
    
        # TODO: Conf ordering
        command_data['conf'] = acmd([
            (name, mod.conf) for name, mod in modules.items()
        ])
    
        startpars = rccmd.StartParams(run=1, disable_data_storage=False)
    
        command_data['start'] = acmd([(name, startpars) for name in start_order])
        command_data['stop'] = acmd([(name, None) for name in stop_order])
        command_data['scrap'] = acmd([(name, None) for name in stop_order])
    
        # Optional commands
    
        # TODO: What does an empty "acmd" actually imply? Does the command get sent to everyone, or no-one?
        command_data['pause'] = acmd([])
        command_data['resume'] = acmd([])
    
        # TODO: handle modules' `extra_commands`, including "record"
    
        return command_data

    def make_app_command_data_from_system(self, the_system):
        return {
            name : self.make_app_command_data(app)
            for name,app in the_system.apps.items()
        }


    def make_system_command_datas(self, the_system):
        """Generate the dictionary of commands and their data for the entire system"""
    
        if the_system.app_start_order is None:
            app_deps = make_app_deps(the_system)
            the_system.app_start_order = list(nx.algorithms.dag.topological_sort(app_deps))
    
        system_command_datas=dict()
    
        for c in cmd_set:
            self.console.log(f"Generating system {c} command")
            cfg = {
                "apps": {app_name: f'data/{app_name}_{c}' for app_name in the_system.apps.keys()}
            }
            if c == 'start':
                cfg['order'] = the_system.app_start_order
            elif c == 'stop':
                cfg['order'] = the_system.app_start_order[::-1]
    
            system_command_datas[c]=cfg
    
            if self.verbose:
                self.console.log(cfg)
    
        self.console.log(f"Generating boot json file")
        system_command_datas['boot'] = self.generate_boot(the_system.apps)
    
        return system_command_datas
