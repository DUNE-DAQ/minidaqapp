# Python configuration generation utilities

This directory contains utilities to help with generating DAQ system
configurations. The aim is to make the part specified by the user as
minimal and straightforward as possible, and to automatically generate
parts that can be inferred from the user's specification.

## Structure

We build up a DAQ `system` from applications and the connections between them. Applications, in turn, are specified by the modules they contain, the input and output "endpoints", and the hosts on which they run.


### Modules

Starting at the bottom, a `DAQModule` is described by the `util.Module` class, which has a plugin name, a suitable configuration object, and a set of _outgoing_ connections to other modules. For example:

```python
# Load moo type for configuration
moo.otypes.load_types('trigger/triggerprimitivemaker.jsonnet')
import dunedaq.trigger.triggerprimitivemaker as tpm

import util
from util import Connection as Conn

tpm_module = util.Module(plugin="TriggerPrimitiveMaker",
                         conf=tpm.ConfParams(number_of_loops=-1,
                                             tpset_time_offset=0),
                         connections={"tpset_sink": Conn("ftpchm.tpset_source")})

```

This creates a `DAQModule` using the `TriggerPrimitiveMaker` plugin, whose configuration is the given `tpm.ConfParams` object. The `connections` argument creates a connection from the `tpset_sink` `DAQSink` in this module to the `tpset_source` `DAQSource` in another module named `ftpchm`. When we generate the full application configuration, the tools in this package will automatically create the necessary queue objects and settings to connect this `DAQSink`/`DAQSource` pair.

Modules are grouped together in a `ModuleGraph`, which holds a dictionary mapping module names to Module objects, along with a dictionary of "endpoints" which are the "public" names for the `ModuleGraph`'s inputs and outputs. Having a list of endpoints allows other applications to make connections to this application without having to know about the internal details of modules and sink/source names. Extending our example from above:

```python
# ... imports, etc, omitted

modules = {}

modules["tpm"] = util.Module(plugin="TriggerPrimitiveMaker",
                             conf=tpm.ConfParams(number_of_loops=-1,
                                                 tpset_time_offset=0),
                             connections={"tpset_sink": Conn("ftpchm.tpset_source")})

modules["ftpchm"] = util.Module(plugin="FakeTPCreatorHeartbeatMaker",
                                # No outgoing connections specified here
                                conf=ftpchm.Conf(heartbeat_interval=50000))


the_modulegraph = ModuleGraph(modules)
# Create an outgoing public endpoint named "tpsets_out", which refers to the "tpset_sink" DAQSink in the "ftpchm" module
the_modulegraph.add_endpoint("tpsets_out", "ftpchm.tpset_sink", util.Direction.OUT)
```

### Applications

An application (represented by the `util.App` class), represents an instance of a `daq_application` running on a particular host. It consists of a `ModuleGraph` and a hostname on which to run. We collect apps into a dicionary keyed on application name, like we did with modules:

```python
apps = { "myapp": util.App(modulegraph=the_modulegraph, host="localhost") }
```

Connections between applications are specified in a dictionary whose keys are the upstream endpoints, and whose values are objects specifying details of the connection and the downstream endpoint(s) to be connected. For example:

```python
app_connections = {
    "myapp.tpsets_out": util.Publisher(msg_type="dunedaq::trigger::TPSet",
                                       msg_module_name="TPSetNQ",
                                       subscribers=["tpset_consumer1.tpsets_in",
                                                    "tpset_consumer2.tpsets_in"])
}
```

This creates a pub/sub connection from the `tpsets_out` endpoint of `myapp` to the `tpsets_in` endpoints of the `tpset_consumer1` and `tpset_consumer2` applications. The details of assigning ports for the connections and creating `QueueToNetwork`/`NetworkToQueue` modules inside the applications are handled automatically (but can also be specified manually, if necessary).

### System

The `util.System` class groups applications and their connections together in a single object:

```python
the_system = util.System(apps, app_connections)
```

A `util.System` object contains all of the information needed to generate a full set of JSON files that can be read by `nanorc`.

### Fragment producers

A system typically includes a number of entities that produce DAQ `Fragment`s in response to data requests. Correctly configuring the system for multiple entities like this can be tricky. The `util` framework aims to simplify this with the concept of `FragmentProducers`. A `FragmentProducer` represents one entity (usually a single `DAQModule` instance) that provides `Fragments` for a given `GeoID` on some output queue, in response to data requests on an input queue. Examples are the module in readout which provides raw TPC/PDS data in response to data requests, and the `TPSetBufferCreator` module in the trigger which provides trigger primitives in a trigger window.

To identify a `FragmentProducer` to the system, use the `ModuleGraph.add_fragment_producer()` function, like so:

```python
# ...
mgraph = ModuleGraph(modules)

mgraph.add_fragment_producer(region=0, element=idx, system="DataSelection",
                             requests_in=f"buf.data_request_source",
                             fragments_out=f"buf.fragment_sink")

```

The `region`, `element` and `system` arguments specify the `GeoID` for
which the producer is creating fragments. The `requests_in` argument
specifies the module and `DAQSource` that receive data requests, while
the `fragments_out` argument specifies the module and `DAQSink` on
which `Fragment`s are sent in response.

The aim of this approach is that developers of applications containing
fragment producers (currently readout and trigger) only have to make
changes to their own app, and do not have to make corresponding
changes to dataflow or elsewhere. Unfortunately the steps needed to
connect everything up consistently are not very elegant, although that
may improve with the introduction of `NetworkManager`.

This approach requires that the dataflow application is generated
after all of the other applications (strictly, after all of the other
applications _that have fragment producers_), since the dataflow
generation step needs to know the list of fragment producers.

Once all of the applications have been created and placed into a
`System` object, the fragment producers must be connected to the
dataflow application using the `util.connect_all_fragment_producers()`
function.

The Module Level Trigger as currently written also needs to know the list
of all possible GeoIDs to put in its `TriggerDecisions`. Doing this is
handled by the `util.set_mlt_links()` function.

Putting this all together, we get:

```python

# generate all other applications, populate System object `the_system` then...

mgraph_dataflow = dataflow_gen.generate(
    FRAGMENT_PRODUCERS = the_system.get_fragment_producers(),
    # other dataflow_gen arguments... )

util.connect_all_fragment_producers(the_system)
util.set_mlt_links(the_system, mlt_app_name="trigger")

# then continue with `add_network()` on each app, followed by app command data generation
```

## Generating JSON files

To get from a `util.System` object to a full set of JSON files involves four steps:

1. Add networking modules (ie, `NetworkToQueue`/`QueueToNetwork`) to applications
2. For each application, create the python data structures for each DAQ command that the application will respond to
3. Create the python data structures for each DAQ command that the _system_ will respond to
4. Convert the python data structures to JSON and dump to the appropriate files

As part of step 1, ports for all of the inter-application connections are assigned and saved in the `util.Dystem` object, and applications' lists of modules are updated if necessary to add N2Q/Q2N modules.

As part of step 2, the necessary queue objects are created to apply the connections between modules, and the order to send start and stop commands to the modules is inferred based on the inter-module connections.

As part of step 3, the order in which to send start and stop commands to applications is inferred from their connections.

For convenience, these four steps are wrapped by the `make_apps_json(the_system, json_dir, verbose=False)` function. If you need finer-grained control, or for debugging intermediate steps, you may wish to run each of the steps manually:

```python
app_command_datas = dict()

for app_name, app in the_system.apps.items():
    # Step 1
    add_network(app_name, the_system)
    # Step 2
    app_command_datas[app_name] = make_app_command_data(app)

# Step 3
system_command_datas=make_system_command_datas(the_system)

# Step 4
write_json_files(app_command_datas, system_command_datas, json_dir)
```

## Overriding defaults

Where possible, the functions in `util` try to provide sensible defaults, so that the minimum amount of information has to be provided in the common case. But there are cases where you may need to override the defaults. Some examples:

* The `util.Connection` class constructor takes optional `queue_type`, `queue_capacity` and `toposort` arguments. Passing `toposort=True` removes this connection from the topological sort that is used to determine the order in which start and stop commands are sent. This is useful for breaking cycles in the connection graph.
* The `util.System` class constructor takes optional `network_endpoints` and `app_start_order` arguments that can be used to override the automatically-assigned set of ports and application start order respectively.

## Complete examples

Some complete examples can be found in subdirectories of this one:

* [faketp_to_sink](faketp_to_sink/) demonstrates a single-application system
* [tpset_pub_sub](tpset_pub_sub/) demonstrates how to connect multiple applications together
