
# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')
moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectreceiver.jsonnet')
moo.otypes.load_types('nwqueueadapters/networkobjectsender.jsonnet')
moo.otypes.load_types('flxlibs/felixcardreader.jsonnet')
moo.otypes.load_types('readout/fakecardreader.jsonnet')
moo.otypes.load_types('readout/datalinkhandler.jsonnet')
moo.otypes.load_types('readout/datarecorder.jsonnet')
moo.otypes.load_types('dfmodules/fakedataprod.jsonnet')


# Import new types
import dunedaq.cmdlib.cmd as basecmd # AddressedCmd,
import dunedaq.rcif.cmd as rccmd # AddressedCmd,
import dunedaq.appfwk.cmd as cmd # AddressedCmd,
import dunedaq.appfwk.app as app # AddressedCmd,
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.readout.fakecardreader as fakecr
import dunedaq.flxlibs.felixcardreader as flxcr
import dunedaq.readout.datalinkhandler as dlh
import dunedaq.readout.datarecorder as dr
import dunedaq.dfmodules.fakedataprod as fdp

from appfwk.utils import acmd, mcmd, mrccmd, mspec

import json
import math
from pprint import pprint
# Time to wait on pop()
QUEUE_POP_WAIT_MS = 100
# local clock speed Hz
# CLOCK_SPEED_HZ = 50000000;

def generate(NETWORK_ENDPOINTS,
        NUMBER_OF_DATA_PRODUCERS=2,
        EMULATOR_MODE=False,
        DATA_RATE_SLOWDOWN_FACTOR=1,
        RUN_NUMBER=333, 
        DATA_FILE="./frames.bin",
        FLX_INPUT=True,
        CLOCK_SPEED_HZ=50000000,
        HOSTIDX=0, CARDID=0,
        RAW_RECORDING_ENABLED=False,
        RAW_RECORDING_OUTPUT_DIR=".",
        FRONTEND_TYPE='wib',
        SYSTEM_TYPE='TPC'):
    """Generate the json configuration for the readout and DF process"""

    cmd_data = {}

    required_eps = {f'timesync_{HOSTIDX}'}
    if not required_eps.issubset(NETWORK_ENDPOINTS):
        raise RuntimeError(f"ERROR: not all the required endpoints ({', '.join(required_eps)}) found in list of endpoints {' '.join(NETWORK_ENDPOINTS.keys())}")



    LATENCY_BUFFER_SIZE = 3 * CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR)
    RATE_KHZ = CLOCK_SPEED_HZ / (25 * 12 * DATA_RATE_SLOWDOWN_FACTOR * 1000)


    MIN_LINK = HOSTIDX*NUMBER_OF_DATA_PRODUCERS
    MAX_LINK = MIN_LINK + NUMBER_OF_DATA_PRODUCERS
    # Define modules and queues
    queue_bare_specs = [
            app.QueueSpec(inst="time_sync_q", kind='FollyMPMCQueue', capacity=100),
            app.QueueSpec(inst="data_fragments_q", kind='FollyMPMCQueue', capacity=1000),
        ] + [
            app.QueueSpec(inst=f"data_requests_{idx}", kind='FollySPSCQueue', capacity=100)
                for idx in range(MIN_LINK,MAX_LINK)
        ]

    if RAW_RECORDING_ENABLED:
        queue_bare_specs = queue_bare_specs + [
            app.QueueSpec(inst=f"{FRONTEND_TYPE}_recording_link_{idx}", kind='FollySPSCQueue', capacity=100000)
            for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    

    # Only needed to reproduce the same order as when using jsonnet
    queue_specs = app.QueueSpecs(sorted(queue_bare_specs, key=lambda x: x.inst))

    mod_specs = [
        mspec("qton_timesync", "QueueToNetwork", [app.QueueInfo(name="input", inst="time_sync_q", dir="input")]),
        mspec("qton_fragments", "QueueToNetwork", [app.QueueInfo(name="input", inst="data_fragments_q", dir="input")]),
    ] + [
        mspec(f"ntoq_datareq_{idx}", "NetworkToQueue", [app.QueueInfo(name="output", inst=f"data_requests_{idx}", dir="output")]) for idx in range(MIN_LINK,MAX_LINK)
    ]

    if RAW_RECORDING_ENABLED:
        mod_specs = mod_specs + [
            mspec(f"datahandler_{idx + MIN_LINK}", "DataLinkHandler", [
                app.QueueInfo(name="raw_input", inst=f"{FRONTEND_TYPE}_link_{idx}", dir="input"),
                app.QueueInfo(name="timesync", inst="time_sync_q", dir="output"),
                app.QueueInfo(name="requests", inst=f"data_requests_{idx + MIN_LINK}", dir="input"),
                app.QueueInfo(name="fragments", inst="data_fragments_q", dir="output"),
                app.QueueInfo(name="raw_recording", inst=f"{FRONTEND_TYPE}_recording_link_{idx}", dir="output")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ] + [
            mspec(f"data_recorder_{idx}", "DataRecorder", [
                app.QueueInfo(name="raw_recording", inst=f"{FRONTEND_TYPE}_recording_link_{idx}", dir="input")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]
    else:
        mod_specs = mod_specs + [
            mspec(f"fakedataprod_{idx + MIN_LINK}", "FakeDataProd", [
                app.QueueInfo(name="timesync_output_queue", inst="time_sync_q", dir="output"),
                app.QueueInfo(name="data_request_input_queue", inst=f"data_requests_{idx + MIN_LINK}", dir="input"),
                app.QueueInfo(name="data_fragment_output_queue", inst="data_fragments_q", dir="output")
            ]) for idx in range(NUMBER_OF_DATA_PRODUCERS)
        ]


    cmd_data['init'] = app.Init(queues=queue_specs, modules=mod_specs)


    cmd_data['conf'] = acmd([("qton_fragments", qton.Conf(msg_type="std::unique_ptr<dunedaq::dataformats::Fragment>",
                                           msg_module_name="FragmentNQ",
                                           sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                  address=NETWORK_ENDPOINTS[f"frags_{HOSTIDX}"],
                                                                  stype="msgpack"))),


                ("qton_timesync", qton.Conf(msg_type="dunedaq::dfmessages::TimeSync",
                                            msg_module_name="TimeSyncNQ",
                                            sender_config=nos.Conf(ipm_plugin_type="ZmqSender",
                                                                   address=NETWORK_ENDPOINTS[f"timesync_{HOSTIDX}"],
                                                                   stype="msgpack"))),
        
            ] + [
                (f"ntoq_datareq_{idx}", ntoq.Conf(msg_type="dunedaq::dfmessages::DataRequest",
                                           msg_module_name="DataRequestNQ",
                                           receiver_config=nor.Conf(ipm_plugin_type="ZmqReceiver",
                                                                    address=NETWORK_ENDPOINTS[f"datareq_{idx}"]))) for idx in range(MIN_LINK,MAX_LINK)
            ] + [
                (f"fakedataprod_{idx}", fdp.Conf(
			system_type = SYSTEM_TYPE,
			apa_number = 0,
			link_number = idx,
			time_tick_diff = 25,
			frame_size = 464,
			response_delay = 0)) for idx in range(MIN_LINK,MAX_LINK)
            ] + [
                (f"data_recorder_{idx}", dr.Conf(
                        output_file = f"output_{idx + MIN_LINK}.out",
                        compression_algorithm = "None",
                        stream_buffer_size = 8388608)) for idx in range(NUMBER_OF_DATA_PRODUCERS)
    ])


    startpars = rccmd.StartParams(run=RUN_NUMBER)
    cmd_data['start'] = acmd([("qton_fragments", startpars),
            ("qton_timesync", startpars),
            ("fakedataprod_.*", startpars),
            ("data_recorder_.*", startpars),
            ("fake_source", startpars),
            ("flxcard.*", startpars),
            ("ntoq_datareq_.*", startpars),
            ("ntoq_trigdec", startpars),])

    cmd_data['stop'] = acmd([("ntoq_trigdec", None),
            ("ntoq_datareq_.*", None),
            ("flxcard.*", None),
            ("fake_source", None),
            ("fakedataprod_.*", None),
            ("data_recorder_.*", None),
            ("qton_timesync", None),
            ("qton_fragments", None),])

    cmd_data['pause'] = acmd([("", None)])

    cmd_data['resume'] = acmd([("", None)])

    cmd_data['scrap'] = acmd([("", None)])

    cmd_data['record'] = acmd([("", None)])

    return cmd_data
