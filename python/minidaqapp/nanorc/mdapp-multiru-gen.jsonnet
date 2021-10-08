// Schema describing the minidaqapp.mdapp_multiru_gen CLI options.

local moo = import "moo.jsonnet";
local ns = "minidaqapp.mdapp_multiru_gen";
local as = moo.oschema.schema(ns);
local nc = moo.oschema.numeric_constraints;

local hier = {
    int: as.number("Int", 'i4', nc(multipleOf=1.0)),
    float: as.number("Float", 'f4', nc(multipleOf=1.0)),

    # fixme: constrain these strings?
    partition: as.string("Partition", doc="Name of a partition"),
    device: as.string("Device", doc="Name of a device"),
    plugin: as.string("Plugin", doc="Name of a plugin"),
    cfgcode: as.string("ConfigCode", doc="Some kind of configuration source code"),

    frontend: as.enum("FrontendType", ['wib', 'wib2', 'pds_queue', 'pds_list']),
    service: as.enum("ServiceType", ['local','json','cern','pocket']),

    flag: as.boolean("Flag", doc="If True, flag should be enabled"),
    path: as.string("Path", pattern=moo.re.hiername),
    host: as.string("Host", pattern='(%s|%s)' % [moo.re.dnshost, moo.re.ipv4]),
    hosts: as.sequence("Hosts", self.host),
    argpath: as.string("ArgbPath", pattern=moo.re.hiername,
                       doc="A path provided as an argument instead of an option"),

    main: as.record("MdappMultiruGen", [
        as.field('partition_name', self.partition, default="${USER}_test", doc="Name of the partition to use, for ERS and OPMON"),
        as.field('number_of_data_producers', self.int, default=2, doc="Number of links to use, either per ru (<=10) or total. If total is given, will be adjusted to the closest multiple of the number of rus"),
        as.field('emulator_mode', self.flag, default=false, doc="If active, timestamps of data frames are overwritten when processed by the readout. This is necessary if the felix card does not set correct timestamps."),
        as.field('data_rate_slowdown_factor', self.int, default=1),
        as.field('run_number', self.int, default=333),
        as.field('trigger_rate_hz', self.float, default=1.0, doc='Fake HSI only: rate at which fake HSIEvents are sent. 0 - disable HSIEvent generation.'),
        as.field('trigger_window_before_ticks', self.int, default=1000),
        as.field('trigger_window_after_ticks', self.int, default=1000),
        as.field('token_count', self.int, default=10),
        as.field('data_file', self.path, default='./frames.bin', doc="File containing data frames to be replayed by the fake cards"),
        as.field('output_path', self.path, default='.'),
        as.field('disable_trace', self.flag, default=false, doc="Do not enable TRACE (default TRACE_FILE is /tmp/trace_buffer_$HOSTNAME_$USER)"),
        as.field('use_felix', self.flag, default=false, doc="Use real felix cards instead of fake ones"),
        as.field('host_df', self.host, default='localhost'),
        as.field('host_ru', self.hosts, default=['localhost'], doc="This option is repeatable, with each repetition adding an additional ru process."),
        as.field('host_trigger', self.host, default='localhost', doc='Host to run the trigger app on'),
        as.field('host_hsi', self.host, default='localhost', doc='Host to run the HSI app on'),
        as.field('host_timing_hw', self.host, default='np04-srv-012.cern.ch', doc='Host to run the timing hardware interface app on'),
        as.field('control_timing_hw', self.flag, default=false, doc='Flag to control whether we are controlling timing hardware'),
        # hsi readout options
        as.field('hsi_device_name', self.device, default="BOREAS_TLU", doc='Real HSI hardware only: device name of HSI hw'),
        as.field('hsi_readout_period', self.float, default=1e3, doc='Real HSI hardware only: Period between HSI hardware polling [us]'),
        # hw hsi options
        as.field('hsi_endpoint_address', self.int, default=1, doc='Timing address of HSI endpoint'),
        as.field('hsi_endpoint_partition', self.int, default=0, doc='Timing partition of HSI endpoint'),
        as.field('hsi_re_mask', self.int, default=std.parseHex("20000"), doc='Rising-edge trigger mask'),
        as.field('hsi_fe_mask', self.int, default=0, doc='Falling-edge trigger mask'),
        as.field('hsi_inv_mask', self.int, default=0, doc='Invert-edge mask'),
        as.field('hsi_source', self.int, default=1, doc='HSI signal source; 0 - hardware, 1 - emulation (trigger timestamp bits)'),
        # fake hsi options
        as.field('use_hsi_hw', self.flag, default=false, doc='Flag to control whether fake or real hardware HSI config is generated. Default is fake'),
        as.field('hsi_device_id', self.int, default=0, doc='Fake HSI only: device ID of fake HSIEvents'),
        as.field('mean_hsi_signal_multiplicity', self.int, default=1, doc='Fake HSI only: rate of individual HSI signals in emulation mode 1'),
        as.field('hsi_signal_emulation_mode', self.int, default=0, doc='Fake HSI only: HSI signal emulation mode'),
        as.field('enabled_hsi_signals', self.int, default=1, doc='Fake HSI only: bit mask of enabled fake HSI signals'),
        # trigger options
        as.field('ttcm_s1', self.int, default=1, doc="Timing trigger candidate maker accepted HSI signal ID 1"),
        as.field('ttcm_s2', self.int, default=2, doc="Timing trigger candidate maker accepted HSI signal ID 2"),

        as.field('trigger_activity_plugin', self.plugin, default='TriggerActivityMakerPrescalePlugin', doc="Trigger activity algorithm plugin"),
        as.field('trigger_activity_config', self.cfgcode, default='dict(prescale=100)', doc="Trigger activity algorithm config (string containing python dictionary)"),
        as.field('trigger_candidate_plugin', self.plugin, default='TriggerCandidateMakerPrescalePlugin', doc="Trigger candidate algorithm plugin"),
        as.field('trigger_candidate_config', self.cfgcode, default='dict(prescale=100)', doc="Trigger candidate algorithm config (string containing python dictionary)"),

        as.field('enable_raw_recording', self.flag, default=false, doc="Add queues and modules necessary for the record command"),
        as.field('raw_recording_output_dir', self.path, default='.', doc="Output directory where recorded data is written to. Data for each link is written to a separate file"),
        as.field('frontend_type', self.frontend, default='wib', doc="Frontend type (wib, wib2 or pds) and latency buffer implementation in case of pds (folly queue or skip list)"),
        as.field('enable_dqm', self.flag, default=false, doc="Enable Data Quality Monitoring"),
        as.field('opmon_impl', self.service, default='json', doc="Info collector service implementation to use"),
        as.field('ers_impl', self.service, default='local', doc="ERS destination (Kafka used for cern and pocket)"),
        as.field('dqm_impl', self.service, default='local', doc="DQM destination (Kafka used for cern and pocket)"),
        as.field('pocket_url', self.host, default='127.0.0.1', doc="URL for connecting to Pocket services"),
        as.field('enable_software_tpg', self.flag, default=false, doc="Enable software TPG"),
        as.field('enable_tpset_writing', self.flag, default=false, doc="Enable the writing of TPSets to disk (only works with enable_software-tpg"),
        as.field('use_fake_data_producers', self.flag, default=false, doc="Use fake data producers that respond with empty fragments immediately instead of (fake) cards and DLHs"),

        as.field("json_dir", self.path, default=".", doc="Output directory to receive JSON files"),
    ], doc="The CLI for minidaqapp.mdapp_multiru_gen module"),
};
moo.oschema.sort_select(hier, ns)
