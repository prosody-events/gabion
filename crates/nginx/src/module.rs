use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use ngx::core;
use ngx::ffi::{
    NGX_CONF_1MORE, NGX_CONF_TAKE1, NGX_CONF_TAKE2, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, ngx_array_push, ngx_command_t,
    ngx_conf_t, ngx_cycle_t, ngx_http_handler_pt, ngx_http_module_t,
    ngx_http_phases_NGX_HTTP_ACCESS_PHASE, ngx_int_t, ngx_module_t, ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_log_debug_http, ngx_log_error, ngx_string};

use crate::{
    DEFAULT_MAX_ACTIVE_BUCKETS, KeyComponentList, MAX_KEY_COMPONENTS, MAX_NAME_BYTES,
    MAX_NGINX_SHM_RULES, NginxConfigError, NginxDiscoveryConfig, NginxPeerConfigError,
    NginxRequestEventSource, NginxRuleBuilder, NginxRuleConfig, NginxSharedCountHandler,
    NginxStatus, NginxVariableLookup, NginxZoneConfig, NgxShmAccessError, NgxShmStore,
    RequestEvent, drain_request_events_into_runtime, parse_duration_millis, parse_rate,
    parse_size_bytes,
};
use gabion::{
    CountAggregate, DescriptorConfig, DiscoveryConfig, DiscoveryMode, EndpointSliceSelectorConfig,
    EnforcementMode, GossipConfig as WireConfig, HashedLimitRequest, LimitRuleConfig,
    OverflowPolicy, Runtime, RuntimeConfig, RuntimeTuningConfig, SafetyMarginConfig, StorageConfig,
    TimedHashedLimitRequest,
};

struct Module;

const DEFAULT_ZONE_KEYS: usize = 1024;
const DEFAULT_WINDOW: &str = "60s";
const DEFAULT_BUCKET: &str = "1s";
const DEFAULT_STALE_AFTER: &str = "2s";
const DEFAULT_KEY_COMPONENTS: [&str; 1] = ["$uri"];

static SHM_PTR: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static SHM_LEN: AtomicUsize = AtomicUsize::new(0);
static RUNTIME_SHUTDOWN: AtomicBool = AtomicBool::new(false);
static RUNTIME_LAUNCH: Mutex<Option<NginxRuntimeLaunch>> = Mutex::new(None);
static RUNTIME_THREAD: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

const RUNTIME_DRAIN_BATCH: usize = 256;
const RUNTIME_WAKE_MILLIS: u64 = 10;
const LEADER_LEASE_TTL_MILLIS: u64 = 1_000;

fn gabion_log_info(args: std::fmt::Arguments<'_>) {
    let log = ngx::log::ngx_cycle_log().as_ptr();
    ngx_log_error!(ngx::ffi::NGX_LOG_INFO, log, "{}", args);
}

#[derive(Clone, Debug)]
struct NginxRuntimeLaunch {
    config: RuntimeConfig,
    ptr: usize,
    len: usize,
}

unsafe extern "C" {
    fn mmap(
        addr: *mut c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: isize,
    ) -> *mut c_void;
}

const PROT_READ: i32 = 0x1;
const PROT_WRITE: i32 = 0x2;
const MAP_SHARED: i32 = 0x01;
const MAP_ANONYMOUS: i32 = 0x20;
const MAP_FAILED: *mut c_void = !0_usize as *mut c_void;

impl http::HttpModule for Module {
    fn module() -> &'static ngx_module_t {
        unsafe { &*(&raw const ngx_http_gabion_module) }
    }

    unsafe extern "C" fn postconfiguration(cf: *mut ngx_conf_t) -> ngx_int_t {
        let cf = unsafe { &mut *cf };
        let Some(cmcf) = NgxHttpCoreModule::main_conf_mut(cf) else {
            return core::Status::NGX_ERROR.into();
        };
        let handler = unsafe {
            ngx_array_push(
                &mut cmcf.phases[ngx_http_phases_NGX_HTTP_ACCESS_PHASE as usize].handlers,
            ) as *mut ngx_http_handler_pt
        };
        if handler.is_null() {
            return core::Status::NGX_ERROR.into();
        }
        unsafe {
            *handler = Some(gabion_access_handler);
        }

        core::Status::NGX_OK.into()
    }
}

#[derive(Debug)]
struct MainConfig {
    zone: Option<NginxZoneConfig>,
    rules: [Option<NginxRuleConfig>; MAX_NGINX_SHM_RULES],
    rule_count: usize,
    discovery: NginxDiscoveryConfig,
}

impl Default for MainConfig {
    fn default() -> Self {
        Self {
            zone: None,
            rules: [None; MAX_NGINX_SHM_RULES],
            rule_count: 0,
            discovery: NginxDiscoveryConfig::default(),
        }
    }
}

#[derive(Debug, Default)]
struct LocationConfig {
    enabled: bool,
    off: bool,
    rule_index: usize,
}

unsafe impl HttpModuleMainConf for Module {
    type MainConf = MainConfig;
}

unsafe impl HttpModuleLocationConf for Module {
    type LocationConf = LocationConfig;
}

static mut NGX_HTTP_GABION_COMMANDS: [ngx_command_t; 17] = [
    ngx_command_t {
        name: ngx_string!("gabion_limit_zone"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2) as ngx_uint_t,
        set: Some(set_zone),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_limit_rule"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE) as ngx_uint_t,
        set: Some(set_rule),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_limit"),
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_limit),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion"),
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gabion),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("overflow"),
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_overflow),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_discovery"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_discovery),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_self"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_self),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_bind"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_bind),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_peer"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_peer),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_fanout"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_fanout),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_payload"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_payload),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_max_cells"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_max_cells),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_cluster"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_cluster),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_peer_file"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_peer_file),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_linger"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_protocol_linger),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_endpoint_slice"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE) as ngx_uint_t,
        set: Some(set_protocol_endpoint_slice),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t::empty(),
];

static NGX_HTTP_GABION_MODULE_CTX: ngx_http_module_t = ngx_http_module_t {
    preconfiguration: Some(Module::preconfiguration),
    postconfiguration: Some(Module::postconfiguration),
    create_main_conf: Some(Module::create_main_conf),
    init_main_conf: None,
    create_srv_conf: None,
    merge_srv_conf: None,
    create_loc_conf: Some(Module::create_loc_conf),
    merge_loc_conf: Some(Module::merge_loc_conf),
};

#[cfg(feature = "export-modules")]
ngx::ngx_modules!(ngx_http_gabion_module);

#[used]
#[allow(non_upper_case_globals)]
#[cfg_attr(not(feature = "export-modules"), unsafe(no_mangle))]
pub static mut ngx_http_gabion_module: ngx_module_t = ngx_module_t {
    ctx: ptr::addr_of!(NGX_HTTP_GABION_MODULE_CTX) as _,
    commands: unsafe { &raw mut NGX_HTTP_GABION_COMMANDS[0] },
    type_: NGX_HTTP_MODULE as _,
    init_process: Some(gabion_init_process),
    exit_process: Some(gabion_exit_process),
    ..ngx_module_t::default()
};

impl http::Merge for LocationConfig {
    fn merge(&mut self, previous: &LocationConfig) -> Result<(), MergeConfigError> {
        if !self.enabled && previous.enabled {
            self.enabled = true;
            self.rule_index = previous.rule_index;
        }
        self.off |= previous.off;
        Ok(())
    }
}

http_request_handler!(gabion_access_handler, |request: &mut http::Request| {
    let Some(config) = Module::location_conf(request) else {
        return core::Status::NGX_DECLINED;
    };
    if !config.enabled || config.off {
        return core::Status::NGX_DECLINED;
    }

    let ptr = SHM_PTR.load(Ordering::Acquire);
    let len = SHM_LEN.load(Ordering::Acquire);
    let Some(mut store) = (unsafe { NgxShmStore::from_initialized(ptr, len) }) else {
        return core::Status::NGX_DECLINED;
    };

    let now = request_time_millis();
    let status = store.access(config.rule_index, &RequestVariables { request }, now);
    match status {
        Ok(NginxStatus::Declined) => {
            ngx_log_debug_http!(request, "gabion rate limit allowed");
            core::Status::NGX_DECLINED
        }
        Ok(NginxStatus::TooManyRequests) => {
            ngx_log_debug_http!(request, "gabion rate limit rejected");
            http::HTTPStatus::TOO_MANY_REQUESTS.into()
        }
        Err(NgxShmAccessError::MissingVariable) => core::Status::NGX_DECLINED,
        Err(NgxShmAccessError::InvalidRule | NgxShmAccessError::StoreFull) => {
            core::Status::NGX_DECLINED
        }
    }
});

extern "C" fn set_zone(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 3 || main.zone.is_some() {
            return core::NGX_CONF_ERROR;
        }
        let Ok(name) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(size) = (*args.add(2)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(bytes) = parse_size_bytes(size) else {
            return core::NGX_CONF_ERROR;
        };
        let Some(required) = NgxShmStore::required_bytes(
            MAX_NGINX_SHM_RULES,
            DEFAULT_ZONE_KEYS,
            DEFAULT_MAX_ACTIVE_BUCKETS,
        ) else {
            return core::NGX_CONF_ERROR;
        };
        let bytes = bytes.max(required);
        let mapped = mmap(
            ptr::null_mut(),
            bytes,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            -1,
            0,
        );
        if mapped == MAP_FAILED {
            return core::NGX_CONF_ERROR;
        }
        let Ok(mut store) = NgxShmStore::initialize(
            mapped.cast(),
            bytes,
            MAX_NGINX_SHM_RULES,
            DEFAULT_ZONE_KEYS,
            DEFAULT_MAX_ACTIVE_BUCKETS,
        ) else {
            return core::NGX_CONF_ERROR;
        };
        for index in 0..main.rule_count {
            let Some(rule) = main.rules[index] else {
                return core::NGX_CONF_ERROR;
            };
            if store.add_rule(index, rule).is_err() {
                return core::NGX_CONF_ERROR;
            }
        }
        SHM_PTR.store(mapped.cast(), Ordering::Release);
        SHM_LEN.store(bytes, Ordering::Release);
        main.zone = match NginxZoneConfig::new(name, bytes, DEFAULT_ZONE_KEYS) {
            Ok(zone) => Some(zone),
            Err(_) => return core::NGX_CONF_ERROR,
        };
        if rebuild_runtime(main).is_err() {
            return core::NGX_CONF_ERROR;
        }
        core::NGX_CONF_OK
    }
}

extern "C" fn set_rule(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        let nelts = (*(*cf).args).nelts as usize;
        if args.is_null() || nelts < 3 || main.rule_count == MAX_NGINX_SHM_RULES {
            return core::NGX_CONF_ERROR;
        }
        let Ok(name) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(limit) = (*args.add(2)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        if parse_rate(limit).is_err() {
            return core::NGX_CONF_ERROR;
        }

        let mut key_storage = [""; MAX_KEY_COMPONENTS];
        let mut key_count = 0_usize;
        let mut window = DEFAULT_WINDOW;
        let mut bucket = DEFAULT_BUCKET;
        for index in 3..nelts {
            let Ok(value) = (*args.add(index)).to_str() else {
                return core::NGX_CONF_ERROR;
            };
            if let Some(key) = value.strip_prefix("key=") {
                if key_count == MAX_KEY_COMPONENTS {
                    return core::NGX_CONF_ERROR;
                }
                key_storage[key_count] = key;
                key_count += 1;
            } else if let Some(value) = value.strip_prefix("window=") {
                window = value;
            } else if let Some(value) = value.strip_prefix("bucket=") {
                bucket = value;
            } else if value == "overflow=aggregate" || value.starts_with("zone=") {
            } else {
                return core::NGX_CONF_ERROR;
            }
        }
        let keys = if key_count == 0 {
            DEFAULT_KEY_COMPONENTS.as_slice()
        } else {
            &key_storage[..key_count]
        };
        if KeyComponentList::new(keys).is_err()
            || parse_duration_millis(window).is_err()
            || parse_duration_millis(bucket).is_err()
        {
            return core::NGX_CONF_ERROR;
        }

        let rule = match (NginxRuleBuilder {
            id: main.rule_count as u32 + 1,
            name,
            domain: "nginx",
            key_components: keys,
            limit,
            window,
            bucket,
            local_fallback: limit,
            local_absolute: limit,
            stale_after: DEFAULT_STALE_AFTER,
            mode: EnforcementMode::Enforce,
        })
        .build()
        {
            Ok(rule) => rule,
            Err(NginxConfigError::NameTooLong)
            | Err(NginxConfigError::InvalidRate)
            | Err(NginxConfigError::InvalidDuration)
            | Err(NginxConfigError::TooManyBuckets)
            | Err(NginxConfigError::NoKeyComponents)
            | Err(NginxConfigError::TooManyKeyComponents) => return core::NGX_CONF_ERROR,
            Err(_) => return core::NGX_CONF_ERROR,
        };
        let rule_index = main.rule_count;
        main.rules[rule_index] = Some(rule);
        main.rule_count += 1;
        if let Some(mut store) = NgxShmStore::from_initialized(
            SHM_PTR.load(Ordering::Acquire),
            SHM_LEN.load(Ordering::Acquire),
        ) && store.add_rule(rule_index, rule).is_err()
        {
            return core::NGX_CONF_ERROR;
        }
        if rebuild_runtime(main).is_err() {
            return core::NGX_CONF_ERROR;
        }
        core::NGX_CONF_OK
    }
}

extern "C" fn set_limit(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let location = &mut *(conf as *mut LocationConfig);
        let Some(main) = Module::main_conf(&*cf) else {
            return core::NGX_CONF_ERROR;
        };
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return core::NGX_CONF_ERROR;
        }
        let Ok(rule_name) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Some(rule_index) = find_rule(main, rule_name) else {
            return core::NGX_CONF_ERROR;
        };
        location.enabled = true;
        location.off = false;
        location.rule_index = rule_index;
        core::NGX_CONF_OK
    }
}

extern "C" fn set_gabion(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let location = &mut *(conf as *mut LocationConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return core::NGX_CONF_ERROR;
        }
        let Ok(value) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        if value != "off" {
            return core::NGX_CONF_ERROR;
        }
        location.off = true;
        core::NGX_CONF_OK
    }
}

extern "C" fn set_overflow(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    _conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return core::NGX_CONF_ERROR;
        }
        let Ok(value) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        if value == "aggregate" {
            core::NGX_CONF_OK
        } else {
            core::NGX_CONF_ERROR
        }
    }
}

fn rebuild_runtime_conf(main: &MainConfig) -> *mut c_char {
    if rebuild_runtime(main).is_ok() {
        core::NGX_CONF_OK
    } else {
        core::NGX_CONF_ERROR
    }
}

extern "C" fn set_protocol_discovery(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Some(kind) = parse_discovery_mode(value) else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_kind(kind);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_self(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(addr) = value.parse() else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_self_addr(addr);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_bind(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(addr) = value.parse() else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_bind_addr(addr);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_peer(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(addr) = value.parse() else {
            return core::NGX_CONF_ERROR;
        };
        match main.discovery.add_static_peer(addr) {
            Ok(()) => rebuild_runtime_conf(main),
            Err(NginxPeerConfigError::PeerTableFull) | Err(NginxPeerConfigError::InvalidPeer) => {
                core::NGX_CONF_ERROR
            }
            Err(_) => core::NGX_CONF_ERROR,
        }
    }
}

extern "C" fn set_protocol_fanout(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(fanout) = value.parse::<usize>() else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_fanout(fanout);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_payload(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(bytes) = parse_size_bytes(value) else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_max_payload_bytes(bytes);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_max_cells(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(cells) = value.parse::<usize>() else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_max_cells_per_frame(cells);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_cluster(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(cluster_id_hash) = value.parse::<u128>() else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_cluster_id_hash(cluster_id_hash);
        rebuild_runtime_conf(main)
    }
}

extern "C" fn set_protocol_peer_file(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        match main.discovery.set_peer_file_path(value) {
            Ok(()) => rebuild_runtime_conf(main),
            Err(_) => core::NGX_CONF_ERROR,
        }
    }
}

extern "C" fn set_protocol_endpoint_slice(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        let nelts = (*(*cf).args).nelts as usize;
        if args.is_null() || !(nelts == 3 || nelts == 4) {
            return core::NGX_CONF_ERROR;
        }
        let Ok(namespace) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(service_name) = (*args.add(2)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let port_name = if nelts == 4 {
            let Ok(port_name) = (*args.add(3)).to_str() else {
                return core::NGX_CONF_ERROR;
            };
            port_name
        } else {
            ""
        };
        match main
            .discovery
            .add_endpoint_slice(namespace, service_name, port_name)
        {
            Ok(()) => rebuild_runtime_conf(main),
            Err(_) => core::NGX_CONF_ERROR,
        }
    }
}

extern "C" fn set_protocol_linger(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(millis) = parse_duration_millis(value) else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.set_linger_ms(millis);
        rebuild_runtime_conf(main)
    }
}

unsafe fn single_arg(cf: *mut ngx_conf_t) -> Option<&'static str> {
    unsafe {
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return None;
        }
        (*args.add(1)).to_str().ok()
    }
}

fn parse_discovery_mode(value: &str) -> Option<DiscoveryMode> {
    match value {
        "auto" => Some(DiscoveryMode::Auto),
        "none" => Some(DiscoveryMode::None),
        "static" => Some(DiscoveryMode::Static),
        "file" => Some(DiscoveryMode::File),
        "kubernetes" => Some(DiscoveryMode::KubernetesEndpointSlice),
        _ => None,
    }
}

fn find_rule(main: &MainConfig, name: &str) -> Option<usize> {
    for index in 0..main.rule_count {
        let rule = main.rules[index]?;
        if rule.name.as_str() == name {
            return Some(index);
        }
    }
    None
}

struct RequestVariables<'a> {
    request: &'a http::Request,
}

impl NginxVariableLookup for RequestVariables<'_> {
    fn value<'a>(&'a self, name: &str) -> Option<&'a [u8]> {
        let raw = self.request.as_ref();
        match name.strip_prefix('$').unwrap_or(name) {
            "uri" => Some(raw.uri.as_bytes()),
            "request_uri" => Some(raw.unparsed_uri.as_bytes()),
            "args" => Some(raw.args.as_bytes()),
            "remote_addr" => {
                unsafe { raw.connection.as_ref() }.map(|conn| conn.addr_text.as_bytes())
            }
            name => name
                .strip_prefix("arg_")
                .and_then(|arg| find_query_arg(raw.args.as_bytes(), arg.as_bytes())),
        }
    }
}

fn find_query_arg<'a>(args: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut rest = args;
    while !rest.is_empty() {
        let next = rest
            .iter()
            .position(|byte| *byte == b'&')
            .unwrap_or(rest.len());
        let pair = &rest[..next];
        if let Some(eq) = pair.iter().position(|byte| *byte == b'=')
            && &pair[..eq] == name
        {
            return Some(&pair[eq + 1..]);
        }
        if next == rest.len() {
            break;
        }
        rest = &rest[next + 1..];
    }
    None
}

fn request_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

unsafe extern "C" fn gabion_init_process(_cycle: *mut ngx_cycle_t) -> ngx_int_t {
    let launch = RUNTIME_LAUNCH.lock().ok().and_then(|launch| launch.clone());
    let Some(launch) = launch else {
        return core::Status::NGX_OK.into();
    };

    RUNTIME_SHUTDOWN.store(false, Ordering::Release);
    let Ok(mut thread) = RUNTIME_THREAD.lock() else {
        return core::Status::NGX_ERROR.into();
    };
    if thread.is_some() {
        return core::Status::NGX_OK.into();
    }
    gabion_log_info(format_args!("gabion runtime thread starting"));
    *thread = Some(std::thread::spawn(move || run_nginx_runtime(launch)));
    core::Status::NGX_OK.into()
}

unsafe extern "C" fn gabion_exit_process(_cycle: *mut ngx_cycle_t) {
    RUNTIME_SHUTDOWN.store(true, Ordering::Release);
    let thread = RUNTIME_THREAD
        .lock()
        .ok()
        .and_then(|mut thread| thread.take());
    if let Some(thread) = thread {
        let _ = thread.join();
        gabion_log_info(format_args!("gabion runtime thread stopped"));
    }
}

fn rebuild_runtime(main: &MainConfig) -> Result<(), NginxConfigError> {
    let ptr = SHM_PTR.load(Ordering::Acquire);
    let len = SHM_LEN.load(Ordering::Acquire);
    if ptr.is_null() || len == 0 || main.zone.is_none() || main.rule_count == 0 {
        if let Ok(mut launch) = RUNTIME_LAUNCH.lock() {
            *launch = None;
        }
        return Ok(());
    }

    let zone = main.zone.as_ref().ok_or(NginxConfigError::RuntimeConfig)?;
    let max_cells = zone
        .max_keys
        .checked_mul(DEFAULT_MAX_ACTIVE_BUCKETS)
        .ok_or(NginxConfigError::InvalidCapacity)?;
    let config = RuntimeConfig {
        storage: StorageConfig {
            max_keys: zone.max_keys,
            max_cells: Some(max_cells),
            dirty_ring_entries: Some(max_cells),
            max_descriptor_count: MAX_KEY_COMPONENTS,
            max_descriptor_bytes: MAX_NAME_BYTES.saturating_mul(MAX_KEY_COMPONENTS),
            max_key_bytes: MAX_NAME_BYTES,
            max_active_buckets: DEFAULT_MAX_ACTIVE_BUCKETS,
        },
        limits: nginx_limit_configs(main)?,
        runtime: RuntimeTuningConfig {
            count_update_batch_size: RUNTIME_DRAIN_BATCH,
        },
        discovery: nginx_discovery_config(&main.discovery),
        gossip: nginx_wire_config(&main.discovery),
    };
    Runtime::with_count_update_handler(config.clone(), unsafe {
        NginxSharedCountHandler::new(ptr, len)
    })
    .map_err(|_| NginxConfigError::RuntimeConfig)?;

    if let Ok(mut launch) = RUNTIME_LAUNCH.lock() {
        *launch = Some(NginxRuntimeLaunch {
            config,
            ptr: ptr as usize,
            len,
        });
    }
    gabion_log_info(format_args!(
        "gabion runtime configured rules={} discovery={:?} gossip_enabled={} bind={:?} \
         endpoints={}",
        main.rule_count,
        main.discovery.kind,
        main.discovery.bind_addr.is_some() && main.discovery.kind != DiscoveryMode::None,
        main.discovery.bind_addr,
        main.discovery.endpoint_slices.len(),
    ));
    Ok(())
}

fn nginx_limit_configs(main: &MainConfig) -> Result<Vec<LimitRuleConfig>, NginxConfigError> {
    let mut limits = Vec::with_capacity(main.rule_count);
    for index in 0..main.rule_count {
        let rule = main.rules[index].ok_or(NginxConfigError::RuntimeConfig)?;
        let mut descriptors = Vec::with_capacity(rule.key_components.len());
        for component in rule.key_components.as_slice() {
            descriptors.push(DescriptorConfig {
                key: component.variable.as_str().to_string(),
                value: "*".to_string(),
            });
        }
        limits.push(LimitRuleConfig {
            name: rule.name.as_str().to_string(),
            domain: rule.domain.as_str().to_string(),
            descriptors,
            limit: rule.limit,
            window: Duration::from_millis(rule.window_millis),
            bucket: Duration::from_millis(rule.bucket_millis),
            local_fallback_limit: rule.local_fallback_limit,
            local_absolute_limit: rule.local_absolute_limit,
            stale_after: Duration::from_millis(rule.stale_after_millis),
            safety_margin: SafetyMarginConfig::default(),
            overflow_policy: OverflowPolicy::UseOverflowKey,
            mode: rule.mode,
        });
    }
    Ok(limits)
}

fn nginx_discovery_config(discovery: &NginxDiscoveryConfig) -> DiscoveryConfig {
    DiscoveryConfig {
        kind: discovery.kind,
        peers: discovery
            .static_peers
            .as_slice()
            .iter()
            .filter_map(|peer| peer.socket_addr())
            .collect(),
        path: if discovery.peer_file_path.as_str().is_empty() {
            None
        } else {
            Some(discovery.peer_file_path.as_str().into())
        },
        self_addr: discovery.self_addr,
        endpoint_slices: discovery
            .endpoint_slices
            .as_slice()
            .iter()
            .map(|selector| EndpointSliceSelectorConfig {
                namespace: selector.namespace.as_str().to_string(),
                service_name: selector.service_name.as_str().to_string(),
                port_name: Some(selector.port_name.as_str().to_string()),
            })
            .collect(),
        namespace: None,
        service_name: None,
        port_name: Some(crate::DEFAULT_GOSSIP_PORT_NAME.to_string()),
        max_peers: crate::MAX_NGINX_PEERS,
        recent_peer_grace: Duration::from_millis(30_000),
    }
}

fn nginx_wire_config(discovery: &NginxDiscoveryConfig) -> WireConfig {
    WireConfig {
        enabled: discovery.bind_addr.is_some() && discovery.kind != DiscoveryMode::None,
        bind: discovery.bind_addr,
        linger: Duration::from_millis(discovery.linger_ms),
        fanout: discovery.fanout,
        max_payload_bytes: discovery.max_payload_bytes,
        max_cells_per_frame: discovery.max_cells_per_frame,
        cluster_id_hash: discovery.cluster_id_hash,
    }
}

fn run_nginx_runtime(launch: NginxRuntimeLaunch) {
    gabion_log_info(format_args!(
        "gabion runtime launching discovery={:?} endpoints={} gossip_enabled={} bind={:?}",
        launch.config.discovery.kind,
        launch.config.discovery.endpoint_slices.len(),
        launch.config.gossip.enabled,
        launch.config.gossip.bind,
    ));
    let Ok(tokio) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    else {
        gabion_log_info(format_args!("gabion runtime tokio build failed"));
        return;
    };
    tokio.block_on(async move {
        let handler = unsafe { NginxSharedCountHandler::new(launch.ptr as *mut u8, launch.len) };
        let Ok(runtime) = Runtime::with_count_update_handler(launch.config, handler) else {
            gabion_log_info(format_args!("gabion runtime construction failed"));
            return;
        };
        gabion_log_info(format_args!("gabion runtime constructed"));
        drain_nginx_events_until_shutdown(&runtime, launch.ptr as *mut u8, launch.len).await;
        runtime.shutdown();
    });
}

async fn drain_nginx_events_until_shutdown(
    runtime: &Runtime<NginxSharedCountHandler>,
    ptr: *mut u8,
    len: usize,
) {
    let mut events = [RequestEvent::default(); RUNTIME_DRAIN_BATCH];
    let empty_request = TimedHashedLimitRequest::new(HashedLimitRequest::new(0, 0_u128, 1), 0);
    let mut requests = [empty_request; RUNTIME_DRAIN_BATCH];
    let mut aggregates = [CountAggregate::default(); RUNTIME_DRAIN_BATCH];
    let worker_id = std::process::id();
    let mut interval = tokio::time::interval(Duration::from_millis(RUNTIME_WAKE_MILLIS));
    let mut background = None;
    let mut leader_logged = false;

    while !RUNTIME_SHUTDOWN.load(Ordering::Acquire) {
        interval.tick().await;
        let now = request_time_millis();
        let Some(mut store) = (unsafe { NgxShmStore::from_initialized(ptr, len) }) else {
            continue;
        };
        if !store.try_acquire_runtime_leader(worker_id, now, LEADER_LEASE_TTL_MILLIS) {
            continue;
        }
        if !leader_logged {
            gabion_log_info(format_args!(
                "gabion runtime leader acquired worker={worker_id}"
            ));
            leader_logged = true;
        }
        if background.is_none() {
            let runtime = runtime.clone();
            gabion_log_info(format_args!("gabion gossip background task starting"));
            background = Some(tokio::spawn(async move {
                if let Err(error) = runtime.run_until_shutdown().await {
                    gabion_log_info(format_args!(
                        "gabion gossip background task failed: {error}"
                    ));
                }
            }));
        }

        let recorded = drain_request_events_into_runtime(
            &mut store,
            runtime,
            &mut events,
            &mut requests,
            &mut aggregates,
        );
        if recorded != 0 {
            gabion_log_info(format_args!(
                "gabion runtime drained request events worker={worker_id} recorded={recorded}"
            ));
        }
    }
    runtime.shutdown();
    if let Some(background) = background {
        let _ = background.await;
    }
}
