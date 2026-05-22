//! NGINX FFI glue: directives, the access-phase handler, and the
//! `init_process` hook that spawns the leader thread when a worker wins the
//! SHM lease. All cross-process state lives in the SHM zone allocated by
//! [`set_zone`] during the master process's config phase.

use std::ffi::{c_char, c_void};
use std::net::SocketAddr;
use std::ptr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Result;
use ngx::core;
use ngx::ffi::{
    NGX_CONF_1MORE, NGX_CONF_TAKE1, NGX_CONF_TAKE2, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, ngx_array_push, ngx_command_t,
    ngx_conf_t, ngx_cycle_t, ngx_http_handler_pt, ngx_http_module_t,
    ngx_http_phases_NGX_HTTP_ACCESS_PHASE, ngx_int_t, ngx_module_t, ngx_str_t, ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_log_error, ngx_string};

use gabion::discovery::DiscoveryConfig;
use gabion::rules::EnforcementMode;

use crate::access::{self, AccessCtx, AccessOutcome, RejectInfo, VariableLookup};
use crate::headers::RejectHeaders;
use crate::identity::derive_identity;
use crate::leader::{self, GossipSettings, LeaderConfig};
use crate::rules::{CompiledRules, DEFAULT_DOMAIN, DescriptorBinding, RuleConfig};
use crate::shm::{Layout, ShmRegion};

const DEFAULT_QUEUE_CAPACITY: usize = 2048;
const DEFAULT_AGGREGATE_CAPACITY: usize = 4096;
const DEFAULT_WINDOW: Duration = Duration::from_secs(60);
const DEFAULT_BUCKET: Duration = Duration::from_secs(1);

static SHM_PTR: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static SHM_LEN: AtomicUsize = AtomicUsize::new(0);
static SHM_QUEUE_CAPACITY: AtomicUsize = AtomicUsize::new(0);
static SHM_AGGREGATE_CAPACITY: AtomicUsize = AtomicUsize::new(0);

/// Per-worker shared state. Set during config phase by the master process so
/// every fork sees the same pointers in this static.
static WORKER_GLOBALS: OnceLock<WorkerGlobals> = OnceLock::new();
static LEADER_THREAD: std::sync::Mutex<Option<JoinHandle<Result<()>>>> =
    std::sync::Mutex::new(None);

struct WorkerGlobals {
    region: ShmRegion,
    rules: Arc<CompiledRules>,
    discovery: DiscoveryConfig,
    gossip: GossipSettings,
    gossip_bind: Option<SocketAddr>,
    identity_seed: Option<String>,
    rng_seed: u64,
}

struct Module;

impl http::HttpModule for Module {
    fn module() -> &'static ngx_module_t {
        // SAFETY: `ngx_http_gabion_module` is initialised once at static-init
        // time and is only read (never written) after that. nginx enumerates
        // modules single-threaded from the master process during config
        // parsing, so there is no concurrent access. `&raw`-style borrow via
        // a shared reference is sound because the data is immutable for the
        // remainder of the process lifetime, and the `'static` reference we
        // hand out points to memory that lives for the whole program. See
        // the nomicon chapter on `static mut` (Send/Sync, "Working with
        // Unsafe").
        #[allow(static_mut_refs)]
        unsafe {
            &ngx_http_gabion_module
        }
    }

    /// nginx invokes this exactly once per cycle, from the master process,
    /// after all configuration directives have been parsed but before any
    /// worker fork. The pointer is a fully-initialised `ngx_conf_t` owned by
    /// nginx and is valid for the duration of the call.
    unsafe extern "C" fn postconfiguration(cf: *mut ngx_conf_t) -> ngx_int_t {
        // SAFETY: nginx guarantees `cf` is non-null and points to a valid,
        // exclusively-owned `ngx_conf_t` for the duration of this callback;
        // no other thread is running during config parsing.
        let cf = unsafe { &mut *cf };
        let Some(cmcf) = NgxHttpCoreModule::main_conf_mut(cf) else {
            return core::Status::NGX_ERROR.into();
        };
        // SAFETY: `cmcf.phases[..].handlers` is an `ngx_array_t` that nginx
        // initialised and owns. `ngx_array_push` returns either a pointer to
        // a newly-reserved slot inside that array (writable, properly aligned
        // for `ngx_http_handler_pt`) or null on allocation failure. We check
        // for null below before dereferencing.
        let handler = unsafe {
            ngx_array_push(
                &mut cmcf.phases[ngx_http_phases_NGX_HTTP_ACCESS_PHASE as usize].handlers,
            ) as *mut ngx_http_handler_pt
        };
        if handler.is_null() {
            return core::Status::NGX_ERROR.into();
        }
        // SAFETY: `handler` is non-null (checked above) and was just reserved
        // by `ngx_array_push`. The slot is writable, correctly aligned for
        // `ngx_http_handler_pt`, and uniquely owned (single-threaded config
        // phase).
        unsafe {
            *handler = Some(gabion_access_handler);
        }

        // Install WORKER_GLOBALS now that all `gabion_limit_*` directives
        // have been processed. From here on out workers fork and inherit
        // the mapping + the OnceLock-populated static.
        // SAFETY: `cf` was obtained from a `&mut ngx_conf_t` above; reborrow
        // as shared for the duration of the `main_conf` accessor call.
        if let Some(main) = Module::main_conf(&*cf) {
            install_worker_globals(main);
        }
        core::Status::NGX_OK.into()
    }
}

#[derive(Debug, Default)]
struct MainConfig {
    zone_name: Option<String>,
    rules: Vec<RuleConfig>,
    discovery: DiscoveryConfig,
    gossip: GossipSettings,
    gossip_bind: Option<SocketAddr>,
    identity_seed: Option<String>,
    rng_seed: u64,
    queue_capacity: usize,
    aggregate_capacity: usize,
}

#[derive(Debug, Default)]
struct LocationConfig {
    enabled: bool,
    off: bool,
    rule_index: usize,
}

// SAFETY: `HttpModuleMainConf` is an `unsafe trait` in ngx-rs because nginx's
// configuration slot machinery is implemented in C: nginx allocates a block of
// memory of size `size_of::<MainConf>()` in `create_main_conf`, treats it as
// an opaque `void*`, and hands it back to our directive callbacks. The trait
// requires that `MainConf` be a plain old data type whose default-initialised
// bit pattern is a valid Rust value, and that no extra invariant ride on top
// of what ngx-rs already documents. `MainConfig` derives `Default` and
// contains only `Option`/`Vec`/`String`/integer fields (all safe to
// default-construct), so it satisfies the contract. No additional invariant
// beyond what ngx-rs requires. See the nomicon chapters on unsafe traits and
// FFI.
unsafe impl HttpModuleMainConf for Module {
    type MainConf = MainConfig;
}

// SAFETY: Same justification as `HttpModuleMainConf` above — `LocationConfig`
// is a POD type with a safe `Default` impl, fitting the ngx-rs contract for
// nginx's C-managed per-location config slot. See the nomicon chapter on
// unsafe traits.
unsafe impl HttpModuleLocationConf for Module {
    type LocationConf = LocationConfig;
}

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

static mut NGX_HTTP_GABION_COMMANDS: [ngx_command_t; 10] = [
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
        name: ngx_string!("gabion_gossip_bind"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_bind),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_fanout"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_fanout),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_cluster"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_cluster),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_node_id_seed"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_identity_seed),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_discovery_namespace"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_discovery_namespace),
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
    // SAFETY: `&raw mut` produces a raw pointer without materialising a
    // `&mut`, so no aliasing rule is violated even though
    // `NGX_HTTP_GABION_COMMANDS` is a `static mut`. The array is mutated
    // only in this initialiser; thereafter nginx walks it (single-threaded,
    // during config parsing) treating each entry as read-only metadata. The
    // pointer points to the first element of an array that lives for the
    // entire program. See the nomicon chapter on raw pointers.
    commands: unsafe { &raw mut NGX_HTTP_GABION_COMMANDS[0] },
    type_: NGX_HTTP_MODULE as _,
    init_process: Some(gabion_init_process),
    exit_process: Some(gabion_exit_process),
    ..ngx_module_t::default()
};

// -- access handler ---------------------------------------------------------

http_request_handler!(gabion_access_handler, |request: &mut http::Request| {
    let Some(config) = Module::location_conf(request) else {
        return core::Status::NGX_DECLINED;
    };
    if !config.enabled || config.off {
        return core::Status::NGX_DECLINED;
    }
    let Some(globals) = WORKER_GLOBALS.get() else {
        return core::Status::NGX_DECLINED;
    };

    let ctx = AccessCtx {
        rules: &globals.rules,
        aggregate: globals.region.aggregate(),
        queue: globals.region.queue(),
        stats: globals.region.stats(),
        domain: DEFAULT_DOMAIN,
    };
    let vars = RequestVariables { request };
    let now = wall_millis();
    let outcome = access::decide(ctx, config.rule_index, &vars, now);
    match outcome {
        AccessOutcome::Allow | AccessOutcome::Decline => core::Status::NGX_DECLINED,
        AccessOutcome::Reject(info) => {
            apply_reject_headers(request, info);
            http::HTTPStatus::TOO_MANY_REQUESTS.into()
        }
        AccessOutcome::Cardinality => http::HTTPStatus::BAD_REQUEST.into(),
    }
});

fn apply_reject_headers(request: &mut http::Request, info: RejectInfo) {
    let headers = RejectHeaders::build(info);
    let _ = request.add_header_out("X-RateLimit-Limit", headers.limit.as_str());
    let _ = request.add_header_out("X-RateLimit-Remaining", headers.remaining.as_str());
    let _ = request.add_header_out("X-RateLimit-Reset", headers.reset.as_str());
    let _ = request.add_header_out("Retry-After", headers.retry_after.as_str());
}

struct RequestVariables<'a> {
    request: &'a http::Request,
}

impl VariableLookup for RequestVariables<'_> {
    fn value(&self, name: &str) -> Option<&[u8]> {
        let raw = self.request.as_ref();
        let stripped = name.strip_prefix('$').unwrap_or(name);
        match stripped {
            "uri" => Some(raw.uri.as_bytes()),
            "request_uri" => Some(raw.unparsed_uri.as_bytes()),
            "args" => Some(raw.args.as_bytes()),
            "remote_addr" => {
                // SAFETY: `raw.connection` is a `*mut ngx_connection_t` set
                // by nginx when the request was created and is non-null and
                // valid for the lifetime of the request (i.e. of `raw`).
                // `<*mut T>::as_ref` does the null check itself and returns
                // an `Option<&T>` bound to the borrow of `raw`, so no
                // aliasing or lifetime extension occurs.
                unsafe { raw.connection.as_ref() }.map(|conn| conn.addr_text.as_bytes())
            }
            other => other
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

// -- directives -------------------------------------------------------------

/// nginx directive handler for `gabion_limit_zone`. Invoked once per
/// occurrence in the config, from the master process during the config phase.
/// nginx guarantees `cf` points to a valid `ngx_conf_t` and `conf` points to
/// the `MainConfig` slot it allocated via `create_main_conf` (i.e. of size
/// `size_of::<MainConfig>()` and uniquely owned for the duration of this
/// call).
extern "C" fn set_zone(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: nginx single-threadedly invokes this handler during config
    // parsing with `conf` pointing to the `MainConfig` slot it created for
    // this module (HttpModuleMainConf contract), so the cast and `&mut`
    // borrow is unique and valid. `cf->args` is an `ngx_array_t` of
    // `ngx_str_t` populated by nginx's tokenizer; the pointer arithmetic
    // below is bounded by the `nelts` check, and each `ngx_str_t` is a
    // valid borrowed view of the config file's token storage which outlives
    // this call. See the nomicon chapters on FFI and raw pointers.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 3 || main.zone_name.is_some() {
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
        let queue_capacity = if main.queue_capacity == 0 {
            DEFAULT_QUEUE_CAPACITY
        } else {
            main.queue_capacity
        };
        let aggregate_capacity = if main.aggregate_capacity == 0 {
            DEFAULT_AGGREGATE_CAPACITY
        } else {
            main.aggregate_capacity
        };

        let Some(layout) = Layout::new(queue_capacity, aggregate_capacity) else {
            log_info(format_args!(
                "gabion: invalid SHM layout queue={queue_capacity} aggregate={aggregate_capacity}"
            ));
            return core::NGX_CONF_ERROR;
        };
        let total = bytes.max(layout.total_bytes);

        let mapped = mmap_shared(total);
        if mapped.is_null() {
            return core::NGX_CONF_ERROR;
        }
        let region = ShmRegion::initialize(mapped, layout);

        // Stamp the node identity into the SHM header — once, before fork.
        let identity = derive_identity(main.identity_seed.as_deref());
        region.header().identity.store_node_id(identity.node_id.0);

        // Anchor the lease's clock at this moment. Without it, the
        // try_acquire pack-and-compare path can't distinguish "active"
        // from "expired" because unix epoch millis exceed the u40 expiry
        // bit budget. See shm::lease module docs.
        region.lease().set_init_millis(wall_millis());

        SHM_PTR.store(mapped, Ordering::Release);
        SHM_LEN.store(total, Ordering::Release);
        SHM_QUEUE_CAPACITY.store(queue_capacity, Ordering::Release);
        SHM_AGGREGATE_CAPACITY.store(aggregate_capacity, Ordering::Release);
        main.zone_name = Some(name.to_string());
        log_info(format_args!(
            "gabion: zone allocated name={name} bytes={total} queue={queue_capacity} \
             aggregate={aggregate_capacity}"
        ));
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_limit_rule`. Invoked once per
/// occurrence in the config, from the master process during the config phase.
/// `cf` and `conf` follow the standard nginx callback contract (see
/// `set_zone` above).
extern "C" fn set_rule(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: Same justification as `set_zone`: `conf` is the `MainConfig`
    // slot nginx allocated for us (HttpModuleMainConf contract), uniquely
    // owned during this single-threaded config-phase callback; `cf->args`
    // is an `ngx_array_t` of `ngx_str_t` whose elements remain valid for
    // the duration of the call, and pointer arithmetic into it is bounded
    // by `nelts`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        let nelts = (*(*cf).args).nelts;
        if args.is_null() || nelts < 3 {
            return core::NGX_CONF_ERROR;
        }
        let Ok(name) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(limit_text) = (*args.add(2)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(limit) = parse_rate(limit_text) else {
            return core::NGX_CONF_ERROR;
        };

        let mut window = DEFAULT_WINDOW;
        let mut bucket = DEFAULT_BUCKET;
        let mut domain = DEFAULT_DOMAIN.to_string();
        let mut bindings: Vec<DescriptorBinding> = Vec::new();
        let mut mode = EnforcementMode::Enforce;
        for index in 3..nelts {
            let Ok(value) = (*args.add(index)).to_str() else {
                return core::NGX_CONF_ERROR;
            };
            if let Some(rest) = value.strip_prefix("window=") {
                let Ok(d) = humantime::parse_duration(rest) else {
                    return core::NGX_CONF_ERROR;
                };
                window = d;
            } else if let Some(rest) = value.strip_prefix("bucket=") {
                let Ok(d) = humantime::parse_duration(rest) else {
                    return core::NGX_CONF_ERROR;
                };
                bucket = d;
            } else if let Some(rest) = value.strip_prefix("domain=") {
                domain = rest.to_string();
            } else if let Some(rest) = value.strip_prefix("mode=") {
                mode = match rest {
                    "enforce" => EnforcementMode::Enforce,
                    "disabled" => EnforcementMode::Disabled,
                    _ => return core::NGX_CONF_ERROR,
                };
            } else if let Some(rest) = value.strip_prefix("key=") {
                let binding = parse_binding(rest);
                let Some(binding) = binding else {
                    return core::NGX_CONF_ERROR;
                };
                bindings.push(binding);
            } else {
                return core::NGX_CONF_ERROR;
            }
        }
        if bindings.is_empty() {
            return core::NGX_CONF_ERROR;
        }

        main.rules.push(RuleConfig {
            name: name.to_string(),
            domain,
            bindings,
            limit,
            window,
            bucket,
            mode,
        });
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_limit`. Invoked once per occurrence in
/// a `location {}` block, from the master process during the config phase.
/// nginx passes the `LocationConfig` slot it allocated via `create_loc_conf`
/// in `conf`.
extern "C" fn set_limit(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: nginx single-threadedly invokes this handler with `conf`
    // pointing to the `LocationConfig` slot it created for this module
    // (HttpModuleLocationConf contract), uniquely owned for the duration of
    // the call. `cf` is a valid `ngx_conf_t`, and `Module::main_conf(&*cf)`
    // simply borrows immutably from the same `cf`. `cf->args` follows the
    // same contract as in `set_zone`.
    unsafe {
        let location = &mut *(conf as *mut LocationConfig);
        let Some(main) = Module::main_conf(&*cf) else {
            return core::NGX_CONF_ERROR;
        };
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return core::NGX_CONF_ERROR;
        }
        let Ok(name) = (*args.add(1)).to_str() else {
            return core::NGX_CONF_ERROR;
        };
        let Some(index) = main.rules.iter().position(|r| r.name == name) else {
            return core::NGX_CONF_ERROR;
        };
        location.enabled = true;
        location.off = false;
        location.rule_index = index;
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion` (currently only accepts `gabion off`
/// to disable the access handler for a `location {}`). Invoked once per
/// occurrence in the config; `cf` and `conf` follow the standard nginx
/// callback contract (see `set_limit` above).
extern "C" fn set_gabion(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: As in `set_limit`, `conf` is the per-location `LocationConfig`
    // slot uniquely owned for the duration of this single-threaded
    // config-phase callback (HttpModuleLocationConf contract); `cf->args`
    // is a populated `ngx_array_t` of `ngx_str_t`.
    unsafe {
        let location = &mut *(conf as *mut LocationConfig);
        let args = (*(*cf).args).elts as *mut ngx_str_t;
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

/// nginx directive handler for `gabion_gossip_bind`. Invoked once per
/// occurrence in the config from the master process during the config phase;
/// `cf` and `conf` follow the standard nginx callback contract.
extern "C" fn set_gossip_bind(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: `conf` is the uniquely-owned `MainConfig` slot per the
    // HttpModuleMainConf contract; `cf` is a valid `ngx_conf_t` passed
    // to `single_arg` which honours the same contract. Single-threaded
    // config phase, so the `&mut` borrow is unique.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(addr) = value.parse::<SocketAddr>() else {
            return core::NGX_CONF_ERROR;
        };
        main.gossip_bind = Some(addr);
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_gossip_fanout`. Standard nginx
/// callback contract (see `set_gossip_bind`).
extern "C" fn set_gossip_fanout(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: Same as `set_gossip_bind` — `conf` is the uniquely-owned
    // `MainConfig` slot in the single-threaded config phase, and `cf` is
    // a valid `ngx_conf_t`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(fanout) = value.parse::<usize>() else {
            return core::NGX_CONF_ERROR;
        };
        main.gossip.fanout = fanout.max(1);
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_gossip_cluster`. Standard nginx
/// callback contract (see `set_gossip_bind`).
extern "C" fn set_gossip_cluster(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: Same as `set_gossip_bind` — `conf` is the uniquely-owned
    // `MainConfig` slot in the single-threaded config phase, and `cf` is
    // a valid `ngx_conf_t`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        let Ok(cluster) = value.parse::<u128>() else {
            return core::NGX_CONF_ERROR;
        };
        main.gossip.cluster_id_hash = cluster;
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_node_id_seed`. Standard nginx callback
/// contract (see `set_gossip_bind`).
extern "C" fn set_identity_seed(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: Same as `set_gossip_bind` — `conf` is the uniquely-owned
    // `MainConfig` slot in the single-threaded config phase, and `cf` is
    // a valid `ngx_conf_t`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        main.identity_seed = Some(value.to_string());
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_gossip_discovery_namespace`. Standard
/// nginx callback contract (see `set_gossip_bind`).
extern "C" fn set_discovery_namespace(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: Same as `set_gossip_bind` — `conf` is the uniquely-owned
    // `MainConfig` slot in the single-threaded config phase, and `cf` is
    // a valid `ngx_conf_t`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let Some(value) = single_arg(cf) else {
            return core::NGX_CONF_ERROR;
        };
        main.discovery.namespace_whitelist.push(value.to_string());
        core::NGX_CONF_OK
    }
}

/// Internal helper invoked only from the directive handlers above; marked
/// `unsafe fn` because callers must pass a `cf` that satisfies the nginx
/// callback contract (non-null, valid `ngx_conf_t`, single-threaded config
/// phase). All current callers do.
///
/// # Safety
///
/// `cf` must be a non-null pointer to a fully-initialised `ngx_conf_t` whose
/// `args` array is populated with `ngx_str_t` elements that remain valid for
/// the duration of the call. The returned `&'static str` actually borrows
/// from nginx's config-file token storage, which (per nginx's contract)
/// lives at least as long as the surrounding directive callback — callers
/// must not retain the slice beyond the callback's lifetime even though the
/// type is `'static`.
unsafe fn single_arg(cf: *mut ngx_conf_t) -> Option<&'static str> {
    // SAFETY: Per this function's documented contract, `cf` is non-null and
    // points to a valid `ngx_conf_t`; `(*cf).args` is a valid `ngx_array_t`;
    // each `ngx_str_t` element is in-bounds when accessed under the
    // `nelts != 2` guard. See the nomicon chapter on raw pointers.
    unsafe {
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts != 2 {
            return None;
        }
        (*args.add(1)).to_str().ok()
    }
}

// -- worker globals + lifecycle --------------------------------------------

fn install_worker_globals(main: &MainConfig) {
    let ptr = SHM_PTR.load(Ordering::Acquire);
    let len = SHM_LEN.load(Ordering::Acquire);
    let queue_capacity = SHM_QUEUE_CAPACITY.load(Ordering::Acquire);
    let aggregate_capacity = SHM_AGGREGATE_CAPACITY.load(Ordering::Acquire);
    if ptr.is_null() || len == 0 || main.rules.is_empty() {
        return;
    }
    let Some(layout) = Layout::new(queue_capacity, aggregate_capacity) else {
        return;
    };
    // SAFETY: `ptr` was set by `set_zone` (running earlier in this same
    // master process during config parsing) to the base of the freshly
    // `mmap`'d shared region, sized to at least `layout.total_bytes`, and
    // already initialised by `ShmRegion::initialize`. The mapping outlives
    // every reader (it is never `munmap`'d), and the `Layout` matches the
    // one originally used. These together are the contract documented on
    // `ShmRegion::from_initialized`.
    let region = unsafe { ShmRegion::from_initialized(ptr, layout) };
    let rules = match CompiledRules::compile(&main.rules) {
        Ok(r) => Arc::new(r),
        Err(error) => {
            log_info(format_args!("gabion: rule compile failed: {error}"));
            return;
        }
    };
    let _ = WORKER_GLOBALS.set(WorkerGlobals {
        region,
        rules,
        discovery: main.discovery.clone(),
        gossip: main.gossip.clone(),
        gossip_bind: main.gossip_bind,
        identity_seed: main.identity_seed.clone(),
        rng_seed: if main.rng_seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            main.rng_seed
        },
    });
}

/// nginx `init_process` hook. Invoked once per worker process immediately
/// after fork, before the worker enters its event loop. nginx guarantees the
/// `cycle` argument points to a valid `ngx_cycle_t` for the duration of the
/// call; we do not dereference it here.
unsafe extern "C" fn gabion_init_process(_cycle: *mut ngx_cycle_t) -> ngx_int_t {
    let Some(globals) = WORKER_GLOBALS.get() else {
        return core::Status::NGX_OK.into();
    };
    let Some(gossip_bind) = globals.gossip_bind else {
        return core::Status::NGX_OK.into();
    };

    let worker_id = std::process::id();
    let now = wall_millis();
    if !globals.region.lease().try_acquire(
        worker_id,
        now,
        leader::DEFAULT_LEASE_TTL.as_millis() as u64,
    ) {
        // Another worker holds the lease — be a follower for this turn.
        log_info(format_args!(
            "gabion: worker {worker_id} did not win leader lease"
        ));
        return core::Status::NGX_OK.into();
    }

    let cfg = LeaderConfig {
        worker_id,
        gossip_bind,
        gossip: globals.gossip.clone(),
        discovery: globals.discovery.clone(),
        cell_store: gabion::crdt::CellStoreConfig::default(),
        rng_seed: globals.rng_seed,
        admin_bind: None,
        max_inflight: leader::DEFAULT_MAX_INFLIGHT,
        drain_tick: leader::DEFAULT_DRAIN_TICK,
        lease_tick: leader::DEFAULT_LEASE_TICK,
        lease_ttl: leader::DEFAULT_LEASE_TTL,
        identity_seed: globals.identity_seed.clone(),
    };

    let handle = leader::spawn(globals.region, globals.rules.clone(), cfg);
    if let Ok(mut slot) = LEADER_THREAD.lock() {
        *slot = Some(handle);
    }
    log_info(format_args!(
        "gabion: leader thread spawned worker={worker_id}"
    ));
    core::Status::NGX_OK.into()
}

/// nginx `exit_process` hook. Invoked once per worker process when nginx is
/// shutting that worker down. `cycle` follows the same contract as in
/// `gabion_init_process`; we do not dereference it.
unsafe extern "C" fn gabion_exit_process(_cycle: *mut ngx_cycle_t) {
    let Some(globals) = WORKER_GLOBALS.get() else {
        return;
    };
    let worker_id = std::process::id();
    globals.region.lease().release(worker_id);
    let handle = LEADER_THREAD.lock().ok().and_then(|mut slot| slot.take());
    if let Some(handle) = handle
        && let Err(error) = handle.join()
    {
        log_info(format_args!("gabion: leader thread panicked: {error:?}"));
    }
}

// -- helpers ----------------------------------------------------------------

fn parse_size_bytes(input: &str) -> Result<usize, ()> {
    let input = input.trim();
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(split);
    let value = number.parse::<usize>().map_err(|_| ())?;
    match unit.trim().to_ascii_lowercase().as_str() {
        "" => Ok(value),
        "k" | "kb" => value.checked_mul(1024).ok_or(()),
        "m" | "mb" => value.checked_mul(1024 * 1024).ok_or(()),
        "g" | "gb" => value.checked_mul(1024 * 1024 * 1024).ok_or(()),
        _ => Err(()),
    }
}

/// Parse a `key=KEY:$VAR` or `key=$VAR` descriptor binding fragment (the
/// `key=` prefix is stripped before calling). Accepts:
/// - `tenant:$http_x_tenant` → key="tenant", variable="http_x_tenant"
/// - `$uri` → key="uri", variable="uri"
fn parse_binding(rest: &str) -> Option<DescriptorBinding> {
    if let Some(stripped) = rest.strip_prefix('$') {
        if stripped.is_empty() {
            return None;
        }
        return Some(DescriptorBinding {
            key: stripped.to_string(),
            variable: stripped.to_string(),
        });
    }
    let (key, var) = rest.split_once(':')?;
    if key.is_empty() || var.is_empty() {
        return None;
    }
    let variable = var.strip_prefix('$').unwrap_or(var);
    Some(DescriptorBinding {
        key: key.to_string(),
        variable: variable.to_string(),
    })
}

fn parse_rate(input: &str) -> Result<u64, ()> {
    let input = input.trim();
    let Some((number, _unit)) = input.split_once("r/") else {
        return Err(());
    };
    number.parse::<u64>().map_err(|_| ())
}

fn log_info(args: std::fmt::Arguments<'_>) {
    let log = ngx::log::ngx_cycle_log().as_ptr();
    ngx_log_error!(ngx::ffi::NGX_LOG_INFO, log, "{}", args);
}

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn mmap_shared(size: usize) -> *mut u8 {
    // SAFETY: This is a plain libc `mmap` declaration. We match the standard
    // POSIX prototype exactly, so calling the libc symbol with these
    // argument types is well-defined. See the nomicon chapter on FFI.
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
    #[cfg(target_os = "linux")]
    const MAP_ANONYMOUS: i32 = 0x20;
    #[cfg(target_os = "macos")]
    const MAP_ANONYMOUS: i32 = 0x1000;
    const MAP_FAILED: *mut c_void = !0_usize as *mut c_void;

    // SAFETY: This is a well-formed POSIX `mmap` call:
    //   - `addr` is null, asking the kernel to choose the location;
    //   - `MAP_ANONYMOUS` is set, so `fd = -1` and `offset = 0` are the
    //     mandated sentinel values (no file is being mapped);
    //   - `MAP_SHARED` produces pages that survive `fork()` and are visible
    //     to every child — exactly what the worker pool needs;
    //   - the returned pages are kernel-owned and live until either the
    //     process exits or we `munmap` them. v1 never `munmap`s, so the
    //     pointer is valid for the full lifetime of the nginx master
    //     process (and inherited workers).
    // On failure mmap returns `MAP_FAILED`, which we convert to a null
    // pointer for the caller.
    let mapped = unsafe {
        mmap(
            ptr::null_mut(),
            size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if mapped == MAP_FAILED {
        return ptr::null_mut();
    }
    mapped.cast()
}
