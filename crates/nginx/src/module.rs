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
    NGX_CONF_1MORE, NGX_CONF_TAKE1, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, NGX_HTTP_SRV_CONF,
    NGX_LOG_EMERG, ngx_array_push, ngx_command_t, ngx_conf_t, ngx_cycle_t, ngx_http_handler_pt,
    ngx_http_module_t, ngx_http_phases_NGX_HTTP_ACCESS_PHASE, ngx_int_t, ngx_module_t, ngx_str_t,
    ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_string};

use gabion::crdt::CellStoreConfig;
use gabion::defaults;
use gabion::discovery::DiscoveryConfig;
use gabion::rules::EnforcementMode;

use crate::access::{
    AccessCtx, AccessOutcome, CardinalitySettings, RejectInfo, VariableLookup, decide_all,
};
use crate::headers::RejectHeaders;
use crate::identity::derive_identity;
use crate::leader::{self, GossipSettings, LeaderConfig};
use crate::log;
use crate::rules::{
    BindingCompiler, BindingLookup, CompiledRules, DEFAULT_DOMAIN, DescriptorBinding, RuleConfig,
    is_descriptor_key, is_dns_label, is_single_ident, is_zone_name, parse_binding, parse_rate,
};
use crate::shm::{Layout, ShmRegion};

const DEFAULT_QUEUE_CAPACITY: usize = 2048;
const DEFAULT_AGGREGATE_CAPACITY: usize = 4096;

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
    storage: StorageSettings,
    cardinality: CardinalitySettings,
    gossip_bind: Option<SocketAddr>,
    identity_seed: Option<Box<str>>,
    rng_seed: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageSettings {
    max_cells: usize,
    rule_dictionary_capacity: u16,
    node_dictionary_capacity: u16,
    local_dirty_capacity: usize,
    forwarded_dirty_capacity: usize,
    peer_capacity: u16,
}

impl Default for StorageSettings {
    fn default() -> Self {
        Self {
            max_cells: defaults::STORAGE_MAX_CELLS,
            rule_dictionary_capacity: defaults::STORAGE_RULE_DICTIONARY_CAPACITY,
            node_dictionary_capacity: defaults::STORAGE_NODE_DICTIONARY_CAPACITY,
            local_dirty_capacity: defaults::STORAGE_LOCAL_DIRTY_CAPACITY,
            forwarded_dirty_capacity: defaults::STORAGE_FORWARDED_DIRTY_CAPACITY,
            peer_capacity: defaults::STORAGE_PEER_CAPACITY,
        }
    }
}

impl StorageSettings {
    fn cell_store_config(self) -> CellStoreConfig {
        CellStoreConfig {
            cell_capacity: self.max_cells.max(1) as u32,
            rule_dictionary_capacity: self.rule_dictionary_capacity,
            node_dictionary_capacity: self.node_dictionary_capacity,
            local_dirty_capacity: self.local_dirty_capacity,
            forwarded_dirty_capacity: self.forwarded_dirty_capacity,
            peer_capacity: self.peer_capacity,
        }
    }
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

    /// nginx invokes this once per cycle in the master process before any
    /// `gabion_*` directive callback runs. We use it to install the
    /// tracing→nginx subscriber so config-phase log lines route correctly.
    unsafe extern "C" fn preconfiguration(_cf: *mut ngx_conf_t) -> ngx_int_t {
        log::install();
        core::Status::NGX_OK.into()
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
        //
        // The FFI-backed `BindingCompiler` resolves every rule's binding
        // sources against the cycle's variable index before fork, so
        // workers never call the config-phase FFI APIs.
        // SAFETY: `cf` is a non-null, fully-initialised pointer received
        // from nginx and exclusively owned for the duration of this
        // callback. The shared reborrow below is bounded by the immediate
        // call.
        if let Some(main) = Module::main_conf(&*cf) {
            if !install_worker_globals(cf, main) {
                return core::Status::NGX_ERROR.into();
            }
        }
        core::Status::NGX_OK.into()
    }
}

#[derive(Debug, Default)]
struct MainConfig {
    zone_name: Option<Box<str>>,
    rules: Vec<RuleConfig>,
    discovery: DiscoveryConfig,
    gossip: GossipSettings,
    storage: StorageSettings,
    cardinality: CardinalitySettings,
    gossip_bind: Option<SocketAddr>,
    identity_seed: Option<Box<str>>,
    rng_seed: Option<u64>,
    queue_capacity: usize,
    aggregate_capacity: usize,
}

#[derive(Debug, Default)]
struct LocationConfig {
    /// `Some(_)` if any `gabion_limit` directive was seen at this level
    /// (including `gabion_limit off`, which yields an empty vec). `None`
    /// means "inherit from the enclosing level."
    rule_indices: Option<Vec<usize>>,
    /// `Some(_)` if any `gabion` directive was seen at this level. The
    /// inner bool is true when `gabion off` and false on `gabion on`.
    /// `None` means "inherit."
    off: Option<bool>,
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
    /// Mirrors nginx's `limit_req` inheritance: a child that names any
    /// `gabion_limit` directive replaces the parent's set entirely; a
    /// child with no `gabion_limit` directive inherits the parent's set.
    /// `gabion on|off` inherits the same way. The two axes are independent.
    fn merge(&mut self, previous: &LocationConfig) -> Result<(), MergeConfigError> {
        if self.rule_indices.is_none() {
            self.rule_indices = previous.rule_indices.clone();
        }
        if self.off.is_none() {
            self.off = previous.off;
        }
        Ok(())
    }
}

/// Build the static `[ngx_command_t; N + 1]` table nginx expects, sized
/// automatically from the entry list and terminated with
/// `ngx_command_t::empty()`. Avoids the previous footgun where adding or
/// removing a directive required hand-bumping a hardcoded length and
/// remembering to keep the sentinel at the end.
macro_rules! ngx_command_table {
    (static mut $name:ident = [ $($cmd:expr),* $(,)? ];) => {
        static mut $name: [ngx_command_t;
            <[()]>::len(&[$(ngx_command_table!(@unit $cmd)),*]) + 1]
        = [
            $($cmd,)*
            ngx_command_t::empty(),
        ];
    };
    (@unit $_e:expr) => { () };
}

ngx_command_table! {
static mut NGX_HTTP_GABION_COMMANDS = [
    ngx_command_t {
        name: ngx_string!("gabion_limit_zone"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
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
        type_: (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE)
            as ngx_uint_t,
        set: Some(set_limit),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1)
            as ngx_uint_t,
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
        name: ngx_string!("gabion_gossip_tick_interval"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_tick_interval),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_max_payload_bytes"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_max_payload_bytes),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_max_cells_per_frame"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_max_cells_per_frame),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_max_cells_per_tick"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_max_cells_per_tick),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_send_queue_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_send_queue_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_limit_queue_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_limit_queue_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_target_err_bps"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_target_err_bps),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_gossip_min_emit_interval"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_gossip_min_emit_interval),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_max_cells"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_max_cells),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_rule_dictionary_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_rule_dictionary_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_node_dictionary_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_node_dictionary_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_local_dirty_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_local_dirty_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_forwarded_dirty_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_forwarded_dirty_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_peer_capacity"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_peer_capacity),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_max_descriptor_count"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_max_descriptor_count),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_max_descriptor_bytes"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_max_descriptor_bytes),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_storage_max_key_bytes"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_storage_max_key_bytes),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_runtime_rng_seed"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_runtime_rng_seed),
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
        name: ngx_string!("gabion_discovery_namespace_allow"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_discovery_namespace),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_discovery_service_allow"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_discovery_service),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gabion_discovery_self_addr"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_discovery_self_addr),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    },
];
}

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
    if config.off == Some(true) {
        return core::Status::NGX_DECLINED;
    }
    let Some(rule_indices) = config.rule_indices.as_deref() else {
        return core::Status::NGX_DECLINED;
    };
    if rule_indices.is_empty() {
        // `gabion_limit off` — locally suppress all rules without
        // disabling the module itself (which `gabion off` does).
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
        cardinality: globals.cardinality,
    };
    let vars = RequestVariables { request };
    let now = wall_millis();
    let outcome = decide_all(ctx, rule_indices, &vars, now);
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
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        let raw = self.request.as_ref();
        match binding {
            BindingLookup::Uri => Some(raw.uri.as_bytes()),
            BindingLookup::RequestUri => Some(raw.unparsed_uri.as_bytes()),
            BindingLookup::Args => Some(raw.args.as_bytes()),
            BindingLookup::RemoteAddr => {
                // SAFETY: `raw.connection` is a `*mut ngx_connection_t` set
                // by nginx when the request was created and is non-null and
                // valid for the lifetime of the request (i.e. of `raw`).
                // `<*mut T>::as_ref` does the null check itself and returns
                // an `Option<&T>` bound to the borrow of `raw`, so no
                // aliasing or lifetime extension occurs.
                unsafe { raw.connection.as_ref() }.map(|conn| conn.addr_text.as_bytes())
            }
            BindingLookup::Arg(name) => find_query_arg(raw.args.as_bytes(), name.as_bytes()),
            BindingLookup::IndexedVariable { index, .. } => {
                // SAFETY: nginx guarantees the request pointer (`self.request`
                // → underlying `ngx_http_request_t`) is valid for the
                // duration of the access-phase handler, and
                // `ngx_http_get_indexed_variable` is the documented
                // accessor for indexed variables. The `index` was returned
                // by `ngx_http_get_variable_index` at config phase against
                // the same cycle's `cmcf->variables`, so it is in-range.
                // The returned pointer is either null or to an
                // `ngx_http_variable_value_t` allocated against the
                // request's pool, valid until the request completes.
                let r = (self.request as *const http::Request) as *mut ngx::ffi::ngx_http_request_t;
                let value =
                    unsafe { ngx::ffi::ngx_http_get_indexed_variable(r, *index as ngx_uint_t) };
                if value.is_null() {
                    return None;
                }
                // SAFETY: the pointer is non-null per the check above. The
                // value's bitfield-encoded `not_found` flag is set when the
                // variable getter declined to produce a value; in either
                // that case or `valid == 0` we treat the lookup as a miss.
                let v = unsafe { &*value };
                if v.not_found() != 0 || v.valid() == 0 {
                    return None;
                }
                let len = v.len() as usize;
                if len == 0 {
                    return Some(&[]);
                }
                // SAFETY: `v.data` is a `*mut u_char` valid for `v.len()`
                // bytes against the request pool; the borrow is tied to the
                // request's lifetime via `&self`.
                Some(unsafe { std::slice::from_raw_parts(v.data, len) })
            }
            BindingLookup::ComplexValue { compiled_value, .. } => {
                // SAFETY: `compiled_value` was produced by
                // `ngx_http_compile_complex_value` against this cycle's
                // pool during config phase. The struct lives until the
                // cycle exits — longer than any request that reads it. The
                // `Request::get_complex_value` wrapper handles the
                // ngx_http_complex_value FFI call and returns an
                // `Option<&NgxStr>` borrowed from the request pool.
                let cv = *compiled_value as *const ngx::ffi::ngx_http_complex_value_t;
                let cv_ref = unsafe { cv.as_ref() }?;
                self.request.get_complex_value(cv_ref).map(|s| s.as_bytes())
            }
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

/// nginx directive handler for `gabion_limit_zone`. Accepts a single
/// `zone=NAME:SIZE` argument, mirroring nginx core directives like
/// `limit_req_zone`. Invoked once per occurrence in the config, from the
/// master process during the config phase. nginx guarantees `cf` points to a
/// valid `ngx_conf_t` and `conf` points to the `MainConfig` slot it
/// allocated via `create_main_conf`.
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
        if main.zone_name.is_some() {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` declared twice; only one zone is supported per http \
                 {{}} block"
            );
            return core::NGX_CONF_ERROR;
        }
        let Some(arg) = single_arg(cf) else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` requires one argument of the form `zone=NAME:SIZE` \
                 (e.g. `zone=api:128m`)"
            );
            return core::NGX_CONF_ERROR;
        };
        let Some(rest) = arg.strip_prefix("zone=") else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` argument must start with `zone=` (e.g. \
                 `zone=api:128m`)"
            );
            return core::NGX_CONF_ERROR;
        };
        let Some((name, size)) = rest.split_once(':') else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` value `{}` is missing `:SIZE` (e.g. `zone=api:128m`)",
                rest
            );
            return core::NGX_CONF_ERROR;
        };
        if !is_zone_name(name) {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` zone name `{}` must match `[A-Za-z0-9_]+` (matches \
                 nginx core's `limit_req_zone` grammar)",
                name
            );
            return core::NGX_CONF_ERROR;
        }
        let Ok(bytes) = parse_size_bytes(size) else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_zone` size `{}` is not a valid byte count (use suffix \
                 k/m/g)",
                size
            );
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
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: invalid SHM layout (queue_capacity={}, aggregate_capacity={})",
                queue_capacity,
                aggregate_capacity
            );
            return core::NGX_CONF_ERROR;
        };
        let total = bytes.max(layout.total_bytes);

        let mapped = mmap_shared(total);
        if mapped.is_null() {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: failed to mmap {} bytes of shared memory for zone `{}`",
                total,
                name
            );
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
        main.zone_name = Some(name.into());
        tracing::info!(
            zone = name,
            bytes = total,
            queue = queue_capacity,
            aggregate = aggregate_capacity,
            "gabion: zone allocated",
        );
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion_limit_rule`. Shape:
///
/// ```text
/// gabion_limit_rule <name> <$var | name:$var> [...]
///                          rate=Nr/<s|m|h|d|DURATION>
///                          [bucket=B]
///                          [mode=enforce|dry_run|disabled] [dry_run]
///                          [except_if=$var] [domain=NAME];
/// ```
///
/// Positional arguments after the rule name are descriptor bindings — `$var`
/// auto-keyed by the variable name, or `name:$var` with an explicit key.
/// Named arguments are recognised by their `keyword=` prefix; the bare
/// `dry_run` flag is an alias for `mode=dry_run`. The `rate=` argument's
/// unit-letter (`s|m|h|d`) defines the window; for non-round periods use a
/// duration (`rate=100r/30s`, `rate=10r/5m`).
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
        if args.is_null() || nelts < 2 {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_rule` requires at least a rule name"
            );
            return core::NGX_CONF_ERROR;
        }
        let Ok(name) = (*args.add(1)).to_str() else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_rule` rule name is not valid UTF-8"
            );
            return core::NGX_CONF_ERROR;
        };

        let mut bucket: Option<Duration> = None;
        let mut domain = DEFAULT_DOMAIN.to_string();
        let mut bindings: Vec<DescriptorBinding> = Vec::new();
        let mut mode = EnforcementMode::Enforce;
        let mut mode_explicit = false;
        let mut rate: Option<(u64, Duration)> = None;
        let mut except_if: Option<Box<str>> = None;

        for index in 2..nelts {
            let Ok(value) = (*args.add(index)).to_str() else {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit_rule` argument is not valid UTF-8"
                );
                return core::NGX_CONF_ERROR;
            };
            match parse_rule_arg(value) {
                RuleArg::Rate(count, period) => rate = Some((count, period)),
                RuleArg::Bucket(d) => bucket = Some(d),
                RuleArg::Domain(d) => domain = d,
                RuleArg::Mode(m) => {
                    mode = m;
                    mode_explicit = true;
                }
                RuleArg::DryRunFlag => {
                    if mode_explicit && mode != EnforcementMode::DryRun {
                        ngx::ngx_conf_log_error!(
                            NGX_LOG_EMERG,
                            cf,
                            "gabion: `gabion_limit_rule` `dry_run` flag conflicts with explicit \
                             `mode=`"
                        );
                        return core::NGX_CONF_ERROR;
                    }
                    mode = EnforcementMode::DryRun;
                    mode_explicit = true;
                }
                RuleArg::ExceptIf(var) => except_if = Some(var.into()),
                RuleArg::Binding(b) => bindings.push(b),
                RuleArg::Invalid(reason) => {
                    ngx::ngx_conf_log_error!(
                        NGX_LOG_EMERG,
                        cf,
                        "gabion: `gabion_limit_rule` argument `{}` is invalid: {}",
                        value,
                        reason
                    );
                    return core::NGX_CONF_ERROR;
                }
            }
        }

        let Some((limit, window)) = rate else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_rule` rule `{}` is missing the required `rate=Nr/s` \
                 argument",
                name
            );
            return core::NGX_CONF_ERROR;
        };
        if bindings.is_empty() {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_rule` rule `{}` declares no descriptor bindings; add at \
                 least one `$variable` (e.g. `$remote_addr`)",
                name
            );
            return core::NGX_CONF_ERROR;
        }
        if main.rules.iter().any(|r| r.name.as_ref() == name) {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit_rule` rule `{}` is declared more than once; rule names \
                 must be unique within an http {{}} block",
                name
            );
            return core::NGX_CONF_ERROR;
        }

        // `bucket=` defaults to the rate's window so the natural shape of
        // a rule is a single fixed-window bucket per period (e.g.
        // `rate=10r/m` → one 60s bucket). Operators who want finer
        // sliding-window enforcement set `bucket=` explicitly.
        let bucket = bucket.unwrap_or(window);
        main.rules.push(RuleConfig {
            name: name.into(),
            domain: domain.into(),
            bindings,
            limit,
            window,
            bucket,
            mode,
            except_if,
        });
        core::NGX_CONF_OK
    }
}

/// Parsed shape of one `gabion_limit_rule` argument.
enum RuleArg {
    /// `rate=Nr/<period>` — `(count, period)`. Period comes from the unit
    /// letter (`s|m|h|d`) or a humantime-parsed duration (`30s`, `5m`).
    Rate(u64, Duration),
    Bucket(Duration),
    Domain(String),
    Mode(EnforcementMode),
    DryRunFlag,
    ExceptIf(String),
    Binding(DescriptorBinding),
    Invalid(&'static str),
}

fn parse_rule_arg(value: &str) -> RuleArg {
    if value == "dry_run" {
        return RuleArg::DryRunFlag;
    }
    if let Some(rest) = value.strip_prefix("rate=") {
        return match parse_rate(rest) {
            Ok((count, period)) => RuleArg::Rate(count, period),
            Err(reason) => RuleArg::Invalid(reason),
        };
    }
    if let Some(rest) = value.strip_prefix("bucket=") {
        return match humantime::parse_duration(rest) {
            Ok(d) if !d.is_zero() => RuleArg::Bucket(d),
            Ok(_) => RuleArg::Invalid("`bucket=` must be greater than zero"),
            Err(_) => RuleArg::Invalid("expected `bucket=DURATION` (e.g. `bucket=1s`)"),
        };
    }
    if let Some(rest) = value.strip_prefix("domain=") {
        if !is_descriptor_key(rest) {
            return RuleArg::Invalid(
                "`domain=` must match `[A-Za-z_][A-Za-z0-9_.-]*` (e.g. `domain=api`)",
            );
        }
        return RuleArg::Domain(rest.to_string());
    }
    if let Some(rest) = value.strip_prefix("mode=") {
        return match rest {
            "enforce" => RuleArg::Mode(EnforcementMode::Enforce),
            "dry_run" => RuleArg::Mode(EnforcementMode::DryRun),
            "disabled" => RuleArg::Mode(EnforcementMode::Disabled),
            _ => RuleArg::Invalid("expected `mode=enforce|dry_run|disabled`"),
        };
    }
    if let Some(rest) = value.strip_prefix("except_if=") {
        let var = rest.strip_prefix('$').unwrap_or(rest);
        if var.is_empty() || !is_single_ident(var) {
            return RuleArg::Invalid("`except_if=` expects `$variable_name`");
        }
        return RuleArg::ExceptIf(var.to_string());
    }
    match parse_binding(value) {
        Ok(b) => RuleArg::Binding(b),
        Err(reason) => RuleArg::Invalid(reason),
    }
}

/// nginx directive handler for `gabion_limit`. Valid at the http, server,
/// and location levels. Shape:
///
/// ```text
/// gabion_limit NAME [NAME ...];
/// gabion_limit off;
/// ```
///
/// `off` locally suppresses all rules at this level without disabling the
/// module entirely (use `gabion off` for that). Per nginx convention,
/// declaring `gabion_limit` at a child level replaces the parent's set
/// rather than appending to it. Multiple `gabion_limit` directives within
/// the same level accumulate (dedup on duplicates).
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
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit` could not access the main config"
            );
            return core::NGX_CONF_ERROR;
        };
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        let nelts = (*(*cf).args).nelts;
        if args.is_null() || nelts < 2 {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion_limit` requires at least one rule name (or `off`)"
            );
            return core::NGX_CONF_ERROR;
        }

        // Detect `gabion_limit off`. Must be the only argument when used.
        let first = match (*args.add(1)).to_str() {
            Ok(s) => s,
            Err(_) => {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit` argument is not valid UTF-8"
                );
                return core::NGX_CONF_ERROR;
            }
        };
        if first == "off" {
            if nelts != 2 {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit off` does not take additional arguments"
                );
                return core::NGX_CONF_ERROR;
            }
            // `off` at this level means "no rules"; record an empty set so
            // it overrides parent inheritance.
            location.rule_indices = Some(Vec::new());
            return core::NGX_CONF_OK;
        }

        // Resolve each named rule into a rule_table index. Multiple
        // `gabion_limit` directives at the same level accumulate; within
        // one directive, duplicates dedup.
        let indices = location.rule_indices.get_or_insert_with(Vec::new);
        for i in 1..nelts {
            let Ok(rule_name) = (*args.add(i)).to_str() else {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit` rule name is not valid UTF-8"
                );
                return core::NGX_CONF_ERROR;
            };
            if rule_name == "off" {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit off` cannot be mixed with rule names"
                );
                return core::NGX_CONF_ERROR;
            }
            let Some(index) = main.rules.iter().position(|r| r.name.as_ref() == rule_name) else {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion_limit` references rule `{}`, which is not declared via \
                     `gabion_limit_rule`",
                    rule_name
                );
                return core::NGX_CONF_ERROR;
            };
            if !indices.contains(&index) {
                indices.push(index);
            }
        }
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for `gabion`. Accepts `on` or `off`. `off`
/// disables the access handler entirely for this scope (no rules evaluated,
/// no access-phase work). `on` re-enables it where a parent had it off.
/// Valid at the http, server, and location levels.
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
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion` requires exactly one argument (`on` or `off`)"
            );
            return core::NGX_CONF_ERROR;
        }
        let Ok(value) = (*args.add(1)).to_str() else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `gabion` argument is not valid UTF-8"
            );
            return core::NGX_CONF_ERROR;
        };
        match value {
            "on" => location.off = Some(false),
            "off" => location.off = Some(true),
            other => {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `gabion {}` is not a valid value; use `on` or `off`",
                    other
                );
                return core::NGX_CONF_ERROR;
            }
        }
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
    set_scalar(
        cf,
        conf,
        "expected a host:port socket address (e.g. `0.0.0.0:9000`, `[::]:9000`)",
        |v| v.parse::<SocketAddr>().ok(),
        |main, addr| main.gossip_bind = Some(addr),
    )
}

/// nginx directive handler for `gabion_gossip_fanout`. Standard nginx
/// callback contract (see `set_gossip_bind`).
extern "C" fn set_gossip_fanout(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (peers per tick, e.g. `6`)",
        |main, value| {
            main.gossip.fanout = value.max(1);
        },
    )
}

/// nginx directive handler for `gabion_gossip_cluster`. Standard nginx
/// callback contract (see `set_gossip_bind`).
extern "C" fn set_gossip_cluster(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a non-zero 128-bit cluster identifier shared by every peer (e.g. `1`, any u128 \
         literal)",
        |v| match v.parse::<u128>() {
            Ok(0) | Err(_) => None,
            Ok(cluster) => Some(cluster),
        },
        |main, cluster| main.gossip.cluster_id_hash = cluster,
    )
}

extern "C" fn set_gossip_tick_interval(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_duration(
        cf,
        conf,
        "expected a duration like `100ms` or `1s`",
        |main, value| {
            main.gossip.tick_interval = value;
        },
    )
}

extern "C" fn set_gossip_max_payload_bytes(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive byte count (e.g. `65536`)",
        |main, value| {
            main.gossip.max_payload_bytes = value.max(1);
        },
    )
}

extern "C" fn set_gossip_max_cells_per_frame(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u32(
        cf,
        conf,
        "expected a positive integer (cells/frame, e.g. `4096`)",
        |main, value| {
            main.gossip.max_cells_per_frame = value.max(1);
        },
    )
}

extern "C" fn set_gossip_max_cells_per_tick(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (cells/tick)",
        |main, value| {
            main.gossip.max_cells_per_tick = value.max(1);
        },
    )
}

extern "C" fn set_gossip_send_queue_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (send-queue slots)",
        |main, value| {
            main.gossip.send_queue_capacity = value.max(1);
        },
    )
}

extern "C" fn set_gossip_limit_queue_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (limit-queue slots)",
        |main, value| {
            main.gossip.limit_queue_capacity = value.max(1);
        },
    )
}

extern "C" fn set_gossip_target_err_bps(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u32(
        cf,
        conf,
        "expected basis points of the rule's limit (e.g. `100` = 1%)",
        |main, value| {
            main.gossip.target_err_bps = value;
        },
    )
}

extern "C" fn set_gossip_min_emit_interval(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_duration(
        cf,
        conf,
        "expected a duration like `5ms` or `100ms`",
        |main, value| {
            main.gossip.min_emit_interval = value;
        },
    )
}

extern "C" fn set_storage_max_cells(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (CRDT cell capacity, e.g. `131072`)",
        |main, value| {
            main.storage.max_cells = value.max(1);
        },
    )
}

extern "C" fn set_storage_rule_dictionary_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u16(
        cf,
        conf,
        "expected a positive integer that fits u16 (e.g. `1024`)",
        |main, value| {
            main.storage.rule_dictionary_capacity = value.max(1);
        },
    )
}

extern "C" fn set_storage_node_dictionary_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u16(
        cf,
        conf,
        "expected a positive integer that fits u16 (e.g. `1024`)",
        |main, value| {
            main.storage.node_dictionary_capacity = value.max(1);
        },
    )
}

extern "C" fn set_storage_local_dirty_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (local dirty-ring slots)",
        |main, value| {
            main.storage.local_dirty_capacity = value.max(1);
        },
    )
}

extern "C" fn set_storage_forwarded_dirty_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive integer (forwarded dirty-ring slots)",
        |main, value| {
            main.storage.forwarded_dirty_capacity = value.max(1);
        },
    )
}

extern "C" fn set_storage_peer_capacity(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u16(
        cf,
        conf,
        "expected a positive integer that fits u16 (e.g. `256`)",
        |main, value| {
            main.storage.peer_capacity = value.max(1);
        },
    )
}

extern "C" fn set_storage_max_descriptor_count(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a positive integer no greater than the compiled-in cap (see \
         `gabion::defaults::STORAGE_MAX_DESCRIPTOR_COUNT`)",
        |v| {
            let count = v.parse::<usize>().ok()?;
            if count == 0 || count > crate::rules::MAX_DESCRIPTORS {
                return None;
            }
            Some(count)
        },
        |main, value| main.cardinality.max_descriptor_count = value,
    )
}

extern "C" fn set_storage_max_descriptor_bytes(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive byte budget per request (e.g. `512`)",
        |main, value| {
            main.cardinality.max_descriptor_bytes = value.max(1);
        },
    )
}

extern "C" fn set_storage_max_key_bytes(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_usize(
        cf,
        conf,
        "expected a positive byte budget per descriptor key (e.g. `64`)",
        |main, value| {
            main.cardinality.max_key_bytes = value.max(1);
        },
    )
}

extern "C" fn set_runtime_rng_seed(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_u64(
        cf,
        conf,
        "expected a u64 seed for deterministic peer sampling (e.g. `42`)",
        |main, value| {
            main.rng_seed = Some(value);
        },
    )
}

/// nginx directive handler for `gabion_node_id_seed`. Standard nginx callback
/// contract (see `set_gossip_bind`).
extern "C" fn set_identity_seed(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a non-empty seed string (e.g. a pod name)",
        |v| (!v.is_empty()).then(|| v.to_string()),
        |main, value| main.identity_seed = Some(value.into()),
    )
}

/// nginx directive handler for `gabion_discovery_namespace_allow`. Standard
/// nginx callback contract (see `set_gossip_bind`).
extern "C" fn set_discovery_namespace(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a Kubernetes DNS label `[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?` (lower-case, ≤63 \
         chars)",
        |v| is_dns_label(v).then(|| v.to_string()),
        |main, value| main.discovery.namespace_allow.push(value),
    )
}

extern "C" fn set_discovery_service(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a Kubernetes DNS label `[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?` (lower-case, ≤63 \
         chars)",
        |v| is_dns_label(v).then(|| v.to_string()),
        |main, value| main.discovery.service_allow.push(value),
    )
}

extern "C" fn set_discovery_self_addr(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        "expected a host:port socket address to exclude from discovered peers",
        |v| v.parse::<SocketAddr>().ok(),
        |main, addr| main.discovery.self_addr = Some(addr),
    )
}

/// Resolve a single string-typed argument and parse it through `parse`,
/// applying the result via `apply`. On parse failure logs an EMERG line of
/// the operator-grade shape:
///
/// ```text
/// gabion: `<directive>` rejected value `<offending>`: <expected>
/// ```
///
/// where `<directive>` is read from `(*cf).args[0]` (always populated by
/// nginx) and `<expected>` is the per-directive format hint. The hint
/// should be a short, complete sentence that names the expected shape and
/// gives an example (e.g. `"expected a positive integer (e.g. 4096)"`).
fn set_scalar<T>(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    parse: impl FnOnce(&str) -> Option<T>,
    apply: impl FnOnce(&mut MainConfig, T),
) -> *mut c_char {
    // SAFETY: `conf` is the `MainConfig` slot nginx allocated for this
    // module (HttpModuleMainConf contract), uniquely owned for the
    // duration of this single-threaded config-phase callback; `cf` is a
    // valid `ngx_conf_t`.
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let directive = directive_name(cf);
        let Some(value) = single_arg(cf) else {
            ngx::ngx_conf_log_error!(
                NGX_LOG_EMERG,
                cf,
                "gabion: `{}` requires exactly one argument; {}",
                directive,
                expected
            );
            return core::NGX_CONF_ERROR;
        };
        match parse(value) {
            Some(parsed) => {
                apply(main, parsed);
                core::NGX_CONF_OK
            }
            None => {
                ngx::ngx_conf_log_error!(
                    NGX_LOG_EMERG,
                    cf,
                    "gabion: `{}` rejected value `{}`: {}",
                    directive,
                    value,
                    expected
                );
                core::NGX_CONF_ERROR
            }
        }
    }
}

fn set_usize(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    apply: impl FnOnce(&mut MainConfig, usize),
) -> *mut c_char {
    set_scalar(cf, conf, expected, |v| v.parse::<usize>().ok(), apply)
}

fn set_u16(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    apply: impl FnOnce(&mut MainConfig, u16),
) -> *mut c_char {
    set_scalar(cf, conf, expected, |v| v.parse::<u16>().ok(), apply)
}

fn set_u32(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    apply: impl FnOnce(&mut MainConfig, u32),
) -> *mut c_char {
    set_scalar(cf, conf, expected, |v| v.parse::<u32>().ok(), apply)
}

fn set_u64(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    apply: impl FnOnce(&mut MainConfig, u64),
) -> *mut c_char {
    set_scalar(cf, conf, expected, |v| v.parse::<u64>().ok(), apply)
}

fn set_duration(
    cf: *mut ngx_conf_t,
    conf: *mut c_void,
    expected: &str,
    apply: impl FnOnce(&mut MainConfig, Duration),
) -> *mut c_char {
    set_scalar(
        cf,
        conf,
        expected,
        |v| humantime::parse_duration(v).ok(),
        apply,
    )
}

/// Read the directive name from `(*cf).args[0]`. nginx always populates
/// this with the keyword that triggered the callback, so it never panics
/// in practice; the fallback covers a hypothetical malformed `ngx_conf_t`
/// without taking down the master process.
fn directive_name(cf: *mut ngx_conf_t) -> &'static str {
    // SAFETY: Same contract as `single_arg`: `cf` is a valid `ngx_conf_t`
    // owned for the duration of this single-threaded callback;
    // `(*cf).args` is an `ngx_array_t` of `ngx_str_t` with at least one
    // element (the directive name itself). The `&'static str` lifetime is
    // a convenience cast; the byte view borrows from cycle-pool memory
    // valid for the call.
    unsafe {
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        if args.is_null() || (*(*cf).args).nelts == 0 {
            return "<gabion-directive>";
        }
        (*args).to_str().unwrap_or("<gabion-directive>")
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
/// the duration of the call. The returned `&'static str` is *technically*
/// `'static` only to keep the type signature ergonomic — in practice the
/// slice borrows from nginx's config-file token storage, which is owned by
/// the cycle pool and lives at least as long as the surrounding directive
/// callback. **Callers must not store the returned slice past the end of the
/// directive callback** (e.g. into a `'static` map or a worker-globals
/// field) without first copying into an owned `String`/`Box<str>`. Every
/// current caller copies via `.into()` or `.to_string()` before storing.
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

fn install_worker_globals(cf: *mut ngx_conf_t, main: &MainConfig) -> bool {
    let ptr = SHM_PTR.load(Ordering::Acquire);
    let len = SHM_LEN.load(Ordering::Acquire);
    let queue_capacity = SHM_QUEUE_CAPACITY.load(Ordering::Acquire);
    let aggregate_capacity = SHM_AGGREGATE_CAPACITY.load(Ordering::Acquire);
    if ptr.is_null() || len == 0 || main.rules.is_empty() {
        return true;
    }
    let Some(layout) = Layout::new(queue_capacity, aggregate_capacity) else {
        return false;
    };
    // SAFETY: `ptr` was set by `set_zone` (running earlier in this same
    // master process during config parsing) to the base of the freshly
    // `mmap`'d shared region, sized to at least `layout.total_bytes`, and
    // already initialised by `ShmRegion::initialize`. The mapping outlives
    // every reader (it is never `munmap`'d), and the `Layout` matches the
    // one originally used. These together are the contract documented on
    // `ShmRegion::from_initialized`.
    let region = unsafe { ShmRegion::from_initialized(ptr, layout) };
    let mut compiler = NgxBindingCompiler { cf };
    let rules = match CompiledRules::compile_with(&main.rules, main.cardinality, &mut compiler) {
        Ok(r) => Arc::new(r),
        Err(error) => {
            ngx::ngx_conf_log_error!(NGX_LOG_EMERG, cf, "gabion: rule compile failed: {}", error);
            return false;
        }
    };
    let _ = WORKER_GLOBALS.set(WorkerGlobals {
        region,
        rules,
        discovery: main.discovery.clone(),
        gossip: main.gossip.clone(),
        storage: main.storage,
        cardinality: main.cardinality,
        gossip_bind: main.gossip_bind,
        identity_seed: main.identity_seed.clone(),
        rng_seed: main.rng_seed,
    });
    true
}

/// FFI-backed implementation of [`crate::rules::BindingCompiler`].
///
/// At config phase, resolves single-variable bindings to a stable
/// `ngx_int_t` index via `ngx_http_get_variable_index`. Templates compile
/// to an `ngx_http_complex_value_t` allocated in the cycle pool by
/// `ngx_http_compile_complex_value`.
struct NgxBindingCompiler {
    cf: *mut ngx_conf_t,
}

#[derive(Debug, thiserror::Error)]
enum NgxBindingError {
    #[error(
        "could not resolve variable `${0}`; load the providing module before `gabion_limit_rule`"
    )]
    UnknownVariable(String),
    #[error("could not compile template `{0}` as a complex value")]
    ComplexCompile(String),
    #[error("could not allocate {0} bytes from the cycle pool")]
    PoolAlloc(usize),
}

impl BindingCompiler for NgxBindingCompiler {
    type Error = NgxBindingError;

    fn compile(&mut self, source: &str) -> Result<BindingLookup, NgxBindingError> {
        // 1. Inline fast path (`$uri`, `$args`, `$arg_*`, …).
        if let Some(b) = crate::rules::compile_inline(source) {
            return Ok(b);
        }
        // 2. Single $identifier: resolve through nginx's indexed-variable table.
        //    Hashing happens inside the call once at config phase; per-request lookups
        //    are O(1).
        if let Some(stripped) = source.strip_prefix('$') {
            if crate::rules::is_single_ident(stripped) {
                let mut name_str = ngx_str_t {
                    len: stripped.len(),
                    data: stripped.as_ptr() as *mut _,
                };
                // SAFETY: `self.cf` is valid for the duration of
                // `postconfiguration`. `name_str` borrows from `source`
                // which lives at least as long as this call. nginx hashes
                // and copies the name internally.
                let index =
                    unsafe { ngx::ffi::ngx_http_get_variable_index(self.cf, &mut name_str) };
                if index == ngx::core::Status::NGX_ERROR.0 {
                    return Err(NgxBindingError::UnknownVariable(stripped.to_string()));
                }
                return Ok(BindingLookup::IndexedVariable {
                    name: stripped.into(),
                    index: index as i64,
                });
            }
        }
        // 3. Template: compile to an ngx_http_complex_value_t in the cycle pool.
        //    Allocates two structs (the input ccv and the output complex_value) against
        //    `cf->pool`.
        // SAFETY: cf is valid; `pool` is the cycle pool; sizeof return
        // values are POD.
        let cv_size = std::mem::size_of::<ngx::ffi::ngx_http_complex_value_t>();
        let cv_ptr: *mut ngx::ffi::ngx_http_complex_value_t =
            unsafe { ngx::ffi::ngx_palloc((*self.cf).pool, cv_size).cast() };
        if cv_ptr.is_null() {
            return Err(NgxBindingError::PoolAlloc(cv_size));
        }
        let mut value_str = ngx_str_t {
            len: source.len(),
            data: source.as_ptr() as *mut _,
        };
        let mut ccv: ngx::ffi::ngx_http_compile_complex_value_t = unsafe { std::mem::zeroed() };
        ccv.cf = self.cf;
        ccv.value = &mut value_str;
        ccv.complex_value = cv_ptr;
        // SAFETY: the input ccv is a well-formed
        // `ngx_http_compile_complex_value_t`. nginx walks `value` once and
        // writes the compiled instructions into `*ccv.complex_value`,
        // allocating any auxiliary arrays against the pool.
        let rc = unsafe { ngx::ffi::ngx_http_compile_complex_value(&mut ccv) };
        if rc != ngx::core::Status::NGX_OK.0 {
            return Err(NgxBindingError::ComplexCompile(source.to_string()));
        }
        Ok(BindingLookup::ComplexValue {
            source: source.into(),
            compiled_value: cv_ptr as usize,
        })
    }
}

/// nginx `init_process` hook. Invoked once per worker process immediately
/// after fork, before the worker enters its event loop. nginx guarantees the
/// `cycle` argument points to a valid `ngx_cycle_t` for the duration of the
/// call; we do not dereference it here.
unsafe extern "C" fn gabion_init_process(_cycle: *mut ngx_cycle_t) -> ngx_int_t {
    // Workers inherit the global tracing dispatch via fork; re-install is
    // idempotent and covers any path that bypassed `preconfiguration`.
    log::install();

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
        tracing::info!(worker_id, "gabion: worker did not win leader lease");
        return core::Status::NGX_OK.into();
    }

    let rng_seed = match globals.rng_seed {
        Some(seed) => seed,
        None => match defaults::random_rng_seed() {
            Ok(seed) => seed,
            Err(error) => {
                tracing::error!(%error, "gabion: failed to draw gossip RNG seed");
                return core::Status::NGX_ERROR.into();
            }
        },
    };

    let cfg = LeaderConfig {
        worker_id,
        gossip_bind,
        gossip: globals.gossip.clone(),
        discovery: globals.discovery.clone(),
        cell_store: globals.storage.cell_store_config(),
        rng_seed,
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
    tracing::info!(worker_id, "gabion: leader thread spawned");
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
        tracing::error!(?error, "gabion: leader thread panicked");
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
    //   - `MAP_ANONYMOUS` is set, so `fd = -1` and `offset = 0` are the mandated
    //     sentinel values (no file is being mapped);
    //   - `MAP_SHARED` produces pages that survive `fork()` and are visible to
    //     every child — exactly what the worker pool needs;
    //   - the returned pages are kernel-owned and live until either the process
    //     exits or we `munmap` them. v1 never `munmap`s, so the pointer is valid
    //     for the full lifetime of the nginx master process (and inherited
    //     workers).
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
