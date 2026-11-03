//! NGINX FFI glue: directives, the access-phase handler, and the
//! `init_process` / `exit_process` hooks that start and stop each worker's
//! leader thread. All cross-process state lives in the SHM zone allocated by
//! [`set_zone`] during the master process's config phase.

use std::cell::Cell;
use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ngx::core;
use ngx::ffi::{
    NGX_CONF_1MORE, NGX_CONF_TAKE1, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, NGX_HTTP_SRV_CONF,
    NGX_HTTP_VAR_NOHASH, NGX_LOG_EMERG, ngx_array_push, ngx_command_t, ngx_conf_t, ngx_cycle_t,
    ngx_exiting, ngx_http_add_variable, ngx_http_get_variable_index, ngx_http_handler_pt,
    ngx_http_module_t, ngx_http_phases_NGX_HTTP_ACCESS_PHASE, ngx_http_request_t,
    ngx_http_variable_value_t, ngx_int_t, ngx_module_t, ngx_pagesize, ngx_pool_cleanup_add,
    ngx_shared_memory_add, ngx_shm_zone_t, ngx_slab_alloc, ngx_str_t, ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_string};

use gabion::crdt::ValidStorageConfig;
use gabion::gossip::ValidGossipConfig;
use gabion::settings::{Setting, Settings};

use crate::access::{AccessCtx, AccessOutcome, AllowInfo, RejectInfo, VariableLookup, decide_all};
use crate::headers::AdmissionHeaders;
use crate::leader::{self, LeaderConfig, LeaderHandle};
use crate::log;
use crate::rules::{
    BindingCompiler, BindingLookup, CompiledRules, RuleConfig, is_zone_name, parse_rule_directive,
};
use crate::shm::lease::now_millis;
use crate::shm::queue::QueueEvent;
use crate::shm::{Layout, MIN_AGGREGATE_CAPACITY, QueueSlot, ShmRegion};

/// Report an `nginx -t` error at the line nginx is reading. The message ends
/// with ` code=<code>`, the key of its entry in the NGINX README's
/// configuration errors, as `tracing` lines end with their fields.
macro_rules! conf_error {
    ($cf:expr, code = $code:literal, $fmt:literal $(, $arg:expr)* $(,)?) => {
        ngx::ngx_conf_log_error!(
            NGX_LOG_EMERG,
            $cf,
            concat!("gabion: ", $fmt, " code=", $code)
            $(, $arg)*
        )
    };
}

static LEADER_THREAD: Mutex<Option<LeaderHandle>> = Mutex::new(None);

thread_local! {
    /// This worker's admission scratch, which `decide_all` reserves on the
    /// worker's first request and never grows after. The access handler
    /// takes it for one request and puts it back.
    static EVENTS: Cell<Vec<QueueEvent>> = const { Cell::new(Vec::new()) };
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
        #[expect(static_mut_refs)]
        unsafe {
            &ngx_http_gabion_module
        }
    }

    /// nginx invokes this once per cycle in the master process before any
    /// `gabion_*` directive callback runs. We use it to install the
    /// tracing→nginx subscriber so config-phase log lines route correctly,
    /// and to register `$gabion_decided`, whose slot marks a decided request.
    unsafe extern "C" fn preconfiguration(cf: *mut ngx_conf_t) -> ngx_int_t {
        log::install();
        let mut name = ngx_string!("gabion_decided");
        // SAFETY: nginx passes a valid `cf`, and the core module has already
        // set up the variable tables. Both calls copy `name`; the first
        // returns null or a variable in the cycle pool. NOHASH hides the
        // variable from lookups by name at runtime (SSI, Lua, Perl), and
        // without CHANGEABLE a `set` or regex capture of the name fails
        // `nginx -t`, so only the access handler writes the slot's `data`.
        let variable =
            unsafe { ngx_http_add_variable(cf, &mut name, NGX_HTTP_VAR_NOHASH as _).as_mut() };
        // SAFETY: as above.
        let index = unsafe { ngx_http_get_variable_index(cf, &mut name) };
        // SAFETY: as above.
        let main = Module::main_conf_mut(unsafe { &*cf });
        let (Some(variable), Ok(index), Some(main)) = (variable, usize::try_from(index), main)
        else {
            conf_error!(
                cf,
                code = "decided_variable_unregistered",
                "could not register `$gabion_decided`, the variable gabion uses to count each \
                 request once across internal redirects. Look for an earlier error naming it: \
                 another module may define `$gabion_decided` too.",
            );
            return core::Status::NGX_ERROR.into();
        };
        variable.get_handler = Some(decided_not_found);
        main.decided = Some(index);
        core::Status::NGX_OK.into()
    }

    /// Override ngx-rs's default `create_main_conf` to heap-allocate
    /// [`MainConfig`].
    ///
    /// ngx-rs's default routes through `Pool::allocate::<T>(value)`, which
    /// calls `ngx_palloc(size_of::<T>())` and ignores `align_of::<T>()`.
    /// nginx's pool only guarantees `NGX_ALIGNMENT` (= `sizeof(unsigned
    /// long)` = 8 on amd64). `MainConfig` contains
    /// [`Settings::cluster_id_hash`], a `NonZeroU128`, so
    /// `align_of::<MainConfig>() == 16`; writing a 16-aligned value into an
    /// 8-aligned pool slot is UB. Native linux/amd64 traps when LLVM
    /// lowers a u128 access to aligned SSE (`movdqa`); macOS arm64 uses
    /// paired 64-bit loads and tolerates the misalignment; QEMU's TCG
    /// doesn't enforce alignment either — which is why this only crashed
    /// on the GHA native amd64 runner.
    ///
    /// Box the value instead: the global allocator honors `align_of`. A
    /// pool cleanup reclaims the box when nginx tears down the cycle's
    /// pool (config reload or shutdown), so we don't leak per-reload.
    unsafe extern "C" fn create_main_conf(cf: *mut ngx_conf_t) -> *mut c_void {
        let boxed = Box::new(MainConfig::default());
        let ptr = Box::into_raw(boxed);

        // SAFETY: nginx hands us a non-null `cf` per the FFI contract, and
        // `(*cf).pool` is the cycle pool, alive for the duration of this
        // cycle. `ngx_pool_cleanup_add(pool, 0)` returns either a valid
        // pointer to a fresh `ngx_pool_cleanup_t` slot or null on
        // allocation failure.
        let cleanup = unsafe { ngx_pool_cleanup_add((*cf).pool, 0) };
        if cleanup.is_null() {
            // SAFETY: we still own `ptr` (no consumer has taken it yet);
            // reconstructing the `Box` from the raw pointer we just leaked
            // is the canonical way to drop it on the failure path.
            unsafe { drop(Box::from_raw(ptr)) };
            return ptr::null_mut();
        }

        // SAFETY: `cleanup` is non-null (checked above) and points to a
        // freshly-allocated, exclusively-owned cleanup slot. Setting
        // `handler` + `data` is the documented contract for installing a
        // pool cleanup callback (`src/core/ngx_palloc.c`).
        unsafe {
            (*cleanup).handler = Some(drop_main_config);
            (*cleanup).data = ptr.cast();
        }

        ptr.cast()
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

        let Some(main) = Module::main_conf_mut(&*cf) else {
            return core::Status::NGX_OK.into();
        };
        // Every setting directive has been read: validate them together, once.
        let (gossip, storage) = match main.settings.validate() {
            Ok(valid) => valid,
            Err(error) => {
                conf_error!(cf, code = "setting_invalid", "{}", error);
                return core::Status::NGX_ERROR.into();
            }
        };
        // The queue takes its share of the zone first, so the layout waits for
        // `gabion_gossip_limit_queue_capacity`, wherever it appears.
        if let Some(bytes) = main.zone_bytes {
            let queue = main.settings.limit_queue_capacity.get();
            // SAFETY: `ngx_pagesize` is read by value; nginx set it at startup,
            // before parsing any config, and never writes it again.
            match Layout::for_zone(bytes, unsafe { ngx_pagesize }, queue) {
                Ok(layout) => main.layout = Some(layout),
                Err(needed) => {
                    conf_error!(
                        cf,
                        code = "limit_zone_too_small",
                        "`gabion_limit_zone` size {} bytes is too small to hold gabion's request \
                         queue (`gabion_gossip_limit_queue_capacity` × {} bytes, {} bytes) and at \
                         least {} counter slots. Raise SIZE to at least {}k, or lower the queue \
                         capacity; the gabion NGINX README's zone-sizing notes give the formula.",
                        bytes,
                        size_of::<QueueSlot>(),
                        queue.next_power_of_two() * size_of::<QueueSlot>(),
                        MIN_AGGREGATE_CAPACITY,
                        needed.div_ceil(1024),
                    );
                    return core::Status::NGX_ERROR.into();
                }
            }
        }
        // Compile the rules now that every `gabion_limit_*` directive has
        // been read. Bindings resolve against this cycle's variables and
        // pool, so each config keeps its own compiled rules: workers forked
        // from it read them, and a reload compiles fresh ones.
        if !main.rules.is_empty() {
            if main.zone_bytes.is_none() {
                conf_error!(
                    cf,
                    code = "rules_without_limit_zone",
                    "rules are declared but `gabion_limit_zone` is not set, so no request would \
                     ever be counted or limited: the zone is the shared memory that holds the \
                     counts. Add `gabion_limit_zone zone=gabion:2m;` to the http {{}} block.",
                );
                return core::Status::NGX_ERROR.into();
            }
            let mut compiler = NgxBindingCompiler { cf };
            match CompiledRules::compile_with(&main.rules, &storage, &mut compiler) {
                Ok(rules) => main.compiled = Some(Arc::new(rules)),
                Err(error) => {
                    conf_error!(cf, code = "rule_compile_failed", "{}", error);
                    return core::Status::NGX_ERROR.into();
                }
            }
        }
        main.valid = Some((gossip, storage));
        core::Status::NGX_OK.into()
    }
}

#[derive(Debug, Default)]
struct MainConfig {
    /// SIZE of `gabion_limit_zone`, set by that directive.
    zone_bytes: Option<usize>,
    /// The zone's layout, set in `postconfiguration`.
    layout: Option<Layout>,
    /// Set by `init_zone` once nginx has mapped the zone.
    region: Option<ShmRegion>,
    rules: Vec<RuleConfig>,
    /// `rules` compiled against this config, set in `postconfiguration`.
    compiled: Option<Arc<CompiledRules>>,
    /// Every setting directive's value, set by `set_setting`.
    settings: Settings,
    /// `settings`, validated once in `postconfiguration`.
    valid: Option<(ValidGossipConfig, ValidStorageConfig)>,
    /// `$gabion_decided`'s `r->variables` index, set in `preconfiguration`.
    decided: Option<usize>,
}

impl MainConfig {
    /// The shared region, the compiled rules and the validated settings,
    /// once a zone and a rule are configured.
    fn active(
        &self,
    ) -> Option<(
        ShmRegion,
        &Arc<CompiledRules>,
        &(ValidGossipConfig, ValidStorageConfig),
    )> {
        Some((self.region?, self.compiled.as_ref()?, self.valid.as_ref()?))
    }
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

/// Pool-cleanup handler installed by [`Module::create_main_conf`].
///
/// nginx invokes this once when the cycle's pool is destroyed, with `data`
/// pointing back at the `*mut MainConfig` we registered in
/// `cleanup.data`. Reconstructing the `Box` and dropping it returns the
/// MainConfig's allocation to the global allocator.
///
/// # Safety
/// `data` must be a pointer produced by `Box::into_raw(Box<MainConfig>)`
/// and must not have been freed already. Both invariants are upheld by
/// nginx's contract: it stores exactly the pointer we handed to it and
/// invokes the handler exactly once.
unsafe extern "C" fn drop_main_config(data: *mut c_void) {
    if data.is_null() {
        return;
    }
    // SAFETY: precondition documented above.
    unsafe { drop(Box::from_raw(data.cast::<MainConfig>())) };
}

// SAFETY: ngx-rs 0.5.1 requires that `MainConf` is the configuration type
// stored in this module's main-conf slot. `MainConf` is `MainConfig`, the type
// this module's `create_main_conf` allocates (a `Box<MainConfig>` whose
// pointer it returns), so every `main_conf` access reads a `MainConfig`.
unsafe impl HttpModuleMainConf for Module {
    type MainConf = MainConfig;
}

// No `align_of::<MainConfig>() <= NGX_ALIGNMENT` assert here. `MainConfig`
// transitively contains `Settings::cluster_id_hash: NonZeroU128`, so its
// alignment is 16 — greater than `NGX_ALIGNMENT` (= 8 on amd64). What
// keeps that sound is `Module::create_main_conf` above: instead of letting
// ngx-rs `Pool::allocate::<MainConfig>` the value into the pool, we
// heap-allocate via `Box::new` and store only the resulting
// `*mut MainConfig` in the pool slot. The pool slot then holds an
// 8-aligned pointer, which palloc handles correctly. An assert on
// `align_of::<MainConfig>()` would fire even though the runtime layout is
// fine, so it can't be expressed at this level.
//
// If you change `create_main_conf` back to ngx-rs's default — or add a
// new over-aligned field to `MainConfig` without keeping the override —
// the CI native-amd64 nginx smokes crash inside the config phase. Keep
// the override; the heap path is the only thing making this safe.

// SAFETY: ngx-rs 0.5.1 requires that `LocationConf` is the configuration type
// stored in this module's location-conf slot. `LocationConf` is
// `LocationConfig`, the type ngx-rs's default `create_loc_conf` allocates for
// this module (`Pool::allocate::<Self::LocationConf>`), so every `loc_conf`
// access reads a `LocationConfig`.
unsafe impl HttpModuleLocationConf for Module {
    type LocationConf = LocationConfig;
}

// Compile-time alignment guard for the default `create_loc_conf` path.
// ngx-rs's default routes through `Pool::allocate::<LocationConfig>(value)`,
// which calls `ngx_palloc(size_of::<T>())` and ignores `align_of::<T>()`.
// The pool only guarantees `NGX_ALIGNMENT` (= `sizeof(unsigned long)` = 8
// on amd64), so a type whose alignment exceeds that placed directly into
// a pool slot is UB — which is exactly the bug that shipped for
// `MainConfig` until commit 2a1a2e8. This assert catches the same
// hazard for `LocationConfig` at `cargo check` time, before nginx tries
// the misaligned write.
//
// Scope: this guards types that go through `Pool::allocate::<T>`. Types
// with a custom `create_*_conf` override (see `Module::create_main_conf`
// for the pattern) sidestep palloc's alignment limit by heap-allocating
// and storing only a pointer in the pool, and the assert can't be
// expressed at that point — so they get a comment instead, next to their
// unsafe-impl.
//
// If you hit this assert: override `create_loc_conf` to heap-allocate
// via Box::new the same way `Module::create_main_conf` does, or split
// the over-aligned field (e.g. `u128` → two paired `u64`s, the
// convention used at the SHM boundary in `crates/nginx/src/shm/`).
// `::core::mem` — not bare `core::mem` — because this file imports
// `use ngx::core;` at the top, which shadows the standard `core` crate
// inside this module. The `::core` prefix bypasses the shadow without
// dragging the rest of the file off `ngx::core::Status` etc.
const _: () = assert!(
    ::core::mem::align_of::<LocationConfig>() <= nginx_sys::NGX_ALIGNMENT,
    "LocationConfig alignment exceeds NGX_ALIGNMENT; ngx_palloc cannot host it. Override \
     `create_loc_conf` to heap-allocate via Box::new (see `Module::create_main_conf` for the \
     cleanup-callback pattern), or split over-aligned fields into paired u64s.",
);

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

/// The four core directives, one directive per setting that has an NGINX
/// spelling, and nginx's terminator.
const COMMAND_COUNT: usize = {
    let mut count = 5;
    let mut index = 0;
    while index < Setting::ALL.len() {
        if Setting::ALL[index].nginx_directive().is_some() {
            count += 1;
        }
        index += 1;
    }
    count
};

static mut NGX_HTTP_GABION_COMMANDS: [ngx_command_t; COMMAND_COUNT] = commands();

/// The command table, its setting directives derived from the library's
/// settings table, so none can be missing or spelled differently. Each
/// setting's entry points `post` at the setting, which [`set_setting`] reads.
const fn commands() -> [ngx_command_t; COMMAND_COUNT] {
    let mut table = [const { ngx_command_t::empty() }; COMMAND_COUNT];
    table[0] = ngx_command_t {
        name: ngx_string!("gabion_limit_zone"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_zone),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    };
    table[1] = ngx_command_t {
        name: ngx_string!("gabion_limit_rule"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE) as ngx_uint_t,
        set: Some(set_rule),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    };
    table[2] = ngx_command_t {
        name: ngx_string!("gabion_limit"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE)
            as ngx_uint_t,
        set: Some(set_limit),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    };
    table[3] = ngx_command_t {
        name: ngx_string!("gabion"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1)
            as ngx_uint_t,
        set: Some(set_gabion),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: ptr::null_mut(),
    };
    let mut next = 4;
    let mut index = 0;
    while index < Setting::ALL.len() {
        if let Some(name) = Setting::ALL[index].nginx_directive_cstr() {
            table[next] = ngx_command_t {
                name: ngx_str_t {
                    len: name.count_bytes(),
                    data: name.as_ptr().cast_mut().cast(),
                },
                type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
                set: Some(set_setting),
                conf: NGX_HTTP_MAIN_CONF_OFFSET,
                offset: 0,
                post: ptr::from_ref(&Setting::ALL[index]).cast_mut().cast(),
            };
            next += 1;
        }
        index += 1;
    }
    table
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
    let Some(main) = Module::main_conf(request) else {
        return core::Status::NGX_DECLINED;
    };
    let (Some((region, rules, (_, storage))), Some(index)) = (main.active(), main.decided) else {
        return core::Status::NGX_DECLINED;
    };
    // Internal redirects rerun this handler but keep `r->variables`, so a
    // non-null slot `data` marks a request already counted or refused, as
    // `limit_req` marks `r->main`. Subrequests skip the access phase.
    //
    // SAFETY: `index` came from `ngx_http_get_variable_index` for this
    // request's cycle, and nginx zero-allocates `r->variables` with a slot
    // per indexed variable of that cycle, so the slot is in bounds and
    // initialised for the whole request. Only this worker's event thread
    // touches it, and no reference to it is held.
    let decided = unsafe { request.as_ref().variables.add(index) };
    // SAFETY: `decided` is in bounds and initialised (above).
    if !unsafe { (*decided).data }.is_null() {
        return core::Status::NGX_DECLINED;
    }

    let ctx = AccessCtx {
        rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        cardinality: storage.cardinality,
        max_matched_rules: storage.max_matched_rules(),
    };
    let vars = RequestVariables { request };
    let now = wall_millis();
    let mut events = EVENTS.take();
    let outcome = decide_all(ctx, rule_indices, &vars, now, &mut events);
    EVENTS.set(events);
    // A pass that counted nothing leaves the next limited location to decide.
    let (status, counted) = match outcome {
        AccessOutcome::Allow(Some(info)) => {
            apply_admit_headers(request, info);
            (core::Status::NGX_DECLINED, true)
        }
        AccessOutcome::Allow(None) | AccessOutcome::Decline => (core::Status::NGX_DECLINED, false),
        AccessOutcome::Reject(info) => {
            apply_reject_headers(request, info);
            (http::HTTPStatus::TOO_MANY_REQUESTS.into(), true)
        }
        AccessOutcome::Cardinality => (http::HTTPStatus::BAD_REQUEST.into(), true),
    };
    if counted {
        // SAFETY: `decided` is in bounds and initialised (above).
        unsafe { (*decided).data = ptr::NonNull::dangling().as_ptr() };
    }
    status
});

/// `$gabion_decided` never has a value, and leaving the slot's `data`
/// alone keeps nginx's own evaluation (`log_format`, `if`) off the marker.
///
/// # Safety
/// `v` must point at a request's slot for this variable, valid for writes.
unsafe extern "C" fn decided_not_found(
    _: *mut ngx_http_request_t,
    v: *mut ngx_http_variable_value_t,
    _: usize,
) -> ngx_int_t {
    // SAFETY: nginx calls get handlers with `v` pointing at the request's
    // slot for this variable, per the doc above.
    unsafe { (*v).set_not_found(1) };
    core::Status::NGX_OK.into()
}

fn apply_admit_headers(request: &mut http::Request, info: AllowInfo) {
    let headers = AdmissionHeaders::build_for_allow(info);
    let _ = request.add_header_out("X-RateLimit-Limit", headers.limit.as_str());
    let _ = request.add_header_out("X-RateLimit-Remaining", headers.remaining.as_str());
    let _ = request.add_header_out("X-RateLimit-Reset", headers.reset.as_str());
}

fn apply_reject_headers(request: &mut http::Request, info: RejectInfo) {
    let headers = AdmissionHeaders::build_for_reject(info);
    let _ = request.add_header_out("X-RateLimit-Limit", headers.limit.as_str());
    let _ = request.add_header_out("X-RateLimit-Remaining", headers.remaining.as_str());
    let _ = request.add_header_out("X-RateLimit-Reset", headers.reset.as_str());
    if let Some(retry_after) = headers.retry_after.as_ref() {
        let _ = request.add_header_out("Retry-After", retry_after.as_str());
    }
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
        if main.zone_bytes.is_some() {
            conf_error!(
                cf,
                code = "limit_zone_declared_twice",
                "`gabion_limit_zone` is declared twice in this http {{}} block, but gabion keeps \
                 one limit zone per block. Remove one and size the other for every rule (the \
                 zone-sizing notes in the gabion NGINX README).",
            );
            return core::NGX_CONF_ERROR;
        }
        let Some(arg) = single_arg(cf) else {
            conf_error!(
                cf,
                code = "limit_zone_arg_count",
                "`gabion_limit_zone` takes one argument, `zone=NAME:SIZE` (for example \
                 `zone=api:2m`). Write it in that form, then run `nginx -t`.",
            );
            return core::NGX_CONF_ERROR;
        };
        let Some(rest) = arg.strip_prefix("zone=") else {
            conf_error!(
                cf,
                code = "limit_zone_missing_prefix",
                "`gabion_limit_zone` argument must start with `zone=` (for example \
                 `zone=api:2m`). Write it as `zone=NAME:SIZE`, then run `nginx -t`.",
            );
            return core::NGX_CONF_ERROR;
        };
        let Some((name, size)) = rest.split_once(':') else {
            conf_error!(
                cf,
                code = "limit_zone_missing_size",
                "`gabion_limit_zone` value `{}` is missing `:SIZE` (for example `zone=api:2m`). \
                 Write it as `zone=NAME:SIZE`, then run `nginx -t`.",
                rest,
            );
            return core::NGX_CONF_ERROR;
        };
        if !is_zone_name(name) {
            conf_error!(
                cf,
                code = "limit_zone_bad_name",
                "`gabion_limit_zone` zone name `{}` must match `[A-Za-z0-9_]+` (matches nginx \
                 core's `limit_req_zone` grammar). Rename the zone, then run `nginx -t`.",
                name,
            );
            return core::NGX_CONF_ERROR;
        }
        let Ok(bytes) = parse_size_bytes(size) else {
            conf_error!(
                cf,
                code = "limit_zone_bad_size",
                "`gabion_limit_zone` size `{}` is not a valid byte count. Write a number with the \
                 suffix k, m or g, for example `2m`; then run `nginx -t`.",
                size,
            );
            return core::NGX_CONF_ERROR;
        };
        let mut zone_name = ngx_str_t {
            len: name.len(),
            data: name.as_ptr().cast_mut(),
        };
        let shm_zone = ngx_shared_memory_add(
            cf,
            &mut zone_name,
            bytes,
            ptr::addr_of_mut!(ngx_http_gabion_module).cast(),
        );
        if shm_zone.is_null() {
            return core::NGX_CONF_ERROR;
        }
        (*shm_zone).init = Some(init_zone);
        (*shm_zone).data = ptr::from_mut(main).cast();
        main.zone_bytes = Some(bytes);
        core::NGX_CONF_OK
    }
}

/// nginx zone `init` callback for `gabion_limit_zone`. nginx runs it in the
/// master after the whole config is parsed. On a reload that keeps the
/// zone's name and size, nginx reuses the mapping and passes the previous
/// config as `data`, so the identity, the leader lease and the queue carry
/// over (the next leader empties the counts and peers' gossip refills
/// them); otherwise the zone is new and the region is carved out of it.
unsafe extern "C" fn init_zone(shm_zone: *mut ngx_shm_zone_t, data: *mut c_void) -> ngx_int_t {
    // SAFETY: `set_zone` pointed this zone's `data` at its config's boxed
    // `MainConfig`, which lives until that config's pool is destroyed. nginx
    // passes the previous config's `data` when it reuses the zone, and keeps
    // that config alive until the new one is initialised.
    let (zone, main, previous) = unsafe {
        (
            &*shm_zone,
            &mut *(*shm_zone).data.cast::<MainConfig>(),
            data.cast::<MainConfig>().as_ref(),
        )
    };
    let Some(layout) = main.layout else {
        return core::Status::NGX_ERROR.into();
    };
    let region = match previous.and_then(|previous| previous.region) {
        Some(region) if region.layout() != layout => {
            tracing::error!(
                code = "limit_queue_resized_on_reload",
                running = region.layout().queue_capacity,
                configured = layout.queue_capacity,
                "gabion: this reload changes `gabion_gossip_limit_queue_capacity`, but nginx \
                 reuses the `gabion_limit_zone` shared memory across a reload while its name and \
                 size stay the same, and the running workers still use its queue. nginx keeps the \
                 previous configuration. Change SIZE in `gabion_limit_zone` along with the queue \
                 capacity, so nginx maps a fresh zone, or restart nginx.",
            );
            return core::Status::NGX_ERROR.into();
        }
        Some(region) => region,
        None => {
            if LEADER_THREAD.lock().is_ok_and(|slot| slot.is_some()) {
                tracing::error!(
                    code = "limit_zone_change_needs_restart",
                    "gabion: this reload changes `gabion_limit_zone`, but with `master_process \
                     off` the running leader thread still uses the old zone, which nginx is about \
                     to free. nginx keeps the previous configuration. Restart nginx to change the \
                     zone.",
                );
                return core::Status::NGX_ERROR.into();
            }
            // SAFETY: nginx initialised a slab pool at the start of every new
            // zone it maps. Allocations above half a page come back
            // page-aligned, which is the alignment `ShmRegion` needs, and the
            // block is ours alone.
            let base = unsafe { ngx_slab_alloc(zone.shm.addr.cast(), layout.total_bytes) };
            if base.is_null() {
                tracing::error!(
                    code = "limit_zone_no_room",
                    zone_bytes = zone.shm.size,
                    needed_bytes = layout.total_bytes,
                    "gabion: the `gabion_limit_zone` zone had no room for gabion's state, so \
                     nginx rejected this configuration. gabion sizes its state to fit the zone, \
                     so nginx's slab bookkeeping used more of it than gabion allowed for. Raise \
                     SIZE in `gabion_limit_zone zone=NAME:SIZE`, and report your nginx version as \
                     a gabion bug.",
                );
                return core::Status::NGX_ERROR.into();
            }
            // SAFETY: `base` is a fresh, exclusively owned, page-aligned block
            // of `layout.total_bytes`, which nginx keeps mapped in every
            // worker forked from this config.
            unsafe { ShmRegion::initialize(base.cast(), layout) }
        }
    };
    // Stamp the identity on every config load so a changed
    // `gabion_node_id_seed` takes effect, and anchor the lease clock, both
    // before fork.
    let node_id = gabion::identity::node_id(main.settings.node_id_seed.as_deref());
    region.header().identity.store_node_id(node_id.0);
    now_millis();
    main.region = Some(region);
    tracing::info!(
        code = "limit_zone_ready",
        limit_zone = %String::from_utf8_lossy(zone.shm.name.as_bytes()),
        bytes = zone.shm.size,
        slots = region.layout().aggregate_capacity,
        queue = region.layout().queue_capacity,
        reused = previous.is_some(),
        "gabion: limit zone ready",
    );
    core::Status::NGX_OK.into()
}

/// nginx directive handler for `gabion_limit_rule`. Shape:
///
/// ```text
/// gabion_limit_rule <name> <$var | name:$var> [...]
///                          rate=Nr/<s|m|h|d|DURATION>
///                          [window=W] [bucket=B]
///                          [mode=enforce|dry_run|disabled] [dry_run]
///                          [except_if=$var] [domain=NAME]
///                          [value=KEY:LITERAL];
/// ```
///
/// [`parse_rule_directive`] reads the arguments, reporting any syntax error
/// at this line; `postconfiguration` builds the rule once every setting is
/// read. See the README's "Rate, window, and bucket" section.
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
        let nelts = if args.is_null() {
            0
        } else {
            (*(*cf).args).nelts
        };
        let mut words = Vec::with_capacity(nelts);
        for index in 1..nelts {
            let Ok(word) = (*args.add(index)).to_str() else {
                return arg_not_utf8(cf, "gabion_limit_rule");
            };
            words.push(word);
        }
        match parse_rule_directive(&words) {
            Ok(rule) => {
                main.rules.push(rule);
                core::NGX_CONF_OK
            }
            Err(error) => {
                conf_error!(
                    cf,
                    code = "limit_rule_invalid",
                    "`gabion_limit_rule` {}",
                    error,
                );
                core::NGX_CONF_ERROR
            }
        }
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
            conf_error!(
                cf,
                code = "limit_main_conf_missing",
                "`gabion_limit` could not read gabion's http-level configuration, which nginx \
                 always provides. This is a gabion bug: report it with your nginx version.",
            );
            return core::NGX_CONF_ERROR;
        };
        let args = (*(*cf).args).elts as *mut ngx_str_t;
        let nelts = (*(*cf).args).nelts;
        if args.is_null() || nelts < 2 {
            conf_error!(
                cf,
                code = "limit_missing_rule",
                "`gabion_limit` names no rule. List rules declared with `gabion_limit_rule`, or \
                 write `gabion_limit off;` to turn them off here.",
            );
            return core::NGX_CONF_ERROR;
        }

        // Detect `gabion_limit off`. Must be the only argument when used.
        let Ok(first) = (*args.add(1)).to_str() else {
            return arg_not_utf8(cf, "gabion_limit");
        };
        if first == "off" {
            if nelts != 2 {
                conf_error!(
                    cf,
                    code = "limit_off_mixed",
                    "`gabion_limit off` turns off every rule at this level, so it takes no rule \
                     names. Write `gabion_limit off;` alone, or list only rule names.",
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
                return arg_not_utf8(cf, "gabion_limit");
            };
            if rule_name == "off" {
                conf_error!(
                    cf,
                    code = "limit_off_mixed",
                    "`gabion_limit off` turns off every rule at this level, so it takes no rule \
                     names. Write `gabion_limit off;` alone, or list only rule names.",
                );
                return core::NGX_CONF_ERROR;
            }
            let Some(index) = main.rules.iter().position(|r| r.name.as_ref() == rule_name) else {
                conf_error!(
                    cf,
                    code = "limit_unknown_rule",
                    "`gabion_limit` references rule `{}`, which no `gabion_limit_rule` in this \
                     http {{}} block declares. Usually a typo, or a rule in a file this block \
                     does not include. Declare it or correct the name.",
                    rule_name,
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
            conf_error!(
                cf,
                code = "gabion_arg_count",
                "`gabion` takes one argument. Write `gabion on;` or `gabion off;`.",
            );
            return core::NGX_CONF_ERROR;
        }
        let Ok(value) = (*args.add(1)).to_str() else {
            return arg_not_utf8(cf, "gabion");
        };
        match value {
            "on" => location.off = Some(false),
            "off" => location.off = Some(true),
            other => {
                conf_error!(
                    cf,
                    code = "gabion_bad_value",
                    "`gabion {}` is not a valid value: `gabion` only switches gabion on or off \
                     here, so a rule name belongs in `gabion_limit`. Write `gabion on;` or \
                     `gabion off;`, then run `nginx -t`.",
                    other,
                );
                return core::NGX_CONF_ERROR;
            }
        }
        core::NGX_CONF_OK
    }
}

/// nginx directive handler for every setting's directive: parses the
/// argument into the setting `commands` pointed `post` at. Invoked once per
/// occurrence from the master process during the config phase; nginx has
/// already checked that there is exactly one argument.
extern "C" fn set_setting(
    cf: *mut ngx_conf_t,
    command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    // SAFETY: nginx single-threadedly invokes this handler during config
    // parsing with `command` pointing at this module's own table entry, whose
    // `post` `commands` pointed at an element of the `'static`
    // `Setting::ALL`, and `conf` at the `MainConfig` slot `create_main_conf`
    // allocated (HttpModuleMainConf contract), so the read is valid and the
    // `&mut` borrow unique. `cf` satisfies `single_arg`'s contract.
    let (setting, main, value) = unsafe {
        (
            *(*command).post.cast::<Setting>(),
            &mut *conf.cast::<MainConfig>(),
            single_arg(cf),
        )
    };
    let Some(value) = value else {
        return arg_not_utf8(cf, setting.nginx_directive().unwrap_or(setting.yaml_key()));
    };
    match main.settings.apply(setting, value) {
        Ok(()) => core::NGX_CONF_OK,
        Err(error) => {
            conf_error!(cf, code = "directive_bad_value", "{}", error);
            core::NGX_CONF_ERROR
        }
    }
}

/// Fails a directive whose argument is not UTF-8 (`directive_arg_not_utf8`).
fn arg_not_utf8(cf: *mut ngx_conf_t, directive: &str) -> *mut c_char {
    conf_error!(
        cf,
        code = "directive_arg_not_utf8",
        "an argument of `{}` is not valid UTF-8, which gabion cannot read. Re-save the file as \
         UTF-8, then run `nginx -t`.",
        directive,
    );
    core::NGX_CONF_ERROR
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
    #[error("the template is not valid; check its `$variable` names and quoting")]
    ComplexCompile,
    #[error("nginx could not allocate memory for it in the configuration pool")]
    OutOfMemory,
}

impl BindingCompiler for NgxBindingCompiler {
    type Error = NgxBindingError;

    fn compile(&mut self, source: &str) -> Result<BindingLookup, NgxBindingError> {
        // 1. Inline fast path (`$uri`, `$args`, `$arg_*`, …).
        if let Some(b) = crate::rules::compile_inline(source) {
            return Ok(b);
        }
        // 2. Single $identifier: resolve through nginx's indexed-variable
        //    table. Hashing happens inside the call once at config phase;
        //    per-request lookups are O(1).
        if let Some(stripped) = source.strip_prefix('$')
            && crate::rules::is_single_ident(stripped)
        {
            let mut name_str = ngx_str_t {
                len: stripped.len(),
                data: stripped.as_ptr() as *mut _,
            };
            // SAFETY: `self.cf` is valid for the duration of
            // `postconfiguration`. `name_str` borrows from `source`
            // which lives at least as long as this call. nginx hashes
            // and copies the name internally.
            let index = unsafe { ngx::ffi::ngx_http_get_variable_index(self.cf, &mut name_str) };
            if index == ngx::core::Status::NGX_ERROR.0 {
                return Err(NgxBindingError::OutOfMemory);
            }
            return Ok(BindingLookup::IndexedVariable {
                name: stripped.into(),
                index: index as i64,
            });
        }
        // 3. Template: compile to an ngx_http_complex_value_t in the cycle
        //    pool. Allocates two structs (the input ccv and the output
        //    complex_value) against `cf->pool`.
        // SAFETY: cf is valid; `pool` is the cycle pool; sizeof return
        // values are POD.
        let cv_size = std::mem::size_of::<ngx::ffi::ngx_http_complex_value_t>();
        let cv_ptr: *mut ngx::ffi::ngx_http_complex_value_t =
            unsafe { ngx::ffi::ngx_palloc((*self.cf).pool, cv_size).cast() };
        if cv_ptr.is_null() {
            return Err(NgxBindingError::OutOfMemory);
        }
        let mut value_str = ngx_str_t {
            len: source.len(),
            data: source.as_ptr() as *mut _,
        };
        // SAFETY: `ngx_http_compile_complex_value_t` is a C struct of
        // pointers, integers and bitfields, for which all-zero is a valid
        // value; nginx's own modules initialise it the same way
        // (`ngx_memzero`).
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
            return Err(NgxBindingError::ComplexCompile);
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
/// call.
unsafe extern "C" fn gabion_init_process(cycle: *mut ngx_cycle_t) -> ngx_int_t {
    // Workers inherit the global tracing dispatch via fork; re-install is
    // idempotent and covers any path that bypassed `preconfiguration`.
    log::install();

    // SAFETY: nginx passes a valid cycle, per the doc above.
    let Some(main) = Module::main_conf(unsafe { &*cycle }) else {
        return core::Status::NGX_OK.into();
    };
    let Some((region, rules, (gossip, storage))) = main.active() else {
        return core::Status::NGX_OK.into();
    };

    let worker_id = std::process::id();
    let cfg = LeaderConfig {
        worker_id,
        gossip: gossip.clone(),
        cell_store: *storage,
        lease_tick: leader::DEFAULT_LEASE_TICK,
    };

    let handle = match leader::spawn(region, rules.clone(), cfg) {
        Ok(h) => h,
        Err(error) => {
            // Panicking here would unwind into nginx's C frame, so log and
            // leave the worker up without a leader thread.
            tracing::error!(
                code = "leader_spawn_failed",
                worker_id,
                %error,
                "gabion: failed to spawn the leader thread. Usually the host \
                 hit RLIMIT_NPROC, ran out of memory, or returned EAGAIN \
                 under fork pressure. This worker is staying up without a \
                 leader thread — requests still admit against the existing \
                 SHM aggregate but this worker can never drive gossip. Raise \
                 the process / thread limit for the nginx user, or check \
                 `dmesg` for OOM signals.",
            );
            return core::Status::NGX_OK.into();
        }
    };
    if let Ok(mut slot) = LEADER_THREAD.lock() {
        *slot = Some(handle);
    }
    watch_for_shutdown();
    tracing::info!(
        code = "leader_thread_spawned",
        worker_id,
        "gabion: leader thread spawned"
    );
    core::Status::NGX_OK.into()
}

/// How often a worker checks whether nginx has begun shutting it down.
const SHUTDOWN_POLL: Duration = leader::DEFAULT_LEASE_TICK;

/// Tell this worker's leader thread to stop as soon as a graceful shutdown
/// begins (`nginx -s quit`, or the old generation on reload). Without it
/// the thread keeps leading, or keeps campaigning and can win the lease
/// ahead of a worker that's staying up, until the worker exits, which can
/// take until `worker_shutdown_timeout` while connections drain.
///
/// Must be called on the worker's event thread (`init_process`). nginx has
/// no shutdown hook, so this polls `ngx_exiting` on a cancelable timer,
/// which never holds up an exiting worker.
fn watch_for_shutdown() {
    ngx::async_::spawn(async {
        // SAFETY: the task only runs on the worker's event thread, the only
        // thread that writes `ngx_exiting`; this is a plain by-value read.
        while unsafe { ngx_exiting } == 0 {
            ngx::async_::sleep(SHUTDOWN_POLL).await;
        }
        if let Ok(slot) = LEADER_THREAD.lock()
            && let Some(handle) = slot.as_ref()
        {
            tracing::info!(
                code = "leader_thread_stopping",
                "gabion: worker is shutting down; stopping its leader thread"
            );
            handle.request_stop();
        }
    })
    .detach();
}

/// nginx `exit_process` hook. Invoked once per worker process when nginx is
/// shutting that worker down. `cycle` follows the same contract as in
/// `gabion_init_process`.
unsafe extern "C" fn gabion_exit_process(cycle: *mut ngx_cycle_t) {
    // SAFETY: nginx passes a valid cycle, per `gabion_init_process`.
    let Some((region, ..)) = Module::main_conf(unsafe { &*cycle }).and_then(MainConfig::active)
    else {
        return;
    };
    // Stop the leader thread before anything else: it renews the lease and
    // holds the gossip socket until told to stop, so joining it without the
    // stop signal blocks this worker's exit forever.
    let handle = LEADER_THREAD.lock().ok().and_then(|mut slot| slot.take());
    if let Some(handle) = handle
        && let Err(payload) = handle.stop()
    {
        // `Debug` on a panic payload prints only `Any { .. }`.
        let panic = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("panic payload is not a string");
        tracing::error!(
            code = "leader_panicked",
            panic,
            "gabion: leader thread panicked, so this worker had stopped campaigning for the \
             leader lease before it exited. Other workers still campaign and take over gossip. \
             This is a gabion bug: report it with the `panic` field and the panic location from \
             earlier error log lines.",
        );
    }
    // The thread releases the lease on its way out; this covers a thread
    // that panicked while holding it.
    region.lease().release(std::process::id());
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
