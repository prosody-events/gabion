use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use ngx::core;
use ngx::ffi::{
    NGX_CONF_1MORE, NGX_CONF_TAKE1, NGX_CONF_TAKE2, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, ngx_array_push, ngx_command_t,
    ngx_conf_t, ngx_http_handler_pt, ngx_http_module_t, ngx_http_phases_NGX_HTTP_ACCESS_PHASE,
    ngx_int_t, ngx_module_t, ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_log_debug_http, ngx_string};

use crate::{
    DEFAULT_MAX_ACTIVE_BUCKETS, EnforcementMode, KeyComponentList, MAX_KEY_COMPONENTS,
    MAX_NGINX_SHM_RULES, NginxConfigError, NginxRuleBuilder, NginxRuleConfig, NginxStatus,
    NginxVariableLookup, NginxZoneConfig, NgxShmAccessError, NgxShmStore, parse_duration_millis,
    parse_rate, parse_size_bytes,
};

struct Module;

const DEFAULT_ZONE_KEYS: usize = 1024;
const DEFAULT_WINDOW: &str = "60s";
const DEFAULT_BUCKET: &str = "1s";
const DEFAULT_STALE_AFTER: &str = "2s";
const DEFAULT_KEY_COMPONENTS: [&str; 1] = ["$uri"];

static SHM_PTR: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static SHM_LEN: AtomicUsize = AtomicUsize::new(0);

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
}

impl Default for MainConfig {
    fn default() -> Self {
        Self {
            zone: None,
            rules: [None; MAX_NGINX_SHM_RULES],
            rule_count: 0,
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

static mut NGX_HTTP_GABION_COMMANDS: [ngx_command_t; 6] = [
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

    let status = store.access(
        config.rule_index,
        &RequestVariables { request },
        request_time_millis(),
    );
    match status {
        Ok(NginxStatus::Declined) => {
            ngx_log_debug_http!(request, "gabion rate limit allowed");
            core::Status::NGX_DECLINED
        }
        Ok(NginxStatus::TooManyRequests) => http::HTTPStatus::TOO_MANY_REQUESTS.into(),
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
