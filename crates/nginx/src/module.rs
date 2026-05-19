use std::ffi::{c_char, c_void};
use std::sync::atomic::{AtomicU64, Ordering};

use ngx::core;
use ngx::ffi::{
    NGX_CONF_TAKE1, NGX_CONF_TAKE2, NGX_HTTP_LOC_CONF, NGX_HTTP_LOC_CONF_OFFSET,
    NGX_HTTP_MAIN_CONF, NGX_HTTP_MAIN_CONF_OFFSET, NGX_HTTP_MODULE, ngx_array_push, ngx_command_t,
    ngx_conf_t, ngx_http_handler_pt, ngx_http_module_t, ngx_http_phases_NGX_HTTP_ACCESS_PHASE,
    ngx_int_t, ngx_module_t, ngx_uint_t,
};
use ngx::http::{
    self, HttpModule, HttpModuleLocationConf, HttpModuleMainConf, MergeConfigError,
    NgxHttpCoreModule,
};
use ngx::{http_request_handler, ngx_log_debug_http, ngx_string};

struct Module;

static REQUEST_LIMIT: AtomicU64 = AtomicU64::new(2);
static REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Default)]
struct MainConfig {
    zones: usize,
    rules: usize,
}

#[derive(Debug, Default)]
struct LocationConfig {
    enabled: bool,
}

unsafe impl HttpModuleMainConf for Module {
    type MainConf = MainConfig;
}

unsafe impl HttpModuleLocationConf for Module {
    type LocationConf = LocationConfig;
}

static mut NGX_HTTP_GABION_COMMANDS: [ngx_command_t; 4] = [
    ngx_command_t {
        name: ngx_string!("gossip_limit_zone"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2) as ngx_uint_t,
        set: Some(set_zone),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: std::ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gossip_limit_rule"),
        type_: (NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2) as ngx_uint_t,
        set: Some(set_rule),
        conf: NGX_HTTP_MAIN_CONF_OFFSET,
        offset: 0,
        post: std::ptr::null_mut(),
    },
    ngx_command_t {
        name: ngx_string!("gossip_limit"),
        type_: (NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1) as ngx_uint_t,
        set: Some(set_limit),
        conf: NGX_HTTP_LOC_CONF_OFFSET,
        offset: 0,
        post: std::ptr::null_mut(),
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
    ctx: std::ptr::addr_of!(NGX_HTTP_GABION_MODULE_CTX) as _,
    commands: unsafe { &raw mut NGX_HTTP_GABION_COMMANDS[0] },
    type_: NGX_HTTP_MODULE as _,
    ..ngx_module_t::default()
};

impl http::Merge for LocationConfig {
    fn merge(&mut self, previous: &LocationConfig) -> Result<(), MergeConfigError> {
        if previous.enabled {
            self.enabled = true;
        }
        Ok(())
    }
}

http_request_handler!(gabion_access_handler, |request: &mut http::Request| {
    let Some(config) = Module::location_conf(request) else {
        return core::Status::NGX_DECLINED;
    };
    if config.enabled {
        ngx_log_debug_http!(request, "gabion rate limit access hook enabled");
        let count = REQUEST_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count > REQUEST_LIMIT.load(Ordering::Relaxed) {
            return http::HTTPStatus::TOO_MANY_REQUESTS.into();
        }
    }
    core::Status::NGX_DECLINED
});

extern "C" fn set_zone(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if !args.is_null() && (*(*cf).args).nelts == 3 {
            main.zones = main.zones.saturating_add(1);
            return core::NGX_CONF_OK;
        }
    }
    core::NGX_CONF_ERROR
}

extern "C" fn set_rule(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let main = &mut *(conf as *mut MainConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if !args.is_null() && (*(*cf).args).nelts == 3 {
            if let Ok(rate) = (*args.add(2)).to_str()
                && let Some(limit) = parse_rate(rate.as_bytes())
            {
                REQUEST_LIMIT.store(limit, Ordering::Relaxed);
                REQUEST_COUNT.store(0, Ordering::Relaxed);
            }
            main.rules = main.rules.saturating_add(1);
            return core::NGX_CONF_OK;
        }
    }
    core::NGX_CONF_ERROR
}

fn parse_rate(input: &[u8]) -> Option<u64> {
    let mut value = 0_u64;
    let mut digits = 0_u8;

    for byte in input {
        if !byte.is_ascii_digit() {
            break;
        }
        value = value.checked_mul(10)?;
        value = value.checked_add(u64::from(byte - b'0'))?;
        digits = digits.checked_add(1)?;
    }

    if digits == 0 { None } else { Some(value) }
}

extern "C" fn set_limit(
    cf: *mut ngx_conf_t,
    _command: *mut ngx_command_t,
    conf: *mut c_void,
) -> *mut c_char {
    unsafe {
        let location = &mut *(conf as *mut LocationConfig);
        let args = (*(*cf).args).elts as *mut ngx::ffi::ngx_str_t;
        if !args.is_null() && (*(*cf).args).nelts == 2 {
            location.enabled = true;
            return core::NGX_CONF_OK;
        }
    }
    core::NGX_CONF_ERROR
}
