//! `tracing::Subscriber` that forwards every event into nginx's `error_log`.
//!
//! All logging in this crate (and in the `gabion::*` crates linked in via
//! the dynamic module) goes through the `tracing` facade. We don't pull in
//! `tracing-subscriber`; instead a tiny custom subscriber routes each event
//! through `ngx_log_error_core`, the idiomatic logging primitive nginx
//! exposes to modules. Operators see our logs in whatever target the
//! surrounding `error_log` directive points at, at the level it configures.
//!
//! The subscriber is installed once per process by [`install`]:
//! - The master calls it from `preconfiguration` so config-phase events
//!   (zone allocation, rule compile failures, …) route correctly.
//! - Workers inherit the global dispatch via `fork`; the OnceLock guard
//!   makes a redundant call from `init_process` a no-op.
//!
//! # Example output
//!
//! With `error_log /dev/stderr info;` in `nginx.conf` (the v1 deployment
//! default — see `deploy/nginx/nginx.module.conf`), a worker handling a
//! request that exercises the gabion module logs lines such as:
//!
//! ```text
//! 2026/05/21 12:34:56 [info]  1#1: gabion_nginx::module: gabion: zone allocated zone=requests bytes=1048576 queue=2048 aggregate=4096
//! 2026/05/21 12:34:57 [info] 12#0: gabion_nginx::module: gabion: leader thread spawned worker_id=12
//! 2026/05/21 12:34:58 [warn] 12#0: gabion_nginx::leader: peer discovery error error=connection refused
//! 2026/05/21 12:35:00 [info] 13#0: gabion::gossip::runtime: peer accepted addr=10.0.0.5:7234
//! ```
//!
//! nginx prepends the timestamp, `[level]`, and `pid#tid:`. Our subscriber
//! emits `<target>: <message> key=value …`, where `<target>` is the Rust
//! module path of the `tracing::*!` call site.
//!
//! # Unsafe and miri
//!
//! The single unsafe code path here is the body of `ngx::ngx_log_error!`,
//! which invokes `ngx_log_error_core` via FFI. miri can't run nginx's C
//! code, so this module is feature-gated to `ngx-module` (the same gate
//! that pulls in the `nginx-sys`/`ngx` deps) and is consequently absent
//! from the library-half build that `cargo +nightly miri test` exercises.
//! Every `unsafe` block in this crate that does *not* call into FFI is
//! covered by `tests/safety.rs`.

use std::fmt::{self, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Metadata, Subscriber};

/// Idempotently install the nginx-backed subscriber as the global default
/// dispatch. The first call wins; subsequent calls return silently.
pub fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        // Ignore a competing installer (e.g. embedded tests double-loading
        // the module): the goal is "some subscriber routed to nginx is
        // active", not "this exact subscriber won".
        let _ = tracing::subscriber::set_global_default(NginxSubscriber::new());
    });
}

struct NginxSubscriber {
    next_span_id: AtomicU64,
}

impl NginxSubscriber {
    fn new() -> Self {
        Self {
            next_span_id: AtomicU64::new(1),
        }
    }
}

impl Subscriber for NginxSubscriber {
    fn enabled(&self, _meta: &Metadata<'_>) -> bool {
        // Defer level filtering to nginx: `ngx_log_error!` short-circuits
        // on `(*log).log_level` inside its macro body. Pre-filtering here
        // would save the field-visit cost but would also require us to
        // dereference `ngx_cycle->log` directly — extra unsafe we don't
        // need given how low-volume our event sites are.
        true
    }

    fn new_span(&self, _attrs: &Attributes<'_>) -> Id {
        // tracing requires span IDs to be non-zero; AtomicU64 starts at 1.
        let n = self.next_span_id.fetch_add(1, Ordering::Relaxed);
        Id::from_u64(n.max(1))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        // The leader (a `std::thread::spawn`-ed worker) and the tokio tasks
        // living on its `LocalSet` can both emit events. Calling
        // `ngx_log_error_core` from a non-nginx thread is a benign data
        // race on nginx's cached log timestamp — worst case a line is
        // stamped a second stale. The file write itself is atomic for
        // line-sized payloads on the targets we ship with (the v1 deploy
        // uses `/dev/stderr`, see `deploy/nginx/nginx.module.conf`).
        let meta = event.metadata();
        let mut visitor = MsgVisitor::default();
        event.record(&mut visitor);
        let ngx_level = level_to_ngx(*meta.level());
        let log = ngx::log::ngx_cycle_log().as_ptr();
        ngx::ngx_log_error!(ngx_level, log, "{}: {}", meta.target(), visitor.render());
    }

    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

fn level_to_ngx(level: Level) -> ngx::ffi::ngx_uint_t {
    let v = match level {
        Level::ERROR => ngx::ffi::NGX_LOG_ERR,
        Level::WARN => ngx::ffi::NGX_LOG_WARN,
        Level::INFO => ngx::ffi::NGX_LOG_INFO,
        Level::DEBUG | Level::TRACE => ngx::ffi::NGX_LOG_DEBUG,
    };
    v as ngx::ffi::ngx_uint_t
}

/// Field visitor that flattens an event into a single `<message> key=value …`
/// line suitable for nginx's per-line `error_log` format. The implicit
/// `message` field is rendered without a `message=` prefix so the natural
/// reading order of `tracing::info!("text", k = v)` is preserved regardless
/// of the order tracing walks the fields in.
#[derive(Default)]
struct MsgVisitor {
    message: String,
    fields: String,
}

impl MsgVisitor {
    fn render(self) -> String {
        match (self.message.is_empty(), self.fields.is_empty()) {
            (true, true) => String::new(),
            (false, true) => self.message,
            (true, false) => self.fields,
            (false, false) => format!("{} {}", self.message, self.fields),
        }
    }

    fn append_field(&mut self, args: fmt::Arguments<'_>) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        let _ = self.fields.write_fmt(args);
    }
}

impl Visit for MsgVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            self.append_field(format_args!("{}={value:?}", field.name()));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            self.append_field(format_args!("{}={value}", field.name()));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.append_field(format_args!("{}={value}", field.name()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.append_field(format_args!("{}={value}", field.name()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.append_field(format_args!("{}={value}", field.name()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.append_field(format_args!("{}={value}", field.name()));
    }
}

#[cfg(test)]
mod tests {
    use super::MsgVisitor;
    use std::fmt::Write;

    #[test]
    fn message_renders_before_fields_regardless_of_arrival_order() {
        let mut v = MsgVisitor::default();
        // tracing walks fields in macro-argument order, which may put
        // structured kvs *before* the message. Verify the renderer puts
        // the message first either way.
        v.append_field(format_args!("{}={}", "key", "value"));
        let _ = write!(v.message, "the message");
        assert_eq!(v.render(), "the message key=value");
    }

    #[test]
    fn fields_only_event_renders_cleanly() {
        let mut v = MsgVisitor::default();
        v.append_field(format_args!("a={}", 1));
        v.append_field(format_args!("b={}", 2));
        assert_eq!(v.render(), "a=1 b=2");
    }

    #[test]
    fn message_only_event_renders_cleanly() {
        let mut v = MsgVisitor::default();
        let _ = write!(v.message, "hello");
        assert_eq!(v.render(), "hello");
    }
}
