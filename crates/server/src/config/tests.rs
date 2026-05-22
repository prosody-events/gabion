//! Env-var tests serialize through a single mutex because `std::env` is
//! process-global; running tests in parallel without the lock can let
//! one test see another's env state and fail in non-obvious ways.

use super::*;
use std::sync::Mutex;

/// Held for the duration of any test that mutates the env. Guarantees
/// `set_env` / `clear_all` see a consistent view of the process env.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Remove every env var declared in [`ENV_BINDINGS`]. Run at the start
/// of each test so leftovers from a prior test (in the same binary
/// run) can't leak across cases.
fn clear_all_env() {
    for binding in ENV_BINDINGS {
        // SAFETY: serialized through `ENV_LOCK`; no concurrent access.
        unsafe { std::env::remove_var(binding.env_name) };
    }
}

/// Set an env var. Asserts the key is a known binding so a typo in the
/// test surfaces immediately instead of silently doing nothing.
fn set_env(key: &str, value: &str) {
    assert!(
        ENV_BINDINGS.iter().any(|b| b.env_name == key),
        "test set unknown env var: {key} (add it to ENV_BINDINGS)",
    );
    // SAFETY: serialized through `ENV_LOCK`; no concurrent access.
    unsafe { std::env::set_var(key, value) };
}

/// RAII wrapper that clears every gabion env var on drop, so a panicking
/// test still cleans up after itself.
struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl EnvGuard {
    fn lock() -> Self {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_all_env();
        Self { _lock }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        clear_all_env();
    }
}

#[test]
fn defaults_apply_when_neither_yaml_nor_env_set_a_value() {
    let _env = EnvGuard::lock();

    let cfg = AppConfig::load(None).expect("load with neither yaml nor env");

    assert_eq!(cfg.envoy_bind, None);
    assert_eq!(
        cfg.storage.rule_dictionary_capacity,
        defaults::STORAGE_RULE_DICTIONARY_CAPACITY
    );
    assert_eq!(cfg.gossip.fanout, defaults::GOSSIP_FANOUT);
    assert_eq!(cfg.gossip.target_err_bps, defaults::GOSSIP_TARGET_ERR_BPS);
    assert_eq!(
        cfg.gossip.min_emit_interval,
        Duration::from_millis(defaults::GOSSIP_MIN_EMIT_INTERVAL_MS)
    );
    assert!(cfg.discovery.namespace_allow.is_empty());
    assert_eq!(cfg.runtime.rng_seed, None);
    assert_eq!(
        cfg.cardinality_limits().max_descriptor_bytes,
        defaults::STORAGE_MAX_DESCRIPTOR_BYTES
    );
    assert_eq!(
        cfg.cell_store_config().cell_capacity,
        defaults::STORAGE_MAX_CELLS as u32
    );
}

#[test]
fn yaml_values_load_when_no_env_overrides() {
    let _env = EnvGuard::lock();

    let cfg = AppConfig::load_with_yaml_str(
        "envoy_bind: \"127.0.0.1:8000\"\nstorage:\n  max_cells: 256\n  rule_dictionary_capacity: \
         8\ngossip:\n  bind: \"127.0.0.1:9000\"\n  fanout: 3\n  target_err_bps: 250\n  \
         min_emit_interval: 7ms\n",
    )
    .expect("load yaml");

    assert_eq!(cfg.envoy_bind, Some("127.0.0.1:8000".parse().unwrap()));
    assert_eq!(cfg.storage.max_cells, Some(256));
    assert_eq!(cfg.storage.rule_dictionary_capacity, 8);
    assert_eq!(cfg.gossip.fanout, 3);
    assert_eq!(cfg.gossip.target_err_bps, 250);
    assert_eq!(cfg.gossip.min_emit_interval, Duration::from_millis(7));
}

#[test]
fn env_overrides_yaml_for_scalars() {
    let _env = EnvGuard::lock();
    set_env("GABION_STORAGE_MAX_CELLS", "9999");
    set_env("GABION_GOSSIP_FANOUT", "12");
    set_env("GABION_GOSSIP_TARGET_ERR_BPS", "250");

    let cfg = AppConfig::load_with_yaml_str(
        "storage:\n  max_cells: 256\n  rule_dictionary_capacity: 8\ngossip:\n  fanout: 3\n  \
         target_err_bps: 100\n",
    )
    .expect("load yaml + env");

    assert_eq!(cfg.storage.max_cells, Some(9999));
    assert_eq!(cfg.gossip.fanout, 12);
    assert_eq!(cfg.gossip.target_err_bps, 250);
    // Untouched YAML value stays.
    assert_eq!(cfg.storage.rule_dictionary_capacity, 8);
}

#[test]
fn env_only_with_no_yaml_file() {
    let _env = EnvGuard::lock();
    set_env("GABION_STORAGE_MAX_CELLS", "5555");
    set_env("GABION_ENVOY_BIND", "0.0.0.0:8081");
    set_env("GABION_RUNTIME_RNG_SEED", "42");

    let cfg = AppConfig::load(None).expect("load env-only");

    assert_eq!(cfg.storage.max_cells, Some(5555));
    assert_eq!(cfg.envoy_bind, Some("0.0.0.0:8081".parse().unwrap()));
    assert_eq!(cfg.runtime.rng_seed, Some(42));
}

#[test]
fn comma_separated_lists_split_into_vec() {
    let _env = EnvGuard::lock();
    set_env("GABION_DISCOVERY_NAMESPACE_ALLOW", "ns-a,ns-b,ns-c");
    set_env("GABION_DISCOVERY_SERVICE_ALLOW", "svc-1,svc-2");

    let cfg = AppConfig::load(None).expect("load env-only with lists");

    assert_eq!(
        cfg.discovery.namespace_allow,
        ["ns-a", "ns-b", "ns-c"].map(String::from),
    );
    assert_eq!(
        cfg.discovery.service_allow,
        ["svc-1", "svc-2"].map(String::from),
    );
}

#[test]
fn list_parsing_trims_whitespace_and_skips_empties() {
    let _env = EnvGuard::lock();
    set_env(
        "GABION_DISCOVERY_NAMESPACE_ALLOW",
        " ns-a , ns-b ,, ns-c , ",
    );

    let cfg = AppConfig::load(None).expect("load env list");

    assert_eq!(
        cfg.discovery.namespace_allow,
        ["ns-a", "ns-b", "ns-c"].map(String::from),
    );
}

#[test]
fn duration_env_uses_humantime_syntax() {
    let _env = EnvGuard::lock();
    set_env("GABION_GOSSIP_TICK_INTERVAL", "250ms");
    set_env("GABION_GOSSIP_MIN_EMIT_INTERVAL", "10ms");

    let cfg = AppConfig::load(None).expect("load tick_interval from env");

    assert_eq!(cfg.gossip.tick_interval, Duration::from_millis(250));
    assert_eq!(cfg.gossip.min_emit_interval, Duration::from_millis(10));
}

#[test]
fn bad_scalar_env_value_returns_error_not_panic() {
    let _env = EnvGuard::lock();
    set_env("GABION_STORAGE_MAX_CELLS", "not_a_number");

    let err = AppConfig::load(None).expect_err("non-integer max_cells should error");

    assert!(
        err.to_string().contains("max_cells"),
        "error should name the offending key, got: {err}",
    );
}

#[test]
fn env_binding_names_use_single_underscores_only() {
    for binding in ENV_BINDINGS {
        assert!(
            binding.env_name.starts_with("GABION_"),
            "{} should be GABION_-prefixed",
            binding.env_name,
        );
        assert!(
            !binding.env_name.contains("__"),
            "{} contains a double underscore",
            binding.env_name,
        );
    }
}
