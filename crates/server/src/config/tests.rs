//! Env-var tests serialize through a single mutex because `std::env` is
//! process-global; running tests in parallel without the lock can let
//! one test see another's env state and fail in non-obvious ways.

use super::*;
use gabion::settings::Millis;
use std::sync::Mutex;

/// Held for the duration of any test that mutates the env. Guarantees
/// `set_env` / `clear_all` see a consistent view of the process env.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Remove every setting's env var. Run at the start of each test so
/// leftovers from a prior test (in the same binary run) can't leak across
/// cases.
fn clear_all_env() {
    for setting in Setting::ALL {
        // SAFETY: serialized through `ENV_LOCK`; no concurrent access.
        unsafe { std::env::remove_var(setting.env_var()) };
    }
}

/// Set an env var. Asserts the key is a setting's so a typo in the test
/// surfaces immediately instead of silently doing nothing.
fn set_env(key: &str, value: &str) {
    assert!(
        Setting::ALL.iter().any(|setting| setting.env_var() == key),
        "test set unknown env var: {key}",
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

fn ms(millis: u64) -> Millis {
    Millis(millis.try_into().unwrap())
}

#[test]
fn defaults_apply_when_neither_yaml_nor_env_set_a_value() {
    let _env = EnvGuard::lock();

    let cfg = AppConfig::load(None).expect("load with neither yaml nor env");

    assert_eq!(cfg.settings, Settings::default());
    assert!(cfg.limits.is_empty());
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

    let settings = &cfg.settings;
    assert_eq!(settings.envoy_bind, Some("127.0.0.1:8000".parse().unwrap()));
    assert_eq!(settings.max_cells.get(), 256);
    assert_eq!(settings.rule_dictionary_capacity.get(), 8);
    assert_eq!(settings.fanout.get(), 3);
    assert_eq!(settings.target_err_bps, 250);
    assert_eq!(settings.min_emit_interval, ms(7));
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

    assert_eq!(cfg.settings.max_cells.get(), 9999);
    assert_eq!(cfg.settings.fanout.get(), 12);
    assert_eq!(cfg.settings.target_err_bps, 250);
    // Untouched YAML value stays.
    assert_eq!(cfg.settings.rule_dictionary_capacity.get(), 8);
}

#[test]
fn env_only_with_no_yaml_file() {
    let _env = EnvGuard::lock();
    set_env("GABION_STORAGE_MAX_CELLS", "5555");
    set_env("GABION_ENVOY_BIND", "0.0.0.0:8081");
    set_env("GABION_RUNTIME_RNG_SEED", "42");

    let cfg = AppConfig::load(None).expect("load env-only");

    assert_eq!(cfg.settings.max_cells.get(), 5555);
    assert_eq!(
        cfg.settings.envoy_bind,
        Some("0.0.0.0:8081".parse().unwrap())
    );
    assert_eq!(cfg.settings.rng_seed, Some(42));
}

#[test]
fn comma_separated_lists_split_into_a_list() {
    let _env = EnvGuard::lock();
    set_env("GABION_DISCOVERY_NAMESPACE_ALLOW", "ns-a,ns-b,ns-c");
    set_env("GABION_DISCOVERY_SERVICE_ALLOW", "svc-1,svc-2");

    let cfg = AppConfig::load(None).expect("load env-only with lists");

    assert_eq!(
        &*cfg.settings.namespace_allow,
        ["ns-a", "ns-b", "ns-c"].map(Box::from),
    );
    assert_eq!(
        &*cfg.settings.service_allow,
        ["svc-1", "svc-2"].map(Box::from)
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
        &*cfg.settings.namespace_allow,
        ["ns-a", "ns-b", "ns-c"].map(Box::from),
    );
}

#[test]
fn duration_env_uses_humantime_syntax() {
    let _env = EnvGuard::lock();
    set_env("GABION_GOSSIP_TICK_INTERVAL", "250ms");
    set_env("GABION_GOSSIP_MIN_EMIT_INTERVAL", "10ms");

    let cfg = AppConfig::load(None).expect("load tick_interval from env");

    assert_eq!(cfg.settings.tick_interval, ms(250));
    assert_eq!(cfg.settings.min_emit_interval, ms(10));
}

#[test]
fn bad_scalar_env_value_returns_error_not_panic() {
    let _env = EnvGuard::lock();
    set_env("GABION_STORAGE_MAX_CELLS", "not_a_number");

    let err = AppConfig::load(None).expect_err("non-integer max_cells should error");

    assert!(
        matches!(
            err,
            ConfigError::Setting(SettingError::Malformed {
                setting: Setting::StorageMaxCells,
                ..
            })
        ),
        "{err:?}"
    );
}

/// A valid value for `setting`, different from its default.
fn sample(setting: Setting) -> &'static str {
    match setting {
        Setting::EnvoyBind => "127.0.0.1:8000",
        Setting::AdminBind => "127.0.0.1:9090",
        Setting::StorageMaxCells => "4096",
        Setting::StorageRuleDictionaryCapacity => "8",
        Setting::StorageNodeDictionaryCapacity => "16",
        Setting::StorageLocalDirtyCapacity => "128",
        Setting::StorageForwardedDirtyCapacity => "256",
        Setting::StoragePeerCapacity => "32",
        Setting::StorageMaxDescriptorCount => "8",
        Setting::StorageMaxDescriptorBytes => "256",
        Setting::StorageMaxKeyBytes => "64",
        Setting::RuntimeNodeIdSeed => "pod-7",
        Setting::RuntimeRngSeed => "42",
        Setting::GossipBind => "127.0.0.1:9000",
        Setting::GossipTickInterval => "250ms",
        Setting::GossipFanout => "5",
        Setting::GossipMaxPayloadBytes => "1200",
        Setting::GossipMaxCellsPerFrame => "1024",
        Setting::GossipMaxCellsPerTick => "2048",
        Setting::GossipLimitQueueCapacity => "1024",
        Setting::GossipRecvBufferBytes => "65536",
        Setting::GossipSendBufferBytes => "2097152",
        Setting::GossipClusterIdHash => "0xc0ffee",
        Setting::GossipTargetErrBps => "250",
        Setting::GossipMinEmitInterval => "10ms",
        Setting::DiscoverySelfAddr => "10.0.0.1:9000",
        Setting::DiscoveryNamespaceAllow => "team-a",
        Setting::DiscoveryServiceAllow => "gabion",
    }
}

/// Every setting's documented environment variable and YAML key reaches
/// `Settings::apply`.
#[test]
fn setting_table_spellings_parse() {
    for &setting in Setting::ALL {
        let mut expected = Settings::default();
        expected
            .apply(setting, sample(setting))
            .expect("valid sample");
        assert_ne!(expected, Settings::default(), "{setting:?}");

        // The documented spelling, derived independently of the table.
        let env_var = format!(
            "GABION_{}",
            setting.yaml_key().replace('.', "_").to_uppercase()
        );
        let _env = EnvGuard::lock();
        set_env(&env_var, sample(setting));
        let from_env = AppConfig::load(None).unwrap_or_else(|error| panic!("{env_var}: {error}"));
        assert_eq!(from_env.settings, expected, "{env_var}");
        clear_all_env();

        let value = format!("\"{}\"", sample(setting));
        let value = if setting.is_list() {
            format!("[{value}]")
        } else {
            value
        };
        let yaml = match setting.yaml_key().split_once('.') {
            Some((block, key)) => format!("{block}:\n  {key}: {value}\n"),
            None => format!("{}: {value}\n", setting.yaml_key()),
        };
        let from_yaml =
            AppConfig::load_with_yaml_str(&yaml).unwrap_or_else(|error| panic!("{yaml}: {error}"));
        assert_eq!(from_yaml.settings, expected, "{yaml}");
    }
}

#[test]
fn unknown_yaml_key_fails_naming_it() {
    let _env = EnvGuard::lock();
    let rule = "limits:\n  - name: per_ip\n    domain: nginx\n    rate: 10r/s\n";
    for (yaml, key) in [
        (
            "gossip:\n  tick_intervl: 1s\n".to_owned(),
            "gossip.tick_intervl",
        ),
        ("gosip:\n  bind: 0.0.0.0:9000\n".to_owned(), "gosip.bind"),
        ("gosip:\n".to_owned(), "gosip"),
        ("gossip:\n  fanoutt:\n".to_owned(), "gossip.fanoutt"),
        (
            format!("{rule}    descriptors: [{{ key: ip }}]\n    ratee: 5r/s\n"),
            "limits[0].ratee",
        ),
        (
            format!("{rule}    descriptors: [{{ key: ip, vaule: \"*\" }}]\n"),
            "limits[0].descriptors[0].vaule",
        ),
    ] {
        let err = AppConfig::load_with_yaml_str(&yaml).expect_err(key);
        assert!(
            matches!(err, ConfigError::UnknownKey(ref unknown) if unknown == key),
            "{yaml}: {err:?}"
        );
    }
    let err = AppConfig::load_with_yaml_str("gossip:\n  tick_intervl: 1s\n").unwrap_err();
    assert_eq!(
        err.to_string(),
        "The configuration file sets `gossip.tick_intervl`, which gabiond does not know. This key \
         was removed or renamed in this release, or it is misspelled; see the release notes for \
         its replacement."
    );
}

/// Each YAML shape either loads as the setting's value or fails naming the
/// setting, never a rounded or silently dropped value.
#[test]
fn yaml_value_shapes_load_or_fail_typed() {
    let _env = EnvGuard::lock();
    let loaded = |yaml: &str| AppConfig::load_with_yaml_str(yaml).map(|cfg| cfg.settings);
    let wrong_shape = |yaml: &str, expected: Setting| match loaded(yaml) {
        Err(ConfigError::WrongShape { setting, .. }) => assert_eq!(setting, expected, "{yaml}"),
        other => panic!("{yaml}: {other:?}"),
    };

    wrong_shape(
        "gossip:\n  cluster_id_hash: 18446744073709551615\n",
        Setting::GossipClusterIdHash,
    );
    wrong_shape("gossip:\n  fanout: [1]\n", Setting::GossipFanout);
    let quoted = loaded("gossip:\n  cluster_id_hash: \"18446744073709551615\"\n").expect("quoted");
    assert_eq!(quoted.cluster_id_hash.get(), u128::from(u64::MAX));
    let bind = loaded("gossip:\n  bind:\n").expect("null bind");
    assert_eq!(bind.gossip_bind, None);
    let allow = loaded("discovery:\n  namespace_allow: team-a\n").expect("scalar list");
    assert_eq!(&*allow.namespace_allow, ["team-a"].map(Box::from));
    assert!(
        matches!(
            loaded("gossip:\n  max_payload_bytes: 100\n"),
            Err(ConfigError::Setting(SettingError::PayloadOutOfRange { .. }))
        ),
        "load validates the settings"
    );
}

#[test]
fn env_list_replaces_the_yaml_list() {
    let _env = EnvGuard::lock();
    set_env("GABION_DISCOVERY_NAMESPACE_ALLOW", "b,c");

    let cfg = AppConfig::load_with_yaml_str("discovery:\n  namespace_allow: [a]\n")
        .expect("load yaml + env list");

    assert_eq!(&*cfg.settings.namespace_allow, ["b", "c"].map(Box::from));
}

#[test]
fn duplicate_rule_names_are_rejected() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/s\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         5r/s\n",
    )
    .expect("yaml parses");
    let err = cfg.rule_table().expect_err("duplicate rules must error");
    assert!(
        matches!(err, ConfigError::DuplicateRule { ref name } if name == "per_ip"),
        "expected DuplicateRule, got {err:?}",
    );
}

#[test]
fn zero_rate_is_rejected_in_yaml() {
    let _env = EnvGuard::lock();
    for rate in ["0r/s", "10r/500us"] {
        let cfg = AppConfig::load_with_yaml_str(&format!(
            "limits:\n  - name: doomed\n    domain: nginx\n    descriptors: [{{ key: ip }}]\n    \
             rate: {rate}\n",
        ))
        .expect("yaml parses");
        let err = cfg.rule_table().expect_err("zero rate must error");
        assert!(
            matches!(err, ConfigError::InvalidRate { ref name, .. } if name == "doomed"),
            "{rate}: expected InvalidRate, got {err:?}",
        );
    }
}

#[test]
fn invalid_descriptor_key_rejected_in_yaml() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: bad_key\n    domain: nginx\n    descriptors: [{ key: \"with space\" \
         }]\n    rate: 10r/s\n",
    )
    .expect("yaml parses");
    let err = cfg
        .rule_table()
        .expect_err("invalid descriptor key must error");
    assert!(
        matches!(err, ConfigError::InvalidDescriptorKey { ref rule, .. } if rule == "bad_key"),
        "expected InvalidDescriptorKey, got {err:?}",
    );
}

#[test]
fn rate_only_yaml_infers_window_and_bucket() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/m\n",
    )
    .expect("yaml parses");
    let table = cfg.rule_table().expect("rule table");
    let spec = table.iter().next().expect("one rule").spec();
    assert_eq!(spec.limit, 10);
    assert_eq!(spec.window_millis, 60_000);
    assert_eq!(spec.bucket_millis, 60_000);
    assert_eq!(spec.live_buckets, 1);
}

#[test]
fn explicit_window_scales_limit_up() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/s\n    window: 5h\n",
    )
    .expect("yaml parses");
    let table = cfg.rule_table().expect("rule table");
    let spec = table.iter().next().expect("one rule").spec();
    assert_eq!(spec.limit, 10 * 5 * 3600);
    assert_eq!(spec.window_millis, 5 * 3600 * 1_000);
    // bucket defaults to the resolved window: one fixed-window bucket.
    assert_eq!(spec.bucket_millis, 5 * 3600 * 1_000);
    assert_eq!(spec.live_buckets, 1);
}

#[test]
fn explicit_window_and_bucket_set_live_buckets() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/s\n    window: 5h\n    bucket: 1h\n",
    )
    .expect("yaml parses");
    let table = cfg.rule_table().expect("rule table");
    let spec = table.iter().next().expect("one rule").spec();
    assert_eq!(spec.limit, 10 * 5 * 3600);
    assert_eq!(spec.bucket_millis, 3600 * 1_000);
    assert_eq!(spec.live_buckets, 5);
}

#[test]
fn bucket_defaults_to_window_when_omitted() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/m\n",
    )
    .expect("yaml parses");
    let table = cfg.rule_table().expect("rule table");
    let spec = table.iter().next().expect("one rule").spec();
    assert_eq!(spec.bucket_millis, 60_000);
    assert_eq!(spec.live_buckets, 1);
}

#[test]
fn explicit_bucket_overrides_window() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: per_ip\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         10r/m\n    bucket: 1s\n",
    )
    .expect("yaml parses");
    let table = cfg.rule_table().expect("rule table");
    let spec = table.iter().next().expect("one rule").spec();
    assert_eq!(spec.bucket_millis, 1_000);
    assert_eq!(spec.live_buckets, 60);
}

#[test]
fn cluster_id_hash_accepts_integer_decimal_and_hex_strings() {
    // Regression: a YAML integer, a decimal string and a hex string all
    // reach the cluster id's parser, so `cluster_id_hash: 1` loads.
    let _env = EnvGuard::lock();

    let integer_cfg = AppConfig::load_with_yaml_str("gossip:\n  cluster_id_hash: 1\n")
        .expect("integer cluster_id_hash should load");
    assert_eq!(integer_cfg.settings.cluster_id_hash.get(), 1);

    let decimal_cfg = AppConfig::load_with_yaml_str("gossip:\n  cluster_id_hash: \"42\"\n")
        .expect("decimal-string cluster_id_hash should load");
    assert_eq!(decimal_cfg.settings.cluster_id_hash.get(), 42);

    let hex_cfg = AppConfig::load_with_yaml_str("gossip:\n  cluster_id_hash: \"0xff\"\n")
        .expect("hex-string cluster_id_hash should load");
    assert_eq!(hex_cfg.settings.cluster_id_hash.get(), 0xff);

    // Above-u64 values must round-trip when expressed as a hex string —
    // YAML's integer scalar can't hold a u128 max anyway, but the env-
    // and string-shaped paths can.
    let big_cfg = AppConfig::load_with_yaml_str(
        "gossip:\n  cluster_id_hash: \"0xffffffffffffffffffffffffffffffff\"\n",
    )
    .expect("u128 max hex string should load");
    assert_eq!(big_cfg.settings.cluster_id_hash.get(), u128::MAX);
}

#[test]
fn cluster_id_hash_rejects_zero() {
    // nginx's `gabion_gossip_cluster` refuses `0`, so gabiond must too or
    // the two adapters could never be configured into one cluster.
    let _env = EnvGuard::lock();
    for yaml in [
        "gossip:\n  cluster_id_hash: 0\n",
        "gossip:\n  cluster_id_hash: \"0x0\"\n",
    ] {
        assert!(
            AppConfig::load_with_yaml_str(yaml).is_err(),
            "{yaml:?} should be rejected"
        );
    }
}

#[test]
fn cluster_id_hash_from_env_var() {
    // The env layer always presents the value as a string.
    let _env = EnvGuard::lock();
    set_env("GABION_GOSSIP_CLUSTER_ID_HASH", "0xdeadbeef");

    let cfg = AppConfig::load(None).expect("env-only cluster_id_hash should load");
    assert_eq!(cfg.settings.cluster_id_hash.get(), 0xdeadbeef);
}

#[test]
fn bad_rate_string_is_rejected() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: bad\n    domain: nginx\n    descriptors: [{ key: ip }]\n    rate: \
         100r/fortnight\n",
    )
    .expect("yaml parses");
    let err = cfg.rule_table().expect_err("bad rate must error");
    assert!(
        matches!(err, ConfigError::InvalidRate { ref name, .. } if name == "bad"),
        "expected InvalidRate, got {err:?}",
    );
}

#[test]
fn window_shorter_than_period_is_rejected() {
    let _env = EnvGuard::lock();
    let cfg = AppConfig::load_with_yaml_str(
        "limits:\n  - name: inverted\n    domain: nginx\n    descriptors: [{ key: ip }]\n    \
         rate: 10r/m\n    window: 500ms\n",
    )
    .expect("yaml parses");
    let err = cfg.rule_table().expect_err("sub-period window must error");
    assert!(
        matches!(
            err,
            ConfigError::ResolveRate {
                ref name,
                source: gabion::rules::RateResolveError::WindowShorterThanPeriod,
            } if name == "inverted"
        ),
        "expected ResolveRate(WindowShorterThanPeriod), got {err:?}",
    );
}
