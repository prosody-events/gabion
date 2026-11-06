use gabion::aggregate::Aggregate;
use gabion::settings::Millis;

use super::*;

/// The controls' gossip tuning reaches `Settings::apply` and
/// `Settings::validate`, so a bad value fails as an adapter's would.
#[test]
fn controls_fail_as_the_settings_table_fails() {
    let tick = SimConfig::default().tick_interval_ms;
    let heartbeat = Millis(tick.try_into().expect("positive default tick"));
    for (config, expected) in [
        (
            SimConfig {
                tick_interval_ms: 0,
                ..SimConfig::default()
            },
            SettingError::NotPositive {
                setting: Setting::GossipTickInterval,
            },
        ),
        (
            SimConfig {
                min_emit_interval_ms: tick,
                ..SimConfig::default()
            },
            SettingError::EmitIntervalOutOfRange {
                configured: heartbeat,
                tick: heartbeat,
            },
        ),
    ] {
        match config.validate().err() {
            Some(ConfigError::Setting(error)) => assert_eq!(error, expected),
            other => panic!("{expected:?}: {other:?}"),
        }
    }
}

/// The watched rule goes through `RuleDef::build`, so a zero limit, window
/// or bucket fails as an adapter's rule would.
#[test]
fn controls_fail_as_the_rule_constructor_fails() {
    for config in [
        SimConfig {
            rule_limit: 0,
            ..SimConfig::default()
        },
        SimConfig {
            rule_window_ms: 0,
            ..SimConfig::default()
        },
        SimConfig {
            rule_bucket_ms: 0,
            ..SimConfig::default()
        },
    ] {
        let fields = (
            config.rule_limit,
            config.rule_window_ms,
            config.rule_bucket_ms,
        );
        assert!(
            matches!(config.validate(), Err(ConfigError::Rule(_))),
            "{fields:?}"
        );
    }
}

/// The zone count is bounded by the built-in name table, and a count the
/// table cannot name, zero included, fails with the same message.
#[test]
fn zone_count_is_bounded() {
    let zones = |zones: usize| SimConfig {
        zones,
        ..SimConfig::default()
    };
    for count in 1..=MAX_ZONES {
        let valid = zones(count).validate().expect("in range");
        assert_eq!(valid.zone_names.len(), count, "{count} zones");
    }
    for count in [0, MAX_ZONES + 1, 300] {
        assert!(
            matches!(
                zones(count).validate(),
                Err(ConfigError::ZonesOutOfRange { got }) if got == count
            ),
            "{count} zones"
        );
    }
}

/// A shared URL's zone count reaches `validate` as it was written, so zero
/// gets the operator message rather than a deserializer's.
#[test]
fn zone_count_from_json_is_refused_by_validate() {
    let config: SimConfig = serde_json::from_str(r#"{ "zones": 0 }"#).expect("any count parses");
    let error = config.validate().err().expect("zero zones").to_string();
    assert!(
        error.contains(&format!("1 to {MAX_ZONES} zones")),
        "{error}"
    );
}

/// Every built-in zone name parses and hashes to its own value, which
/// `zone_index` in the engine relies on to name a peer's zone.
#[test]
fn built_in_zone_names_are_valid_and_hash_apart() {
    let mut hashes: Vec<_> = ZONE_NAMES
        .iter()
        .map(|name| ZoneName::parse(name).expect("a valid zone name").hash())
        .collect();
    hashes.sort_unstable();
    hashes.dedup();
    assert_eq!(hashes.len(), MAX_ZONES);
}

/// Each node's count table is sized for the watched keys, not per replica of
/// the cell store (1 MiB a node).
#[test]
fn aggregate_is_sized_for_the_watched_keys() {
    for window in [10_000, 60_000] {
        let config = SimConfig {
            rule_window_ms: window,
            ..SimConfig::default()
        };
        let cells = SLACK as usize * AGGREGATE_KEYS as usize * config.per_origin() as usize;
        let valid = config.validate().expect("the config validates");
        let slots = Aggregate::new(&valid.aggregate).table().capacity();
        assert!(slots <= 1_024, "{slots} slots at a {window} ms window");
        assert!(slots >= cells, "{slots} slots at a {window} ms window");
    }
}
