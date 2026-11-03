use std::num::NonZeroU8;

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

/// The zone count is bounded by the built-in name table.
#[test]
fn zone_count_is_bounded() {
    let zones = |count: usize| SimConfig {
        zones: NonZeroU8::new(count as u8).unwrap(),
        ..SimConfig::default()
    };
    for count in 1..=MAX_ZONES {
        let valid = zones(count).validate().expect("in range");
        assert_eq!(valid.zone_names.len(), count, "{count} zones");
    }
    assert!(matches!(
        zones(MAX_ZONES + 1).validate(),
        Err(ConfigError::ZonesOutOfRange { got }) if usize::from(got) == MAX_ZONES + 1
    ));
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
