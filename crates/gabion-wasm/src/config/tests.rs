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
                fanout: 0,
                ..SimConfig::default()
            },
            SettingError::NotPositive {
                setting: Setting::GossipFanout,
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
