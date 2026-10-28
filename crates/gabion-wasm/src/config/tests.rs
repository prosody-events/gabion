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
