use gabion::settings::{Setting, SettingError, Settings};
use gabion::zone::ZoneError;

/// On `main` these directives turned 0 into 1 without a word. Through the
/// settings table a 0 now fails `nginx -t`: at the directive for a count, and
/// at validation for the packet size, whose zero is out of range.
#[test]
fn nginx_zero_is_rejected_not_clamped() {
    for directive in [
        "gabion_gossip_max_payload_bytes",
        "gabion_storage_max_cells",
        "gabion_storage_rule_dictionary_capacity",
        "gabion_storage_node_dictionary_capacity",
        "gabion_storage_max_descriptor_bytes",
        "gabion_storage_max_key_bytes",
        "gabion_storage_max_matched_rules",
    ] {
        let setting = self::directive(directive);
        let mut settings = Settings::default();
        let applied = settings.apply(setting, "0");
        if setting == Setting::GossipMaxPayloadBytes {
            assert_eq!(applied, Ok(()));
            assert!(
                matches!(
                    settings.validate(),
                    Err(SettingError::PayloadOutOfRange { configured: 0, .. })
                ),
                "{directive}"
            );
        } else {
            assert_eq!(
                applied,
                Err(SettingError::NotPositive { setting }),
                "{directive}"
            );
        }
    }
}

/// The setting a directive names.
fn directive(name: &str) -> Setting {
    *Setting::ALL
        .iter()
        .find(|setting| setting.nginx_directive() == Some(name))
        .expect(name)
}

/// A repeated `gabion_gossip_bootstrap_peers` appends each address in order
/// (the gabiond half is in its config tests).
#[test]
fn bootstrap_peers_parse_in_both_adapters() {
    let setting = directive("gabion_gossip_bootstrap_peers");
    let mut settings = Settings::default();
    for addr in ["10.0.0.2:9000", "10.0.0.3:9000"] {
        settings.apply(setting, addr).expect(addr);
    }
    let expected = ["10.0.0.2:9000", "10.0.0.3:9000"].map(|a| a.parse().unwrap());
    assert_eq!(&*settings.bootstrap_peers, expected);
}

/// U17 through NGINX: an empty or invalid `gabion_gossip_zone` fails
/// `nginx -t`.
#[test]
fn nginx_invalid_zone_fails_nginx_t() {
    let setting = directive("gabion_gossip_zone");
    for (text, reason) in [
        ("", ZoneError::Empty),
        ("bad zone", ZoneError::BadChar { char: ' ' }),
    ] {
        assert_eq!(
            Settings::default().apply(setting, text),
            Err(SettingError::InvalidZone {
                value: text.into(),
                reason
            })
        );
    }
}
