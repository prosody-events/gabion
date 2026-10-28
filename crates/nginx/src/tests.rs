use gabion::settings::{Setting, SettingError, Settings};

/// On `main` these directives turned 0 into 1 without a word. Through the
/// settings table a 0 now fails `nginx -t`: at the directive for a count, and
/// at validation for the packet size, whose zero is out of range.
#[test]
fn nginx_zero_is_rejected_not_clamped() {
    for directive in [
        "gabion_gossip_fanout",
        "gabion_gossip_max_payload_bytes",
        "gabion_gossip_max_cells_per_tick",
        "gabion_storage_max_cells",
        "gabion_storage_rule_dictionary_capacity",
        "gabion_storage_node_dictionary_capacity",
        "gabion_storage_local_dirty_capacity",
        "gabion_storage_forwarded_dirty_capacity",
        "gabion_storage_max_descriptor_bytes",
        "gabion_storage_max_key_bytes",
        "gabion_storage_max_matched_rules",
    ] {
        let setting = *Setting::ALL
            .iter()
            .find(|setting| setting.nginx_directive() == Some(directive))
            .expect(directive);
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
