use super::*;

fn applied(key: Setting, text: &str) -> Result<Settings, SettingError> {
    let mut settings = Settings::default();
    settings.apply(key, text).map(|()| settings)
}

fn validated(tune: impl FnOnce(&mut Settings)) -> Result<(), SettingError> {
    let mut settings = Settings::default();
    tune(&mut settings);
    settings.validate().map(drop)
}

/// Every setting that is not a count, capacity or duration, or whose zero
/// [`Settings::validate`] rejects as out of range. Listed here, not read off
/// the table, so a count that loses its zero reason fails the test.
const ZERO_PARSES: &[Setting] = &[
    Setting::EnvoyBind,
    Setting::AdminBind,
    Setting::RuntimeNodeIdSeed,
    Setting::GossipBind,
    Setting::GossipMaxPayloadBytes,
    Setting::GossipClusterIdHash,
    Setting::GossipTargetErrBps,
    Setting::GossipZone,
    Setting::GossipBootstrapPeers,
    Setting::DiscoverySelfAddr,
    Setting::DiscoveryNamespaceAllow,
    Setting::DiscoveryServiceAllow,
];

#[test]
fn zero_is_not_positive() {
    for &setting in Setting::ALL {
        let Some(reason) = setting.zero_reason() else {
            assert!(
                ZERO_PARSES.contains(&setting),
                "{setting:?} has no zero reason"
            );
            assert!(
                !matches!(applied(setting, "0"), Err(SettingError::NotPositive { .. })),
                "{setting:?} rejects 0 without a reason"
            );
            continue;
        };
        let zeros: &[&str] = match setting {
            Setting::GossipTickInterval
            | Setting::GossipMinEmitInterval
            | Setting::GossipSuspectAfter
            | Setting::GossipZoneGrace => &["0", "0ms"],
            _ => &["0"],
        };
        for zero in zeros {
            let error = applied(setting, zero).expect_err(setting.yaml_key());
            assert_eq!(error, SettingError::NotPositive { setting });
            let text = error.to_string();
            let default = setting.default_text().unwrap_or_default();
            for part in [setting.yaml_key(), setting.env_var(), reason, &default] {
                assert!(text.contains(part), "{text:?} lacks {part:?}");
            }
            match setting.nginx_directive() {
                Some(directive) => {
                    assert!(text.contains(directive), "{text:?} lacks {directive:?}")
                }
                None => assert!(!text.contains("NGINX"), "{text:?} names an NGINX directive"),
            }
        }
    }
    assert_eq!(
        applied(Setting::GossipTickInterval, "0")
            .unwrap_err()
            .to_string(),
        "`gossip.tick_interval` is 0, but it must be at least 1: gossip needs a positive interval \
         between heartbeats. Remove the setting to use the default, 500ms, or set a positive \
         value (`GABION_GOSSIP_TICK_INTERVAL`; NGINX `gabion_gossip_tick_interval`)."
    );
    assert_eq!(
        applied(Setting::StorageRuleDictionaryCapacity, "0")
            .unwrap_err()
            .to_string(),
        "`storage.rule_dictionary_capacity` is 0, but it must be at least 1: gabion could not \
         register any rate-limit rule; the default is twice the configured rules, at least 256. \
         Remove the setting to use the default, or set a positive value \
         (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`; NGINX \
         `gabion_storage_rule_dictionary_capacity`)."
    );
}

#[test]
fn malformed_names_the_setting_and_an_example() {
    for (setting, text) in [
        (Setting::GossipTickInterval, "fast"),
        (Setting::GossipTickInterval, "1500us"),
        (Setting::GossipBind, "nope"),
        (Setting::GossipClusterIdHash, "0"),
        (Setting::StorageMaxCells, "-1"),
        (Setting::DiscoveryNamespaceAllow, "Not_A_Label"),
        (Setting::RuntimeNodeIdSeed, ""),
    ] {
        assert!(
            matches!(
                applied(setting, text),
                Err(SettingError::Malformed { setting: s, ref value, .. })
                    if s == setting && &**value == text
            ),
            "{setting:?} {text:?}"
        );
    }
    assert_eq!(
        applied(Setting::GossipBind, "nope")
            .unwrap_err()
            .to_string(),
        "`gossip.bind` is `nope`, which is not a socket address, for example `0.0.0.0:9000`. \
         Correct it (`GABION_GOSSIP_BIND`; NGINX `gabion_gossip_bind`) or remove it to leave it \
         unset."
    );
    assert_eq!(
        applied(Setting::GossipTickInterval, "fast")
            .unwrap_err()
            .to_string(),
        "`gossip.tick_interval` is `fast`, which is not a duration in whole milliseconds, for \
         example `500ms`. Correct it (`GABION_GOSSIP_TICK_INTERVAL`; NGINX \
         `gabion_gossip_tick_interval`) or remove it to use the default, 500ms."
    );
}

#[test]
fn cluster_id_accepts_decimal_and_prefixed_hex() {
    let parse = |text| applied(Setting::GossipClusterIdHash, text).map(|s| s.cluster_id_hash.get());
    assert_eq!(parse("32201612552594806"), Ok(32_201_612_552_594_806));
    assert_eq!(parse("0xc0ffee"), Ok(0x00c0_ffee));
    assert_eq!(parse("0XC0FFEE"), Ok(0x00c0_ffee));
    assert_eq!(parse(" 1 "), Ok(1));
    assert_eq!(parse("0xffffffffffffffffffffffffffffffff"), Ok(u128::MAX));
}

#[test]
fn cluster_id_rejects_zero_signs_and_garbage() {
    for text in [
        "0",
        "0x0",
        "+1",
        "0x+1",
        "-1",
        "c0ffee",
        "0x",
        "",
        "0xg1",
        "1e3",
        "0x100000000000000000000000000000000",
        "340282366920938463463374607431768211456",
    ] {
        assert!(
            applied(Setting::GossipClusterIdHash, text).is_err(),
            "{text:?} should not parse"
        );
    }
}

#[test]
fn namespace_allow_takes_dns_labels_one_per_apply() {
    let mut settings = Settings::default();
    for label in ["team-a", "0ns", "a"] {
        settings
            .apply(Setting::DiscoveryNamespaceAllow, label)
            .expect(label);
    }
    let longest = "a".repeat(63);
    settings
        .apply(Setting::DiscoveryNamespaceAllow, &longest)
        .expect("63 characters");
    assert_eq!(
        &*settings.namespace_allow,
        ["team-a", "0ns", "a", &longest].map(Box::from)
    );
    for label in [
        "UPPER",
        "-lead",
        "trail-",
        "under_score",
        "dot.ted",
        &"a".repeat(64),
    ] {
        assert!(
            settings
                .apply(Setting::DiscoveryNamespaceAllow, label)
                .is_err(),
            "{label:?} is not a DNS label"
        );
    }
}

#[test]
fn payload_outside_158_to_65507_fails_validation() {
    let payload = |bytes| validated(|s| s.max_payload_bytes = bytes);
    let error = payload(157).unwrap_err();
    assert_eq!(
        error,
        SettingError::PayloadOutOfRange {
            configured: 157,
            min: 158,
            max: 65_507
        }
    );
    assert_eq!(payload(158), Ok(()));
    assert_eq!(payload(65_507), Ok(()));
    assert!(payload(65_508).is_err());
    assert_eq!(
        error.to_string(),
        "The gossip packet size is 157 bytes, but gabion needs between 158 and 65507: 158 bytes \
         hold the packet header and one counter, and 65507 is the largest UDP payload over IPv4. \
         Set `gossip.max_payload_bytes` (`GABION_GOSSIP_MAX_PAYLOAD_BYTES`; NGINX \
         `gabion_gossip_max_payload_bytes`) inside that range, and to the same value on every \
         node. The default, 1400, fits a standard 1,500-byte Ethernet path."
    );
}

#[test]
fn error_budget_outside_1_to_10000_fails() {
    let budget = |bps| validated(|s| s.target_err_bps = bps);
    assert_eq!(
        budget(0),
        Err(SettingError::ErrorBudgetOutOfRange { configured: 0 })
    );
    assert!(budget(10_001).is_err());
    assert_eq!(budget(1), Ok(()));
    assert_eq!(budget(10_000), Ok(()));
    assert_eq!(
        budget(0).unwrap_err().to_string(),
        "The gossip error budget is 0 basis points, outside 1 to 10000 (0.01 % to 100 % of a \
         rule's limit). Set `gossip.target_err_bps` (`GABION_GOSSIP_TARGET_ERR_BPS`; NGINX \
         `gabion_gossip_target_err_bps`) in that range; the default, 100, is 1 % of each rule's \
         limit."
    );
}

fn ms(millis: u64) -> Millis {
    Millis(millis.try_into().unwrap())
}

#[test]
fn emit_interval_at_or_above_the_heartbeat_fails() {
    let emit = |millis| {
        validated(|s| {
            s.tick_interval = ms(100);
            s.min_emit_interval = ms(millis);
        })
    };
    let error = emit(100).unwrap_err();
    assert_eq!(
        error,
        SettingError::EmitIntervalOutOfRange {
            configured: ms(100),
            tick: ms(100)
        }
    );
    assert_eq!(emit(99), Ok(()));
    assert!(emit(101).is_err());
    assert_eq!(
        error.to_string(),
        "`gossip.min_emit_interval` (`GABION_GOSSIP_MIN_EMIT_INTERVAL`; NGINX \
         `gabion_gossip_min_emit_interval`) is 100ms, but it must be at least 1ms and shorter \
         than `gossip.tick_interval` (100ms). It is the shortest gap between two gossip sends \
         other than the heartbeat. At or above the heartbeat, every change would wait for the \
         heartbeat and limits would be enforced late. Set it below 100ms, or remove it to use the \
         default of 5ms."
    );
}

#[test]
fn capacity_outside_its_range_fails() {
    let capacity = |setting: Setting, value: u64| {
        applied(setting, &value.to_string()).and_then(|s| s.validate().map(drop))
    };
    for (setting, min, max) in [
        (Setting::StorageMaxCells, 1, 1 << 30),
        (Setting::GossipLimitQueueCapacity, 64, 1 << 20),
        (
            Setting::GossipRecvBufferBytes,
            u64::from(MIN_SOCKET_BUFFER_BYTES.get()),
            MAX_SOCKET_BUFFER_BYTES,
        ),
        (
            Setting::GossipSendBufferBytes,
            u64::from(MIN_SOCKET_BUFFER_BYTES.get()),
            MAX_SOCKET_BUFFER_BYTES,
        ),
        (Setting::StorageRuleDictionaryCapacity, 1, 65_534),
        (Setting::GossipPeerCapacity, 1, 32_767),
        (Setting::StorageNodeDictionaryCapacity, 513, 65_534),
    ] {
        assert_eq!(capacity(setting, min), Ok(()), "{setting:?} {min}");
        assert_eq!(capacity(setting, max), Ok(()), "{setting:?} {max}");
        for configured in [min - 1, max + 1].into_iter().filter(|&v| v > 0) {
            assert!(
                matches!(
                    capacity(setting, configured),
                    Err(SettingError::CapacityOutOfRange { setting: s, configured: c, min: lo, max: hi, .. })
                        if (s, c, lo, hi) == (setting, configured, min, max)
                ),
                "{setting:?} {configured}"
            );
        }
    }
    assert_eq!(
        capacity(Setting::GossipLimitQueueCapacity, 63)
            .unwrap_err()
            .to_string(),
        "`gossip.limit_queue_capacity` is 63; gabion supports 64 to 1048576. The default is 8192. \
         Set it inside that range (`GABION_GOSSIP_LIMIT_QUEUE_CAPACITY`; NGINX \
         `gabion_gossip_limit_queue_capacity`). It may be 64 to 1,048,576; each slot is 72 bytes \
         of the NGINX zone (72 MiB at the ceiling), so raise SIZE in `gabion_limit_zone` with it."
    );
    assert_eq!(
        capacity(Setting::StorageRuleDictionaryCapacity, 65_535)
            .unwrap_err()
            .to_string(),
        "`storage.rule_dictionary_capacity` is 65535; gabion supports 1 to 65534. Set it inside \
         that range (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`; NGINX \
         `gabion_storage_rule_dictionary_capacity`). Its slots are numbered by a `u16` whose \
         largest value marks an empty slot, so it holds at most 65,534 rules. Remove it to use \
         the default, twice the configured rules and at least 256."
    );
    assert_eq!(
        capacity(Setting::StorageNodeDictionaryCapacity, 512)
            .unwrap_err()
            .to_string(),
        "`storage.node_dictionary_capacity` is 512; gabion supports 513 to 65534. Set it inside \
         that range (`GABION_STORAGE_NODE_DICTIONARY_CAPACITY`; NGINX \
         `gabion_storage_node_dictionary_capacity`). It must exceed `gossip.peer_capacity`, \
         because every peer and this node need an entry, and its slots are numbered by a `u16` \
         whose largest value marks an empty slot, so it holds at most 65,534. Remove it to use \
         the default, twice `gossip.peer_capacity`."
    );
}

/// The node dictionary defaults to twice the peer table; setting it wins.
#[test]
fn node_dictionary_defaults_to_twice_the_peer_table_and_honours_an_override() {
    let nodes = |peers: &str, over: Option<&str>| {
        let mut s = applied(Setting::GossipPeerCapacity, peers).unwrap();
        if let Some(n) = over {
            s.apply(Setting::StorageNodeDictionaryCapacity, n).unwrap();
        }
        s.validate().unwrap().1.node_dictionary_capacity().get()
    };
    assert_eq!(nodes("100", None), 200);
    assert_eq!(nodes("100", Some("150")), 150);
}

/// The per-request bounds have no build-time cap: each takes any value its
/// `u16` holds, including a descriptor count of 64, above `main`'s cap of 16.
#[test]
fn admission_bounds_have_no_build_cap() {
    for setting in [
        Setting::StorageMaxDescriptorCount,
        Setting::StorageMaxMatchedRules,
        Setting::StorageMaxRequestDescriptors,
    ] {
        for value in [1, 64, 65_535] {
            let (_, storage) = applied(setting, &value.to_string())
                .and_then(|s| s.validate())
                .unwrap_or_else(|error| panic!("{setting:?} {value}: {error}"));
            let validated = match setting {
                Setting::StorageMaxDescriptorCount => storage.cardinality.max_descriptor_count(),
                Setting::StorageMaxMatchedRules => storage.max_matched_rules().get(),
                _ => storage.cardinality.max_request_descriptors(),
            };
            assert_eq!(validated, value, "{setting:?}");
        }
        assert!(
            matches!(
                applied(setting, "65536"),
                Err(SettingError::Malformed { .. })
            ),
            "{setting:?}"
        );
    }
}

/// Every message's "use the default, …" clause names text that parses back
/// to the default.
#[test]
fn every_printed_default_parses_back_to_the_default() {
    for &setting in Setting::ALL {
        if let Some(text) = setting.default_text() {
            assert_eq!(
                applied(setting, &text),
                Ok(Settings::default()),
                "{setting:?}"
            );
        }
    }
}

#[test]
fn defaults_validate_to_the_production_values() {
    let (gossip, storage) = Settings::default().validate().expect("defaults validate");
    assert_eq!(
        gossip.tick_interval.0.get(),
        defaults::GOSSIP_TICK_INTERVAL_MILLIS
    );
    assert_eq!(
        gossip.min_emit_interval.0.get(),
        defaults::GOSSIP_MIN_EMIT_INTERVAL_MS
    );
    assert_eq!(
        Some(gossip.wire_limits),
        FrameLimits::new(defaults::GOSSIP_MAX_PAYLOAD_BYTES)
    );
    // ⌈131,072 / 128⌉ cells, above the 33 a 1,400-byte packet holds.
    assert_eq!(gossip.quantum.get(), 1024);
    assert_eq!(
        gossip.sweep_concurrency.get(),
        defaults::GOSSIP_SWEEP_CONCURRENCY
    );
    #[cfg(feature = "transport-udp")]
    assert_eq!(
        gossip.recv_buffer_bytes.get(),
        defaults::GOSSIP_RECV_BUFFER_BYTES
    );
    assert_eq!(
        gossip.send_buffer_bytes.get(),
        defaults::GOSSIP_SEND_BUFFER_BYTES
    );
    assert_eq!(
        gossip.limit_queue_capacity.get(),
        defaults::GOSSIP_LIMIT_QUEUE_CAPACITY
    );
    assert_eq!(
        gossip.cluster_id_hash.get(),
        defaults::GOSSIP_CLUSTER_ID_HASH
    );
    assert_eq!(gossip.target_err_bps.get(), defaults::GOSSIP_TARGET_ERR_BPS);
    assert_eq!(
        storage.cell_capacity.get() as usize,
        defaults::STORAGE_MAX_CELLS
    );
    assert_eq!(storage.rule_dictionary_capacity, None);
    assert_eq!(
        storage.max_matched_rules().get(),
        usize::from(defaults::STORAGE_MAX_MATCHED_RULES)
    );
    assert_eq!(gossip.peer_capacity.get(), defaults::GOSSIP_PEER_CAPACITY);
    assert_eq!(
        storage.node_dictionary_capacity.get(),
        2 * defaults::GOSSIP_PEER_CAPACITY
    );
    assert_eq!(
        storage.cardinality,
        CardinalityLimits {
            max_descriptor_count: defaults::STORAGE_MAX_DESCRIPTOR_COUNT.into(),
            max_descriptor_bytes: defaults::STORAGE_MAX_DESCRIPTOR_BYTES,
            max_key_bytes: defaults::STORAGE_MAX_KEY_BYTES,
            max_request_descriptors: defaults::STORAGE_MAX_REQUEST_DESCRIPTORS.into(),
        }
    );
    assert_eq!(gossip.zone_grace.0.get(), defaults::GOSSIP_ZONE_GRACE_MS);
    assert_eq!(gossip.zone, None);
    assert_eq!(gossip.membership, Membership::Alone);
}

/// U17: a zone setting that is not a zone name fails to load, an empty one
/// included, naming the reason and every spelling; a valid one loads.
#[test]
fn invalid_zone_names_the_reason_and_both_spellings() {
    let invalid = |text: &str, reason| {
        let error = applied(Setting::GossipZone, text).expect_err(text);
        assert_eq!(
            error,
            SettingError::InvalidZone {
                value: text.into(),
                reason
            }
        );
        error.to_string()
    };
    let text = invalid("", ZoneError::Empty);
    for part in [
        "gossip.zone",
        "GABION_GOSSIP_ZONE",
        "gabion_gossip_zone",
        "it is empty",
        "use1-az2",
    ] {
        assert!(text.contains(part), "{text:?} lacks {part:?}");
    }
    invalid("bad zone", ZoneError::BadChar { char: ' ' });
    invalid(&"a".repeat(64), ZoneError::TooLong { len: 64 });
    let valid = applied(Setting::GossipZone, "use1-az2").expect("a zone name");
    assert_eq!(valid.zone, ZoneName::parse("use1-az2").ok());
}

/// U17: a static list longer than the peer table fails validation, naming
/// both settings; one that fits validates.
#[test]
fn too_many_bootstrap_peers_fails_validation() {
    let peers = |n: u8| -> Box<[SocketAddr]> {
        (1..=n)
            .map(|i| SocketAddr::from(([192, 0, 2, i], 9000)))
            .collect()
    };
    let tune = |n| {
        move |s: &mut Settings| {
            s.peer_capacity = NonZeroU16::new(2).unwrap();
            s.bootstrap_peers = peers(n);
            s.gossip_bind = Some(SocketAddr::from(([192, 0, 2, 1], 9000)));
        }
    };
    let error = validated(tune(3)).expect_err("three peers");
    assert_eq!(
        error,
        SettingError::TooManyBootstrapPeers {
            listed: 3,
            capacity: NonZeroU16::new(2).unwrap()
        }
    );
    let text = error.to_string();
    for part in [
        "gossip.bootstrap_peers",
        "gossip.peer_capacity",
        "GABION_GOSSIP_PEER_CAPACITY",
        "at least 3",
    ] {
        assert!(text.contains(part), "{text:?} lacks {part:?}");
    }
    assert_eq!(validated(tune(2)), Ok(()));
}

/// Without a gossip bind a node is alone, and a static list fails validation,
/// naming both settings; with one, a static list makes the membership static
/// and an empty one Kubernetes, whose own entry `discovery.self_addr` matches
/// when set. The zone settings reach the runtime's config.
#[test]
fn membership_follows_bind_and_bootstrap_peers() {
    let bind = SocketAddr::from(([10, 0, 0, 1], 9000));
    let peer = SocketAddr::from(([10, 0, 0, 2], 9000));
    fn membership(tune: impl FnOnce(&mut Settings)) -> Membership {
        let mut settings = Settings::default();
        tune(&mut settings);
        settings.validate().expect("validates").0.membership
    }
    assert_eq!(membership(|_| {}), Membership::Alone);
    let error = validated(|s| s.bootstrap_peers = [peer].into()).expect_err("no bind");
    assert_eq!(error, SettingError::BootstrapPeersWithoutBind { listed: 1 });
    let text = error.to_string();
    for part in ["gossip.bind", "GABION_GOSSIP_BIND", "gabion_gossip_bind"] {
        assert!(text.contains(part), "{text:?} lacks {part:?}");
    }
    let kubernetes = membership(|s| {
        s.gossip_bind = Some(bind);
        s.self_addr = Some(bind);
        s.namespace_allow = ["team-a".into()].into();
        s.service_allow = ["gabion".into()].into();
    });
    let expected = DiscoveryConfig {
        self_match: Some(SelfMatch::Addr(bind)),
        namespace_allow: ["team-a".into()].into(),
        service_allow: ["gabion".into()].into(),
    };
    assert_eq!(kubernetes, Membership::Kubernetes(expected));
    let listed = membership(|s| {
        s.gossip_bind = Some(bind);
        s.bootstrap_peers = [bind, peer].into();
    });
    assert_eq!(listed, Membership::Static([bind, peer].into()));

    let mut settings = Settings::default();
    settings
        .apply(Setting::GossipZone, "use1-az2")
        .expect("zone");
    settings
        .apply(Setting::GossipZoneGrace, "3s")
        .expect("grace");
    let (gossip, _) = settings.validate().expect("validates");
    assert_eq!(gossip.zone, ZoneName::parse("use1-az2").ok());
    assert_eq!(gossip.zone_grace, ms(3_000));
}

/// Derives each environment variable and NGINX directive from the YAML key,
/// so a row misspelling one fails here.
#[test]
fn spellings_follow_the_yaml_key() {
    for (i, &setting) in Setting::ALL.iter().enumerate() {
        assert!(
            Setting::ALL[..i]
                .iter()
                .all(|earlier| earlier.yaml_key() != setting.yaml_key()),
            "{setting:?} repeats a YAML key"
        );
        let snake = setting.yaml_key().replace('.', "_");
        assert_eq!(
            setting.env_var(),
            format!("GABION_{}", snake.to_uppercase())
        );
        let directive = match setting {
            Setting::EnvoyBind | Setting::AdminBind | Setting::StorageMaxRequestDescriptors => None,
            Setting::GossipClusterIdHash => Some("gabion_gossip_cluster".to_owned()),
            Setting::RuntimeNodeIdSeed => Some("gabion_node_id_seed".to_owned()),
            _ => Some(format!("gabion_{snake}")),
        };
        assert_eq!(setting.nginx_directive().map(str::to_owned), directive);
        assert_eq!(
            setting
                .nginx_directive_cstr()
                .map(|name| name.to_str().unwrap()),
            setting.nginx_directive(),
        );
    }
}

/// The per-round quantum drains a full store in 128 rounds, and never sends
/// less than one full packet of the configured size: 33 cells fit 1,400
/// bytes and 233 fit 9,000.
#[test]
fn quantum_is_a_128th_of_the_store_and_at_least_one_packet() {
    for (max_cells, max_payload_bytes, quantum) in [
        (262_144, 1_400, 2048),
        (131_073, 1_400, 1025),
        (4_224, 1_400, 33),
        (4_225, 1_400, 34),
        (1_000, 1_400, 33),
        (1_000, 9_000, 233),
    ] {
        let settings = Settings {
            max_cells: NonZeroU32::new(max_cells).unwrap(),
            max_payload_bytes,
            ..Settings::default()
        };
        let (gossip, _) = settings.validate().expect("validates");
        assert_eq!(gossip.quantum.get(), quantum, "{max_cells} cells");
    }
}

/// A configured `gossip.suspect_after` must cover two heartbeats; the text
/// names every spelling and the bound.
#[test]
fn suspect_after_too_short_fails_and_two_heartbeats_pass() {
    let suspect = |millis| {
        validated(|s| {
            s.tick_interval = ms(100);
            s.suspect_after = Some(ms(millis));
        })
    };
    let error = suspect(199).unwrap_err();
    assert_eq!(
        error,
        SettingError::SuspectAfterTooShort {
            configured: ms(199),
            min: ms(200)
        }
    );
    let text = error.to_string();
    for part in [
        "`gossip.suspect_after`",
        "GABION_GOSSIP_SUSPECT_AFTER",
        "gabion_gossip_suspect_after",
        "200ms",
    ] {
        assert!(text.contains(part), "{text:?} lacks {part:?}");
    }
    assert_eq!(suspect(200), Ok(()));
}

/// Unset, `gossip.suspect_after` is four heartbeats and at least 2 s; a
/// configured value wins.
#[test]
fn suspect_after_defaults_to_four_heartbeats_and_at_least_2_s() {
    let suspect_after = |tick, configured: Option<u64>| {
        let mut settings = Settings {
            tick_interval: ms(tick),
            suspect_after: configured.map(ms),
            ..Settings::default()
        };
        settings.min_emit_interval = ms(1);
        settings.validate().unwrap().0.suspect_after
    };
    assert_eq!(suspect_after(100, None), ms(2_000));
    assert_eq!(suspect_after(500, None), ms(2_000));
    assert_eq!(suspect_after(1_000, None), ms(4_000));
    assert_eq!(suspect_after(100, Some(300)), ms(300));
}
