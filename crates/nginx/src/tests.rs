
use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

#[derive(Clone, Debug)]
struct NginxPeerTableCase {
    self_octet: u8,
    peers: Vec<u8>,
}

impl Arbitrary for NginxPeerTableCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut peers = Vec::<u8>::arbitrary(g);
        peers.truncate(MAX_NGINX_PEERS + 16);
        Self {
            self_octet: (u8::arbitrary(g) % 64).saturating_add(1),
            peers,
        }
    }
}

#[derive(Clone, Debug)]
struct NginxAccessCase {
    attempts: u8,
    missing_variable: bool,
}

impl Arbitrary for NginxAccessCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            attempts: (u8::arbitrary(g) % 16).max(1),
            missing_variable: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct NginxAggregateAccessCase {
    limit: u8,
    published_count: u8,
    key: u8,
    other_key: u8,
    expired: bool,
}

impl Arbitrary for NginxAggregateAccessCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let key = u8::arbitrary(g);
        let mut other_key = u8::arbitrary(g);
        if other_key == key {
            other_key = other_key.wrapping_add(1);
        }
        Self {
            limit: (u8::arbitrary(g) % 8).saturating_add(1),
            published_count: u8::arbitrary(g) % 16,
            key,
            other_key,
            expired: bool::arbitrary(g),
        }
    }
}

fn rule() -> NginxRuleConfig {
    NginxRuleBuilder {
        id: 1,
        name: "tenant_api",
        domain: "api",
        key_components: &["$tenant", "$uri"],
        limit: "10r/m",
        window: "60s",
        bucket: "1s",
        local_fallback: "3r/m",
        local_absolute: "6r/m",
        stale_after: "2s",
        mode: EnforcementMode::Enforce,
    }
    .build()
    .expect("rule")
}

fn shm_rule(name: &str, limit: &str, keys: &[&str]) -> NginxRuleConfig {
    NginxRuleBuilder {
        id: 1,
        name,
        domain: "nginx",
        key_components: keys,
        limit,
        window: "100ms",
        bucket: "10ms",
        local_fallback: limit,
        local_absolute: limit,
        stale_after: "2s",
        mode: EnforcementMode::Enforce,
    }
    .build()
    .expect("shared memory rule")
}

fn shm_store(bytes: &mut [u8], rule: NginxRuleConfig, max_keys: usize) -> NgxShmStore {
    let mut store = unsafe {
        NgxShmStore::initialize(
            bytes.as_mut_ptr(),
            bytes.len(),
            MAX_NGINX_SHM_RULES,
            max_keys,
            DEFAULT_MAX_ACTIVE_BUCKETS,
        )
    }
    .expect("initialize shared store");
    store.add_rule(0, rule).expect("add rule");
    store
}

#[derive(Clone, Copy)]
struct TestVariables<'a> {
    values: &'a [(&'a str, &'a [u8])],
}

impl NginxVariableLookup for TestVariables<'_> {
    fn value<'a>(&'a self, name: &str) -> Option<&'a [u8]> {
        self.values
            .iter()
            .find_map(|(key, value)| (*key == name).then_some(*value))
    }
}

#[test]
fn parses_nginx_rule_config() {
    let rule = rule();

    assert_eq!(rule.name.as_str(), "tenant_api");
    assert_eq!(rule.key_components.len(), 2);
    assert_eq!(rule.limit, 10);
    assert_eq!(rule.window_millis, 60_000);
}

#[test]
fn nginx_endpoint_slice_selector_defaults_empty_port_name_to_gossip() {
    let selector = NginxEndpointSliceSelector::new("default", "gabion-grpc", "").expect("selector");

    assert_eq!(selector.port_name.as_str(), "gossip");
}

#[test]
fn nginx_discovery_rejects_too_many_endpoint_slice_selectors() {
    let mut selectors = NginxEndpointSliceSelectors::empty();
    for index in 0..MAX_ENDPOINT_SLICE_SELECTORS {
        selectors
            .push(
                NginxEndpointSliceSelector::new(
                    "default",
                    "gabion-grpc",
                    &format!("gossip-{index}"),
                )
                .expect("selector"),
            )
            .expect("push selector");
    }

    let error = selectors.push(
        NginxEndpointSliceSelector::new("default", "gabion-nginx", "gossip").expect("selector"),
    );

    assert_eq!(error, Err(NginxConfigError::TooManyEndpointSliceSelectors));
}

#[test]
fn nginx_discovery_stores_static_and_file_modes_without_heap_state() {
    let self_addr = "127.0.0.1:9000".parse().expect("self addr");
    let peer_addr = "127.0.0.2:9000".parse().expect("peer addr");
    let mut discovery = NginxDiscoveryConfig::default();

    discovery.set_kind(DiscoveryMode::Static);
    discovery.set_self_addr(self_addr);
    discovery.add_static_peer(self_addr).expect("ignore self");
    discovery.add_static_peer(peer_addr).expect("static peer");
    discovery
        .set_peer_file_path("/etc/gabion/peers.txt")
        .expect("peer file path");

    assert_eq!(discovery.kind, DiscoveryMode::Static);
    assert_eq!(discovery.self_addr, Some(self_addr));
    assert_eq!(discovery.static_peers.len(), 1);
    assert_eq!(
        discovery.static_peers.as_slice()[0].socket_addr(),
        Some(peer_addr)
    );
    assert_eq!(discovery.peer_file_path.as_str(), "/etc/gabion/peers.txt");
}

#[test]
fn shared_memory_records_are_c_layout_copy_types() {
    assert_eq!(std::mem::size_of::<StoreHeader>(), 24);
    assert_eq!(std::mem::size_of::<StatsCounters>(), 32);
    assert_eq!(std::mem::size_of::<LeaderLease>(), 24);
    assert_eq!(std::mem::size_of::<SharedCountAggregateRecord>(), 48);
}

#[test]
fn shm_store_enqueues_requests_and_rejects_from_runtime_counts() {
    let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
        .expect("required bytes");
    let mut memory = vec![0_u8; bytes];
    let mut store = shm_store(&mut memory, shm_rule("tenant_api", "2r/m", &["$uri"]), 8);
    let variables = TestVariables {
        values: &[("uri", b"/api/a")],
    };

    assert_eq!(store.access(0, &variables, 0), Ok(NginxStatus::Declined));
    assert_eq!(store.access(0, &variables, 1), Ok(NginxStatus::Declined));

    let mut events = [RequestEvent::default(); 4];
    assert_eq!(store.drain_request_events(&mut events), 2);
    assert_eq!(events[0].rule_id, 1);
    assert_eq!(events[1].rule_id, 1);
    assert_eq!(events[0].key_hash, events[1].key_hash);

    assert_eq!(
        store.apply_count_aggregates(&[CountAggregate {
            rule_id: events[0].rule_id,
            key_hash: events[0].key_hash.into(),
            bucket_start_millis: 0,
            count: 2,
        }]),
        ApplyBatchOutcome {
            applied: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        store.access(0, &variables, 2),
        Ok(NginxStatus::TooManyRequests)
    );
}

#[test]
fn nginx_shared_count_handler_applies_runtime_aggregates_to_shared_memory() {
    let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 2, DEFAULT_MAX_ACTIVE_BUCKETS)
        .expect("required bytes");
    let mut memory = vec![0_u8; bytes];
    let store = shm_store(&mut memory, shm_rule("tenant_api", "2r/m", &["$uri"]), 2);
    let handler = unsafe { NginxSharedCountHandler::new(memory.as_mut_ptr(), memory.len()) };
    let aggregates = [
        CountAggregate {
            rule_id: 1,
            key_hash: 7_u128.into(),
            bucket_start_millis: 100,
            count: 3,
        },
        CountAggregate {
            rule_id: 1,
            key_hash: 10_u128.into(),
            bucket_start_millis: 100,
            count: 4,
        },
    ];

    assert_eq!(
        handler.apply_batch(&aggregates),
        ApplyBatchOutcome {
            applied: 2,
            dropped: 0
        }
    );

    let first = unsafe { *store.aggregate_ptr(0) }.as_aggregate();
    let second = unsafe { *store.aggregate_ptr(1) }.as_aggregate();
    assert_eq!(first, aggregates[0]);
    assert_eq!(second, aggregates[1]);

    assert_eq!(
        handler.apply_batch(&[CountAggregate {
            count: 8,
            ..aggregates[0]
        }]),
        ApplyBatchOutcome {
            applied: 1,
            dropped: 0
        }
    );
    assert_eq!(unsafe { *store.aggregate_ptr(0) }.count, 8);
}

#[test]
fn runtime_drain_records_request_ring_and_updates_shared_aggregates() {
    let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
        .expect("required bytes");
    let mut memory = vec![0_u8; bytes];
    let mut store = shm_store(&mut memory, shm_rule("tenant_api", "2r/m", &["$uri"]), 8);
    let variables = TestVariables {
        values: &[("uri", b"/api/a")],
    };
    assert_eq!(store.access(0, &variables, 0), Ok(NginxStatus::Declined));
    assert_eq!(store.access(0, &variables, 1), Ok(NginxStatus::Declined));

    let handler = unsafe { NginxSharedCountHandler::new(memory.as_mut_ptr(), memory.len()) };
    let runtime = Runtime::with_count_update_handler(
        gabion::Config {
            storage: gabion::StorageConfig {
                max_keys: 8,
                max_cells: Some(8 * DEFAULT_MAX_ACTIVE_BUCKETS),
                dirty_ring_entries: Some(8 * DEFAULT_MAX_ACTIVE_BUCKETS),
                max_descriptor_count: MAX_KEY_COMPONENTS,
                max_descriptor_bytes: MAX_NAME_BYTES * MAX_KEY_COMPONENTS,
                max_key_bytes: MAX_NAME_BYTES,
                max_active_buckets: DEFAULT_MAX_ACTIVE_BUCKETS,
            },
            limits: vec![gabion::LimitRuleConfig {
                name: "tenant_api".to_string(),
                domain: "nginx".to_string(),
                descriptors: vec![gabion::DescriptorConfig {
                    key: "$uri".to_string(),
                    value: "*".to_string(),
                }],
                limit: 2,
                window: "100ms".to_string(),
                bucket: "10ms".to_string(),
                local_fallback_limit: 2,
                local_absolute_limit: 2,
                stale_after: "2000ms".to_string(),
                safety_margin: gabion::SafetyMarginConfig::default(),
                overflow_policy: gabion::OverflowPolicy::UseOverflowKey,
                mode: EnforcementMode::Enforce,
            }],
            runtime: gabion::RuntimeConfig {
                count_update_batch_size: 2,
            },
            server: gabion::ServerConfig::default(),
            discovery: gabion::DiscoveryConfig::default(),
            gossip: gabion::GossipConfig::default(),
        },
        handler,
    )
    .expect("runtime");

    let mut events = [RequestEvent::default(); 1];
    let empty_request = TimedHashedLimitRequest::new(HashedLimitRequest::new(0, 0_u128, 1), 0);
    let mut requests = [empty_request; 1];
    let mut aggregates = [CountAggregate::default(); 1];

    assert_eq!(
        drain_request_events_into_runtime(
            &mut store,
            &runtime,
            &mut events,
            &mut requests,
            &mut aggregates,
        ),
        2
    );
    assert_eq!(store.drain_request_events(&mut events), 0);
    assert_eq!(
        store.access(0, &variables, 2),
        Ok(NginxStatus::TooManyRequests)
    );
}

#[test]
fn shm_store_handles_share_counts_across_workers() {
    let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
        .expect("required bytes");
    let mut memory = vec![0_u8; bytes];
    let mut first = shm_store(&mut memory, shm_rule("tenant_api", "1r/m", &["$uri"]), 8);
    let mut second = unsafe { NgxShmStore::from_initialized(memory.as_mut_ptr(), memory.len()) }
        .expect("second worker handle");
    let variables = TestVariables {
        values: &[("uri", b"/api/a")],
    };

    assert_eq!(first.access(0, &variables, 0), Ok(NginxStatus::Declined));
    let mut events = [RequestEvent::default(); 1];
    assert_eq!(first.drain_request_events(&mut events), 1);
    assert_eq!(
        first.apply_count_aggregates(&[CountAggregate {
            rule_id: events[0].rule_id,
            key_hash: events[0].key_hash.into(),
            bucket_start_millis: 0,
            count: 1,
        }]),
        ApplyBatchOutcome {
            applied: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        second.access(0, &variables, 1),
        Ok(NginxStatus::TooManyRequests)
    );
}

#[test]
fn shm_store_supports_ip_based_rate_limiting() {
    let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
        .expect("required bytes");
    let mut memory = vec![0_u8; bytes];
    let mut store = shm_store(
        &mut memory,
        shm_rule("ip_api", "1r/m", &["$remote_addr"]),
        8,
    );
    let first_ip = TestVariables {
        values: &[("remote_addr", b"192.0.2.1")],
    };
    let second_ip = TestVariables {
        values: &[("remote_addr", b"192.0.2.2")],
    };

    assert_eq!(store.access(0, &first_ip, 0), Ok(NginxStatus::Declined));
    assert_eq!(store.access(0, &second_ip, 1), Ok(NginxStatus::Declined));
    let mut events = [RequestEvent::default(); 2];
    assert_eq!(store.drain_request_events(&mut events), 2);
    assert_ne!(events[0].key_hash, events[1].key_hash);
    assert_eq!(
        store.apply_count_aggregates(&[CountAggregate {
            rule_id: events[0].rule_id,
            key_hash: events[0].key_hash.into(),
            bucket_start_millis: 0,
            count: 1,
        }]),
        ApplyBatchOutcome {
            applied: 1,
            dropped: 0,
        }
    );
    assert_eq!(
        store.access(0, &first_ip, 2),
        Ok(NginxStatus::TooManyRequests)
    );
}

#[test]
fn peer_table_parses_deduplicates_and_ignores_self() {
    let self_addr = "127.0.0.1:9000".parse().expect("addr");
    let table = NginxPeerTable::parse_lines(
        "
            # comment
            127.0.0.1:9000
            127.0.0.2:9000
            127.0.0.2:9000
            [::1]:9001
            ",
        Some(self_addr),
    )
    .expect("peers");

    assert_eq!(table.len(), 2);
    assert_eq!(
        table.as_slice()[0].socket_addr(),
        Some("127.0.0.2:9000".parse().expect("addr"))
    );
    assert_eq!(
        table.as_slice()[1].socket_addr(),
        Some("[::1]:9001".parse().expect("addr"))
    );
}

#[test]
fn peer_file_loads_through_caller_scratch_buffer() {
    let path = std::env::temp_dir().join(format!("gabion-nginx-peers-{}.txt", std::process::id()));
    std::fs::write(&path, "127.0.0.2:9000\n127.0.0.3:9000\n").expect("write peers");
    let mut scratch = [0_u8; 128];

    let table = load_peer_file(&path, &mut scratch, None).expect("load peers");
    let too_small = load_peer_file(&path, &mut scratch[..8], None);

    let _ = std::fs::remove_file(path);
    assert_eq!(table.len(), 2);
    assert_eq!(too_small, Err(NginxPeerConfigError::PeerFileTooLarge));
}

#[test]
fn leader_lease_allows_one_runtime_owner_and_expires() {
    let lease = SharedLeaderLease::default();

    assert!(lease.try_acquire(1, 100, 50));
    assert!(!lease.try_acquire(2, 110, 50));
    assert!(lease.try_acquire(1, 120, 50));
    assert_eq!(lease.snapshot().owner_worker, 1);
    assert!(lease.try_acquire(2, 171, 50));

    let snapshot = lease.snapshot();
    assert_eq!(snapshot.owner_worker, 2);
    assert_eq!(snapshot.epoch, 2);
}

#[quickcheck]
fn peer_table_property_sorts_deduplicates_excludes_self(case: NginxPeerTableCase) -> TestResult {
    let self_addr: SocketAddr = format!("127.0.0.{}:9000", case.self_octet)
        .parse()
        .expect("self addr");
    let mut input = String::new();
    for octet in case.peers {
        let octet = (octet % 96).saturating_add(1);
        input.push_str(&format!("127.0.0.{octet}:9000\n"));
    }

    let table = match NginxPeerTable::parse_lines(&input, Some(self_addr)) {
        Ok(table) => table,
        Err(error) => return TestResult::error(format!("parse failed: {error:?}")),
    };

    if table.len() > MAX_NGINX_PEERS {
        return TestResult::error("peer table exceeded maximum capacity");
    }
    if table
        .as_slice()
        .iter()
        .any(|peer| peer.socket_addr() == Some(self_addr))
    {
        return TestResult::error("peer table retained self address");
    }
    if !table.as_slice().windows(2).all(|pair| pair[0] < pair[1]) {
        return TestResult::error("peer table is not sorted and deduplicated");
    }
    TestResult::passed()
}

#[quickcheck]
fn shared_memory_access_property_respects_missing_variables_and_limit(
    case: NginxAccessCase,
) -> TestResult {
    let bytes =
        match NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 4, DEFAULT_MAX_ACTIVE_BUCKETS) {
            Some(bytes) => bytes,
            None => return TestResult::error("required bytes overflowed"),
        };
    let mut memory = vec![0_u8; bytes];
    let mut store = shm_store(&mut memory, shm_rule("tenant_api", "4r/m", &["$uri"]), 4);
    let present = TestVariables {
        values: &[("uri", b"/api/a")],
    };
    let missing = TestVariables { values: &[] };

    for attempt in 0..case.attempts {
        let result = if case.missing_variable {
            store.access(0, &missing, u64::from(attempt))
        } else {
            store.access(0, &present, u64::from(attempt))
        };
        if case.missing_variable {
            if result != Err(NgxShmAccessError::MissingVariable) {
                return TestResult::error(format!("missing variable result: {result:?}"));
            }
        } else if result != Ok(NginxStatus::Declined) {
            return TestResult::error(format!("request was not enqueued: {result:?}"));
        }
    }
    let mut events = [RequestEvent::default(); 16];
    let drained = store.drain_request_events(&mut events);
    let expected = if case.missing_variable {
        0
    } else {
        usize::from(case.attempts)
    };
    if drained != expected {
        return TestResult::error(format!("drained {drained} events, expected {expected}"));
    }
    TestResult::passed()
}

#[quickcheck]
fn shared_memory_access_property_uses_runtime_aggregates_and_isolates_keys(
    case: NginxAggregateAccessCase,
) -> TestResult {
    let bytes =
        match NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS) {
            Some(bytes) => bytes,
            None => return TestResult::error("required bytes overflowed"),
        };
    let mut memory = vec![0_u8; bytes];
    let limit = format!("{}r/m", case.limit);
    let mut store = shm_store(
        &mut memory,
        shm_rule("runtime_counts", &limit, &["$remote_addr"]),
        8,
    );
    let key = [case.key];
    let other_key = [case.other_key];
    let variables = TestVariables {
        values: &[("remote_addr", &key)],
    };
    let other_variables = TestVariables {
        values: &[("remote_addr", &other_key)],
    };

    if store.access(0, &variables, 200) != Ok(NginxStatus::Declined) {
        return TestResult::error("initial access did not enqueue request event");
    }
    let mut events = [RequestEvent::default(); 1];
    if store.drain_request_events(&mut events) != 1 {
        return TestResult::error("initial request event was not drainable");
    }

    let bucket_start_millis = if case.expired { 0 } else { 200 };
    if case.published_count != 0 {
        let outcome = store.apply_count_aggregates(&[CountAggregate {
            rule_id: events[0].rule_id,
            key_hash: events[0].key_hash.into(),
            bucket_start_millis,
            count: u64::from(case.published_count),
        }]);
        if outcome
            != (ApplyBatchOutcome {
                applied: 1,
                dropped: 0,
            })
        {
            return TestResult::error(format!("aggregate apply outcome: {outcome:?}"));
        }
    }

    let observed = store.access(0, &variables, 201);
    let expected_reject = !case.expired && u64::from(case.published_count) >= u64::from(case.limit);
    let expected = if expected_reject {
        Ok(NginxStatus::TooManyRequests)
    } else {
        Ok(NginxStatus::Declined)
    };
    if observed != expected {
        return TestResult::error(format!(
            "aggregate-backed access returned {observed:?}, expected {expected:?}"
        ));
    }

    let other_observed = store.access(0, &other_variables, 202);
    if other_observed != Ok(NginxStatus::Declined) {
        return TestResult::error(format!(
            "aggregate for one key affected a different key: {other_observed:?}"
        ));
    }
    TestResult::passed()
}
