use super::*;
use crate::core::{LocalEngine, NodeIdentity, Rule, RuleTable};
use crate::discovery::{
    DEFAULT_GOSSIP_PORT_NAME, FilePeerHandler, Peer, PeerSnapshot, SnapshotPeerHandler,
    StaticPeerHandler,
};
use crate::gossip::GossipSendPolicy;
use crate::gossip_runtime::{
    GossipTickSummary, GossipTransport, StandaloneGossipConfig, StandaloneGossipRuntime,
    UdpGossipTransport,
};
use crate::{Decision, Descriptor, LimitRequest, RejectReason};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
struct RuntimeRecentPeerCase {
    grace_millis: u8,
    elapsed_millis: u8,
}

impl Arbitrary for RuntimeRecentPeerCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            grace_millis: (u8::arbitrary(g) % 32).saturating_add(1),
            elapsed_millis: u8::arbitrary(g) % 64,
        }
    }
}

#[derive(Clone, Debug)]
struct EndpointConfigCase {
    selector_count: u8,
    include_legacy_selector: bool,
}

impl Arbitrary for EndpointConfigCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            selector_count: u8::arbitrary(g) % 8,
            include_legacy_selector: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeUnknownPeerCase {
    known_octet: u8,
    unknown_octet: u8,
    sender: u8,
}

impl Arbitrary for RuntimeUnknownPeerCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            known_octet: (u8::arbitrary(g) % 64).saturating_add(1),
            unknown_octet: (u8::arbitrary(g) % 64).saturating_add(65),
            sender: (u8::arbitrary(g) % 64).saturating_add(2),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeDirtyResyncCase {
    tenant_count: u8,
    max_cells_per_frame: u8,
}

impl Arbitrary for RuntimeDirtyResyncCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            tenant_count: (u8::arbitrary(g) % 8).saturating_add(2),
            max_cells_per_frame: (u8::arbitrary(g) % 8).saturating_add(1),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeDeterministicTickCase {
    peer_count: u8,
    fanout: u8,
    now_millis: u16,
}

impl Arbitrary for RuntimeDeterministicTickCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            peer_count: u8::arbitrary(g) % 8,
            fanout: (u8::arbitrary(g) % 8).saturating_add(1),
            now_millis: u16::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeBufferReuseCase {
    peer_count: u8,
    tick_count: u8,
}

impl Arbitrary for RuntimeBufferReuseCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            peer_count: (u8::arbitrary(g) % 8).saturating_add(1),
            tick_count: (u8::arbitrary(g) % 16).saturating_add(1),
        }
    }
}

#[derive(Default)]
struct MemoryTransport {
    inbox: VecDeque<(Peer, Vec<u8>)>,
    outbox: VecDeque<(Peer, Vec<u8>)>,
}

impl MemoryTransport {
    fn push_inbox(&mut self, peer: Peer, payload: Vec<u8>) {
        self.inbox.push_back((peer, payload));
    }

    fn drain_outbox(&mut self) -> Vec<(Peer, Vec<u8>)> {
        self.outbox.drain(..).collect()
    }
}

impl GossipTransport for MemoryTransport {
    fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool {
        self.outbox.push_back((peer, payload.to_vec()));
        true
    }

    fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)> {
        let (peer, payload) = self.inbox.pop_front()?;
        if payload.len() > buffer.len() {
            return None;
        }
        buffer[..payload.len()].copy_from_slice(&payload);
        Some((peer, payload.len()))
    }
}

fn test_config() -> Config {
    Config {
        storage: StorageConfig {
            max_keys: 16,
            max_cells: Some(64),
            dirty_ring_entries: Some(64),
            ..Default::default()
        },
        limits: vec![tenant_rule(1, 100, 100)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::None,
            ..Default::default()
        },
        gossip: GossipConfig::default(),
        runtime: RuntimeTuningConfig::default(),
    }
}

fn tenant_rule(limit: u64, local_fallback_limit: u64, local_absolute_limit: u64) -> LimitRuleConfig {
    LimitRuleConfig {
        name: "tenant_api_minute".to_string(),
        domain: "api".to_string(),
        descriptors: vec![DescriptorConfig {
            key: "tenant".to_string(),
            value: "*".to_string(),
        }],
        limit,
        window: Duration::from_secs(60),
        bucket: Duration::from_secs(1),
        local_fallback_limit,
        local_absolute_limit,
        stale_after: Duration::from_secs(2),
        safety_margin: SafetyMarginConfig::default(),
        overflow_policy: OverflowPolicy::UseOverflowKey,
        mode: EnforcementMode::Enforce,
    }
}

#[derive(Clone, Debug, Default)]
struct RecordingCountHandler {
    batch_sizes: Arc<Mutex<Vec<usize>>>,
}

impl CountUpdateHandler for RecordingCountHandler {
    fn apply_batch(&self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome {
        self.batch_sizes
            .lock()
            .expect("batch sizes")
            .push(aggregates.len());
        ApplyBatchOutcome {
            applied: aggregates.len(),
            dropped: 0,
        }
    }
}

#[test]
fn runtime_publishes_count_aggregates_in_configured_batches() {
    let mut config = test_config();
    config.runtime.count_update_batch_size = 2;
    let handler = RecordingCountHandler::default();
    let seen = Arc::clone(&handler.batch_sizes);
    let runtime =
        Runtime::with_count_update_handler(config, handler).expect("runtime with handler");
    let requests = [
        TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 1_u128, 1), 0),
        TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 2_u128, 1), 0),
        TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 3_u128, 1), 0),
    ];
    let mut scratch = [CountAggregate::default(); 3];

    let recorded = runtime.record_timed_hashed_batch(&requests, &mut scratch);

    assert_eq!(recorded, 3);
    assert_eq!(*seen.lock().expect("batch sizes"), vec![2, 1]);
}

#[test]
fn runtime_does_not_publish_count_aggregates_after_shutdown() {
    let mut config = test_config();
    config.runtime.count_update_batch_size = 1;
    let handler = RecordingCountHandler::default();
    let seen = Arc::clone(&handler.batch_sizes);
    let runtime =
        Runtime::with_count_update_handler(config, handler).expect("runtime with handler");
    runtime.shutdown();
    let requests = [TimedHashedLimitRequest::new(
        HashedLimitRequest::new(1, 1_u128, 1),
        0,
    )];
    let mut scratch = [CountAggregate::default(); 1];

    let recorded = runtime.record_timed_hashed_batch(&requests, &mut scratch);

    assert_eq!(recorded, 0);
    assert!(seen.lock().expect("batch sizes").is_empty());
}

fn engine_with_node(node: u64) -> LocalEngine {
    let config = test_config();
    let bucket_count = config
        .limits
        .iter()
        .map(|limit| limit.bucket_count())
        .max()
        .unwrap_or(1);
    let rules = config
        .limits
        .iter()
        .enumerate()
        .map(|(index, limit)| limit.to_rule(index as RuleId + 1))
        .collect::<Result<Vec<_>, _>>()
        .expect("rules");

    LocalEngine::with_identity(
        RuleTable::new(rules),
        config.storage.max_keys,
        bucket_count,
        64,
        64,
        NodeIdentity {
            node_id: u128::from(node).into(),
            incarnation: 1,
        },
    )
}

fn rule_config_for_runtime(id: RuleId) -> Rule {
    test_config().limits[0].to_rule(id).expect("rule")
}

fn runtime_config(node: u64) -> StandaloneGossipConfig {
    StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: u128::from(node).into(),
        sender_incarnation: 1,
        fanout: 3,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: None,
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(30_000),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    }
}

fn empty_gossip_packet(sender: u64) -> Vec<u8> {
    let message = crate::gossip::GossipMessage {
        header: crate::gossip::GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: u128::from(sender).into(),
            sender_incarnation: 1,
            min_bucket: 0,
            max_bucket: 0,
            flags: 0,
        },
        digests: Vec::new(),
        cells: Vec::new(),
        truncated: false,
    };
    let mut packet = Vec::with_capacity(128);
    assert!(!crate::gossip::encode_message(&message, &mut packet, 128));
    packet
}

fn peer_addr(octet: u8) -> SocketAddr {
    format!("127.0.0.{octet}:18080").parse().expect("addr")
}

fn runtime(node: u64, peer_addr: SocketAddr) -> StandaloneGossipRuntime<MemoryTransport> {
    let limiter = shared_limiter(engine_with_node(node));
    let peers = RuntimePeerHandler::Static(StaticPeerHandler::new(vec![peer_addr], None));
    let config = runtime_config(node);
    StandaloneGossipRuntime::new(limiter, peers, MemoryTransport::default(), config)
}

fn request<'a>(descriptors: &'a [Descriptor<'a>]) -> LimitRequest<'a> {
    LimitRequest {
        domain: "api",
        descriptors,
        hits: 1,
    }
}

fn record_tenant(limiter: &SharedLimiter, tenant: &str, now_millis: u64) {
    let descriptors = [Descriptor {
        key: "tenant",
        value: tenant,
    }];
    let mut limiter = limiter.lock().expect("limiter");
    assert_eq!(
        limiter.check_and_record(request(&descriptors), now_millis),
        Decision::Allow
    );
}

#[test]
fn admin_introspection_is_bounded_by_query_limits() {
    let limiter = shared_limiter(engine_with_node(1));
    record_tenant(&limiter, "a", 0);
    record_tenant(&limiter, "b", 1);

    let gossip = std::sync::Arc::new(std::sync::Mutex::new(gossip_runtime::GossipAdminSnapshot {
        cluster_id_hash: 42,
        sender_node_id: 1_u128.into(),
        sender_incarnation: 1,
        active_peers: vec![
            "127.0.0.2:9000".parse().expect("addr"),
            "127.0.0.3:9000".parse().expect("addr"),
        ],
        recent_peers: vec![gossip_runtime::RecentPeerSnapshot {
            addr: "127.0.0.4:9000".parse().expect("addr"),
            expires_millis: 30_000,
        }],
        discovery_generation: 7,
        local_only: false,
        discovery_stale: true,
        remote_active_cells: 2,
        remote_cell_capacity: 8,
        remote_dirty_ring_len: 1,
        remote_dirty_overflow: false,
        remote_cells_sample: vec![
            crate::gossip::CounterCell {
                rule_id: 1,
                key_hash: 2_u128.into(),
                bucket_start_millis: 0,
                origin_node_id: 2_u128.into(),
                origin_incarnation: 1,
                count: 1,
                last_update_millis: 1,
                sequence: 1,
            },
            crate::gossip::CounterCell {
                rule_id: 1,
                key_hash: 4_u128.into(),
                bucket_start_millis: 0,
                origin_node_id: 3_u128.into(),
                origin_incarnation: 1,
                count: 1,
                last_update_millis: 1,
                sequence: 1,
            },
        ],
        metrics: crate::gossip::GossipMetrics {
            send_bytes: 10,
            recv_bytes: 20,
            merge_cells: 2,
            digest_mismatch: 1,
            truncated: 0,
            auth_failures: 0,
            decode_errors: 0,
            dirty_overflow: 0,
        },
    }));
    let state = admin::AdminState::new(limiter, Some(gossip));
    let snapshot = admin::introspection_snapshot(
        &state,
        admin::DebugLimits {
            max_rules: Some(1),
            max_cells: Some(1),
            max_peers: Some(1),
        },
    );

    assert_eq!(snapshot.cluster_id_hash, 42);
    assert_eq!(snapshot.active_rule_ids, vec![1]);
    assert_eq!(snapshot.local_cells.len(), 1);
    assert_eq!(snapshot.remote_cells.len(), 1);
    assert_eq!(snapshot.peers.active_peers.len(), 1);
    assert_eq!(snapshot.peers.recent_peers.len(), 1);
    assert!(snapshot.peers.discovery_stale);
    assert!(snapshot.truncated);
    assert_eq!(snapshot.gossip.expect("gossip").merge_cells, 2);
}

#[test]
fn admin_prometheus_metrics_include_discovery_and_gossip_state() {
    let limiter = shared_limiter(engine_with_node(1));
    record_tenant(&limiter, "a", 0);
    let gossip = std::sync::Arc::new(std::sync::Mutex::new(gossip_runtime::GossipAdminSnapshot {
        local_only: false,
        discovery_stale: true,
        active_peers: vec!["127.0.0.2:9000".parse().expect("addr")],
        metrics: crate::gossip::GossipMetrics {
            send_bytes: 11,
            recv_bytes: 22,
            merge_cells: 3,
            ..Default::default()
        },
        ..Default::default()
    }));
    let state = admin::AdminState::new(limiter, Some(gossip));
    let metrics = admin::prometheus_metrics(&state);

    assert!(metrics.contains("limiter_local_only 0\n"));
    assert!(metrics.contains("limiter_discovery_stale 1\n"));
    assert!(metrics.contains("limiter_peers 1\n"));
    assert!(metrics.contains("gossip_send_bytes_total 11\n"));
    assert!(metrics.contains("gossip_recv_bytes_total 22\n"));
    assert!(metrics.contains("gossip_merge_cells_total 3\n"));
}

#[test]
fn admin_storage_summary_reports_cells_and_dirty_ring() {
    let limiter = shared_limiter(engine_with_node(1));
    record_tenant(&limiter, "a", 0);

    let storage = admin::snapshot(&limiter).storage;

    assert_eq!(storage.active_keys, 1);
    assert_eq!(storage.active_cells, 1);
    assert_eq!(storage.max_keys, 16);
    assert_eq!(storage.max_cells, 64);
    assert_eq!(storage.dirty_ring_len, 1);
    assert!(!storage.dirty_overflow);
    assert!(storage.estimated_memory_bytes > 0);
}

#[test]
fn parses_local_only_config_into_engine() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::None,
            ..Default::default()
        },
        ..Default::default()
    };

    let engine = config.into_engine().expect("local-only engine builds");

    assert_eq!(engine.rules().len(), 1);
    assert_eq!(engine.active_keys(), 0);
}

#[test]
fn parses_static_peer_config_without_boxed_provider() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::Static,
            self_addr: Some("127.0.0.1:18080".parse().expect("addr")),
            peers: vec![
                "127.0.0.1:18080".parse().expect("addr"),
                "127.0.0.2:18080".parse().expect("addr"),
            ],
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("127.0.0.1:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };
    let provider = peer_provider_from_config(&config.discovery).expect("provider");

    assert_eq!(provider.snapshot().peers().len(), 1);
}

#[test]
fn discovery_defaults_to_auto_and_sync_provider_is_local_only() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };
    let provider = peer_provider_from_config(&config.discovery).expect("provider");

    assert_eq!(config.discovery.kind, DiscoveryMode::Auto);
    assert!(provider.snapshot().local_only());
}

#[test]
fn discovery_section_without_kind_defaults_to_auto() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };

    assert_eq!(config.discovery.kind, DiscoveryMode::Auto);
    assert_eq!(
        config.discovery.self_addr,
        Some("10.0.0.1:18080".parse().expect("addr"))
    );
}

#[test]
fn gossip_tick_sends_dirty_cells_and_receiver_merges_estimate() {
    let addr_a = "127.0.0.1:10001".parse().expect("addr");
    let addr_b = "127.0.0.1:10002".parse().expect("addr");
    let mut runtime_a = runtime(1, addr_b);
    let mut runtime_b = runtime(2, addr_a);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }

    let sent = runtime_a.tick(1);
    for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
        runtime_b
            .transport_mut()
            .push_inbox(Peer::new(addr_a), payload);
    }
    let received = runtime_b.tick(2);

    assert_eq!(sent.cells_sent, 1);
    assert_eq!(received.frames_received, 1);
    assert_eq!(received.cells_merged, 1);
    assert_eq!(runtime_b.metrics().merge_cells, 1);
    {
        let mut limiter = runtime_b.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 3),
            Decision::Reject(RejectReason::GlobalLimit)
        );
    }
}

#[test]
fn deterministic_gossip_uses_peer_and_transport_traits_without_udp() {
    let addr_a = "127.0.0.1:10101".parse().expect("addr");
    let addr_b = "127.0.0.1:10102".parse().expect("addr");
    let peers_a = SnapshotPeerHandler::with_capacity(4);
    let peers_b = SnapshotPeerHandler::with_capacity(4);
    peers_a.peer_added(Peer::new(addr_b));
    peers_b.peer_added(Peer::new(addr_a));
    let config_a = StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: 1_u128.into(),
        sender_incarnation: 1,
        fanout: 1,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: None,
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(30_000),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    };
    let mut config_b = config_a;
    config_b.sender_node_id = 2_u128.into();
    let mut runtime_a = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        peers_a.clone(),
        MemoryTransport::default(),
        config_a,
    );
    let mut runtime_b = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(2)),
        peers_b,
        MemoryTransport::default(),
        config_b,
    );
    let descriptors = [Descriptor {
        key: "tenant",
        value: "trait",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }
    assert_eq!(runtime_a.tick(1).peers_sent, 1);
    for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
        runtime_b
            .transport_mut()
            .push_inbox(Peer::new(addr_a), payload);
    }
    assert_eq!(runtime_b.tick(2).cells_merged, 1);

    peers_a.peer_removed(Peer::new(addr_b));
    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 3),
            Decision::Allow
        );
    }
    let summary = runtime_a.tick(4);
    assert_eq!(summary.peers_seen, 0);
    assert_eq!(summary.peers_sent, 0);
}

#[test]
fn gossip_tick_keeps_last_good_file_peers_when_read_fails() {
    let missing = std::env::temp_dir().join(format!("gabion-missing-peers-{}", std::process::id()));
    let provider = RuntimePeerHandler::File(FilePeerHandler::new(
        &missing,
        None,
        vec!["127.0.0.2:18080".parse().expect("addr")],
    ));
    let limiter = shared_limiter(engine_with_node(1));
    let config = StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: 1_u128.into(),
        sender_incarnation: 1,
        fanout: 1,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: None,
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(30_000),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    };
    let mut runtime =
        StandaloneGossipRuntime::new(limiter, provider, MemoryTransport::default(), config);

    let summary: GossipTickSummary = runtime.tick(0);

    assert!(!summary.discovery_stale);
    assert!(!summary.local_only);
    assert_eq!(summary.peers_seen, 1);
}

#[test]
fn kubernetes_endpoint_slice_config_uses_snapshot_provider() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            namespace: Some("default".to_string()),
            service_name: Some("gabion".to_string()),
            port_name: Some("gossip".to_string()),
            self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };
    let endpoint_config =
        endpoint_slice_config_from_discovery(&config.discovery).expect("endpoint config");
    let provider = peer_provider_from_config(&config.discovery).expect("provider");

    assert_eq!(endpoint_config.namespace, "default");
    assert_eq!(endpoint_config.service_name, "gabion");
    assert_eq!(endpoint_config.port_name.as_deref(), Some("gossip"));
    assert!(provider.snapshot().local_only());
}

#[quickcheck]
fn quickcheck_endpoint_slice_config_preserves_selector_shape(
    case: EndpointConfigCase,
) -> TestResult {
    let mut discovery = DiscoveryConfig {
        kind: DiscoveryMode::KubernetesEndpointSlice,
        self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
        ..Default::default()
    };
    if case.include_legacy_selector {
        discovery.namespace = Some("default".to_string());
        discovery.service_name = Some("gabion".to_string());
        discovery.port_name = Some(DEFAULT_GOSSIP_PORT_NAME.to_string());
    }
    for index in 0..case.selector_count {
        discovery.endpoint_slices.push(EndpointSliceSelectorConfig {
            namespace: "default".to_string(),
            service_name: format!("gabion-{index}"),
            port_name: Some(DEFAULT_GOSSIP_PORT_NAME.to_string()),
        });
    }

    let configs = endpoint_slice_configs_from_discovery(&discovery);
    if case.selector_count == 0 && !case.include_legacy_selector {
        return if configs == Err(ConfigError::MissingKubernetesNamespace) {
            TestResult::passed()
        } else {
            TestResult::error("missing Kubernetes selector did not report missing namespace")
        };
    }

    let Ok(configs) = configs else {
        return TestResult::error("valid Kubernetes selector config was rejected");
    };
    let expected = usize::from(case.selector_count.max(1));
    if configs.len() != expected {
        return TestResult::error("EndpointSlice config count did not match selectors");
    }
    if configs
        .iter()
        .any(|config| config.port_name.as_deref() != Some(DEFAULT_GOSSIP_PORT_NAME))
    {
        return TestResult::error("EndpointSlice config lost default gossip port name");
    }
    TestResult::passed()
}

#[test]
fn kubernetes_endpoint_slice_config_accepts_multiple_selectors() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
            endpoint_slices: vec![
                EndpointSliceSelectorConfig {
                    namespace: "default".to_string(),
                    service_name: "gabion-grpc".to_string(),
                    port_name: Some("gossip".to_string()),
                },
                EndpointSliceSelectorConfig {
                    namespace: "default".to_string(),
                    service_name: "gabion-nginx".to_string(),
                    port_name: Some("gossip".to_string()),
                },
            ],
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };

    let configs =
        endpoint_slice_configs_from_discovery(&config.discovery).expect("endpoint configs");

    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].service_name, "gabion-grpc");
    assert_eq!(configs[1].service_name, "gabion-nginx");
    assert_eq!(
        configs[0].self_addr,
        Some("10.0.0.1:18080".parse().expect("addr"))
    );
}

#[test]
fn kubernetes_endpoint_slice_config_defaults_port_name_to_gossip() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            namespace: Some("default".to_string()),
            service_name: Some("gabion".to_string()),
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };
    let endpoint_config =
        endpoint_slice_config_from_discovery(&config.discovery).expect("endpoint config");

    assert_eq!(endpoint_config.port_name.as_deref(), Some("gossip"));
}

#[quickcheck]
fn quickcheck_removed_peers_are_accepted_only_within_grace_window(
    case: RuntimeRecentPeerCase,
) -> TestResult {
    let addr_b = "127.0.0.1:10202".parse().expect("addr");
    let peers = SnapshotPeerHandler::with_capacity(4);
    peers.peer_added(Peer::new(addr_b));
    let mut config = runtime_config(1);
    config.recent_peer_grace = Duration::from_millis(u64::from(case.grace_millis));
    let mut runtime = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        peers.clone(),
        MemoryTransport::default(),
        config,
    );

    runtime.tick(0);
    peers.peer_removed(Peer::new(addr_b));
    runtime.tick(1);
    runtime
        .transport_mut()
        .push_inbox(Peer::new(addr_b), empty_gossip_packet(2));
    let summary = runtime.tick(1 + u64::from(case.elapsed_millis));
    let should_accept = u64::from(case.elapsed_millis) < u64::from(case.grace_millis);

    if should_accept
        && summary.frames_received == 1
        && summary.peer_rejected == 0
        && runtime.metrics().recv_bytes > 0
    {
        return TestResult::passed();
    }
    if !should_accept
        && summary.frames_received == 0
        && summary.peer_rejected == 1
        && runtime.metrics().recv_bytes == 0
    {
        return TestResult::passed();
    }
    TestResult::error("recent peer grace window did not match acceptance behavior")
}

#[quickcheck]
fn quickcheck_unknown_senders_are_rejected_before_decode(
    case: RuntimeUnknownPeerCase,
) -> TestResult {
    let known = peer_addr(case.known_octet);
    let unknown = peer_addr(case.unknown_octet);
    let mut runtime = runtime(1, known);
    runtime.transport_mut().push_inbox(
        Peer::new(unknown),
        empty_gossip_packet(u64::from(case.sender)),
    );

    let summary = runtime.tick(0);

    if summary.frames_received == 0
        && summary.cells_merged == 0
        && summary.peer_rejected == 1
        && runtime.metrics().recv_bytes == 0
    {
        TestResult::passed()
    } else {
        TestResult::error("unknown peer was decoded or merged before rejection")
    }
}

#[quickcheck]
fn quickcheck_dirty_overflow_forces_bounded_resync(case: RuntimeDirtyResyncCase) -> TestResult {
    let mut config = runtime_config(7);
    config.fanout = 1;
    config.max_cells_per_frame = usize::from(case.max_cells_per_frame);
    let mut runtime = StandaloneGossipRuntime::new(
        shared_limiter(LocalEngine::with_identity(
            RuleTable::new(vec![rule_config_for_runtime(1)]),
            16,
            10,
            16,
            1,
            NodeIdentity {
                node_id: 7_u128.into(),
                incarnation: 1,
            },
        )),
        RuntimePeerHandler::Static(StaticPeerHandler::new(vec![peer_addr(10)], None)),
        MemoryTransport::default(),
        config,
    );

    for index in 0..case.tenant_count {
        let value = format!("tenant-{index}");
        let descriptors = [Descriptor {
            key: "tenant",
            value: value.as_str(),
        }];
        let mut limiter = runtime.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), u64::from(index)),
            Decision::Allow
        );
    }

    let summary = runtime.tick(10);
    let expected = usize::from(case.tenant_count).min(usize::from(case.max_cells_per_frame));

    if summary.cells_sent != expected {
        return TestResult::error("dirty overflow resync did not send bounded active-cell set");
    }
    if runtime.metrics().dirty_overflow != 1 {
        return TestResult::error("dirty overflow was not reported exactly once");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_ticks_are_deterministic_for_supplied_timestamp(
    case: RuntimeDeterministicTickCase,
) -> TestResult {
    let mut peers = Vec::with_capacity(usize::from(case.peer_count));
    for index in 0..case.peer_count {
        peers.push(peer_addr(index.saturating_add(1)));
    }
    let mut config = runtime_config(1);
    config.fanout = usize::from(case.fanout);
    let peer_handler_a = RuntimePeerHandler::Static(StaticPeerHandler::new(peers.clone(), None));
    let peer_handler_b = RuntimePeerHandler::Static(StaticPeerHandler::new(peers, None));
    let mut runtime_a = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        peer_handler_a,
        MemoryTransport::default(),
        config,
    );
    let mut runtime_b = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        peer_handler_b,
        MemoryTransport::default(),
        config,
    );

    let summary_a = runtime_a.tick(u64::from(case.now_millis));
    let summary_b = runtime_b.tick(u64::from(case.now_millis));
    let outbox_a = runtime_a.transport_mut().drain_outbox();
    let outbox_b = runtime_b.transport_mut().drain_outbox();

    if summary_a != summary_b {
        return TestResult::error("same runtime state and timestamp produced different summaries");
    }
    if outbox_a.len() != outbox_b.len()
        || outbox_a
            .iter()
            .zip(outbox_b.iter())
            .any(|((peer_a, packet_a), (peer_b, packet_b))| {
                peer_a != peer_b || packet_a.len() != packet_b.len()
            })
    {
        return TestResult::error("same runtime state and timestamp produced different sends");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_runtime_reuses_construction_buffers(case: RuntimeBufferReuseCase) -> TestResult {
    let mut peers = Vec::with_capacity(usize::from(case.peer_count));
    for index in 0..case.peer_count {
        peers.push(peer_addr(index.saturating_add(1)));
    }
    let mut runtime = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        RuntimePeerHandler::Static(StaticPeerHandler::new(peers, None)),
        MemoryTransport::default(),
        runtime_config(1),
    );
    let initial = runtime.buffer_capacities();

    for tick in 0..case.tick_count {
        let _ = runtime.tick(u64::from(tick));
        if runtime.buffer_capacities() != initial {
            return TestResult::error("runtime buffer capacity changed after a tick");
        }
        let _ = runtime.transport_mut().drain_outbox();
    }

    TestResult::passed()
}

#[test]
fn endpoint_slice_selector_defaults_port_name_to_gossip() {
    let config = Config {
        storage: StorageConfig {
            max_keys: 16,
            ..Default::default()
        },
        limits: vec![tenant_rule(10, 3, 6)],
        discovery: DiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            endpoint_slices: vec![EndpointSliceSelectorConfig {
                namespace: "default".to_string(),
                service_name: "gabion".to_string(),
                port_name: Some(DEFAULT_GOSSIP_PORT_NAME.to_string()),
            }],
            ..Default::default()
        },
        gossip: GossipConfig {
            enabled: true,
            bind: Some("0.0.0.0:18080".parse().expect("addr")),
            ..Default::default()
        },
        ..Default::default()
    };
    let configs =
        endpoint_slice_configs_from_discovery(&config.discovery).expect("endpoint configs");

    assert_eq!(configs[0].port_name.as_deref(), Some("gossip"));
}

#[test]
fn gossip_runtime_rejects_unknown_peer_before_decode() {
    let known = "127.0.0.1:12001".parse().expect("addr");
    let unknown = "127.0.0.1:12002".parse().expect("addr");
    let mut runtime_a = runtime(1, known);
    let mut runtime_b = runtime(2, known);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }
    runtime_a.tick(1);
    for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
        runtime_b
            .transport_mut()
            .push_inbox(Peer::new(unknown), payload);
    }

    let summary = runtime_b.tick(2);

    assert_eq!(summary.frames_received, 0);
    assert_eq!(summary.cells_merged, 0);
    assert_eq!(summary.peer_rejected, 1);
}

#[test]
fn gossip_runtime_accepts_recently_known_peer_during_grace_window() {
    let old_peer_addr = "127.0.0.1:13001".parse().expect("addr");
    let new_peer_addr = "127.0.0.1:13002".parse().expect("addr");
    let provider =
        SnapshotPeerHandler::new(PeerSnapshot::new(vec![Peer::new(old_peer_addr)], false, 0));
    let limiter = shared_limiter(engine_with_node(2));
    let config = StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: 2_u128.into(),
        sender_incarnation: 1,
        fanout: 1,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: None,
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(10),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    };
    let mut receiver = StandaloneGossipRuntime::new(
        limiter,
        provider.clone(),
        MemoryTransport::default(),
        config,
    );
    let mut sender = runtime(1, old_peer_addr);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];

    receiver.tick(0);
    provider.peer_removed(Peer::new(old_peer_addr));
    provider.peer_added(Peer::new(new_peer_addr));
    {
        let mut limiter = sender.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 1),
            Decision::Allow
        );
    }
    sender.tick(2);
    for (_peer, payload) in sender.transport_mut().drain_outbox() {
        receiver
            .transport_mut()
            .push_inbox(Peer::new(old_peer_addr), payload);
    }

    let summary = receiver.tick(3);

    assert_eq!(summary.peer_rejected, 0);
    assert_eq!(summary.cells_merged, 1);
}

#[test]
fn packet_loss_does_not_drop_dirty_cell_retry() {
    let addr_a = "127.0.0.1:14001".parse().expect("addr");
    let addr_b = "127.0.0.1:14002".parse().expect("addr");
    let mut runtime_a = runtime(1, addr_b);
    let mut runtime_b = runtime(2, addr_a);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "loss",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }

    let dropped = runtime_a.tick(1);
    let lost_packets = runtime_a.transport_mut().drain_outbox();
    assert_eq!(dropped.cells_sent, 1);
    assert_eq!(lost_packets.len(), 1);
    assert_eq!(runtime_b.tick(2).cells_merged, 0);

    let retried = runtime_a.tick(3);
    for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
        runtime_b
            .transport_mut()
            .push_inbox(Peer::new(addr_a), payload);
    }
    let received = runtime_b.tick(4);

    assert_eq!(retried.cells_sent, 1);
    assert_eq!(received.cells_merged, 1);
}

#[test]
fn authenticated_runtime_rejects_mismatched_hmac_frames() {
    let addr_a = "127.0.0.1:11001".parse().expect("addr");
    let addr_b = "127.0.0.1:11002".parse().expect("addr");
    let config_a = StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: 1_u128.into(),
        sender_incarnation: 1,
        fanout: 1,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: Some(crate::gossip::HmacKey::new([1; 32])),
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(30_000),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    };
    let mut config_b = config_a;
    config_b.sender_node_id = 2_u128.into();
    config_b.auth_key = Some(crate::gossip::HmacKey::new([2; 32]));
    let mut runtime_a = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_b], None)),
        MemoryTransport::default(),
        config_a,
    );
    let mut runtime_b = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(2)),
        RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_a], None)),
        MemoryTransport::default(),
        config_b,
    );
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }
    runtime_a.tick(1);
    for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
        runtime_b
            .transport_mut()
            .push_inbox(Peer::new(addr_a), payload);
    }

    let received = runtime_b.tick(2);

    assert_eq!(received.frames_received, 1);
    assert_eq!(received.cells_merged, 0);
    assert_eq!(runtime_b.metrics().decode_errors, 1);
    assert_eq!(runtime_b.metrics().auth_failures, 1);
    {
        let mut limiter = runtime_b.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 3),
            Decision::Allow
        );
    }
}

#[test]
fn dirty_overflow_forces_bounded_resync_cells() {
    let mut runtime = StandaloneGossipRuntime::new(
        shared_limiter(LocalEngine::with_identity(
            RuleTable::new(vec![rule_config_for_runtime(1)]),
            8,
            10,
            8,
            1,
            NodeIdentity {
                node_id: 7_u128.into(),
                incarnation: 1,
            },
        )),
        RuntimePeerHandler::Static(StaticPeerHandler::new(
            vec!["127.0.0.1:19001".parse().expect("addr")],
            None,
        )),
        MemoryTransport::default(),
        StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 7_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace: Duration::from_millis(30_000),
            send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
        },
    );

    for value in ["a", "b"] {
        let descriptors = [Descriptor {
            key: "tenant",
            value,
        }];
        let mut limiter = runtime.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }

    let sent = runtime.tick(1);

    assert_eq!(sent.cells_sent, 2);
    assert_eq!(runtime.metrics().dirty_overflow, 1);
}

#[test]
#[ignore = "requires localhost UDP sockets"]
fn local_udp_gossip_converges_end_to_end() {
    let transport_a =
        UdpGossipTransport::bind("127.0.0.1:0".parse().expect("addr")).expect("udp a");
    let transport_b =
        UdpGossipTransport::bind("127.0.0.1:0".parse().expect("addr")).expect("udp b");
    let addr_a = transport_a.local_addr().expect("addr a");
    let addr_b = transport_b.local_addr().expect("addr b");
    let config_a = StandaloneGossipConfig {
        cluster_id_hash: 42,
        sender_node_id: 1_u128.into(),
        sender_incarnation: 1,
        fanout: 1,
        max_payload_bytes: 4096,
        max_cells_per_frame: 16,
        remote_cell_capacity: 64,
        remote_dirty_capacity: 64,
        auth_key: None,
        max_peers: 16,
        recent_peer_grace: Duration::from_millis(30_000),
        send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
    };
    let mut config_b = config_a;
    config_b.sender_node_id = 2_u128.into();
    let mut runtime_a = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(1)),
        RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_b], None)),
        transport_a,
        config_a,
    );
    let mut runtime_b = StandaloneGossipRuntime::new(
        shared_limiter(engine_with_node(2)),
        RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_a], None)),
        transport_b,
        config_b,
    );
    let descriptors = [Descriptor {
        key: "tenant",
        value: "udp",
    }];

    {
        let mut limiter = runtime_a.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 0),
            Decision::Allow
        );
    }
    assert_eq!(runtime_a.tick(1).peers_sent, 1);

    let mut received = GossipTickSummary::default();
    for attempt in 0..1_000 {
        received = runtime_b.tick(2 + attempt);
        if received.cells_merged == 1 {
            break;
        }
    }

    assert_eq!(received.cells_merged, 1);
    {
        let mut limiter = runtime_b.limiter().lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), 100),
            Decision::Reject(RejectReason::GlobalLimit)
        );
    }
}

#[tokio::test]
#[ignore = "requires local Kubernetes API"]
async fn local_kubernetes_endpoint_slice_watcher_drives_gossip_convergence() {
    use crate::discovery::kubernetes::{EndpointSliceDiscoveryConfig, run_endpoint_slice_watchers};
    use k8s_openapi::api::core::v1::{Namespace, Service, ServicePort, ServiceSpec};
    use k8s_openapi::api::discovery::v1::{
        Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::{DeleteParams, PostParams};
    use kube::{Api, Client};
    use std::collections::BTreeMap;

    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let client = Client::try_default().await.expect("local kube client");
        let namespace = format!("gabion-kube-e2e-{}", std::process::id());
        let grpc_service_name = "gabion-grpc";
        let nginx_service_name = "gabion-nginx";

        let namespaces: Api<Namespace> = Api::all(client.clone());
        let _ = namespaces
            .create(
                &PostParams::default(),
                &Namespace {
                    metadata: ObjectMeta {
                        name: Some(namespace.clone()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("create namespace");

        let services: Api<Service> = Api::namespaced(client.clone(), &namespace);
        for service_name in [grpc_service_name, nginx_service_name] {
            services
                .create(
                    &PostParams::default(),
                    &Service {
                        metadata: ObjectMeta {
                            name: Some(service_name.to_string()),
                            ..Default::default()
                        },
                        spec: Some(ServiceSpec {
                            selector: Some(BTreeMap::from([(
                                "app".to_string(),
                                service_name.to_string(),
                            )])),
                            ports: Some(vec![ServicePort {
                                name: Some("gossip".to_string()),
                                port: 18080,
                                target_port: None,
                                ..Default::default()
                            }]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await
                .expect("create service");
        }

        let advertised_ip: std::net::IpAddr = "10.0.0.1".parse().expect("ip");
        let advertised_a: SocketAddr = "10.0.0.1:30001".parse().expect("addr a");
        let advertised_b: SocketAddr = "10.0.0.1:30002".parse().expect("addr b");

        let provider = SnapshotPeerHandler::new(PeerSnapshot::new(Vec::new(), false, 0));
        let watcher_configs = vec![
            EndpointSliceDiscoveryConfig {
                namespace: namespace.clone(),
                service_name: grpc_service_name.to_string(),
                port_name: Some("gossip".to_string()),
                self_addr: Some(advertised_a),
            },
            EndpointSliceDiscoveryConfig {
                namespace: namespace.clone(),
                service_name: nginx_service_name.to_string(),
                port_name: Some("gossip".to_string()),
                self_addr: Some(advertised_a),
            },
        ];
        let watcher_provider = provider.clone();
        let watcher = tokio::spawn(run_endpoint_slice_watchers(
            client.clone(),
            watcher_configs,
            watcher_provider,
        ));

        let endpoint_slices: Api<EndpointSlice> = Api::namespaced(client.clone(), &namespace);
        for (slice_name, service_name, port) in [
            ("gabion-grpc-a", grpc_service_name, advertised_a.port()),
            ("gabion-nginx-b", nginx_service_name, advertised_b.port()),
        ] {
            endpoint_slices
                .create(
                    &PostParams::default(),
                    &EndpointSlice {
                        address_type: "IPv4".to_string(),
                        metadata: ObjectMeta {
                            name: Some(slice_name.to_string()),
                            labels: Some(BTreeMap::from([(
                                "kubernetes.io/service-name".to_string(),
                                service_name.to_string(),
                            )])),
                            ..Default::default()
                        },
                        endpoints: vec![Endpoint {
                            addresses: vec![advertised_ip.to_string()],
                            conditions: Some(EndpointConditions {
                                ready: Some(true),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ports: Some(vec![EndpointPort {
                            name: Some("gossip".to_string()),
                            port: Some(i32::from(port)),
                            ..Default::default()
                        }]),
                    },
                )
                .await
                .expect("create endpoint slice");
        }

        for _ in 0..50 {
            if provider
                .snapshot()
                .peers()
                .contains(&Peer::new(advertised_b))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(provider.snapshot().peers(), &[Peer::new(advertised_b)]);

        let config_a = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 1_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace: Duration::from_millis(30_000),
            send_policy: GossipSendPolicy::with_linger(Duration::from_millis(1)),
        };
        let mut config_b = config_a;
        config_b.sender_node_id = 2_u128.into();
        let mut runtime_a = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            RuntimePeerHandler::Snapshot(provider.clone()),
            MemoryTransport::default(),
            config_a,
        );
        let mut runtime_b = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(2)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![advertised_a], None)),
            MemoryTransport::default(),
            config_b,
        );
        let descriptors = [Descriptor {
            key: "tenant",
            value: "kube",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }
        assert_eq!(runtime_a.tick(1).peers_sent, 1);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(advertised_a), payload);
        }

        let mut received = GossipTickSummary::default();
        for attempt in 0..50 {
            received = runtime_b.tick(2 + attempt);
            if received.cells_merged == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(received.cells_merged, 1);

        {
            let mut limiter = runtime_b.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 100),
                Decision::Reject(RejectReason::GlobalLimit)
            );
        }

        endpoint_slices
            .delete("gabion-nginx-b", &DeleteParams::default())
            .await
            .expect("delete endpoint slice");
        for _ in 0..50 {
            if provider.snapshot().peers().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(provider.snapshot().peers().is_empty());

        watcher.abort();
        let _ = namespaces
            .delete(&namespace, &DeleteParams::default())
            .await;
    })
    .await
    .expect("local Kubernetes convergence test timed out");
}
