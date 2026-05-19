//! Peer discovery state and Kubernetes EndpointSlice helpers.
//!
//! Invariants:
//! - Discovery sources push individual peer add/remove events through
//!   `PeerHandler`.
//! - The runtime reads bounded snapshots and never polls discovery with refresh
//!   calls.
//! - Peer snapshots are sorted, deduplicated, and exclude the configured self
//!   address.
//! - Dynamic peer handlers retain the last good peer set when file reads or
//!   watches fail.
//! - Capacity overflow is bounded and does not clear or stale the last good
//!   peer set.
//! - EndpointSlice relists replace missing slices, deletes remove only their
//!   slice, and multi-selector snapshots are merged and deduplicated.
//! - Not-ready Kubernetes endpoints are ignored.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Deserialize;

pub const DEFAULT_MAX_PEERS: usize = 128;
pub const DEFAULT_RECENT_PEER_GRACE_MILLIS: u64 = 30_000;
pub const DEFAULT_GOSSIP_PORT_NAME: &str = "gossip";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    #[default]
    Auto,
    None,
    Static,
    File,
    KubernetesEndpointSlice,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub addr: SocketAddr,
}

impl Peer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSnapshot {
    peers: Vec<Peer>,
    stale: bool,
    local_only: bool,
    generation: u64,
}

impl Default for PeerSnapshot {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            stale: false,
            local_only: true,
            generation: 0,
        }
    }
}

impl PeerSnapshot {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            peers: Vec::with_capacity(capacity),
            stale: false,
            local_only: true,
            generation: 0,
        }
    }

    pub fn new(peers: Vec<Peer>, stale: bool, generation: u64) -> Self {
        let mut peers = peers;
        dedupe_in_place(&mut peers);
        let local_only = peers.is_empty() || stale;
        Self {
            peers,
            stale,
            local_only,
            generation,
        }
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn stale(&self) -> bool {
        self.stale
    }

    pub fn local_only(&self) -> bool {
        self.local_only
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

pub trait PeerHandler {
    fn snapshot(&self) -> PeerSnapshot;

    fn peer_added(&self, _peer: Peer) {}

    fn peer_removed(&self, _peer: Peer) {}
}

#[derive(Clone, Debug)]
pub struct SnapshotPeerHandler {
    inner: Arc<RwLock<PeerSnapshot>>,
}

impl SnapshotPeerHandler {
    pub fn new(mut snapshot: PeerSnapshot) -> Self {
        if snapshot.peers.capacity() == snapshot.peers.len() {
            snapshot
                .peers
                .reserve(DEFAULT_MAX_PEERS.saturating_sub(snapshot.peers.len()));
        }
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub fn with_capacity(max_peers: usize) -> Self {
        Self::new(PeerSnapshot::with_capacity(max_peers))
    }

    pub fn peer_added(&self, peer: Peer) {
        if let Ok(mut current) = self.inner.write() {
            if current.peers.binary_search(&peer).is_ok() {
                return;
            }
            if current.peers.len() == current.peers.capacity() {
                return;
            }
            let index = current.peers.partition_point(|stored| stored < &peer);
            current.peers.insert(index, peer);
            current.stale = false;
            current.local_only = current.peers.is_empty();
            current.generation = current.generation.saturating_add(1);
        }
    }

    pub fn peer_removed(&self, peer: Peer) {
        if let Ok(mut current) = self.inner.write()
            && let Ok(index) = current.peers.binary_search(&peer)
        {
            current.peers.remove(index);
            current.local_only = current.peers.is_empty() || current.stale;
            current.generation = current.generation.saturating_add(1);
        }
    }
}

impl PeerHandler for SnapshotPeerHandler {
    fn snapshot(&self) -> PeerSnapshot {
        self.inner
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    fn peer_added(&self, peer: Peer) {
        Self::peer_added(self, peer);
    }

    fn peer_removed(&self, peer: Peer) {
        Self::peer_removed(self, peer);
    }
}

#[derive(Clone, Debug)]
pub struct StaticPeerHandler {
    snapshot: PeerSnapshot,
}

impl StaticPeerHandler {
    pub fn new(peers: Vec<SocketAddr>, self_addr: Option<SocketAddr>) -> Self {
        let peers = peers
            .into_iter()
            .filter(|addr| Some(*addr) != self_addr)
            .map(Peer::new)
            .collect();
        Self {
            snapshot: PeerSnapshot::new(peers, false, 0),
        }
    }
}

impl PeerHandler for StaticPeerHandler {
    fn snapshot(&self) -> PeerSnapshot {
        self.snapshot.clone()
    }
}

#[derive(Clone, Debug)]
pub struct FilePeerHandler {
    path: PathBuf,
    self_addr: Option<SocketAddr>,
    fallback: SnapshotPeerHandler,
}

impl FilePeerHandler {
    pub fn new(
        path: impl Into<PathBuf>,
        self_addr: Option<SocketAddr>,
        initial: Vec<SocketAddr>,
    ) -> Self {
        Self::with_capacity(path, self_addr, initial, DEFAULT_MAX_PEERS)
    }

    pub fn with_capacity(
        path: impl Into<PathBuf>,
        self_addr: Option<SocketAddr>,
        initial: Vec<SocketAddr>,
        max_peers: usize,
    ) -> Self {
        let fallback = SnapshotPeerHandler::with_capacity(max_peers);
        for addr in initial {
            if Some(addr) != self_addr {
                fallback.peer_added(Peer::new(addr));
            }
        }
        Self {
            path: path.into(),
            self_addr,
            fallback,
        }
    }

    pub fn publish_current_file(&self) -> std::io::Result<PeerSnapshot> {
        match parse_peer_file(&self.path, self.self_addr) {
            Ok(peers) => {
                let current = self.fallback.snapshot();
                apply_peer_diff(&self.fallback, current.peers(), &peers);
                Ok(self.fallback.snapshot())
            }
            Err(error) => Err(error),
        }
    }
}

impl PeerHandler for FilePeerHandler {
    fn snapshot(&self) -> PeerSnapshot {
        self.fallback.snapshot()
    }

    fn peer_added(&self, peer: Peer) {
        self.fallback.peer_added(peer);
    }

    fn peer_removed(&self, peer: Peer) {
        self.fallback.peer_removed(peer);
    }
}

pub async fn run_file_peer_events(handler: FilePeerHandler, interval_millis: u64) {
    let mut interval =
        tokio::time::interval(std::time::Duration::from_millis(interval_millis.max(1)));

    loop {
        publish_file_peer_event(&handler, &mut interval).await;
    }
}

pub async fn publish_file_peer_event(
    handler: &FilePeerHandler,
    interval: &mut tokio::time::Interval,
) {
    interval.tick().await;
    let _ = handler.publish_current_file();
}

pub fn parse_peer_lines(input: &str, self_addr: Option<SocketAddr>) -> Vec<Peer> {
    let peers = input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.parse::<SocketAddr>().ok())
        .filter(|addr| Some(*addr) != self_addr)
        .map(Peer::new)
        .collect();
    dedupe(peers)
}

fn parse_peer_file(path: &Path, self_addr: Option<SocketAddr>) -> std::io::Result<Vec<Peer>> {
    Ok(parse_peer_lines(&fs::read_to_string(path)?, self_addr))
}

fn dedupe(peers: Vec<Peer>) -> Vec<Peer> {
    let mut peers = peers;
    dedupe_in_place(&mut peers);
    peers
}

fn dedupe_in_place(peers: &mut Vec<Peer>) {
    peers.sort();
    peers.dedup();
}

fn apply_peer_diff(target: &impl PeerHandler, current: &[Peer], next: &[Peer]) {
    for peer in current {
        if next.binary_search(peer).is_err() {
            target.peer_removed(*peer);
        }
    }
    for peer in next {
        if current.binary_search(peer).is_err() {
            target.peer_added(*peer);
        }
    }
}

fn publish_peer_snapshot(target: &impl PeerHandler, next: &[Peer]) {
    let current = target.snapshot();
    apply_peer_diff(target, current.peers(), next);
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use quickcheck_macros::quickcheck;

    #[derive(Clone, Debug)]
    struct PeerSnapshotCase {
        octets: Vec<u8>,
    }

    impl Arbitrary for PeerSnapshotCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut octets = Vec::<u8>::arbitrary(g);
            octets.truncate(64);
            Self { octets }
        }
    }

    #[derive(Clone, Debug)]
    struct PeerEventCase {
        events: Vec<PeerEvent>,
    }

    #[derive(Clone, Debug)]
    struct PeerEvent {
        octet: u8,
        add: bool,
    }

    #[derive(Clone, Debug)]
    struct FileRetryCase {
        first_octet: u8,
        second_octet: u8,
    }

    impl Arbitrary for PeerEventCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut events = Vec::<PeerEvent>::arbitrary(g);
            events.truncate(128);
            Self { events }
        }
    }

    impl Arbitrary for PeerEvent {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                octet: (u8::arbitrary(g) % 16) + 1,
                add: bool::arbitrary(g),
            }
        }
    }

    impl Arbitrary for FileRetryCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                first_octet: (u8::arbitrary(g) % 16) + 1,
                second_octet: (u8::arbitrary(g) % 16) + 17,
            }
        }
    }

    macro_rules! async_quickcheck {
        (fn $name:ident($case:ident: $case_ty:ty) -> TestResult $body:block) => {
            #[test]
            fn $name() {
                fn property($case: $case_ty) -> TestResult {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .start_paused(true)
                        .build()
                        .expect("paused tokio runtime")
                        .block_on(async move $body)
                }

                QuickCheck::new().quickcheck(property as fn($case_ty) -> TestResult);
            }
        };
    }

    #[test]
    fn static_provider_deduplicates_and_ignores_self() {
        let self_addr = "127.0.0.1:18080".parse().expect("addr");
        let peer = "127.0.0.2:18080".parse().expect("addr");
        let provider = StaticPeerHandler::new(vec![self_addr, peer, peer], Some(self_addr));

        let snapshot = provider.snapshot();

        assert_eq!(snapshot.peers(), &[Peer::new(peer)]);
        assert!(!snapshot.stale());
        assert!(!snapshot.local_only());
    }

    #[test]
    fn snapshot_peer_handler_adds_and_removes_individual_peers() {
        let peer = "127.0.0.2:18080".parse().expect("addr");
        let provider = SnapshotPeerHandler::new(PeerSnapshot::default());

        provider.peer_added(Peer::new(peer));
        provider.peer_removed(Peer::new(peer));
        let snapshot = provider.snapshot();

        assert!(snapshot.peers().is_empty());
        assert!(!snapshot.stale());
        assert!(snapshot.local_only());
    }

    #[test]
    fn snapshot_peer_handler_retains_last_good_set_when_capacity_is_full() {
        let first = Peer::new("127.0.0.2:18080".parse().expect("addr"));
        let second = Peer::new("127.0.0.3:18080".parse().expect("addr"));
        let provider = SnapshotPeerHandler::with_capacity(1);

        provider.peer_added(first);
        provider.peer_added(second);
        let snapshot = provider.snapshot();

        assert_eq!(snapshot.peers(), &[first]);
        assert!(!snapshot.stale());
        assert!(!snapshot.local_only());
    }

    #[test]
    fn peer_file_parser_accepts_line_based_addresses() {
        let self_addr = "127.0.0.1:18080".parse().expect("addr");
        let peers = parse_peer_lines(
            "
            # ignored
            127.0.0.1:18080
            127.0.0.2:18080
            127.0.0.2:18080
            ",
            Some(self_addr),
        );

        assert_eq!(
            peers,
            vec![Peer::new("127.0.0.2:18080".parse().expect("addr"))]
        );
    }

    #[quickcheck]
    fn quickcheck_peer_snapshots_are_sorted_deduped_bounded_and_selfless(
        case: PeerSnapshotCase,
    ) -> TestResult {
        let self_addr = "127.0.0.1:18080".parse().expect("addr");
        let mut raw = Vec::new();
        let mut expected = Vec::new();
        for octet in case.octets {
            let addr = SocketAddr::from(([127, 0, 0, (octet % 16) + 1], 18080));
            raw.push(addr);
            if addr != self_addr {
                expected.push(Peer::new(addr));
            }
        }
        expected.sort();
        expected.dedup();

        let static_handler = StaticPeerHandler::new(raw, Some(self_addr));
        let snapshot = static_handler.snapshot();

        if snapshot.peers() != expected.as_slice() {
            return TestResult::error("peer snapshot is not sorted, deduped, or selfless");
        }
        if snapshot.stale() {
            return TestResult::error("static peer snapshot was unexpectedly stale");
        }
        if snapshot.local_only() != expected.is_empty() {
            return TestResult::error("peer snapshot local_only flag does not match peer set");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_snapshot_handler_add_remove_events_match_bounded_set_model(
        case: PeerEventCase,
    ) -> TestResult {
        let handler = SnapshotPeerHandler::with_capacity(6);
        let mut expected = Vec::with_capacity(6);

        for event in case.events {
            let peer = Peer::new(SocketAddr::from(([10, 0, 0, event.octet], 18080)));
            if event.add {
                handler.peer_added(peer);
                if expected.binary_search(&peer).is_err() && expected.len() < 6 {
                    let index = expected.partition_point(|stored| stored < &peer);
                    expected.insert(index, peer);
                }
            } else {
                handler.peer_removed(peer);
                if let Ok(index) = expected.binary_search(&peer) {
                    expected.remove(index);
                }
            }

            let snapshot = handler.snapshot();
            if snapshot.peers() != expected.as_slice()
                || snapshot.stale()
                || snapshot.local_only() != expected.is_empty()
                || !snapshot.peers().windows(2).all(|peers| peers[0] < peers[1])
                || snapshot.peers().len() > 6
            {
                return TestResult::error(
                    "snapshot handler diverged from bounded add/remove set model",
                );
            }
        }
        TestResult::passed()
    }

    async_quickcheck! {
        fn quickcheck_file_peer_events_keep_retrying_under_paused_time(case: FileRetryCase) -> TestResult {
            let path = std::env::temp_dir().join(format!(
                "gabion-peer-events-{}-{}-{}",
                std::process::id(),
                case.first_octet,
                case.second_octet,
            ));
            let first = Peer::new(SocketAddr::from(([127, 0, 0, case.first_octet], 18080)));
            let second = Peer::new(SocketAddr::from(([127, 0, 0, case.second_octet], 18080)));
            let handler = FilePeerHandler::new(&path, None, Vec::new());
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

            publish_file_peer_event(&handler, &mut interval).await;
            if !handler.snapshot().peers().is_empty() {
                let _ = fs::remove_file(&path);
                return TestResult::error("missing peer file did not keep empty last-good snapshot");
            }

            if fs::write(&path, first.addr.to_string()).is_err() {
                return TestResult::error("failed to write first generated peer file");
            }
            tokio::time::advance(std::time::Duration::from_millis(1_001)).await;
            publish_file_peer_event(&handler, &mut interval).await;
            if handler.snapshot().peers() != [first].as_slice() {
                let _ = fs::remove_file(&path);
                return TestResult::error("file peer retry did not publish first peer set");
            }

            if fs::write(&path, second.addr.to_string()).is_err() {
                let _ = fs::remove_file(&path);
                return TestResult::error("failed to write second generated peer file");
            }
            tokio::time::advance(std::time::Duration::from_millis(1_001)).await;
            publish_file_peer_event(&handler, &mut interval).await;
            let passed = handler.snapshot().peers() == [second].as_slice();

            let _ = fs::remove_file(path);
            if passed {
                TestResult::passed()
            } else {
                TestResult::error("file peer retry did not replace old peers")
            }
        }
    }
}

pub mod kubernetes {
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::{Arc, Mutex};

    use futures::{StreamExt, TryStreamExt};
    use k8s_openapi::api::core::v1::{Pod, Service};
    use k8s_openapi::api::discovery::v1::EndpointSlice;
    use kube::api::ListParams;
    use kube::runtime::watcher::{Config as WatcherConfig, Event, watcher};
    use kube::{Api, Client};

    use crate::{DEFAULT_GOSSIP_PORT_NAME, Peer, SnapshotPeerHandler, publish_peer_snapshot};

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct EndpointSliceDiscoveryConfig {
        pub namespace: String,
        pub service_name: String,
        pub port_name: Option<String>,
        pub self_addr: Option<SocketAddr>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum RunningServiceDiscoveryError {
        Namespace,
        PodName,
        Pod,
        Services,
        NoSelectingService,
    }

    pub fn incluster_client() -> Option<Client> {
        kube::Config::incluster_env()
            .ok()
            .and_then(|config| Client::try_from(config).ok())
    }

    pub async fn running_service_endpoint_slice_configs(
        client: Client,
        self_addr: Option<SocketAddr>,
    ) -> Result<Vec<EndpointSliceDiscoveryConfig>, RunningServiceDiscoveryError> {
        let namespace =
            fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .map_err(|_| RunningServiceDiscoveryError::Namespace)?
                .trim()
                .to_string();
        let pod_name = env::var("HOSTNAME").map_err(|_| RunningServiceDiscoveryError::PodName)?;
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let services: Api<Service> = Api::namespaced(client, &namespace);
        let pod = pods
            .get(&pod_name)
            .await
            .map_err(|_| RunningServiceDiscoveryError::Pod)?;
        let labels = pod.metadata.labels.unwrap_or_default();
        let service_list = services
            .list(&Default::default())
            .await
            .map_err(|_| RunningServiceDiscoveryError::Services)?;
        let mut configs = Vec::new();

        for service in service_list {
            let Some(spec) = service.spec else {
                continue;
            };
            let Some(selector) = spec.selector else {
                continue;
            };
            if !selector
                .iter()
                .all(|(key, value)| labels.get(key) == Some(value))
            {
                continue;
            }
            let Some(name) = service.metadata.name else {
                continue;
            };
            let port_name = spec.ports.as_ref().and_then(|ports| {
                ports
                    .iter()
                    .find(|port| port.name.as_deref() == Some(DEFAULT_GOSSIP_PORT_NAME))
                    .or_else(|| (ports.len() == 1).then(|| &ports[0]))
                    .and_then(|port| port.name.clone())
            });
            configs.push(EndpointSliceDiscoveryConfig {
                namespace: namespace.clone(),
                service_name: name,
                port_name,
                self_addr,
            });
        }

        if configs.is_empty() {
            return Err(RunningServiceDiscoveryError::NoSelectingService);
        }
        configs.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.service_name.cmp(&right.service_name))
                .then_with(|| left.port_name.cmp(&right.port_name))
        });
        Ok(configs)
    }

    #[derive(Clone, Debug, Default)]
    pub struct EndpointSlicePeerSet {
        slices: BTreeMap<String, Vec<Peer>>,
        generation: u64,
    }

    impl EndpointSlicePeerSet {
        pub fn clear(&mut self) {
            self.slices.clear();
            self.generation = self.generation.saturating_add(1);
        }

        pub fn replace(&mut self, next: Self) {
            *self = next;
        }

        pub fn apply(&mut self, slice: &EndpointSlice, config: &EndpointSliceDiscoveryConfig) {
            let name = slice
                .metadata
                .name
                .clone()
                .unwrap_or_else(|| format!("anonymous-{}", self.generation));
            let peers = peers_from_endpoint_slice(slice, config);
            self.slices.insert(name, peers);
            self.generation = self.generation.saturating_add(1);
        }

        pub fn delete(&mut self, slice: &EndpointSlice) {
            if let Some(name) = &slice.metadata.name {
                self.slices.remove(name);
                self.generation = self.generation.saturating_add(1);
            }
        }

        pub fn snapshot_peers(&self) -> Vec<Peer> {
            let mut peers = self.slices.values().flatten().copied().collect::<Vec<_>>();
            peers.sort();
            peers.dedup();
            peers
        }

        pub fn generation(&self) -> u64 {
            self.generation
        }
    }

    pub async fn run_endpoint_slice_watcher(
        client: Client,
        config: EndpointSliceDiscoveryConfig,
        provider: SnapshotPeerHandler,
    ) -> Result<(), kube::runtime::watcher::Error> {
        let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
        let labels = format!("kubernetes.io/service-name={}", config.service_name);
        let watcher_config = WatcherConfig::default().labels(&labels);
        let mut events = watcher(api, watcher_config).boxed();
        let mut peer_set = EndpointSlicePeerSet::default();
        let mut init_peer_set = None;

        while let Some(event) = events.try_next().await? {
            match event {
                Event::Apply(slice) => {
                    peer_set.apply(&slice, &config);
                    publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
                }
                Event::Init => {
                    init_peer_set = Some(EndpointSlicePeerSet::default());
                }
                Event::InitApply(slice) => {
                    init_peer_set
                        .get_or_insert_with(EndpointSlicePeerSet::default)
                        .apply(&slice, &config);
                }
                Event::InitDone => {
                    if let Some(next) = init_peer_set.take() {
                        peer_set.replace(next);
                    } else {
                        peer_set.clear();
                    }
                    publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
                }
                Event::Delete(slice) => {
                    peer_set.delete(&slice);
                    publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
                }
            }
        }

        Ok(())
    }

    #[derive(Debug)]
    struct MergedEndpointSliceState {
        peers_by_selector: Vec<Vec<Peer>>,
    }

    impl MergedEndpointSliceState {
        fn new(selector_count: usize) -> Self {
            Self {
                peers_by_selector: vec![Vec::new(); selector_count],
            }
        }

        fn update(&mut self, selector_index: usize, peers: Vec<Peer>) -> Vec<Peer> {
            if let Some(slot) = self.peers_by_selector.get_mut(selector_index) {
                *slot = peers;
            }
            self.snapshot()
        }

        fn snapshot(&self) -> Vec<Peer> {
            let mut peers = self
                .peers_by_selector
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            peers.sort();
            peers.dedup();
            peers
        }
    }

    pub async fn run_endpoint_slice_watchers(
        client: Client,
        configs: Vec<EndpointSliceDiscoveryConfig>,
        provider: SnapshotPeerHandler,
    ) {
        let state = Arc::new(Mutex::new(MergedEndpointSliceState::new(configs.len())));

        for (selector_index, config) in configs.into_iter().enumerate() {
            let client = client.clone();
            let provider = provider.clone();
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                loop {
                    let api: Api<EndpointSlice> =
                        Api::namespaced(client.clone(), &config.namespace);
                    let labels = format!("kubernetes.io/service-name={}", config.service_name);
                    let watcher_config = WatcherConfig::default().labels(&labels);
                    let mut events = watcher(api, watcher_config).boxed();
                    let mut peer_set = EndpointSlicePeerSet::default();
                    let mut init_peer_set = None;

                    while let Ok(Some(event)) = events.try_next().await {
                        match event {
                            Event::Apply(slice) => {
                                peer_set.apply(&slice, &config);
                            }
                            Event::Init => {
                                init_peer_set = Some(EndpointSlicePeerSet::default());
                            }
                            Event::InitApply(slice) => {
                                init_peer_set
                                    .get_or_insert_with(EndpointSlicePeerSet::default)
                                    .apply(&slice, &config);
                            }
                            Event::InitDone => {
                                if let Some(next) = init_peer_set.take() {
                                    peer_set.replace(next);
                                } else {
                                    peer_set.clear();
                                }
                            }
                            Event::Delete(slice) => {
                                peer_set.delete(&slice);
                            }
                        }

                        if let Ok(mut state) = state.lock() {
                            publish_peer_snapshot(
                                &provider,
                                &state.update(selector_index, peer_set.snapshot_peers()),
                            );
                        }
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            });
        }
    }

    pub async fn initial_endpoint_slice_snapshots(
        client: Client,
        configs: &[EndpointSliceDiscoveryConfig],
    ) -> Result<Vec<Peer>, kube::Error> {
        let mut peers = Vec::new();
        for config in configs {
            peers.extend(initial_endpoint_slice_snapshot(client.clone(), config).await?);
        }
        peers.sort();
        peers.dedup();
        Ok(peers)
    }

    pub async fn initial_endpoint_slice_snapshot(
        client: Client,
        config: &EndpointSliceDiscoveryConfig,
    ) -> Result<Vec<Peer>, kube::Error> {
        let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
        let labels = format!("kubernetes.io/service-name={}", config.service_name);
        let list = api.list(&ListParams::default().labels(&labels)).await?;
        let mut peer_set = EndpointSlicePeerSet::default();
        for slice in &list.items {
            peer_set.apply(slice, config);
        }
        Ok(peer_set.snapshot_peers())
    }

    pub fn peers_from_endpoint_slice(
        slice: &EndpointSlice,
        config: &EndpointSliceDiscoveryConfig,
    ) -> Vec<Peer> {
        let Some(port) = select_port(slice, config.port_name.as_deref()) else {
            return Vec::new();
        };

        let mut peers = slice
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint
                    .conditions
                    .as_ref()
                    .and_then(|conditions| conditions.ready)
                    .unwrap_or(true)
            })
            .flat_map(|endpoint| endpoint.addresses.iter())
            .filter_map(|address| address.parse::<IpAddr>().ok())
            .map(|ip| Peer::new(SocketAddr::new(ip, port)))
            .filter(|peer| Some(peer.addr) != config.self_addr)
            .collect::<Vec<_>>();

        peers.sort();
        peers.dedup();
        peers
    }

    fn select_port(slice: &EndpointSlice, port_name: Option<&str>) -> Option<u16> {
        let ports = slice.ports.as_ref()?;
        let selected = match port_name {
            Some(name) => ports
                .iter()
                .find(|port| port.name.as_deref() == Some(name))?,
            None => ports.iter().find(|port| port.port.is_some())?,
        };
        selected.port.and_then(|port| u16::try_from(port).ok())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use quickcheck::{Arbitrary, Gen, TestResult};
        use quickcheck_macros::quickcheck;
        use std::collections::BTreeMap;

        #[derive(Clone, Debug)]
        struct EndpointSliceEventsCase {
            events: Vec<EndpointSliceEvent>,
        }

        #[derive(Clone, Debug)]
        struct EndpointSliceEvent {
            slice: u8,
            first: u8,
            second: u8,
            ready: bool,
            delete: bool,
        }

        impl Arbitrary for EndpointSliceEventsCase {
            fn arbitrary(g: &mut Gen) -> Self {
                let mut events = Vec::<EndpointSliceEvent>::arbitrary(g);
                events.truncate(96);
                Self { events }
            }
        }

        impl Arbitrary for EndpointSliceEvent {
            fn arbitrary(g: &mut Gen) -> Self {
                Self {
                    slice: u8::arbitrary(g) % 8,
                    first: (u8::arbitrary(g) % 8) + 2,
                    second: (u8::arbitrary(g) % 8) + 2,
                    ready: bool::arbitrary(g),
                    delete: bool::arbitrary(g),
                }
            }
        }

        fn slice(name: &str, addresses: &[&str], ready: Option<bool>) -> EndpointSlice {
            EndpointSlice {
                address_type: "IPv4".to_string(),
                metadata: ObjectMeta {
                    name: Some(name.to_string()),
                    labels: Some(BTreeMap::from([(
                        "kubernetes.io/service-name".to_string(),
                        "gabion".to_string(),
                    )])),
                    ..Default::default()
                },
                endpoints: vec![Endpoint {
                    addresses: addresses
                        .iter()
                        .map(|address| (*address).to_string())
                        .collect(),
                    conditions: ready.map(|ready| EndpointConditions {
                        ready: Some(ready),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ports: Some(vec![EndpointPort {
                    name: Some("gossip".to_string()),
                    port: Some(18080),
                    ..Default::default()
                }]),
            }
        }

        fn config() -> EndpointSliceDiscoveryConfig {
            EndpointSliceDiscoveryConfig {
                namespace: "default".to_string(),
                service_name: "gabion".to_string(),
                port_name: Some("gossip".to_string()),
                self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
            }
        }

        #[test]
        fn endpoint_slice_parser_deduplicates_and_ignores_self() {
            let peers = peers_from_endpoint_slice(
                &slice("slice-a", &["10.0.0.1", "10.0.0.2", "10.0.0.2"], Some(true)),
                &config(),
            );

            assert_eq!(
                peers,
                vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
            );
        }

        #[test]
        fn endpoint_slice_parser_ignores_not_ready_endpoints() {
            let peers =
                peers_from_endpoint_slice(&slice("slice-a", &["10.0.0.2"], Some(false)), &config());

            assert!(peers.is_empty());
        }

        #[test]
        fn endpoint_slice_peer_set_updates_and_deletes_snapshots() {
            let config = config();
            let mut peer_set = EndpointSlicePeerSet::default();
            let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
            let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));

            peer_set.apply(&slice_a, &config);
            peer_set.apply(&slice_b, &config);
            assert_eq!(peer_set.snapshot_peers().len(), 2);

            peer_set.delete(&slice_a);
            assert_eq!(
                peer_set.snapshot_peers(),
                vec![Peer::new("10.0.0.3:18080".parse().expect("addr"))]
            );
        }

        #[test]
        fn endpoint_slice_peer_set_relist_replaces_missing_slices() {
            let config = config();
            let mut peer_set = EndpointSlicePeerSet::default();
            let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
            let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));
            let slice_c = slice("slice-c", &["10.0.0.4"], Some(true));

            peer_set.apply(&slice_a, &config);
            peer_set.apply(&slice_b, &config);

            let mut relist = EndpointSlicePeerSet::default();
            relist.apply(&slice_c, &config);
            peer_set.replace(relist);

            assert_eq!(
                peer_set.snapshot_peers(),
                vec![Peer::new("10.0.0.4:18080".parse().expect("addr"))]
            );
        }

        #[test]
        fn merged_endpoint_slice_state_deduplicates_across_selectors() {
            let mut state = MergedEndpointSliceState::new(2);
            let peer_a = Peer::new("10.0.0.2:18080".parse().expect("addr"));
            let peer_b = Peer::new("10.0.0.3:18080".parse().expect("addr"));

            let first = state.update(0, vec![peer_a, peer_b]);
            let second = state.update(1, vec![peer_b]);

            assert_eq!(first, vec![peer_a, peer_b]);
            assert_eq!(second, vec![peer_a, peer_b]);
        }

        #[quickcheck]
        fn quickcheck_endpoint_slice_events_match_live_slice_model(
            case: EndpointSliceEventsCase,
        ) -> TestResult {
            let config = config();
            let mut peer_set = EndpointSlicePeerSet::default();
            let mut live = BTreeMap::new();

            for event in case.events {
                let name = format!("slice-{}", event.slice);
                if event.delete {
                    peer_set.delete(&slice(&name, &["10.0.0.9"], Some(true)));
                    live.remove(&name);
                } else {
                    let first = format!("10.0.0.{}", event.first);
                    let second = format!("10.0.0.{}", event.second);
                    let current = slice(&name, &[&first, &second], Some(event.ready));
                    peer_set.apply(&current, &config);

                    let mut peers = if event.ready {
                        vec![
                            Peer::new(format!("{first}:18080").parse().expect("first peer addr")),
                            Peer::new(format!("{second}:18080").parse().expect("second peer addr")),
                        ]
                    } else {
                        Vec::new()
                    };
                    peers.sort();
                    peers.dedup();
                    live.insert(name, peers);
                }

                let mut expected = live.values().flatten().copied().collect::<Vec<_>>();
                expected.sort();
                expected.dedup();
                if peer_set.snapshot_peers() != expected {
                    return TestResult::error(
                        "EndpointSlice peer set diverged from live slice model",
                    );
                }
            }
            TestResult::passed()
        }
    }
}
