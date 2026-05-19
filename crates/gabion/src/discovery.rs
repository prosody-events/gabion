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

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub mod kubernetes;
#[cfg(test)]
mod tests;

pub const DEFAULT_MAX_PEERS: usize = 128;
pub const DEFAULT_RECENT_PEER_GRACE_MILLIS: u64 = 30_000;
pub const DEFAULT_GOSSIP_PORT_NAME: &str = "gossip";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    #[default]
    Auto,
    None,
    Static,
    File,
    #[serde(rename = "kubernetes")]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    Added(Peer),
    Removed(Peer),
}

#[derive(Clone, Debug)]
pub struct ChannelPeerHandler {
    snapshot: SnapshotPeerHandler,
    events: mpsc::Sender<PeerEvent>,
}

impl ChannelPeerHandler {
    pub fn with_capacity(max_peers: usize, events: mpsc::Sender<PeerEvent>) -> Self {
        Self {
            snapshot: SnapshotPeerHandler::with_capacity(max_peers),
            events,
        }
    }
}

impl PeerHandler for ChannelPeerHandler {
    fn snapshot(&self) -> PeerSnapshot {
        self.snapshot.snapshot()
    }

    fn peer_added(&self, peer: Peer) {
        let before = self.snapshot.snapshot().generation();
        self.snapshot.peer_added(peer);
        if self.snapshot.snapshot().generation() != before {
            let _ = self.events.try_send(PeerEvent::Added(peer));
        }
    }

    fn peer_removed(&self, peer: Peer) {
        let before = self.snapshot.snapshot().generation();
        self.snapshot.peer_removed(peer);
        if self.snapshot.snapshot().generation() != before {
            let _ = self.events.try_send(PeerEvent::Removed(peer));
        }
    }
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

pub async fn run_file_peer_events(handler: FilePeerHandler, poll_millis: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(poll_millis.max(1)));

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
