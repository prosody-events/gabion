//! Standalone gossip runtime.
//!
//! Invariants:
//! - Peer discovery is injected through `PeerHandler`.
//! - Message communication is injected through `GossipTransport`; UDP is
//!   optional.
//! - Unknown senders are rejected before decode and merge.
//! - Recently removed peers remain accepted only through the configured grace
//!   window.
//! - Dirty overflow forces a bounded resync rather than dropping convergence
//!   forever.
//! - Send and receive buffers are allocated at construction and reused per
//!   tick.
//! - Ticks are deterministic for a supplied timestamp.

use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::SharedLimiter;
use crate::discovery::{Peer, PeerHandler, PeerSnapshot};
use crate::gossip::{
    CellTable, DecodeError, GossipHeader, GossipLimits, GossipMetrics, GossipSendPolicy,
    GossipSendReason, GossipSpaceUsage, HmacKey, ShardDigest,
    decode_authenticated_message_visit_checked, decode_message_visit_checked,
    encode_authenticated_message_parts, encode_message_parts,
};
use thiserror::Error;

use crate::{GossipConfig, RuntimePeerHandler};

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct GossipAdminSnapshot {
    pub cluster_id_hash: u128,
    pub sender_node_id: crate::gossip::NodeId,
    pub sender_incarnation: u64,
    pub active_peers: Vec<SocketAddr>,
    pub recent_peers: Vec<RecentPeerSnapshot>,
    pub discovery_generation: u64,
    pub local_only: bool,
    pub discovery_stale: bool,
    pub remote_active_cells: usize,
    pub remote_cell_capacity: usize,
    pub remote_dirty_ring_len: usize,
    pub remote_dirty_overflow: bool,
    pub remote_cells_sample: Vec<crate::gossip::CounterCell>,
    pub metrics: GossipMetrics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
pub struct RecentPeerSnapshot {
    pub addr: SocketAddr,
    pub expires_millis: u64,
}

pub type SharedGossipAdminSnapshot = Arc<Mutex<GossipAdminSnapshot>>;

pub trait GossipTransport {
    fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool;
    fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)>;
}

#[derive(Debug)]
pub struct UdpGossipTransport {
    socket: UdpSocket,
}

impl UdpGossipTransport {
    pub fn bind(bind: SocketAddr) -> Result<Self, GossipRuntimeError> {
        let socket = UdpSocket::bind(bind).map_err(GossipRuntimeError::Bind)?;
        socket
            .set_nonblocking(true)
            .map_err(GossipRuntimeError::Configure)?;
        Ok(Self { socket })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, GossipRuntimeError> {
        self.socket
            .local_addr()
            .map_err(GossipRuntimeError::LocalAddr)
    }
}

impl GossipTransport for UdpGossipTransport {
    fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool {
        matches!(self.socket.send_to(payload, peer.addr), Ok(sent) if sent == payload.len())
    }

    fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)> {
        match self.socket.recv_from(buffer) {
            Ok((len, addr)) => Some((Peer::new(addr), len)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandaloneGossipConfig {
    pub cluster_id_hash: u128,
    pub sender_node_id: crate::gossip::NodeId,
    pub sender_incarnation: u64,
    pub fanout: usize,
    pub max_payload_bytes: usize,
    pub max_cells_per_frame: usize,
    pub remote_cell_capacity: usize,
    pub remote_dirty_capacity: usize,
    pub auth_key: Option<HmacKey>,
    pub max_peers: usize,
    pub recent_peer_grace_millis: u64,
    pub send_policy: GossipSendPolicy,
}

impl StandaloneGossipConfig {
    pub fn from_config(config: &GossipConfig, remote_cell_capacity: usize) -> Self {
        Self {
            cluster_id_hash: config.cluster_id_hash,
            sender_node_id: crate::gossip::NodeId::from(1_u128),
            sender_incarnation: 1,
            fanout: config.fanout.max(1),
            max_payload_bytes: config
                .max_payload_bytes
                .max(crate::gossip::GOSSIP_HEADER_LEN),
            max_cells_per_frame: config.max_cells_per_frame.max(1),
            remote_cell_capacity,
            remote_dirty_capacity: remote_cell_capacity,
            auth_key: None,
            max_peers: 128,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(config.linger_ms),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GossipTickSummary {
    pub peers_seen: usize,
    pub peers_sent: usize,
    pub send_failures: usize,
    pub cells_sent: usize,
    pub frames_received: usize,
    pub cells_merged: usize,
    pub peer_rejected: usize,
    pub local_only: bool,
    pub discovery_stale: bool,
    pub send_reason: Option<GossipSendReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecentPeer {
    peer: Peer,
    expires_millis: u64,
}

#[derive(Clone, Debug)]
struct PeerAuthorizer {
    current: Vec<Peer>,
    recent: Vec<RecentPeer>,
    grace_millis: u64,
}

impl PeerAuthorizer {
    fn with_capacity(max_peers: usize, grace_millis: u64) -> Self {
        Self {
            current: Vec::with_capacity(max_peers),
            recent: Vec::with_capacity(max_peers),
            grace_millis,
        }
    }

    fn update(&mut self, now_millis: u64, peers: &[Peer]) {
        self.expire(now_millis);

        for index in 0..self.current.len() {
            let peer = self.current[index];
            if !peers.contains(&peer) {
                self.insert_recent(peer, now_millis.saturating_add(self.grace_millis));
            }
        }

        self.current.clear();
        for peer in peers.iter().copied().take(self.current.capacity()) {
            if !self.current.contains(&peer) {
                self.current.push(peer);
            }
        }
    }

    fn accepts(&mut self, peer: Peer, now_millis: u64) -> bool {
        self.expire(now_millis);
        self.current.contains(&peer) || self.recent.iter().any(|entry| entry.peer == peer)
    }

    fn expire(&mut self, now_millis: u64) {
        self.recent
            .retain(|entry| entry.expires_millis > now_millis);
    }

    fn insert_recent(&mut self, peer: Peer, expires_millis: u64) {
        if self.recent.iter().any(|entry| entry.peer == peer) {
            return;
        }
        if self.recent.len() == self.recent.capacity() && !self.recent.is_empty() {
            self.recent.swap_remove(0);
        }
        if self.recent.len() < self.recent.capacity() {
            self.recent.push(RecentPeer {
                peer,
                expires_millis,
            });
        }
    }

    fn recent_snapshot(&self) -> Vec<RecentPeerSnapshot> {
        self.recent
            .iter()
            .map(|entry| RecentPeerSnapshot {
                addr: entry.peer.addr,
                expires_millis: entry.expires_millis,
            })
            .collect()
    }
}

pub struct StandaloneGossipRuntime<T: GossipTransport, P: PeerHandler = RuntimePeerHandler> {
    limiter: SharedLimiter,
    peers: P,
    transport: T,
    config: StandaloneGossipConfig,
    remote_cells: CellTable,
    send_buffer: Vec<u8>,
    recv_buffer: Vec<u8>,
    cell_buffer: Vec<crate::gossip::CounterCell>,
    digest_buffer: Vec<ShardDigest>,
    peer_authorizer: PeerAuthorizer,
    force_resync: bool,
    last_send_millis: u64,
    metrics: GossipMetrics,
    admin_snapshot: Option<SharedGossipAdminSnapshot>,
}

impl<T: GossipTransport, P: PeerHandler> StandaloneGossipRuntime<T, P> {
    pub fn new(
        limiter: SharedLimiter,
        peers: P,
        transport: T,
        config: StandaloneGossipConfig,
    ) -> Self {
        Self::new_with_admin(limiter, peers, transport, config, None)
    }

    pub fn new_with_admin(
        limiter: SharedLimiter,
        peers: P,
        transport: T,
        config: StandaloneGossipConfig,
        admin_snapshot: Option<SharedGossipAdminSnapshot>,
    ) -> Self {
        Self {
            limiter,
            peers,
            transport,
            config,
            remote_cells: CellTable::with_capacity(
                config.remote_cell_capacity,
                config.remote_dirty_capacity,
            ),
            send_buffer: Vec::with_capacity(config.max_payload_bytes),
            recv_buffer: vec![0; config.max_payload_bytes],
            cell_buffer: Vec::with_capacity(config.max_cells_per_frame),
            digest_buffer: Vec::with_capacity(1),
            peer_authorizer: PeerAuthorizer::with_capacity(
                config.max_peers,
                config.recent_peer_grace_millis,
            ),
            force_resync: false,
            last_send_millis: 0,
            metrics: GossipMetrics::default(),
            admin_snapshot,
        }
    }

    pub fn tick(&mut self, now_millis: u64) -> GossipTickSummary {
        let snapshot = self.peers.snapshot();
        self.peer_authorizer.update(now_millis, snapshot.peers());
        let mut summary = GossipTickSummary {
            peers_seen: snapshot.peers().len(),
            local_only: snapshot.local_only(),
            discovery_stale: snapshot.stale(),
            ..GossipTickSummary::default()
        };

        if !snapshot.local_only() {
            let send_reason = if self.force_resync {
                Some(GossipSendReason::DirtyOverflow)
            } else {
                self.local_usage().and_then(|usage| {
                    self.config
                        .send_policy
                        .should_send(now_millis, self.last_send_millis, usage)
                })
            };
            summary.send_reason = send_reason;

            if send_reason.is_some() {
                self.collect_dirty_cells();
                let header = GossipHeader {
                    cluster_id_hash: self.config.cluster_id_hash,
                    sender_node_id: self.config.sender_node_id,
                    sender_incarnation: self.config.sender_incarnation,
                    min_bucket: 0,
                    max_bucket: 0,
                    flags: 0,
                };
                let truncated = if let Some(key) = self.config.auth_key {
                    encode_authenticated_message_parts(
                        header,
                        self.digest_buffer.as_slice(),
                        self.cell_buffer.as_slice(),
                        false,
                        key,
                        &mut self.send_buffer,
                        GossipLimits {
                            max_payload_bytes: self.config.max_payload_bytes,
                            max_digests: 64,
                            max_cells: self.config.max_cells_per_frame,
                        },
                    )
                } else {
                    encode_message_parts(
                        header,
                        self.digest_buffer.as_slice(),
                        self.cell_buffer.as_slice(),
                        false,
                        &mut self.send_buffer,
                        self.config.max_payload_bytes,
                    )
                };
                self.metrics.record_send(self.send_buffer.len(), truncated);
                summary.cells_sent = self.cell_buffer.len();

                for peer in snapshot.peers().iter().take(self.config.fanout) {
                    if self.transport.send_to(*peer, self.send_buffer.as_slice()) {
                        summary.peers_sent = summary.peers_sent.saturating_add(1);
                    } else {
                        summary.send_failures = summary.send_failures.saturating_add(1);
                    }
                }
                self.last_send_millis = now_millis;
                tracing::debug!(
                    ?send_reason,
                    cells = summary.cells_sent,
                    peers = summary.peers_sent,
                    failures = summary.send_failures,
                    bytes = self.send_buffer.len(),
                    truncated,
                    "gossip frame sent"
                );
            }
        }

        while let Some((peer, len)) = self.transport.recv_into(self.recv_buffer.as_mut_slice()) {
            if !self.peer_authorizer.accepts(peer, now_millis) {
                summary.peer_rejected = summary.peer_rejected.saturating_add(1);
                continue;
            }
            self.metrics.record_recv(len);
            summary.frames_received = summary.frames_received.saturating_add(1);
            summary.cells_merged = summary
                .cells_merged
                .saturating_add(self.merge_frame(now_millis, len));
        }

        self.publish_admin_snapshot(&snapshot);
        summary
    }

    pub fn metrics(&self) -> GossipMetrics {
        self.metrics
    }

    pub fn limiter(&self) -> &SharedLimiter {
        &self.limiter
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn admin_snapshot(&self) -> GossipAdminSnapshot {
        let snapshot = self.peers.snapshot();
        self.build_admin_snapshot(&snapshot)
    }

    #[cfg(test)]
    pub(crate) fn buffer_capacities(&self) -> RuntimeBufferCapacities {
        RuntimeBufferCapacities {
            send: self.send_buffer.capacity(),
            recv: self.recv_buffer.capacity(),
            cells: self.cell_buffer.capacity(),
            digests: self.digest_buffer.capacity(),
        }
    }

    fn publish_admin_snapshot(&self, snapshot: &PeerSnapshot) {
        let Some(shared) = &self.admin_snapshot else {
            return;
        };
        if let Ok(mut admin_snapshot) = shared.lock() {
            *admin_snapshot = self.build_admin_snapshot(snapshot);
        }
    }

    fn build_admin_snapshot(&self, snapshot: &PeerSnapshot) -> GossipAdminSnapshot {
        GossipAdminSnapshot {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.config.sender_node_id,
            sender_incarnation: self.config.sender_incarnation,
            active_peers: snapshot.peers().iter().map(|peer| peer.addr).collect(),
            recent_peers: self.peer_authorizer.recent_snapshot(),
            discovery_generation: snapshot.generation(),
            local_only: snapshot.local_only(),
            discovery_stale: snapshot.stale(),
            remote_active_cells: self.remote_cells.active_cell_count(),
            remote_cell_capacity: self.remote_cells.capacity(),
            remote_dirty_ring_len: self.remote_cells.dirty_len(),
            remote_dirty_overflow: self.remote_cells.dirty_overflowed(),
            remote_cells_sample: self
                .remote_cells
                .cells()
                .take(self.config.max_cells_per_frame)
                .map(|(_, cell)| cell)
                .collect(),
            metrics: self.metrics,
        }
    }

    fn local_usage(&self) -> Option<GossipSpaceUsage> {
        let Ok(limiter) = self.limiter.lock() else {
            return None;
        };
        let summary = limiter.storage_summary();
        Some(GossipSpaceUsage {
            active_cells: summary.active_cells,
            max_cells: summary.max_cells,
            dirty_cells: summary.dirty_ring_len,
            dirty_capacity: summary.dirty_ring_capacity,
            dirty_overflowed: summary.dirty_overflow,
        })
    }

    fn collect_dirty_cells(&mut self) {
        self.cell_buffer.clear();
        self.digest_buffer.clear();
        let Ok(limiter) = self.limiter.lock() else {
            return;
        };

        self.digest_buffer.push(crate::gossip::digest_cells(
            limiter.cells().map(convert_core_cell),
            0,
            1,
        ));

        if limiter.dirty_overflowed() || self.force_resync {
            self.metrics.dirty_overflow = self.metrics.dirty_overflow.saturating_add(1);
            for cell in limiter.cells().take(self.config.max_cells_per_frame) {
                self.cell_buffer.push(convert_core_cell(cell));
            }
            self.force_resync = false;
        } else {
            for cell in limiter.dirty_cells().take(self.config.max_cells_per_frame) {
                self.cell_buffer.push(convert_core_cell(cell));
            }
        }
    }

    fn merge_frame(&mut self, now_millis: u64, len: usize) -> usize {
        let limits = GossipLimits {
            max_payload_bytes: self.config.max_payload_bytes,
            max_digests: 64,
            max_cells: self.config.max_cells_per_frame,
        };
        let mut merged = 0_usize;
        let mut decode_error = None;
        let accept_header = |header: GossipHeader| {
            header.cluster_id_hash == self.config.cluster_id_hash
                && header.sender_node_id != self.config.sender_node_id
        };
        let mut received_digest = None;
        let on_digest = |digest: ShardDigest| {
            received_digest = Some(digest);
        };
        let mut on_cell = |cell| {
            let Ok(mut limiter) = self.limiter.lock() else {
                return;
            };
            match self
                .remote_cells
                .merge_remote(cell, Some(&mut limiter), now_millis)
            {
                Ok(outcome) => {
                    if outcome.changed {
                        merged = merged.saturating_add(1);
                        self.metrics.merge_cells = self.metrics.merge_cells.saturating_add(1);
                    }
                }
                Err(_) => {
                    decode_error = Some(DecodeError::CapacityExceeded);
                }
            }
        };

        let result = if let Some(key) = self.config.auth_key {
            decode_authenticated_message_visit_checked(
                &self.recv_buffer[..len],
                key,
                limits,
                accept_header,
                on_digest,
                &mut on_cell,
            )
        } else {
            decode_message_visit_checked(
                &self.recv_buffer[..len],
                limits,
                accept_header,
                on_digest,
                &mut on_cell,
            )
        };
        if matches!(result, Err(DecodeError::AuthenticationFailed)) {
            self.metrics.auth_failures = self.metrics.auth_failures.saturating_add(1);
        }
        if result.is_err() || decode_error.is_some() {
            self.metrics.decode_errors = self.metrics.decode_errors.saturating_add(1);
        }
        if let Some(digest) = received_digest
            && self.remote_cells.digest(digest.shard_id, 1) != digest
        {
            self.metrics.digest_mismatch = self.metrics.digest_mismatch.saturating_add(1);
            self.force_resync = true;
        }
        merged
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeBufferCapacities {
    pub send: usize,
    pub recv: usize,
    pub cells: usize,
    pub digests: usize,
}

pub async fn run_udp_runtime(
    limiter: SharedLimiter,
    peers: RuntimePeerHandler,
    config: GossipConfig,
    remote_cell_capacity: usize,
) -> Result<(), GossipRuntimeError> {
    run_udp_runtime_with_admin(limiter, peers, config, remote_cell_capacity, None).await
}

pub async fn run_udp_runtime_with_admin(
    limiter: SharedLimiter,
    peers: RuntimePeerHandler,
    config: GossipConfig,
    remote_cell_capacity: usize,
    admin_snapshot: Option<SharedGossipAdminSnapshot>,
) -> Result<(), GossipRuntimeError> {
    let bind = config.bind.ok_or(GossipRuntimeError::MissingBind)?;
    let transport = UdpGossipTransport::bind(bind)?;
    let runtime_config = StandaloneGossipConfig::from_config(&config, remote_cell_capacity);
    let runtime = StandaloneGossipRuntime::new_with_admin(
        limiter,
        peers,
        transport,
        runtime_config,
        admin_snapshot,
    );
    run_runtime(runtime, config.linger_ms).await
}

pub async fn run_runtime<T: GossipTransport, P: PeerHandler>(
    mut runtime: StandaloneGossipRuntime<T, P>,
    linger_ms: u64,
) -> Result<(), GossipRuntimeError> {
    let wake_millis = linger_ms.max(1).div_ceil(4).max(1);
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(wake_millis));

    loop {
        interval.tick().await;
        runtime.tick(now_millis());
    }
}

fn convert_core_cell(cell: crate::core::CounterCell) -> crate::gossip::CounterCell {
    crate::gossip::CounterCell {
        rule_id: cell.rule_id,
        key_hash: cell.key_hash,
        bucket_start_millis: cell.bucket_start_millis.min(i64::MAX as u64) as i64,
        origin_node_id: u128::from(cell.origin_node_id).into(),
        origin_incarnation: cell.origin_incarnation,
        count: cell.count,
        last_update_millis: cell.last_update_millis,
        sequence: cell.sequence,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, Error)]
pub enum GossipRuntimeError {
    #[error("gossip bind address is required")]
    MissingBind,
    #[error("invalid gossip runtime config: {0}")]
    Config(#[from] crate::ConfigError),
    #[error("gossip discovery failed: {0}")]
    Discovery(String),
    #[error("failed to bind gossip socket: {0}")]
    Bind(std::io::Error),
    #[error("failed to configure gossip socket: {0}")]
    Configure(std::io::Error),
    #[error("failed to read gossip socket address: {0}")]
    LocalAddr(std::io::Error),
}
