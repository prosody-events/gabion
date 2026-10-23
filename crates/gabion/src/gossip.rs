//! Anti-entropy gossip runtime.
//!
//! The runtime ([`GossipRuntime`]) ties together a per-origin counter
//! [`crate::crdt::CellStore`], a datagram [`GossipTransport`], a
//! [`Clock`] for bucket arithmetic and tick scheduling, and a downstream
//! [`AggregateStore`] that absorbs the resulting delta and expiration rows.
//! A single `tokio::select!` drives five arms (limit request, inbound,
//! writable, peer event, tick) and calls `aggregates.apply(...)` once per
//! CRDT-touching iteration.
//!
//! All polymorphic boundaries are monomorphized generics — no `dyn`, no
//! `Box`. The production hot path is branch-free; tests and simulators
//! parameterize the same code paths with [`sim::SimTransport`] and a
//! `TokioClock::from_millis(0)` anchored under `tokio::time::pause()`.

use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU128;
use std::time::Duration;

use crate::crdt::{CellStoreStats, NodeId, NodeIdentity};
use crate::wire::{self, FrameLimits, HmacKey};

mod admin;
mod client;
mod clock;
mod runtime;
pub mod sim;
mod store;
mod transport;

#[cfg(test)]
mod tests;

pub use admin::{AdminCommand, AdminSnapshot, PeerEntry};
#[cfg(feature = "cell-dump")]
pub use admin::{CellDumpEntry, CellDumpSnapshot};
pub use client::{GossipClient, RecordPermit};
pub use clock::{Clock, FixedClock, Ticker, TokioClock};
pub use runtime::GossipRuntime;
pub use store::AggregateStore;
pub use transport::GossipTransport;
#[cfg(feature = "transport-udp")]
pub use transport::UdpTransport;

/// Configuration for [`GossipRuntime`]. Non-generic — holds scalar tuning
/// knobs only. [`Clock`] and [`GossipTransport`] are constructor parameters
/// because they are generic on the runtime.
#[derive(Clone, Debug)]
pub struct GossipConfig {
    /// Local identity used in outbound packet headers and as the cell origin
    /// for locally-observed hits.
    pub local_identity: NodeIdentity,
    /// Cluster identifier mixed into every outbound packet header.
    pub cluster_id_hash: u128,
    /// Peers seeded at startup (in addition to any later peer events).
    pub bootstrap_peers: Vec<SocketAddr>,
    /// Unused by the Plumtree overlay, whose active view is sized by the
    /// cluster alone (`⌈ln(n) + c⌉` peers, `c` =
    /// [`crate::defaults::GOSSIP_COVERAGE_MARGIN`]). Kept so adapter configs
    /// compile unchanged.
    pub fanout: usize,
    /// Cap on the changes one round pushes to one peer, and on the queues of
    /// changes waiting for a round. The wire codec splits a frame into
    /// multiple packets if needed.
    pub max_cells_per_tick: usize,
    /// Per-packet UDP budget + decoder safety limits.
    pub wire_limits: FrameLimits,
    /// Outbound send queue capacity. Each slot owns a pre-allocated
    /// [`wire::PacketBuf`]. A frame that finds the queue full loses its tail.
    pub send_queue_capacity: usize,
    /// Capacity of the incoming limit-request mpsc channel.
    pub limit_queue_capacity: usize,
    /// Period between proactive gossip ticks.
    pub tick_interval: Duration,
    /// Optional HMAC key — when set, every outbound packet is authenticated
    /// and inbound packets must verify.
    pub auth_key: Option<HmacKey>,
    /// Deterministic RNG seed for peer sampling.
    pub rng_seed: u64,
    /// Per-rule error budget in basis points of the rule's own limit, used
    /// for threshold-triggered anti-entropy. See
    /// [`crate::defaults::GOSSIP_TARGET_ERR_BPS`].
    pub target_err_bps: u32,
    /// Minimum gap between two threshold-fire emissions, in milliseconds.
    /// See [`crate::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS`].
    pub min_emit_interval: Duration,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            local_identity: NodeIdentity::new(crate::crdt::NodeId(0), 1),
            cluster_id_hash: 0,
            bootstrap_peers: Vec::new(),
            fanout: 3,
            max_cells_per_tick: 1024,
            wire_limits: FrameLimits::default(),
            send_queue_capacity: 32,
            limit_queue_capacity: 1024,
            tick_interval: Duration::from_millis(100),
            auth_key: None,
            rng_seed: crate::defaults::random_rng_seed().expect("OS entropy for gossip RNG seed"),
            target_err_bps: crate::defaults::GOSSIP_TARGET_ERR_BPS,
            min_emit_interval: Duration::from_millis(crate::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS),
        }
    }
}

/// Parse a cluster identifier as both adapters' configs spell it: decimal
/// (`12345`) or `0x`-prefixed hex (`0xc0ffee`), ignoring surrounding
/// whitespace. `None` for anything else, including `0`.
pub fn parse_cluster_id(value: &str) -> Option<NonZeroU128> {
    let value = value.trim();
    let (digits, radix) = match value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        Some(hex) => (hex, 16),
        None => (value, 10),
    };
    // `from_str_radix` also takes a leading `+`; an id is digits only.
    if !digits.starts_with(|c: char| c.is_digit(radix)) {
        return None;
    }
    u128::from_str_radix(digits, radix)
        .ok()
        .and_then(NonZeroU128::new)
}

/// Parameters of the Plumtree overlay. The defaults are
/// [`crate::defaults`]'s `PLUMTREE_*` constants; a runtime takes other values
/// through [`GossipRuntime::with_plumtree`].
#[derive(Clone, Copy, Debug)]
pub struct PlumtreeConfig {
    /// See [`crate::defaults::PLUMTREE_MISSING_TIMEOUT_MS`].
    pub missing_timeout: Duration,
    /// See [`crate::defaults::PLUMTREE_LAZY_TARGETS`].
    pub lazy_targets: usize,
    /// See [`crate::defaults::PLUMTREE_FULL_IHAVE_EVERY`].
    pub full_ihave_every: u32,
    /// See [`crate::defaults::PLUMTREE_RELAY_FLOOR_MS`].
    pub relay_floor: Duration,
    /// See [`crate::defaults::PLUMTREE_NACK_RETRY_MS`].
    pub nack_retry: Duration,
    /// See [`crate::defaults::PLUMTREE_EXTRA_RANDOM`].
    pub extra_random: usize,
    /// See [`crate::defaults::PLUMTREE_FIRST_CONTACT_PULL`].
    pub first_contact_pull: bool,
}

impl Default for PlumtreeConfig {
    fn default() -> Self {
        use crate::defaults as d;
        Self {
            missing_timeout: Duration::from_millis(d::PLUMTREE_MISSING_TIMEOUT_MS),
            lazy_targets: d::PLUMTREE_LAZY_TARGETS,
            full_ihave_every: d::PLUMTREE_FULL_IHAVE_EVERY,
            relay_floor: Duration::from_millis(d::PLUMTREE_RELAY_FLOOR_MS),
            nack_retry: Duration::from_millis(d::PLUMTREE_NACK_RETRY_MS),
            extra_random: d::PLUMTREE_EXTRA_RANDOM,
            first_contact_pull: d::PLUMTREE_FIRST_CONTACT_PULL,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GossipError {
    #[error("gossip runtime is no longer running")]
    RuntimeShutDown,
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("wire: {0}")]
    Encode(#[from] wire::EncodeError),
}
