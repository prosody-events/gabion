//! Anti-entropy gossip runtime.
//!
//! The runtime ([`GossipRuntime`]) ties together a per-origin counter
//! [`crate::crdt::CellStore`], a datagram [`GossipTransport`], a
//! [`Clock`] for bucket arithmetic and tick scheduling, and a downstream
//! [`AggregateStore`] that absorbs the resulting delta and expiration rows.
//! Each `(rule, key)` has one owner chosen by rendezvous hash over the
//! discovered membership: contributors send their hits to it, it folds them
//! and broadcasts the changed cells to every peer. A single `tokio::select!`
//! drives the arms (limit request, inbound, writable, peer event, admin, and
//! three tickers) and calls `aggregates.apply(...)` once per CRDT-touching
//! iteration.
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
mod membership;
mod outbox;
mod runtime;
mod send_queue;
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
    /// Unused: owner aggregation has no random fanout.
    pub fanout: usize,
    /// Most local cells a node sends to key owners in one sync. The wire
    /// codec splits them into as many packets as they need.
    pub max_cells_per_tick: usize,
    /// Per-packet UDP budget + decoder safety limits.
    pub wire_limits: FrameLimits,
    /// Outbound send queue capacity. Each slot owns a pre-allocated
    /// [`wire::PacketBuf`].
    pub send_queue_capacity: usize,
    /// Capacity of the incoming limit-request mpsc channel.
    pub limit_queue_capacity: usize,
    /// Period of the housekeeping tick that expires cells whose bucket has
    /// left its rule's window.
    pub tick_interval: Duration,
    /// How often hits are sent to key owners and unacknowledged hits are
    /// re-sent. See [`crate::defaults::GOSSIP_SYNC_WAIT_MS`].
    pub sync_wait: Duration,
    /// How often an owner broadcasts its changed cells to every peer. See
    /// [`crate::defaults::GOSSIP_BROADCAST_INTERVAL_MS`].
    pub broadcast_interval: Duration,
    /// Most keys in one owner broadcast. See
    /// [`crate::defaults::GOSSIP_BROADCAST_BATCH_LIMIT`].
    pub broadcast_batch_limit: usize,
    /// Optional HMAC key — when set, every outbound packet is authenticated
    /// and inbound packets must verify.
    pub auth_key: Option<HmacKey>,
    /// Unused: owner aggregation draws no random numbers.
    pub rng_seed: u64,
    /// Unused: owner aggregation has no threshold-triggered emit.
    pub target_err_bps: u32,
    /// Unused: owner aggregation has no threshold-triggered emit.
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
            sync_wait: Duration::from_millis(crate::defaults::GOSSIP_SYNC_WAIT_MS),
            broadcast_interval: Duration::from_millis(
                crate::defaults::GOSSIP_BROADCAST_INTERVAL_MS,
            ),
            broadcast_batch_limit: crate::defaults::GOSSIP_BROADCAST_BATCH_LIMIT,
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

#[derive(Debug, thiserror::Error)]
pub enum GossipError {
    #[error("gossip runtime is no longer running")]
    RuntimeShutDown,
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("wire: {0}")]
    Encode(#[from] wire::EncodeError),
}
