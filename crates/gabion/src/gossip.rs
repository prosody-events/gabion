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
use std::time::Duration;

use crate::crdt::NodeIdentity;
use crate::wire::{self, FrameLimits, HmacKey};

mod client;
mod clock;
mod runtime;
pub mod sim;
mod store;
mod transport;

#[cfg(test)]
mod tests;

pub use client::GossipClient;
pub use clock::{Clock, FixedClock, TokioClock};
pub use runtime::GossipRuntime;
pub use store::AggregateStore;
pub use transport::{GossipTransport, UdpTransport};

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
    /// Number of peers contacted per gossip tick.
    pub fanout: usize,
    /// Frame composition: how many cells `fill_gossip_frame*` may emit per
    /// tick. The wire codec splits this into multiple packets if needed.
    pub max_cells_per_tick: usize,
    /// Per-packet UDP budget + decoder safety limits.
    pub wire_limits: FrameLimits,
    /// Outbound send queue capacity. Each slot owns a pre-allocated
    /// [`wire::PacketBuf`].
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
            rng_seed: 0x9E37_79B9_7F4A_7C15,
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
