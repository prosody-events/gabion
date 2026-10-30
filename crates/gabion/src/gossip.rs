//! Anti-entropy gossip runtime.
//!
//! The runtime ([`GossipRuntime`]) ties together a per-origin counter
//! [`crate::crdt::CellStore`], a datagram [`GossipTransport`], a
//! [`Clock`] for bucket arithmetic and tick scheduling, and a downstream
//! [`AggregateStore`] that absorbs the resulting delta and expiration rows.
//! A single `tokio::select!` drives five arms (limit request, heartbeat,
//! prompt round, inbound, peer event), plus admin commands when wired, and
//! calls `aggregates.apply(...)` once per CRDT-touching iteration. Each node
//! sends its own changed cells as a per-peer stream the peer acknowledges,
//! directly to every peer of its zone and, flagged `RELAY_INTO_ZONE`, to one
//! rendezvous-chosen gateway in each other zone, which relays them into its
//! zone on a second stream; a gap report or a heartbeat without an ack brings
//! a resend. A peer that leaves a packet unanswered for
//! `gossip.suspect_after` is suspect: it loses the gateway role and is sent
//! only header-only probes until it answers.
//!
//! All polymorphic boundaries are monomorphized generics — no `dyn`, no
//! `Box`. Tests and simulators parameterize the same code paths with
//! [`sim::SimTransport`] and a `TokioClock::from_millis(0)` anchored under
//! `tokio::time::pause()`.

use std::io;
use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU16, NonZeroU32, NonZeroU128, NonZeroUsize};

use crate::defaults;
use crate::settings::{MIN_LIMIT_QUEUE_CAPACITY, Millis, Setting};
use crate::wire::FrameLimits;
use crate::zone::ZoneName;

mod admin;
mod client;
mod clock;
#[cfg(test)]
mod mock;
mod peer;
mod runtime;
pub mod sim;
mod store;
mod sweep;
mod transport;

#[cfg(test)]
mod tests;

pub use admin::{
    AdminCommand, AdminSnapshot, AnswerState, GossipCounters, PeerEntry, PeerZone, RoleCounts,
};
#[cfg(feature = "cell-dump")]
pub use admin::{CellDumpEntry, CellDumpSnapshot};
pub use client::{GossipClient, RecordPermit, TryRecordError};
pub use clock::{Clock, FixedClock, Ticker, TokioClock};
pub use peer::Role;
pub use runtime::GossipRuntime;
pub use store::AggregateStore;
pub use transport::GossipTransport;
#[cfg(feature = "transport-udp")]
pub use transport::UdpTransport;

/// Configuration for [`GossipRuntime`], built only by
/// [`crate::settings::Settings::validate`], so every value is in range.
/// [`Clock`] and [`GossipTransport`] are constructor parameters because they
/// are generic on the runtime. The public fields are the adapter's to set.
#[derive(Clone, Debug)]
pub struct ValidGossipConfig {
    /// Cluster identifier written into every outbound packet header; a
    /// packet carrying any other is dropped.
    pub(crate) cluster_id_hash: NonZeroU128,
    /// Peers seeded at startup (in addition to any later peer events).
    pub bootstrap_peers: Vec<SocketAddr>,
    /// Rows of the runtime's peer table, `gossip.peer_capacity`; a peer
    /// discovery reports past it is declined and logged as `peer_table_full`.
    pub(crate) peer_capacity: NonZeroU16,
    /// The per-round quantum `q`, from `storage.max_cells` and the packet
    /// size (see [`defaults::GOSSIP_BACKLOG_ROUNDS`]); a stream with more
    /// waiting goes on in the next prompt round.
    pub(crate) quantum: NonZeroUsize,
    /// `gossip.sweep_concurrency`: the most join sweeps this node answers at
    /// once.
    pub(crate) sweep_concurrency: NonZeroU8,
    /// The packet size limit.
    pub(crate) wire_limits: FrameLimits,
    /// Capacity of the incoming limit-request channel.
    pub(crate) limit_queue_capacity: NonZeroUsize,
    /// Receive buffer, in bytes, the UDP socket asks the kernel for.
    #[cfg(feature = "transport-udp")]
    pub(crate) recv_buffer_bytes: NonZeroU32,
    /// Send buffer, in bytes, the UDP socket asks the kernel for; the
    /// `send_blocked` warning also names it, so it exists in every build.
    pub(crate) send_buffer_bytes: NonZeroU32,
    /// Period between proactive gossip ticks.
    pub(crate) tick_interval: Millis,
    /// Per-rule error budget in basis points of the rule's own limit, used
    /// for threshold-triggered anti-entropy. See
    /// [`crate::defaults::GOSSIP_TARGET_ERR_BPS`].
    pub(crate) target_err_bps: NonZeroU32,
    /// Minimum gap between two threshold-fire emissions. See
    /// [`crate::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS`].
    pub(crate) min_emit_interval: Millis,
    /// `gossip.suspect_after`: how long a packet may go unanswered before its
    /// peer is suspect.
    pub(crate) suspect_after: Millis,
    /// This node's zone, announced in every header; `None` runs the node as
    /// one zone with every peer.
    pub zone: Option<ZoneName>,
}

/// A heartbeat shorter than this many milliseconds may resend before the
/// answer to the first copy arrives: four times a generous cross-zone round
/// trip.
const TICK_NEAR_ROUND_TRIP_MS: u64 = 20;

impl ValidGossipConfig {
    /// Shrink the runtime's request channel to [`MIN_LIMIT_QUEUE_CAPACITY`],
    /// for an adapter whose `gossip.limit_queue_capacity` sizes a queue of its
    /// own that it drains into the runtime (NGINX's shared-memory queue). A
    /// backlog then waits in that queue, and the shallow channel recycles
    /// tokio's storage instead of allocating.
    pub fn with_shallow_limit_queue(self) -> Self {
        Self {
            limit_queue_capacity: MIN_LIMIT_QUEUE_CAPACITY,
            ..self
        }
    }

    /// Ask for the smallest socket buffers, for a node with no peers: its
    /// socket carries no gossip, so a host cap is no reason to warn.
    #[cfg(feature = "transport-udp")]
    pub fn with_minimal_socket_buffers(self) -> Self {
        Self {
            recv_buffer_bytes: crate::settings::MIN_SOCKET_BUFFER_BYTES,
            send_buffer_bytes: crate::settings::MIN_SOCKET_BUFFER_BYTES,
            ..self
        }
    }

    /// Log what the settings say at startup: `default_cluster_id` when this
    /// node uses the built-in cluster id, and `setting_warning` for a
    /// heartbeat near a round trip. Each adapter calls it once, when the node
    /// starts gossiping.
    pub fn note_startup(&self) {
        let tick_ms = self.tick_interval.0.get();
        if tick_ms < TICK_NEAR_ROUND_TRIP_MS {
            tracing::warn!(
                code = "setting_warning",
                cause = "tick_near_round_trip",
                tick_ms,
                config_key = Setting::GossipTickInterval.yaml_key(),
                "{} is {tick_ms} ms. gabion resends any packet left unanswered for one heartbeat, \
                 so with a heartbeat this short a resend can leave before the answer to the first \
                 copy arrives (a round trip between zones takes a few milliseconds). That costs \
                 bytes, not counts: suspicion waits for `gossip.suspect_after`, whose default \
                 stays at least 2 s however short the heartbeat. If `/snapshot` shows \
                 `unanswered_ms` regularly above {tick_ms} for healthy peers, raise the heartbeat \
                 to at least four times the round trip between your zones.",
                Setting::GossipTickInterval,
            );
        }
        if self.cluster_id_hash.get() == defaults::GOSSIP_CLUSTER_ID_HASH {
            tracing::info!(
                code = "default_cluster_id",
                cluster_id_hash = self.cluster_id_hash,
                config_key = Setting::GossipClusterIdHash.yaml_key(),
                "This node uses the default gossip cluster id, 1. Any other gabion cluster left \
                 at the default on the same network and gossip port would merge counts with this \
                 one. Set {} to a value unique to this cluster.",
                Setting::GossipClusterIdHash,
            );
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GossipError {
    #[error("gossip runtime is no longer running")]
    RuntimeShutDown,
    #[error("io: {0}")]
    Io(#[from] io::Error),
}
