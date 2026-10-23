//! Admin snapshot channel for observability.
//!
//! Lives outside the hot path: the runtime services [`AdminCommand`] as a
//! sixth `select!` arm only when the constructor was handed a receiver, so the
//! production loop without admin is unchanged. The reply is built inside the
//! gossip task — callers never get a `Sync` borrow of the runtime's internals.

use std::net::SocketAddr;

use tokio::sync::oneshot;

use super::{CellStoreStats, NodeId, NodeIdentity};
#[cfg(feature = "cell-dump")]
use crate::crdt::{BucketEpoch, Incarnation};

/// Request sent into the gossip runtime over the admin channel. New shapes
/// (e.g. forced repair) can be added without changing the `select!` arm.
pub enum AdminCommand {
    Snapshot {
        reply: oneshot::Sender<AdminSnapshot>,
    },
    /// Full per-cell dump of the runtime's `CellStore`. Built inside the
    /// gossip task — the store is owned there and cannot be aliased from the
    /// driver, so the reply carries owned scalars (the same owned-data-out
    /// contract as [`AdminCommand::Snapshot`]). Feature-gated behind
    /// `cell-dump` so the production runtime never compiles the
    /// cell-iteration path.
    #[cfg(feature = "cell-dump")]
    CellDump {
        reply: oneshot::Sender<CellDumpSnapshot>,
    },
}

/// Point-in-time view of the gossip runtime. Built inside the task so no
/// internal state crosses a thread boundary by reference. Cheap to produce —
/// every field is a small scalar copy.
#[derive(Clone, Debug)]
pub struct AdminSnapshot {
    pub local_identity: NodeIdentity,
    pub peers: Vec<PeerEntry>,
    pub store_stats: CellStoreStats,
    /// Local cells queued for sync to their owners.
    pub local_dirty_len: u32,
    /// Owned cells queued for the next broadcast.
    pub forwarded_dirty_len: u32,
    pub send_pending_depth: usize,
    pub decode_reject_count: u64,
    /// High-water mark of `send_pending_depth` since startup. Useful for
    /// verifying that the `try_send` `WouldBlock` re-queue path was
    /// exercised under saturation — without re-queue, the depth never
    /// rises above 1.
    pub max_send_pending_depth: usize,
    /// Cumulative sync and broadcast ticks the runtime has processed.
    pub ticks_total: u64,
    /// Always zero: owner aggregation has no threshold-triggered emit.
    pub threshold_fires: u64,
    /// Cumulative ticks that sent at least one frame.
    pub dirty_ticks: u64,
    /// The audience size of the most recent owner broadcast: every peer.
    /// `0` before the first broadcast.
    pub last_effective_fanout: usize,
    /// High-water mark of `last_effective_fanout` since startup.
    pub peak_effective_fanout: usize,
    /// Always zero: owner aggregation has no per-rule error budget.
    pub last_error_budget: u64,
}

/// One peer known to the runtime. `node_id` is `None` until the runtime has
/// received a packet from this peer.
#[derive(Clone, Copy, Debug)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub node_id: Option<NodeId>,
}

/// Full per-cell view of one runtime's `CellStore`, built inside the gossip
/// task in response to [`AdminCommand::CellDump`]. Every field is an owned
/// scalar resolved through the rule and node dictionaries so the consumer can
/// render counts, ages, and per-origin attribution without a second channel
/// round-trip or any borrow of runtime internals.
#[cfg(feature = "cell-dump")]
#[derive(Clone, Debug)]
pub struct CellDumpSnapshot {
    pub local_identity: NodeIdentity,
    pub cells: Vec<CellDumpEntry>,
}

/// One active CRDT cell. Identity fields are the resolved descriptor values,
/// not the compact dictionary slots: `rule_fingerprint` is the rule's stable
/// fingerprint and `origin_node_id` is the originating node's id (both `u128`,
/// rendered as hex across the wasm boundary).
#[cfg(feature = "cell-dump")]
#[derive(Clone, Copy, Debug)]
pub struct CellDumpEntry {
    pub rule_fingerprint: u128,
    pub key_hash: u128,
    pub bucket: BucketEpoch,
    pub count: u64,
    pub last_update_millis: u64,
    pub origin_sequence: u64,
    /// Originating node identity, resolved through the node dictionary.
    /// `None` if the origin slot is no longer interned (not expected for an
    /// active cell, but the lookup is fallible by contract).
    pub origin_node_id: Option<u128>,
    pub origin_incarnation: Option<Incarnation>,
    /// Rule parameters from the rule dictionary, so the consumer can do
    /// window/bucket and limit math directly. Zeroed if the rule slot is no
    /// longer interned.
    pub rule_window_millis: u32,
    pub rule_bucket_millis: u32,
    pub rule_limit: u64,
    /// Whether this cell's rule contributes to the local rate-limit
    /// aggregate (`false` for wire-only rules this node hasn't registered).
    pub applies_locally: bool,
}
