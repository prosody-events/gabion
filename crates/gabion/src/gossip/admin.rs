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
    pub local_dirty_len: u32,
    pub forwarded_dirty_len: u32,
    pub send_pending_depth: usize,
    pub decode_reject_count: u64,
    /// High-water mark of `send_pending_depth` since startup. Useful for
    /// verifying that the `try_send` `WouldBlock` re-queue path was
    /// exercised under saturation — without re-queue, the depth never
    /// rises above 1.
    pub max_send_pending_depth: usize,
    /// Cumulative ticks the runtime has processed. Includes both
    /// heartbeat ticks (the proactive timer) and synthetic ticks
    /// triggered by `handle_limit_request` crossing a per-rule error
    /// budget. Combined with `threshold_fires`, lets observers split
    /// the two.
    pub ticks_total: u64,
    /// Subset of `ticks_total` that were threshold-triggered — i.e. a
    /// `LimitRequest` crossed `target_err_bps × limit / (10_000 × N)`
    /// and `min_emit_interval` had elapsed, so the run loop dispatched
    /// a synthetic tick without waiting for the heartbeat.
    pub threshold_fires: u64,
    /// Cumulative ticks (heartbeat + threshold) during which at least
    /// one cell was dirty when `handle_gossip_tick` entered the peer
    /// pick. Used by the bench to compute *effective fanout*
    /// (`packets_emitted / dirty_ticks`) — see
    /// `crates/gossip-bench/README.md`.
    pub dirty_ticks: u64,
    /// The adaptive fanout the most recent dirty tick chose:
    /// `config.fanout.max(⌊log₂(dirty)⌋ + 1).min(peers)`. Grows above the
    /// configured base when the dirty set is large (a burst), so a burst
    /// that fans out wider is directly observable. `0` before the first
    /// dirty tick.
    pub last_effective_fanout: usize,
    /// High-water mark of `last_effective_fanout` since startup — the widest
    /// the node has ever fanned out, so a transient burst's peak survives
    /// even after the dirty set drains and fanout relaxes to the base.
    pub peak_effective_fanout: usize,
    /// The per-rule error budget ε the most recent `handle_limit_request`
    /// computed: `limit × target_err_bps / (10_000 × peers)` (floored at 1).
    /// A rule's accumulated `pending` crossing ε is what triggers an eager
    /// (threshold) flush — so this is the threshold behind `threshold_fires`.
    /// `0` before the first request is seen.
    pub last_error_budget: u64,
}

/// One peer known to the runtime. `node_id` is `None` until the runtime has
/// received an inbound packet from this peer; `peer_slot` is `None` until the
/// peer has been interned in the `PeerFrontierTable`. The two fields are set
/// together — see the invariant tested in
/// `quickcheck_peer_slot_pairing_holds_across_lifecycle`.
#[derive(Clone, Copy, Debug)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub node_id: Option<NodeId>,
    pub peer_slot: Option<u16>,
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
