//! Admin snapshot channel for observability.
//!
//! Lives outside the hot path: the runtime services [`AdminCommand`] as a
//! sixth `select!` arm only when the constructor was handed a receiver, so the
//! production loop without admin is unchanged. The reply is built inside the
//! gossip task — callers never get a `Sync` borrow of the runtime's internals.

use std::net::SocketAddr;

use tokio::sync::oneshot;

use super::{CellStoreStats, NodeId, NodeIdentity};

/// Request sent into the gossip runtime over the admin channel. Today the
/// only variant is a snapshot request; new shapes (e.g. forced repair) can
/// be added without changing the `select!` arm.
pub enum AdminCommand {
    Snapshot {
        reply: oneshot::Sender<AdminSnapshot>,
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
