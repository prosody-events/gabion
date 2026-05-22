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
