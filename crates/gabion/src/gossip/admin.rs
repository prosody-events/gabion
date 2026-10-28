//! Admin snapshot channel for observability.
//!
//! Lives outside the hot path: the runtime services [`AdminCommand`] on its
//! admin arm; without a receiver the admin arm never fires. The reply is built
//! inside the gossip task — callers never get a `Sync` borrow of the runtime's
//! internals.

use std::net::SocketAddr;

use tokio::sync::oneshot;

use super::peer::Role;
#[cfg(feature = "cell-dump")]
use crate::crdt::{BucketEpoch, Incarnation};
use crate::crdt::{CellStoreStats, NodeId, NodeIdentity};

/// Request sent into the gossip runtime over the admin channel. New shapes
/// (e.g. a forced sweep) can be added without changing the `select!` arm.
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
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct AdminSnapshot {
    pub local_identity: NodeIdentity,
    pub peers: Vec<PeerEntry>,
    pub store_stats: CellStoreStats,
    /// Event counts since startup.
    pub counters: GossipCounters,
    /// The per-rule error budget ε the most recent `handle_limit_request`
    /// computed: `limit × target_err_bps / (10_000 × peers)` (floored at 1).
    /// A rule's accumulated `pending` crossing ε is what triggers an eager
    /// (threshold) round — so this is the threshold behind
    /// `counters.rounds_threshold`.
    /// `0` before the first request is seen.
    pub last_error_budget: u64,
}

/// What the runtime has sent, received and failed to send since startup.
#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct GossipCounters {
    /// Heartbeat rounds, one per `gossip.tick_interval`.
    pub rounds_heartbeat: u64,
    /// Threshold rounds: a rule's unsent local hits crossed its error budget.
    pub rounds_threshold: u64,
    /// Prompt rounds, run at most every `gossip.min_emit_interval` while a
    /// resend, gap report, probe answer or sweep is waiting.
    pub rounds_prompt: u64,
    /// Datagrams the socket accepted.
    pub packets_sent: u64,
    /// Bytes in those datagrams.
    pub bytes_sent: u64,
    /// Datagrams received, whether they decoded or not.
    pub packets_received: u64,
    /// Bytes in those datagrams.
    pub bytes_received: u64,
    /// Sends the kernel refused for lack of buffer space; logged as
    /// `send_blocked`.
    pub send_blocked: u64,
    /// Sends that failed for any other reason; logged as `send_error`.
    pub send_errors: u64,
    /// Packets dropped as not valid gossip; logged as `decode_rejected` with
    /// `reason = "framing"`.
    pub decode_rejects_framing: u64,
    /// Packets dropped as larger than `gossip.max_payload_bytes`; logged as
    /// `decode_rejected` with `reason = "peer_limits"`.
    pub decode_rejects_peer_limits: u64,
    /// Packets dropped for another protocol version; logged as
    /// `peer_version_mismatch`.
    pub decode_rejects_version: u64,
    /// Packets dropped for another cluster id; logged as `decode_rejected`
    /// with `reason = "cluster"`.
    pub decode_rejects_cluster: u64,
    /// Peers discovery reported while the peer table was full; logged as
    /// `peer_table_full`.
    pub peers_declined: u64,
    /// Received cells that raised a stored count.
    pub cells_merged: u64,
    /// Own-stream timeouts: a heartbeat passed without an ack, so the stream
    /// was resent from the last ack with `PROBE` set.
    pub rto_rewinds_own: u64,
    /// Gap reports sent: a peer's stream skipped positions.
    pub gap_reports_sent: u64,
    /// Gaps given up after their reports went unanswered; logged as
    /// `gap_abandoned`.
    pub gaps_abandoned: u64,
    /// Join-sweep requests this node sent.
    pub sweeps_requested: u64,
    /// Join sweeps this node started answering.
    pub sweeps_served: u64,
    /// Cells sent in sweep pages.
    pub sweep_cells_sent: u64,
    /// Sweep requests declined because `gossip.sweep_concurrency` sweeps
    /// were running; logged as `sweep_declined`.
    pub sweeps_refused: u64,
    /// Sweep requests from an address that is not a peer; not logged, so a
    /// spoofed flood cannot amplify.
    pub sweeps_ignored: u64,
}

impl GossipCounters {
    /// Packets dropped for any cause.
    pub fn decode_rejects(&self) -> u64 {
        self.decode_rejects_framing
            + self.decode_rejects_peer_limits
            + self.decode_rejects_version
            + self.decode_rejects_cluster
    }
}

/// One peer known to the runtime. `node_id` is `None` until a packet from
/// this peer has decoded.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub node_id: Option<NodeId>,
    pub role: Role,
    /// Own-stream positions sent to the peer and not yet acknowledged.
    pub own_unacked: u64,
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
