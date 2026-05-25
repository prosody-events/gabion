//! The typed event log and snapshot shapes the frontend renders.
//!
//! Nodes are identified by a **stable id** (never `SocketAddr`, never a dense
//! array position): the id is assigned once when the node joins and never
//! reused, so it survives other nodes joining and leaving without renumbering.
//! Ids therefore have gaps once a node has been removed — a gap *is* the honest
//! record that a member left. Every [`Event`] carries the `tick` and
//! `virtual_ms` at which the engine observed it, so the frontend can place it
//! on a shared timeline and scrub. `u128` identifiers serialize as hex strings
//! (see [`crate::hex`]).

use serde::{Deserialize, Serialize};

use crate::hex::{option_u128_hex, u128_hex};

/// One thing that happened in the simulation, stamped with when. `kind` is a
/// `{ "type": … }`-tagged payload so the frontend can switch on it directly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    /// Gossip-tick index (`virtual_ms / tick_interval_ms`) the event fell in.
    pub tick: u64,
    /// Virtual time, in milliseconds since the session began.
    pub virtual_ms: u64,
    pub kind: EventKind,
}

/// The event payloads. Cell events mirror the CRDT `DeltaSink` /
/// `ExpirationSink` rows; packet events come from the logging transport;
/// tick / threshold events come from diffing the admin snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum EventKind {
    /// A gossip tick fired on `node` (heartbeat or threshold).
    Tick { node: u32 },
    /// A threshold-triggered (not heartbeat) tick fired on `node`.
    ThresholdFire { node: u32 },
    /// `src` enqueued a packet bound for `dst`; it will be delivered.
    PacketSent { src: u32, dst: u32, bytes: u32 },
    /// `dst` consumed a packet from `src`.
    PacketDelivered { src: u32, dst: u32, bytes: u32 },
    /// `src`'s packet to `dst` was lost on the link (or `dst` had no
    /// receiver). Distinct from [`EventKind::PacketSent`]: a dropped packet
    /// never produces a matching delivery.
    PacketDropped { src: u32, dst: u32, bytes: u32 },
    /// A cell appeared on `node` (previous stored count was zero).
    CellCreated {
        node: u32,
        #[serde(with = "u128_hex")]
        rule: u128,
        #[serde(with = "u128_hex")]
        key: u128,
        bucket: u32,
        count: u64,
    },
    /// An existing cell's count rose on `node`.
    CellUpdated {
        node: u32,
        #[serde(with = "u128_hex")]
        rule: u128,
        #[serde(with = "u128_hex")]
        key: u128,
        bucket: u32,
        count: u64,
    },
    /// A cell aged out of the window on `node`.
    CellExpired {
        node: u32,
        #[serde(with = "u128_hex")]
        rule: u128,
        #[serde(with = "u128_hex")]
        key: u128,
        bucket: u32,
    },
}

/// The events produced by one `step` / `submit_request` / `step_to` call,
/// plus the virtual time the engine reached.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
    /// Virtual time after this batch, in milliseconds.
    pub virtual_ms: u64,
    /// Gossip-tick index after this batch.
    pub tick: u64,
}

/// Full per-node cluster state, pulled on seek / re-render. Bounded by
/// `nodes × cell_capacity`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ClusterState {
    pub virtual_ms: u64,
    pub tick: u64,
    pub nodes: Vec<NodeState>,
    /// The ground-truth cluster total for the watched key — the
    /// simulator-only oracle the convergence fan races toward. Summed across
    /// every node's locally-originated *live* cells, so it is independent of how
    /// far gossip has propagated and decays with the window as buckets age out
    /// (it is not a monotonic accumulator).
    pub oracle_total: u64,
    /// The bucket epoch the watched rule sits in at `virtual_ms`, straight from
    /// `RuleDescriptor::current_epoch` — so the Strata renders the CRDT's window
    /// layout rather than recomputing it. Rule-global in v1 (one watched rule);
    /// moves per-rule in the v1.1 multi-rule shape.
    pub bucket_epoch_now: u32,
    /// The oldest epoch the rule still retains at `virtual_ms`, from
    /// `RuleDescriptor::oldest_live_epoch` — the trailing edge of the slide.
    pub oldest_live_epoch: u32,
}

/// One node's view at snapshot time.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodeState {
    /// The node's stable id (see the module note). The gossip `NodeId` is a
    /// pure function of this (`id·256 + 1`), so it is not duplicated here.
    pub id: u32,
    /// This node's cluster-aggregate total across all cells (what its local
    /// admission decision reads).
    pub aggregate_total: u64,
    pub ticks_total: u64,
    pub threshold_fires: u64,
    pub cells: Vec<CellView>,
    pub peers: Vec<PeerView>,
}

/// One CRDT cell as this node currently holds it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CellView {
    #[serde(with = "u128_hex")]
    pub rule: u128,
    #[serde(with = "u128_hex")]
    pub key: u128,
    pub bucket: u32,
    pub count: u64,
    /// How long since this cell was last updated, in milliseconds.
    pub age_ms: u64,
    /// The stable id of the node that originated this cell, if its origin
    /// identity is still interned and known to the engine.
    pub origin: Option<u32>,
    /// Whether this node is itself the origin of the cell.
    pub is_local: bool,
}

/// One peer entry from a node's gossip peer table.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PeerView {
    /// The peer's stable id, if the engine can resolve its address.
    pub id: Option<u32>,
    /// The peer's gossip node id once an inbound packet has revealed it
    /// (distinct from `id`: this is the on-the-wire identity the peer
    /// announced, unknown until the first packet from it arrives).
    #[serde(with = "option_u128_hex")]
    pub node_id: Option<u128>,
}
