//! The typed event log and snapshot shapes the frontend renders.
//!
//! Nodes are identified by a **stable id** (never `SocketAddr`, never a dense
//! array position): the id is assigned once when the node joins and never
//! reused, so it survives other nodes joining and leaving without renumbering.
//! Ids therefore have gaps once a node has been removed — a gap *is* the honest
//! record that a member left. Every [`Event`] carries the `tick` and
//! `virtual_ms` at which the engine observed it, so the frontend can place it
//! on a shared timeline and scrub. `u128` identifiers serialize as hex strings
//! (see `crate::hex`).

use gabion::gossip::{AnswerState, Role};
use gabion::wire::{CellPayload, Payload};
use serde::Serialize;

use crate::hex::{option_u128_hex, u128_hex};

/// One thing that happened in the simulation, stamped with when. `kind` is a
/// `{ "type": … }`-tagged payload so the frontend can switch on it directly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Event {
    /// Gossip-tick index (`virtual_ms / tick_interval_ms`) the event fell in.
    pub tick: u64,
    /// Virtual time, in milliseconds since the session began.
    pub virtual_ms: u64,
    pub kind: EventKind,
}

/// The event payloads. Cell events mirror the CRDT `DeltaSink` /
/// `ExpirationSink` rows; packet events come from the logging transport;
/// tick, threshold and gateway events come from diffing each runtime's
/// counters.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum EventKind {
    /// A heartbeat round ran on `node`.
    Tick { node: u32 },
    /// A threshold round ran on `node`.
    ThresholdFire { node: u32 },
    /// `src` enqueued a packet of `kind` bound for `dst`; it will be
    /// delivered. `tier` is by placement, never by the zone either node
    /// reports.
    PacketSent {
        src: u32,
        dst: u32,
        bytes: u32,
        kind: PacketKind,
        tier: Tier,
    },
    /// `dst` consumed a packet from `src`.
    PacketDelivered { src: u32, dst: u32, bytes: u32 },
    /// `src`'s packet to `dst` was lost on the link (or `dst` had no
    /// receiver). Distinct from [`EventKind::PacketSent`]: a dropped packet
    /// never produces a matching delivery. `kind` and `tier` are as for
    /// [`EventKind::PacketSent`].
    PacketDropped {
        src: u32,
        dst: u32,
        bytes: u32,
        kind: PacketKind,
        tier: Tier,
    },
    /// `origin`'s gateway into remote zone `zone` changed, as its runtime
    /// reports it; `None` while it has none there.
    GatewayChanged {
        origin: u32,
        zone: u8,
        from: Option<u32>,
        to: Option<u32>,
    },
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

/// A packet's kind, read from its header: the kinds of the wire format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum PacketKind {
    Ack,
    Own,
    Relay,
    Sweep,
    SweepRequest,
}

impl PacketKind {
    /// Whether the kind streams cells (data) rather than controlling the
    /// streams (control). Classed by kind alone, so a closing packet or an
    /// empty sweep page counts as data even when it holds no cell.
    fn carries_cells(self) -> bool {
        match self {
            Self::Own | Self::Relay | Self::Sweep => true,
            Self::Ack | Self::SweepRequest => false,
        }
    }
}

impl From<Payload> for PacketKind {
    fn from(payload: Payload) -> Self {
        match payload {
            Payload::Ack { .. } => Self::Ack,
            Payload::Cells(CellPayload::Own { .. }) => Self::Own,
            Payload::Cells(CellPayload::Relay { .. }) => Self::Relay,
            Payload::Cells(CellPayload::Sweep) => Self::Sweep,
            Payload::SweepRequest => Self::SweepRequest,
        }
    }
}

/// Whether a packet stayed inside its sender's zone, by placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Tier {
    InZone,
    CrossZone,
}

impl Tier {
    /// The tier of a packet from a node placed in zone `src` to one in `dst`.
    pub fn between(src: u8, dst: u8) -> Self {
        if src == dst {
            Self::InZone
        } else {
            Self::CrossZone
        }
    }
}

/// The events produced by one `step` / `submit_request` / `step_to` call,
/// plus the virtual time the engine reached.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
    /// Virtual time after this batch, in milliseconds.
    pub virtual_ms: u64,
    /// Gossip-tick index after this batch.
    pub tick: u64,
}

/// Per-node cluster state, pulled every frame. Only the inspected node's cells
/// and peer rows are included, so the frame costs one row per node rather than
/// one per node pair.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClusterState {
    pub virtual_ms: u64,
    pub tick: u64,
    pub nodes: Vec<NodeState>,
    /// The inspected node's rows; `None` when no node is inspected or the
    /// inspected id is not live.
    pub inspected: Option<Inspected>,
    /// Every zone in index order, with its live members.
    pub zones: Vec<ZoneView>,
    /// Bytes every sender put on the wire since the session began, delivered
    /// or lost, by tier and class.
    pub bytes_by_tier: BytesByTier,
    /// The ground-truth cluster total for the watched key — the
    /// simulator-only oracle the convergence fan races toward. Summed across
    /// every node's locally-originated *live* cells, so it is independent of
    /// how far gossip has propagated and decays with the window as buckets
    /// age out (it is not a monotonic accumulator).
    pub oracle_total: u64,
    /// The bucket epoch the watched rule sits in at `virtual_ms`, straight from
    /// `RuleDescriptor::current_epoch` — so the Strata renders the CRDT's
    /// window layout rather than recomputing it. Rule-global in v1 (one
    /// watched rule); moves per-rule in the v1.1 multi-rule shape.
    pub bucket_epoch_now: u32,
}

/// The rows of the one node the page inspects, read at the same instant as
/// the rest of the snapshot.
#[derive(Clone, Debug, Serialize)]
pub struct Inspected {
    /// The inspected node's stable id.
    pub id: u32,
    pub cells: Vec<CellView>,
    pub peers: Vec<PeerView>,
}

/// One zone and how many live nodes are placed in it.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct ZoneView {
    pub name: &'static str,
    pub live: u32,
}

/// Bytes sent, by [`Tier`] and class.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct BytesByTier {
    pub in_zone: ClassBytes,
    pub cross_zone: ClassBytes,
}

/// Bytes sent, by class, classed by packet kind. Data is `Own`, `Relay` and
/// `Sweep` packets; control is acks, probes and their answers, gap reports and
/// sweep requests.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ClassBytes {
    pub data: u64,
    pub control: u64,
}

impl BytesByTier {
    pub(crate) fn add(&mut self, tier: Tier, kind: PacketKind, bytes: u32) {
        let class = match tier {
            Tier::InZone => &mut self.in_zone,
            Tier::CrossZone => &mut self.cross_zone,
        };
        let total = if kind.carries_cells() {
            &mut class.data
        } else {
            &mut class.control
        };
        *total += u64::from(bytes);
    }
}

/// One node's view at snapshot time. The counters mirror the runtime's
/// [`AdminSnapshot`](gabion::gossip::AdminSnapshot); `gateway_for`,
/// `own_data_bytes` and `missing_for_ms` the engine counts; and `store_stats`
/// mirrors its [`CellStoreStats`](gabion::crdt::CellStoreStats). The node
/// inspector renders them directly: see `GossipTargets.svelte` and
/// `NodeInspector.svelte` in `web/src/lib/components`.
#[derive(Clone, Debug, Serialize)]
pub struct NodeState {
    /// The node's stable id (see the module note). Distinct from `node_id`
    /// below: this is the frontend's display/identity handle; `node_id` is the
    /// on-the-wire gossip identity (a pure function of it, `id·256 + 1`).
    pub id: u32,
    /// The zone it is placed in.
    pub zone: u8,
    /// The on-the-wire gossip identity this node announces (`NodeIdentity`).
    #[serde(with = "u128_hex")]
    pub node_id: u128,
    /// The node's incarnation — bumped on restart so peers can supersede a
    /// stale alias. Always 1 in the sim (no restart path), shown to teach the
    /// concept.
    pub incarnation: u32,
    /// This node's cluster-aggregate total across all cells (what its local
    /// admission decision reads).
    pub aggregate_total: u64,
    /// Heartbeat rounds this runtime has run.
    pub rounds_heartbeat: u64,
    /// Gateways replaced because they went unanswered for `suspect_after` or
    /// said they cannot relay.
    pub gateway_failovers: u64,
    /// Gap reports sent: a peer's stream skipped positions.
    pub gap_reports_sent: u64,
    /// Own-stream timeouts that resent from the last ack.
    pub rto_rewinds_own: u64,
    /// The same for the relay stream.
    pub rto_rewinds_relay: u64,
    /// Join sweeps this node started answering.
    pub sweeps_served: u64,
    /// Origins whose runtime sends this node's zone their changes through this
    /// node: the gateway rows of every node, inverted.
    pub gateway_for: u32,
    /// Bytes of the node's own-stream (`Own`) packets it has sent since it
    /// joined, delivered or lost.
    pub own_data_bytes: u64,
    /// How long ago the origin of the oldest change this node does not yet
    /// hold last updated that change's cell: a lower bound on how long the
    /// node has been behind. `None` while it holds every change.
    pub missing_for_ms: Option<u64>,
    /// The per-rule error budget ε at the most recent request: a rule's pending
    /// hits crossing it triggers an eager (threshold) flush. `0` until seen.
    pub error_budget: u64,
    /// CRDT store occupancy and saturation counters.
    pub store_stats: StoreStats,
}

/// Mirror of [`gabion::crdt::CellStoreStats`] — the CRDT store's occupancy and
/// saturation counters, surfaced for the inspector's storage section.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct StoreStats {
    pub active_cells: u32,
    pub cell_capacity: u32,
    pub rule_slots_used: u16,
    pub rule_slots_capacity: u16,
    pub node_slots_used: u16,
    pub node_slots_capacity: u16,
    pub cell_store_full_rejects: u64,
    pub rule_dictionary_full_rejects: u64,
    pub node_dictionary_full_rejects: u64,
}

impl From<gabion::crdt::CellStoreStats> for StoreStats {
    fn from(s: gabion::crdt::CellStoreStats) -> Self {
        Self {
            active_cells: s.active_cells,
            cell_capacity: s.cell_capacity,
            rule_slots_used: s.rule_slots_used,
            rule_slots_capacity: s.rule_slots_capacity,
            node_slots_used: s.node_slots_used,
            node_slots_capacity: s.node_slots_capacity,
            cell_store_full_rejects: s.cell_store_full_rejects,
            rule_dictionary_full_rejects: s.rule_dictionary_full_rejects,
            node_dictionary_full_rejects: s.node_dictionary_full_rejects,
        }
    }
}

/// One CRDT cell as this node currently holds it.
#[derive(Clone, Debug, Serialize)]
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
#[derive(Clone, Debug, Serialize)]
pub struct PeerView {
    /// The peer's stable id, if the engine can resolve its address.
    pub id: Option<u32>,
    /// The zone the peer reports, by index; `None` while unknown or with no
    /// zone.
    pub zone: Option<u8>,
    /// What the peer is to this node.
    pub role: Role,
    /// Whether this node waits for the peer's answer.
    pub answer: AnswerState,
    /// How long the oldest packet waiting for the peer's answer has waited.
    pub unanswered_ms: Option<u32>,
    /// Own-stream positions sent to the peer and not yet acknowledged.
    pub own_unacked: u64,
    /// The same for the relay stream.
    pub relay_unacked: u64,
    /// The peer's gossip node id once an inbound packet has revealed it
    /// (distinct from `id`: this is the on-the-wire identity the peer
    /// announced, unknown until the first packet from it arrives).
    #[serde(with = "option_u128_hex")]
    pub node_id: Option<u128>,
}
