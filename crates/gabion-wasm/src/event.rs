//! The typed event log and snapshot shapes the frontend renders.
//!
//! Nodes are identified by a **stable id** (never `SocketAddr`, never a dense
//! array position): the id is assigned once when the node joins and never
//! reused, so it survives other nodes joining and leaving without renumbering.
//! Ids therefore have gaps once a node has been removed — a gap *is* the honest
//! record that a member left. Every [`Event`] carries the `virtual_ms` at
//! which the engine observed it, so the frontend can place it on a shared
//! timeline and scrub. `u128` identifiers serialize as hex strings (see
//! `crate::hex`).

use gabion::gossip::{AnswerState, Role};
use gabion::wire::{CellPayload, Payload};
use serde::Serialize;
use std::num::NonZeroU64;

use crate::hex::{option_u128_hex, u128_hex};

/// One thing that happened in the simulation, stamped with when. `kind` is a
/// `{ "type": … }`-tagged payload so the frontend can switch on it directly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Event {
    /// Virtual time, in milliseconds since the session began.
    pub virtual_ms: u64,
    pub kind: EventKind,
}

/// The event payloads. Cell events mirror the CRDT `DeltaSink` /
/// `ExpirationSink` rows; packet events come from the logging transport;
/// tick, threshold and gateway events come from diffing each runtime's
/// counters; a decision comes from the request's node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum EventKind {
    /// `node` decided a request of `hits` against its own count table. Find
    /// it by kind: a batch may open with an earlier wave's packets landing.
    Decided {
        node: u32,
        hits: NonZeroU64,
        admission: Admission,
    },
    /// A heartbeat round ran on `node`.
    Tick { node: u32 },
    /// A threshold round ran on `node`.
    ThresholdFire { node: u32 },
    /// `src` enqueued a packet of `kind` bound for `dst`, and the router
    /// accepted it: it lands at the end of its sub-step, which may be the
    /// next engine call. `tier` is by placement, never by the zone either
    /// node reports.
    PacketSent {
        src: u32,
        dst: u32,
        bytes: u32,
        kind: PacketKind,
        tier: Tier,
    },
    /// `dst` consumed a packet from `src`.
    PacketDelivered { src: u32, dst: u32, bytes: u32 },
    /// `src`'s packet to `dst` was lost on the link, refused by a full router
    /// queue, or `dst` had no receiver. Distinct from
    /// [`EventKind::PacketSent`]: a dropped packet never produces a matching
    /// delivery. `kind` and `tier` are as for [`EventKind::PacketSent`].
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
    pub(crate) fn carries_cells(self) -> bool {
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

/// The events one engine command produced, plus the virtual time the engine
/// reached. A command that returns no batch leaves its events for the next.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
    /// Virtual time after this batch, in milliseconds.
    pub virtual_ms: u64,
}

/// A node's decision on a request, admitted or rejected whole, against the
/// node's own window, as gabion's adapters decide.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Admission {
    /// Recorded: the window held `total_before`, and the request fits under
    /// the limit on top of it.
    Admitted { total_before: u64 },
    /// Refused whole and never recorded: the window holds `total`, and the
    /// request would pass the limit.
    Rejected { total: u64, reopens: Reopens },
}

/// When a rejected request would be admitted if sent again.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Reopens {
    /// In `ms`, once enough of the window's oldest hits leave it.
    In { ms: u64 },
    /// Never: the request alone is heavier than the limit (`hits > limit`).
    Never,
}

/// Whether a node holds every in-window change the cluster has made.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Lag {
    CaughtUp,
    /// It lacks a change that some holder can still reach it with, over
    /// links that do not block: waiting can help. `missing_for_ms` is how
    /// long ago the origin of the oldest change it lacks last updated that
    /// change's cell, a lower bound on how long it has been behind.
    Behind {
        missing_for_ms: u64,
    },
    /// No node that holds a change it lacks has a path of links into it that
    /// do not block, so waiting cannot help. `missing_for_ms` as for
    /// `Behind`.
    Stranded {
        missing_for_ms: u64,
    },
}

/// Per-node cluster state, pulled every frame. Only the inspected node's cells
/// and peer rows are included, so the frame costs one row per node rather than
/// one per node pair.
#[derive(Clone, Debug, Serialize)]
pub struct ClusterState {
    pub virtual_ms: u64,
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
    /// simulator-only oracle the convergence fan races toward. Summed over
    /// every origin's own cells of the watched key whose bucket lies in the
    /// window, a departed origin's as the fullest replica a live node still
    /// holds, so it is independent of how far gossip has propagated and
    /// drops a bucket at the instant every node's `window_total` does.
    pub oracle_total: u64,
    /// `oracle_total` by live bucket, oldest first: one entry per bucket of
    /// the window.
    pub oracle_buckets: Vec<u64>,
    /// Every node's count of the watched key in each live bucket, oldest
    /// first, row by row in `nodes` order: `nodes.len() × oracle_buckets.len()`
    /// entries.
    pub window_buckets: Vec<u64>,
    /// Hits the cluster admitted since the session began, of every key, never
    /// decremented, so a removal cannot make it fall.
    pub admitted_hits: u64,
    /// Hits the cluster rejected since the session began, likewise.
    pub rejected_hits: u64,
    /// The next instant some node's window of the watched key drops a bucket
    /// that holds hits, if any node's window holds some.
    pub next_expiry_ms: Option<u64>,
    /// The bucket epoch the watched rule sits in at `virtual_ms`, straight
    /// from `RuleSpec::bucket_at` — so the Strata renders the CRDT's window
    /// layout rather than recomputing it. Rule-global in v1 (one watched
    /// rule); moves per-rule in the v1.1 multi-rule shape.
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
    /// The decision this node would make now on a request of the watched
    /// probe's weight.
    pub next_send: Admission,
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
/// [`AdminSnapshot`](gabion::gossip::AdminSnapshot); the window, decision,
/// lag, heartbeat and hit fields the engine reads or counts; and `store_stats`
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
    /// gabion's `window_total` of the watched key on this node's own count
    /// table: the number its admission reads.
    pub window_total: u64,
    /// The decision a one-hit request would get here now: `Rejected` exactly
    /// while the node's window has reached its limit (`window_total >=
    /// limit`).
    pub brim: Admission,
    /// Whether the node holds every in-window change, and if not, whether
    /// waiting can help.
    pub lag: Lag,
    /// The next instant this node's heartbeat fires, in virtual ms.
    pub next_heartbeat_ms: u64,
    /// Hits this node admitted since it joined, of every key.
    pub admitted_hits: u64,
    /// Hits this node rejected since it joined, of every key.
    pub rejected_hits: u64,
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
