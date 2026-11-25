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
use gabion::wire::{CellPayload, Header, Payload};
use serde::Serialize;
use std::num::NonZeroU64;

use crate::hex::u128_hex;

/// One thing that happened in the simulation, stamped with when. `kind` is a
/// `{ "type": … }`-tagged payload so the frontend can switch on it directly.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Event {
    /// Virtual time, in milliseconds since the session began.
    pub virtual_ms: u64,
    pub kind: EventKind,
}

/// The event payloads. Cell events mirror the CRDT `DeltaSink` rows; packet
/// events come from the logging transport; tick and gateway events come from
/// diffing each runtime's counters; a decision comes from the request's node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum EventKind {
    /// `node` decided a burst of one-hit requests against its own count
    /// table. Find it by kind: a batch may open with an earlier wave's packets
    /// landing.
    Decided { node: u32, burst: Burst },
    /// A heartbeat round ran on `node`.
    Tick { node: u32 },
    /// `src` enqueued the message bound for `dst`, and the router accepted
    /// it: it lands at the end of its sub-step, which may be the next engine
    /// call.
    PacketSent(Message),
    /// `dst` consumed a packet from `src`.
    PacketDelivered { src: u32, dst: u32, bytes: u32 },
    /// `src`'s message to `dst` was lost on the link, refused by a full
    /// router queue, or `dst` had no receiver. Distinct from
    /// [`EventKind::PacketSent`]: a dropped packet never produces a matching
    /// delivery.
    PacketDropped(Message),
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
}

/// A message `src` sent `dst`, numbered `seq` in the run. Its `role`, `riders`
/// and `repeat` are read once from its header ([`MessageRole::of`]); `tier`
/// is by placement, never by the zone either node reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct Message {
    pub seq: Seq,
    pub src: u32,
    pub dst: u32,
    pub bytes: u32,
    pub role: MessageRole,
    pub riders: Riders,
    pub repeat: Option<ListedRepeat>,
    pub cells: u16,
    pub tier: Tier,
}

/// A packet's kind on the wire, derived from its [`MessageRole`].
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

/// A message's number in the run: 1 for the first captured after the run's
/// start, then one more per message. Crosses as a JS number, exact to 2⁵³.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Seq(pub u64);

/// The one role a message is listed and drawn as. Precedence, first match
/// wins: data roles by payload; then, on an ack-kind packet, gap report >
/// probe > probe answer > ack. Cannot relay is never a role: it is a
/// [`Riders::CANNOT_RELAY`] rider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum MessageRole {
    /// `Own` with cells.
    OwnData,
    /// `Relay` with cells.
    RelayData,
    /// `Sweep`, with or without cells.
    SweepPage,
    /// `Own` with a range and no cells: changes that expired before they
    /// were sent.
    OwnSkip,
    /// `Relay` with a range and no cells.
    RelaySkip,
    /// An ack-kind packet with a gap flag.
    GapReport,
    /// An ack-kind packet with `PROBE`; a greeting is the same bytes.
    Probe,
    SweepRequest,
    /// An ack-kind packet with `PROBE_REPLY`.
    ProbeAnswer,
    /// An ack-kind packet with no other listed signal.
    Ack,
}

/// Every other signal a message carries that its receiver acts on, one bit
/// each. The role's own signal is never a rider.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Riders(u8);

impl Riders {
    /// `CANNOT_RELAY` on a message that crosses zones, the only place a
    /// receiver acts on it.
    pub const CANNOT_RELAY: Self = Self(8);
    /// `RELAY_INTO_ZONE`: the gateway passes these changes on into its zone.
    pub const FOR_ZONE: Self = Self(16);
    /// A gap flag on a message that is not a gap report.
    pub const GAP_REPORT: Self = Self(1);
    pub const NONE: Self = Self(0);
    /// `PROBE` on a message that is not a probe: a greeting riding data to a
    /// peer not heard yet, or riding a gap report.
    pub const PROBE: Self = Self(2);
    /// `PROBE_REPLY` on a message that is not a probe answer.
    pub const PROBE_ANSWER: Self = Self(4);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0 && other.0 != 0
    }

    fn with(self, other: Self, on: bool) -> Self {
        if on { Self(self.0 | other.0) } else { self }
    }

    /// The bits, as the page and the capture store them.
    pub fn bits(self) -> u8 {
        self.0
    }
}

/// Why a data message repeats what its sender already sent, where the list
/// can say so from the capture alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ListedRepeat {
    /// `PROBE` on data to a peer the sender has heard: its retransmission
    /// timer fell due.
    AfterTimeout,
    /// The first data on its link and stream after a delivered gap report
    /// whose mark its range starts at or below.
    AfterGapReport,
}

impl MessageRole {
    /// The role, riders and header-derived repeat of a message of `cells`
    /// cells with `header`, sent across `tier`. Total over every header the
    /// decoder admits.
    pub fn of(header: &Header, cells: u16, tier: Tier) -> (Self, Riders, Option<ListedRepeat>) {
        let acks = header.acks;
        let gap = acks.gap_own || acks.gap_relay;
        let (role, probe, for_zone) = match header.payload {
            Payload::Cells(CellPayload::Own {
                relay_into_zone,
                probe,
                ..
            }) => (
                if cells > 0 {
                    Self::OwnData
                } else {
                    Self::OwnSkip
                },
                probe,
                relay_into_zone,
            ),
            Payload::Cells(CellPayload::Relay { probe, .. }) => (
                if cells > 0 {
                    Self::RelayData
                } else {
                    Self::RelaySkip
                },
                probe,
                false,
            ),
            Payload::Cells(CellPayload::Sweep) => (Self::SweepPage, false, false),
            Payload::SweepRequest => (Self::SweepRequest, false, false),
            Payload::Ack { probe } => (
                if gap {
                    Self::GapReport
                } else if probe {
                    Self::Probe
                } else if acks.probe_reply {
                    Self::ProbeAnswer
                } else {
                    Self::Ack
                },
                probe,
                false,
            ),
        };
        // On data, PROBE to a peer already heard is a timeout repeat; to one
        // not heard yet it is a greeting riding the data.
        let data = matches!(role.kind(), PacketKind::Own | PacketKind::Relay);
        let repeat = (data && probe && acks.incarnation != 0).then_some(ListedRepeat::AfterTimeout);
        let riders = Riders::NONE
            .with(Riders::GAP_REPORT, gap && role != Self::GapReport)
            .with(
                Riders::PROBE,
                probe && role != Self::Probe && repeat.is_none(),
            )
            .with(
                Riders::PROBE_ANSWER,
                acks.probe_reply && role != Self::ProbeAnswer,
            )
            .with(
                Riders::CANNOT_RELAY,
                acks.cannot_relay && tier == Tier::CrossZone,
            )
            .with(Riders::FOR_ZONE, for_zone);
        (role, riders, repeat)
    }

    /// The wire kind this role is sent as.
    pub fn kind(self) -> PacketKind {
        match self {
            Self::OwnData | Self::OwnSkip => PacketKind::Own,
            Self::RelayData | Self::RelaySkip => PacketKind::Relay,
            Self::SweepPage => PacketKind::Sweep,
            Self::SweepRequest => PacketKind::SweepRequest,
            Self::GapReport | Self::Probe | Self::ProbeAnswer | Self::Ack => PacketKind::Ack,
        }
    }
}

/// One row of the page's By type table, the type filter: not every row is a
/// role ("repeat" is the repeat column, "skip" two roles, "cannot relay" a
/// rider), so a message with two signals matches two rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, serde::Deserialize)]
pub enum MessageType {
    OwnData,
    RelayData,
    Repeat,
    Skip,
    Ack,
    GapReport,
    Probe,
    ProbeAnswer,
    CannotRelay,
    SweepRequest,
    SweepPage,
}

impl MessageType {
    /// Every type, in the table's fixed order.
    pub const ALL: [Self; 11] = [
        Self::OwnData,
        Self::RelayData,
        Self::Repeat,
        Self::Skip,
        Self::Ack,
        Self::GapReport,
        Self::Probe,
        Self::ProbeAnswer,
        Self::CannotRelay,
        Self::SweepRequest,
        Self::SweepPage,
    ];

    /// Whether a message of `role` with `riders` and `repeat` is of this type.
    pub fn matches(self, role: MessageRole, riders: Riders, repeat: Option<ListedRepeat>) -> bool {
        match self {
            Self::OwnData => role == MessageRole::OwnData,
            Self::RelayData => role == MessageRole::RelayData,
            Self::Repeat => repeat.is_some(),
            Self::Skip => matches!(role, MessageRole::OwnSkip | MessageRole::RelaySkip),
            Self::Ack => role == MessageRole::Ack,
            Self::GapReport => {
                role == MessageRole::GapReport || riders.contains(Riders::GAP_REPORT)
            }
            Self::Probe => role == MessageRole::Probe || riders.contains(Riders::PROBE),
            Self::ProbeAnswer => {
                role == MessageRole::ProbeAnswer || riders.contains(Riders::PROBE_ANSWER)
            }
            Self::CannotRelay => riders.contains(Riders::CANNOT_RELAY),
            Self::SweepRequest => role == MessageRole::SweepRequest,
            Self::SweepPage => role == MessageRole::SweepPage,
        }
    }
}

/// A set of [`MessageType`]s, one bit each in `ALL`'s order; empty means
/// every type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TypeSet(pub u16);

impl TypeSet {
    /// Whether a message passes this filter.
    pub fn admits(self, role: MessageRole, riders: Riders, repeat: Option<ListedRepeat>) -> bool {
        self.0 == 0
            || MessageType::ALL
                .iter()
                .enumerate()
                .any(|(i, t)| self.0 & (1 << i) != 0 && t.matches(role, riders, repeat))
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

/// A node's decisions on a burst of one-hit requests, taken in order against
/// its own window, each with
/// [`RuleSpec::admits`](gabion::rules::RuleSpec::admits), as gabion's adapters
/// decide each request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Burst {
    /// Every request fit and was recorded; the window held `total_before`.
    Admitted {
        requests: NonZeroU64,
        total_before: u64,
    },
    /// The first `admitted` requests fit and were recorded (none if the
    /// window was already full); the other `rejected` would pass the limit,
    /// so they were refused and never recorded. The window holds `total`
    /// after the admitted ones, and one more request fits in `reopens_ms`.
    Rejected {
        admitted: u64,
        rejected: NonZeroU64,
        total: u64,
        reopens_ms: u64,
    },
}

/// A node's decision on one more request, of one hit, against its own
/// window, as gabion's adapters decide.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Admission {
    /// It fits: the window holds `total_before`, under the limit.
    Admitted { total_before: u64 },
    /// It would pass the limit: the window holds `total`, and one more
    /// request fits in `reopens_ms`, once enough of the window's oldest hits
    /// leave it.
    Rejected { total: u64, reopens_ms: u64 },
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

/// Per-node cluster state, pulled every frame. Only the inspected node's peer
/// rows are included, so the frame costs one row per node rather than one per
/// node pair.
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
    /// from `RuleSpec::bucket_at`, so the page dates Sends and window events
    /// by the CRDT's own buckets rather than recomputing them. Rule-global in
    /// v1 (one watched rule); moves per-rule in the v1.1 multi-rule shape.
    pub bucket_epoch_now: u32,
    /// `gossip.suspect_after`, as the runtimes validated it: how long a
    /// node waits on an unanswered gateway before it gives up on it.
    pub suspect_after_ms: u64,
}

/// The one node the page inspects, from its runtime's admin snapshot, read
/// at the same instant as the rest of the snapshot.
#[derive(Clone, Debug, Serialize)]
pub struct Inspected {
    /// The inspected node's stable id.
    pub id: u32,
    /// The on-the-wire gossip identity it announces (`NodeIdentity`): a pure
    /// function of `id`, `id·256 + 1`.
    #[serde(with = "u128_hex")]
    pub node_id: u128,
    /// Its incarnation, bumped on restart so peers can supersede a stale
    /// alias. Always 1 in the sim (no restart path), shown to teach the
    /// concept.
    pub incarnation: u32,
    /// The per-rule error budget ε at the most recent request: a rule's
    /// pending hits crossing it trigger an eager (threshold) flush. `0` until
    /// seen.
    pub error_budget: u64,
    /// Cells its CRDT store holds, and the most it can.
    pub active_cells: u32,
    pub cell_capacity: u32,
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
/// [`GossipCounters`](gabion::gossip::GossipCounters); the window, decision,
/// lag, heartbeat and hit fields the engine reads or counts. The node
/// inspector renders them directly: see `GossipTargets.svelte` and
/// `NodeInspector.svelte` in `web/src/lib/components`.
#[derive(Clone, Debug, Serialize)]
pub struct NodeState {
    /// The node's stable id (see the module note).
    pub id: u32,
    /// The zone it is placed in.
    pub zone: u8,
    /// gabion's `window_total` of the watched key on this node's own count
    /// table: the number its admission reads.
    pub window_total: u64,
    /// The decision one more request, of one hit, would get here now:
    /// `Rejected` exactly while the node's window has reached its limit
    /// (`window_total >= limit`).
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
    /// Gateways replaced because they went unanswered for `suspect_after` or
    /// said they cannot relay.
    pub gateway_failovers: u64,
    /// Gap reports sent: a peer's stream skipped positions.
    pub gap_reports_sent: u64,
    /// Gaps given up after their reports went unanswered.
    pub gaps_abandoned: u64,
    /// Own-stream timeouts that resent from the last ack.
    pub rto_rewinds_own: u64,
    /// The same for the relay stream.
    pub rto_rewinds_relay: u64,
    /// Join sweeps this node started answering.
    pub sweeps_served: u64,
    /// Origins whose runtime sends this node's zone their changes through this
    /// node: the gateway rows of every node, inverted.
    pub gateway_for: u32,
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
}
