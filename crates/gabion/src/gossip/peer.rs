//! The runtime's peer table: one dense row per discovered peer, sized once to
//! `gossip.peer_capacity` and indexed by address. The columns every round
//! scans (role, flags, each stream's `sent`, the earliest timer) are hot; the
//! stream, zone and answer state read only when a packet goes to or comes
//! from one peer is cold, in [`PeerLink`].
//!
//! Each row's role follows from the zones its peer's headers announce: a peer
//! not yet heard from gets only the own cells raised since it was added, a
//! peer of this node's zone (or of no zone) gets both streams directly, and in
//! each other zone the member that ranks first for this node
//! (rendezvous weight, suspects last) is the gateway that gets the own stream
//! flagged for relay. A peer is suspect once a packet that asks for an answer
//! has gone unanswered for `gossip.suspect_after`, or it said it cannot relay.

use std::cmp::Reverse;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;
use std::ops::{Index, IndexMut};

use crate::crdt::{
    CellIndex, Incarnation, NodeIdentity, Stream, hash_fingerprint, mix64, pow2_index_capacity_for,
};
use crate::defaults::GOSSIP_GAP_REPORTS;
use crate::wire::{AckInfo, OwnAck, StreamRange};
use crate::zone::ZoneHash;

use super::admin::{AnswerState, PeerEntry, PeerZone, RoleCounts};
use super::transport::Sent;

/// What [`PeerTable::insert`] did with an address.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Insert {
    Added,
    Present,
    Full,
}

/// One value for each stream, so the own and relay values cannot trade
/// places.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PerStream<T> {
    pub(super) own: T,
    pub(super) relay: T,
}

impl PerStream<bool> {
    pub(super) fn any(self) -> bool {
        self.own || self.relay
    }
}

impl<T> Index<Stream> for PerStream<T> {
    type Output = T;

    fn index(&self, stream: Stream) -> &T {
        match stream {
            Stream::Own => &self.own,
            Stream::Relay => &self.relay,
        }
    }
}

impl<T> IndexMut<Stream> for PerStream<T> {
    fn index_mut(&mut self, stream: Stream) -> &mut T {
        match stream {
            Stream::Own => &mut self.own,
            Stream::Relay => &mut self.relay,
        }
    }
}

/// What a peer is to this node.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(rename_all = "snake_case")
)]
pub enum Role {
    /// A peer not heard from yet: it is sent the own cells raised since it
    /// was added and a greeting probe at once and each heartbeat, but no
    /// relay stream, until its first packet arrives.
    Unheard,
    /// A peer of this node's zone, or of no zone: it gets the own and relay
    /// streams directly.
    Zone,
    /// The member of a remote zone this node sends its own stream to,
    /// flagged for relay into that zone.
    Gateway,
    /// Any other member of a remote zone: it is sent only acks.
    Remote,
    /// A peer that speaks another wire version: it is sent nothing until a
    /// packet of this version arrives from it.
    Incompatible,
}

impl Role {
    /// Whether the row gets this node's own stream.
    fn gets_own(self) -> bool {
        matches!(self, Self::Unheard | Self::Zone | Self::Gateway)
    }
}

/// The row's owed work, one bit each.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PeerFlags(u8);

impl PeerFlags {
    /// The peer is owed a header-only packet: at a heartbeat a standalone
    /// ack, since it sent stream data after this node's last packet to it;
    /// on a new row, its first greeting, on the next round of any kind.
    pub(super) const ACK_DUE: Self = Self(0x01);
    /// A gap open on the peer's own stream (the mark `OwnMode` selects) or
    /// relay stream, whose report is prompt work.
    pub(super) const GAP_OPEN: Self = Self(0x08);
    /// The bits that make a row prompt work.
    pub(super) const PROMPT: Self = Self(Self::RESEND_OWN.0 | Self::REPLY_DUE.0 | Self::GAP_OPEN.0);
    /// The peer sent `PROBE`; the next packet to it carries `PROBE_REPLY`.
    pub(super) const REPLY_DUE: Self = Self(0x04);
    /// The own stream has cells above `sent` that a prompt round sends.
    pub(super) const RESEND_OWN: Self = Self(0x02);

    pub(super) fn has(self, bits: Self) -> bool {
        self.0 & bits.0 != 0
    }

    fn set(&mut self, bits: Self, on: bool) {
        if on {
            self.0 |= bits.0;
        } else {
            self.0 &= !bits.0;
        }
    }
}

/// A row's earliest timer, in clock milliseconds; `NEVER` is the identity of
/// `min`, so a round tests each row with one comparison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Deadline(u64);

impl Deadline {
    const NEVER: Self = Self(u64::MAX);
}

/// One stream's sender state besides `sent`, which is hot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Outbound {
    /// The peer's last credited ack.
    pub(super) acked: u64,
    /// When the data in flight was last sent or progressed, for the timeout.
    pub(super) timer_ms: u64,
}

/// A stream sender's state viewed together; `acked ≤ sent ≤ head` always.
pub(super) struct StreamTx<'a> {
    sent: &'a mut u64,
    state: &'a mut Outbound,
}

impl StreamTx<'_> {
    /// Packets closing through `upper` left. The timer starts only when this
    /// send puts data in flight; a send never moves a running timer.
    pub(super) fn note_sent(&mut self, upper: u64, now: u64) {
        if self.state.acked == *self.sent {
            self.state.timer_ms = now;
        }
        *self.sent = (*self.sent).max(upper);
    }

    /// Credit an ack; returns whether it was progress. One above the head,
    /// or not above `acked`, changes nothing; progress that leaves data in
    /// flight restarts the timer.
    pub(super) fn note_ack(&mut self, ack: u64, head: u64, now: u64) -> bool {
        if ack > head || ack <= self.state.acked {
            return false;
        }
        self.state.acked = ack;
        *self.sent = (*self.sent).max(ack);
        if ack < *self.sent {
            self.state.timer_ms = now;
        }
        true
    }

    /// A gap report says "I hold through `ack`": resend from there, even
    /// below `acked`, which means the receiver forgot the stream. Returns
    /// whether cells are owed.
    pub(super) fn gap_report(&mut self, ack: u64, head: u64, now: u64) -> bool {
        if ack > head {
            return false;
        }
        self.state.acked = ack;
        *self.sent = ack;
        self.state.timer_ms = now;
        ack < head
    }

    pub(super) fn in_flight(&self) -> bool {
        self.state.acked < *self.sent
    }

    /// Resend everything above `acked`.
    pub(super) fn rewind(&mut self, now: u64) {
        *self.sent = self.state.acked;
        self.restart(now);
    }

    /// Time the next retry one timeout from `now`, resending nothing.
    pub(super) fn restart(&mut self, now: u64) {
        self.state.timer_ms = now;
    }

    /// A new stream: nothing sent, nothing acked.
    fn reset(&mut self) {
        *self.sent = 0;
        self.state.acked = 0;
    }
}

/// A receiver's gap. Only `Open` reports, so no other state can.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum Gap {
    #[default]
    Closed,
    /// A range above `have` arrived; `stash` is the one kept.
    Open {
        stash: (u64, u64),
        report_at_ms: u64,
        reports: u8,
    },
    /// Reported [`GOSSIP_GAP_REPORTS`] times without progress; reports stop
    /// until the next range above `have` reopens it.
    Abandoned { stash: (u64, u64) },
}

/// What [`Inbound::take_report`] found due.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum GapDue {
    #[default]
    Nothing,
    /// Send a gap report now.
    Report,
    /// The gap is abandoned; `missing` stream positions never arrived.
    Abandoned { missing: u64 },
}

/// One stream's receiver state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Inbound {
    /// The highest position held contiguously from 0.
    have: u64,
    gap: Gap,
}

impl Inbound {
    fn is_open(&self) -> bool {
        matches!(self.gap, Gap::Open { .. })
    }

    /// A packet closing `range` arrived. `have` moves only over contiguous
    /// ranges; a range above it is stashed and opens a gap, or reopens an
    /// abandoned one.
    pub(super) fn receive(&mut self, range: StreamRange, now: u64, floor_ms: u64) {
        let (lower, upper) = (range.lower(), range.upper());
        if lower > self.have {
            self.gap = match self.gap {
                Gap::Closed => Gap::Open {
                    stash: (lower, upper),
                    report_at_ms: now.saturating_add(floor_ms),
                    reports: 0,
                },
                Gap::Open {
                    stash,
                    report_at_ms,
                    reports,
                } => Gap::Open {
                    stash: keep(stash, (lower, upper)),
                    report_at_ms,
                    reports,
                },
                // Its sender still sends: ask again.
                Gap::Abandoned { stash } => Gap::Open {
                    stash: keep(stash, (lower, upper)),
                    report_at_ms: now.saturating_add(floor_ms),
                    reports: 0,
                },
            };
            return;
        }
        if upper <= self.have {
            return;
        }
        self.have = upper;
        self.gap = match self.gap {
            Gap::Closed => Gap::Closed,
            Gap::Open { stash, .. } | Gap::Abandoned { stash } if stash.0 <= self.have => {
                self.have = self.have.max(stash.1);
                Gap::Closed
            }
            Gap::Open {
                stash,
                report_at_ms,
                ..
            } => Gap::Open {
                stash,
                report_at_ms,
                reports: 0,
            },
            Gap::Abandoned { stash } => Gap::Open {
                stash,
                report_at_ms: now.saturating_add(floor_ms),
                reports: 0,
            },
        };
    }

    /// When the open gap's next report falls due.
    fn report_at(&self) -> Option<u64> {
        match self.gap {
            Gap::Open { report_at_ms, .. } => Some(report_at_ms),
            Gap::Closed | Gap::Abandoned { .. } => None,
        }
    }

    /// Take the report due at `now`: the next one is due `4 · F_r` later,
    /// and the one after the last allowed abandons the gap.
    pub(super) fn take_report(&mut self, now: u64, floor_ms: u64) -> GapDue {
        let Gap::Open {
            stash,
            report_at_ms,
            reports,
        } = self.gap
        else {
            return GapDue::Nothing;
        };
        if now < report_at_ms {
            return GapDue::Nothing;
        }
        if reports == GOSSIP_GAP_REPORTS {
            self.gap = Gap::Abandoned { stash };
            return GapDue::Abandoned {
                missing: stash.0 - self.have,
            };
        }
        self.gap = Gap::Open {
            stash,
            report_at_ms: now.saturating_add(4 * floor_ms),
            reports: reports + 1,
        };
        GapDue::Report
    }
}

/// Two stashed ranges: joined when they touch, else the higher.
fn keep(stash: (u64, u64), range: (u64, u64)) -> (u64, u64) {
    if range.0 <= stash.1 && range.1 >= stash.0 {
        (stash.0.min(range.0), stash.1.max(range.1))
    } else {
        stash.max(range)
    }
}

/// What this node waits for from a peer. Only these transitions change it: a
/// send that asks for an answer starts `Owed`; progress, or a probe answer
/// when nothing is in flight, settles it, or restarts it while data is owed;
/// a held packet's release shifts it; `CANNOT_RELAY` from a remote zone
/// replaces it; a role that receives nothing settles it unless suspect; and
/// a restart or another wire version clears it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum Answer {
    /// Nothing sent waits for an answer.
    #[default]
    Settled,
    /// The oldest unanswered packet that asked for an answer left at
    /// `since_ms`.
    Owed { since_ms: u64 },
    /// The peer said it cannot reach most of its zone; suspect from
    /// `since_ms`.
    CannotRelay { since_ms: u64 },
}

impl Answer {
    /// When the peer is or becomes suspect: `suspect_after_ms` after the
    /// oldest unanswered packet, or at once for `CannotRelay`.
    fn suspect_from(self, suspect_after_ms: u64) -> Option<u64> {
        match self {
            Self::Settled => None,
            Self::Owed { since_ms } => Some(since_ms.saturating_add(suspect_after_ms)),
            Self::CannotRelay { since_ms } => Some(since_ms),
        }
    }

    fn since_ms(self) -> Option<u64> {
        match self {
            Self::Settled => None,
            Self::Owed { since_ms } | Self::CannotRelay { since_ms } => Some(since_ms),
        }
    }
}

/// Which kind of own-stream packet the peer sent last: it picks the mark this
/// node acks, the `ACK_FLAGGED` bit, and the gap it reports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum OwnMode {
    #[default]
    Plain,
    /// The last own packet carried `RELAY_INTO_ZONE`.
    Flagged,
}

/// A row's cold state.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PeerLink {
    /// The peer's rendezvous weight for this node: join sweeps ask, and
    /// gateways are chosen, highest first.
    weight: u64,
    /// The zone the peer's headers announce: it moves only from unknown to
    /// known, or on a new incarnation.
    zone: Option<ZoneHash>,
    /// From the peer's latest current packet; `None` until its first.
    pub(super) identity: Option<NodeIdentity>,
    /// The incarnation before the last restart; a late packet carrying it
    /// changes no stream state.
    prev_incarnation: Option<Incarnation>,
    answer: Answer,
    out: PerStream<Outbound>,
    /// The peer's own stream as this node receives it: unflagged ranges, and
    /// the ranges flagged `RELAY_INTO_ZONE`, each with its own mark.
    own_in: Inbound,
    flagged_in: Inbound,
    own_mode: OwnMode,
    /// The peer's relay stream.
    relay_in: Inbound,
}

impl PeerLink {
    /// This node's ack of the peer's own stream: the mark of the kind of
    /// packet it sent last.
    pub(super) fn own_ack(&self) -> OwnAck {
        match self.own_mode {
            OwnMode::Plain => OwnAck::Plain(self.own_in.have),
            OwnMode::Flagged => OwnAck::Flagged(self.flagged_in.have),
        }
    }

    pub(super) fn relay_ack(&self) -> u64 {
        self.relay_in.have
    }

    /// The own-stream mark whose gap is reported; the other is inert.
    fn own_mark(&mut self) -> &mut Inbound {
        match self.own_mode {
            OwnMode::Plain => &mut self.own_in,
            OwnMode::Flagged => &mut self.flagged_in,
        }
    }
}

// 240 bytes (`Option<NodeIdentity>` 48 at `u128` alignment, each of the
// three inbound marks 40, the answer and each outbound 16): with the 26 hot
// bytes, the 32-byte address, the 8-byte `zones` entry and the index, the
// about-330-byte per-slot cost the `gossip.peer_capacity` warning quotes.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<PeerLink>() == 240);

/// What a packet's sender identity means for its row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Contact {
    /// The peer's first packet.
    First,
    Current,
    /// A new incarnation: the row was reset.
    Restarted,
    /// From the incarnation before the last restart: merge it, nothing else.
    Late,
}

/// What [`PeerTable::credit`] asks of the runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Credit {
    /// The relay stream was rewound: prompt work.
    pub(super) resend_relay: bool,
    /// A suspect's probe answer rewound these streams.
    pub(super) answer_rewound: PerStream<bool>,
}

/// A zone row with relay data in flight turned suspect.
#[derive(Clone, Copy, Debug)]
pub(super) struct Stall {
    pub(super) peer: SocketAddr,
    pub(super) stalled_ms: u64,
    pub(super) relay_unacked: u64,
}

/// A suspect gateway lost the role.
#[derive(Clone, Copy, Debug)]
pub(super) struct Failover {
    pub(super) zone: ZoneHash,
    pub(super) old: SocketAddr,
    pub(super) new: SocketAddr,
    /// Whether the old gateway said it cannot relay.
    pub(super) cannot_relay: bool,
    pub(super) unanswered_ms: u64,
}

/// The best gateway candidate of one remote zone seen so far in a refresh.
#[derive(Clone, Copy, Debug)]
struct ZoneBest {
    zone: ZoneHash,
    row: u32,
}

/// A gateway candidate's rank, highest wins: every live member above every
/// suspect, live members by weight, suspects by the earliest suspicion, and
/// the address last.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Rank {
    Suspect(Reverse<u64>, SocketAddr),
    Live(u64, SocketAddr),
}

/// Rows `0..len` hold the peers in no particular order: a removed row is
/// filled by the last. Every column is sized at construction and never grows.
pub(super) struct PeerTable {
    len: usize,
    /// The retransmission timeout: one heartbeat.
    rto_ms: u64,
    /// `gossip.suspect_after`.
    suspect_after_ms: u64,
    /// This node's id hashed, for the rendezvous weights.
    local_key: u64,
    local_zone: Option<ZoneHash>,
    /// A zone, a suspicion or the membership changed: roles need a refresh.
    stale: bool,
    /// When suspicion was last judged; a row is suspect as of then.
    judged_ms: u64,
    // Hot: every round's scan.
    role: Box<[Role]>,
    flags: Box<[PeerFlags]>,
    /// The highest position of each stream sent to the peer.
    sent: PerStream<Box<[u64]>>,
    deadline: Box<[Deadline]>,
    // Cold: one packet to or from this peer.
    addr: Box<[SocketAddr]>,
    link: Box<[PeerLink]>,
    /// Address hash to row; `addr[row]` confirms a match.
    by_addr: CellIndex,
    /// The refresh's best member per remote zone: there are never more zones
    /// than peers.
    zones: Box<[ZoneBest]>,
}

impl PeerTable {
    pub(super) fn with_capacity(
        capacity: NonZeroU16,
        local_key: u64,
        local_zone: Option<ZoneHash>,
        rto_ms: u64,
        suspect_after_ms: u64,
    ) -> Self {
        let rows = usize::from(capacity.get());
        let unused = ZoneBest {
            zone: ZoneHash::MIN,
            row: 0,
        };
        Self {
            len: 0,
            rto_ms,
            suspect_after_ms,
            local_key,
            local_zone,
            stale: false,
            judged_ms: 0,
            role: vec![Role::Zone; rows].into_boxed_slice(),
            flags: vec![PeerFlags::default(); rows].into_boxed_slice(),
            sent: PerStream {
                own: vec![0; rows].into_boxed_slice(),
                relay: vec![0; rows].into_boxed_slice(),
            },
            deadline: vec![Deadline::NEVER; rows].into_boxed_slice(),
            addr: vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)); rows].into_boxed_slice(),
            link: vec![PeerLink::default(); rows].into_boxed_slice(),
            by_addr: CellIndex::with_capacity(pow2_index_capacity_for(capacity.into())),
            zones: vec![unused; rows].into_boxed_slice(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    /// A live row's address; a row at or past `len` is out of bounds.
    pub(super) fn addr(&self, row: usize) -> SocketAddr {
        self.addr[..self.len][row]
    }

    pub(super) fn find(&self, addr: SocketAddr) -> Option<usize> {
        self.lookup(addr_hash(addr), addr).map(|row| row as usize)
    }

    /// A known address is `Present` even when the table is full, so
    /// re-announcing a peer is never a decline. A new row is `Unheard`: it
    /// owes the peer a greeting, and its own stream starts at `own_head`, so
    /// until its first packet it is sent only the cells raised from now on.
    pub(super) fn insert(&mut self, addr: SocketAddr, own_head: u64) -> Insert {
        let hash = addr_hash(addr);
        if self.lookup(hash, addr).is_some() {
            return Insert::Present;
        }
        let row = self.len;
        if row == self.addr.len() {
            return Insert::Full;
        }
        self.role[row] = Role::Unheard;
        self.flags[row] = PeerFlags::ACK_DUE;
        self.sent.own[row] = own_head;
        self.sent.relay[row] = 0;
        self.deadline[row] = Deadline::NEVER;
        self.addr[row] = addr;
        self.link[row] = PeerLink {
            weight: rendezvous_weight(self.local_key, addr),
            ..PeerLink::default()
        };
        self.link[row].out.own.acked = own_head;
        self.by_addr.insert_unchecked(hash, row as u32);
        self.len += 1;
        Insert::Added
    }

    /// Swap-remove: the last row fills the hole and its index entry follows.
    /// Returns the removed row's cold state.
    pub(super) fn remove(&mut self, addr: SocketAddr) -> Option<PeerLink> {
        let hash = addr_hash(addr);
        let row = self.lookup(hash, addr)?;
        self.by_addr.remove(hash, row);
        self.len -= 1;
        self.stale = true;
        let (row, last) = (row as usize, self.len);
        let removed = self.link[row];
        if row != last {
            let moved = addr_hash(self.addr[last]);
            self.by_addr.remove(moved, last as u32);
            self.by_addr.insert_unchecked(moved, row as u32);
            self.role[row] = self.role[last];
            self.flags[row] = self.flags[last];
            self.sent.own[row] = self.sent.own[last];
            self.sent.relay[row] = self.sent.relay[last];
            self.deadline[row] = self.deadline[last];
            self.addr[row] = self.addr[last];
            self.link[row] = self.link[last];
        }
        Some(removed)
    }

    /// Note the sender identity and zone of a packet from `row`. A known zone
    /// changes only with the incarnation, so a zone-0 header never clears it.
    /// A first packet refreshes the roles, so an unheard row takes its role;
    /// a new incarnation resets the whole row.
    pub(super) fn contact(
        &mut self,
        row: usize,
        sender: NodeIdentity,
        zone: Option<ZoneHash>,
    ) -> Contact {
        let link = &mut self.link[row];
        let contact = match link.identity {
            None => {
                // Its role follows at the next refresh.
                self.stale = true;
                Contact::First
            }
            Some(known) if known == sender => Contact::Current,
            Some(known)
                if known.node_id == sender.node_id
                    && link.prev_incarnation == Some(sender.incarnation) =>
            {
                return Contact::Late;
            }
            Some(known) => {
                *link = PeerLink {
                    weight: link.weight,
                    prev_incarnation: Some(known.incarnation),
                    ..PeerLink::default()
                };
                self.sent.own[row] = 0;
                self.sent.relay[row] = 0;
                self.flags[row] = PeerFlags::default();
                self.stale = true;
                Contact::Restarted
            }
        };
        let link = &mut self.link[row];
        link.identity = Some(sender);
        if link.zone.is_none() && zone.is_some() {
            link.zone = zone;
            self.stale = true;
        }
        self.refresh_deadline(row);
        contact
    }

    /// The peer speaks another wire version: send it nothing, and wait for
    /// nothing from it. Returns whether the role changed.
    pub(super) fn mute(&mut self, row: usize) -> bool {
        let changed = self.role[row] != Role::Incompatible;
        self.role[row] = Role::Incompatible;
        self.flags[row] = PeerFlags::default();
        self.link[row].answer = Answer::Settled;
        self.stale |= changed;
        self.refresh_deadline(row);
        changed
    }

    /// A packet of this version arrived from `row`. A muted row becomes a
    /// zone peer until the next refresh, both streams rewound to 0 as prompt
    /// work; returns whether it was muted.
    pub(super) fn restore(&mut self, row: usize) -> bool {
        if self.role[row] != Role::Incompatible {
            return false;
        }
        self.apply_role(row, Role::Zone);
        self.stale = true;
        true
    }

    /// This node's zone, which every header announces.
    pub(super) fn local_zone(&self) -> Option<ZoneHash> {
        self.local_zone
    }

    /// Learn this node's zone, unless it is already known (configured or
    /// learned): returns whether it was adopted.
    pub(super) fn adopt_zone(&mut self, zone: ZoneHash) -> bool {
        let adopted = self.local_zone.is_none();
        if adopted {
            self.local_zone = Some(zone);
            self.stale = true;
        }
        adopted
    }

    /// Whether roles need a refresh: prompt work.
    pub(super) fn stale(&self) -> bool {
        self.stale
    }

    pub(super) fn role(&self, row: usize) -> Role {
        self.role[row]
    }

    pub(super) fn flags(&self, row: usize) -> PeerFlags {
        self.flags[row]
    }

    pub(super) fn set_flag(&mut self, row: usize, bits: PeerFlags, on: bool) {
        self.flags[row].set(bits, on);
    }

    pub(super) fn sent(&self, row: usize, stream: Stream) -> u64 {
        self.sent[stream][row]
    }

    pub(super) fn tx(&mut self, row: usize, stream: Stream) -> StreamTx<'_> {
        StreamTx {
            sent: &mut self.sent[stream][row],
            state: &mut self.link[row].out[stream],
        }
    }

    pub(super) fn link(&self, row: usize) -> &PeerLink {
        &self.link[row]
    }

    /// Whether `row` is suspect, as of the last judgment.
    pub(super) fn suspect(&self, row: usize) -> bool {
        let answer = self.link[row].answer;
        answer
            .suspect_from(self.suspect_after_ms)
            .is_some_and(|at| at <= self.judged_ms)
    }

    /// Whether `row`'s retries are header-only probes: it is suspect, or a
    /// gateway that has acked nothing of the stream it was sent from 0.
    pub(super) fn probes_only(&self, row: usize) -> bool {
        self.suspect(row) || (self.role[row] == Role::Gateway && self.link[row].out.own.acked == 0)
    }

    /// A packet of the peer's own stream closing `range` arrived, flagged or
    /// not; returns whether a flagged one is fresh, above the flagged mark
    /// as it was. The kind picks the mark it moves and the ack sent back. A
    /// plain range from a peer of another known zone is not tracked.
    pub(super) fn receive_own(
        &mut self,
        row: usize,
        range: StreamRange,
        flagged: bool,
        now: u64,
        floor_ms: u64,
    ) -> bool {
        // A peer of another zone sends its own stream unflagged only until it
        // hears this node's zone, then stops: merged, not tracked, so no gap
        // opens on a stream that stops.
        if !flagged && self.remote_zone(row).is_some() {
            return false;
        }
        let link = &mut self.link[row];
        let fresh = flagged && range.upper() > link.flagged_in.have;
        link.own_mode = if flagged {
            OwnMode::Flagged
        } else {
            OwnMode::Plain
        };
        link.own_mark().receive(range, now, floor_ms);
        self.flags[row].set(PeerFlags::ACK_DUE, true);
        self.refresh_deadline(row);
        fresh
    }

    /// A packet of the peer's relay stream closing `range` arrived.
    pub(super) fn receive_relay(
        &mut self,
        row: usize,
        range: StreamRange,
        now: u64,
        floor_ms: u64,
    ) {
        self.link[row].relay_in.receive(range, now, floor_ms);
        self.flags[row].set(PeerFlags::ACK_DUE, true);
        self.refresh_deadline(row);
    }

    /// Take the gap reports due at `now`: the selected own mark's and the
    /// relay stream's.
    pub(super) fn take_reports(
        &mut self,
        row: usize,
        now: u64,
        floor_ms: u64,
    ) -> PerStream<GapDue> {
        let link = &mut self.link[row];
        PerStream {
            own: link.own_mark().take_report(now, floor_ms),
            relay: link.relay_in.take_report(now, floor_ms),
        }
    }

    /// Credit a packet's acks against `heads`, the own and relay heads. The
    /// own-stream ack counts on a `Gateway` row only flagged and on a `Zone`
    /// or `Unheard` row only plain; the relay ack only on a `Zone` row; and
    /// only for this node's current incarnation. `CANNOT_RELAY` from a remote
    /// zone makes the row suspect at once and its acks no progress; other
    /// progress settles the row, or restarts its wait while data is owed, and
    /// a probe answer from a suspect, or from a gateway that has acked
    /// nothing, rewinds each stream it is owed.
    pub(super) fn credit(
        &mut self,
        row: usize,
        acks: AckInfo,
        incarnation: Incarnation,
        heads: PerStream<u64>,
        now: u64,
    ) -> Credit {
        let role = self.role[row];
        let was = self.suspect(row);
        let probed = self.probes_only(row);
        let current = acks.incarnation == incarnation;
        let mut credit = Credit::default();
        let mut progress = false;
        let own_ack = match (role, acks.own) {
            (Role::Zone | Role::Unheard, OwnAck::Plain(ack))
            | (Role::Gateway, OwnAck::Flagged(ack)) => Some(ack),
            _ => None,
        };
        if let Some(ack) = own_ack.filter(|_| current) {
            let mut own = self.tx(row, Stream::Own);
            progress |= own.note_ack(ack, heads.own, now);
            if acks.gap_own && own.gap_report(ack, heads.own, now) {
                self.flags[row].set(PeerFlags::RESEND_OWN, true);
            }
        }
        if current && role == Role::Zone {
            let mut relay = self.tx(row, Stream::Relay);
            progress |= relay.note_ack(acks.relay, heads.relay, now);
            credit.resend_relay = acks.gap_relay && relay.gap_report(acks.relay, heads.relay, now);
        }
        let owed = self.owed(row);
        let counts = acks.cannot_relay && matches!(role, Role::Gateway | Role::Remote);
        let answer = &mut self.link[row].answer;
        if counts {
            let from = answer.suspect_from(self.suspect_after_ms).unwrap_or(now);
            let cannot = Answer::CannotRelay {
                since_ms: from.min(now),
            };
            // A repeat changes no rank, so it refreshes no role.
            self.stale |= std::mem::replace(answer, cannot) != cannot;
        } else if current && (progress || (acks.probe_reply && !owed.any())) {
            *answer = if !owed.any() {
                Answer::Settled
            } else {
                Answer::Owed { since_ms: now }
            };
        }
        if current && !counts && acks.probe_reply && probed {
            for stream in [Stream::Own, Stream::Relay] {
                if owed[stream] {
                    self.tx(row, stream).rewind(now);
                }
            }
            credit.answer_rewound = owed;
            if owed.own {
                self.flags[row].set(PeerFlags::RESEND_OWN, true);
            }
            credit.resend_relay |= owed.relay;
        }
        self.stale |= was != self.suspect(row);
        self.refresh_deadline(row);
        credit
    }

    /// The streams `row` receives that have data in flight.
    fn owed(&mut self, row: usize) -> PerStream<bool> {
        let role = self.role[row];
        PerStream {
            own: role.gets_own() && self.tx(row, Stream::Own).in_flight(),
            relay: role == Role::Zone && self.tx(row, Stream::Relay).in_flight(),
        }
    }

    /// A packet to `row` went to the socket. One that starts a wait and left
    /// this node starts it, unless one is running; a held packet starts
    /// nothing until it leaves.
    pub(super) fn note_send(&mut self, row: usize, sent: Sent, now: u64) {
        let answer = &mut self.link[row].answer;
        if sent.waits && *answer == Answer::Settled {
            *answer = Answer::Owed { since_ms: now };
            self.refresh_deadline(row);
        }
    }

    /// A held packet left after `held_ms`: no peer could answer meanwhile, so
    /// every wait moves forward by that much, never past `now`.
    pub(super) fn shift_owed(&mut self, held_ms: u64, now: u64) {
        for row in 0..self.len {
            if let Answer::Owed { since_ms } = &mut self.link[row].answer {
                *since_ms = since_ms.saturating_add(held_ms).min(now);
                self.refresh_deadline(row);
            }
        }
        self.stale = true;
    }

    /// Judge suspicion as of `now`, the start of a round that read its
    /// waiting datagrams and holds no packet. Each row whose suspicion began
    /// since the last judgment makes the roles stale; a zone row among them
    /// with relay data in flight is a stalled relay. A heartbeat stamped
    /// before the last judgment judges nothing again.
    pub(super) fn judge(&mut self, now: u64, mut on_stall: impl FnMut(Stall)) {
        let now = now.max(self.judged_ms);
        let last = std::mem::replace(&mut self.judged_ms, now);
        for row in 0..self.len {
            // Only a due row can have crossed: the hot column first.
            if self.deadline[row].0 > now {
                continue;
            }
            let answer = self.link[row].answer;
            let crossed = (answer.suspect_from(self.suspect_after_ms))
                .is_some_and(|at| last < at && at <= now);
            if !crossed {
                continue;
            }
            self.stale = true;
            let relay = self.link[row].out.relay;
            if self.role[row] == Role::Zone && relay.acked < self.sent.relay[row] {
                on_stall(Stall {
                    peer: self.addr[row],
                    stalled_ms: answer
                        .since_ms()
                        .map_or(0, |since| now.saturating_sub(since)),
                    relay_unacked: self.sent.relay[row] - relay.acked,
                });
            }
            self.refresh_deadline(row);
        }
    }

    /// Whether this node cannot reach most of its zone: more than half of the
    /// zone peers it has heard are suspect.
    pub(super) fn impaired(&self) -> bool {
        let (mut heard, mut stuck) = (0_usize, 0_usize);
        for row in 0..self.len {
            if self.role[row] == Role::Zone {
                heard += 1;
                stuck += usize::from(self.suspect(row));
            }
        }
        heard > 0 && 2 * stuck > heard
    }

    fn rank(&self, row: usize) -> Rank {
        let link = &self.link[row];
        match link.answer.suspect_from(self.suspect_after_ms) {
            Some(from) if from <= self.judged_ms => Rank::Suspect(Reverse(from), self.addr[row]),
            _ => Rank::Live(link.weight, self.addr[row]),
        }
    }

    /// Recompute every role when stale: one pass keeps the best member of
    /// each remote zone, a second sets each row's role. Returns how many
    /// rows changed role; `on_failover` hears of each suspect gateway
    /// replaced. An unheard row keeps `Unheard` until its first packet.
    pub(super) fn refresh(&mut self, now: u64, mut on_failover: impl FnMut(Failover)) -> u64 {
        if !std::mem::take(&mut self.stale) {
            return 0;
        }
        let mut used = 0;
        for row in 0..self.len {
            let Some(zone) = self.remote_zone(row) else {
                continue;
            };
            match self.zones[..used].iter().position(|best| best.zone == zone) {
                Some(i) if self.rank(row) > self.rank(self.zones[i].row as usize) => {
                    self.zones[i].row = row as u32;
                }
                Some(_) => {}
                None => {
                    self.zones[used] = ZoneBest {
                        zone,
                        row: row as u32,
                    };
                    used += 1;
                }
            }
        }
        let mut changes = 0;
        for row in 0..self.len {
            let old = self.role[row];
            let new = match self.remote_zone(row) {
                None if old == Role::Incompatible || self.link[row].identity.is_none() => continue,
                None => Role::Zone,
                Some(zone) => {
                    let best = self.zones[..used].iter().find(|best| best.zone == zone);
                    match best {
                        Some(best) if best.row as usize == row => Role::Gateway,
                        _ => Role::Remote,
                    }
                }
            };
            if old == Role::Gateway && new != Role::Gateway && self.suspect(row) {
                let answer = self.link[row].answer;
                let zone = self.link[row].zone;
                let best = self.zones[..used]
                    .iter()
                    .find(|best| Some(best.zone) == zone);
                if let (Some(zone), Some(best)) = (zone, best) {
                    on_failover(Failover {
                        zone,
                        old: self.addr[row],
                        new: self.addr[best.row as usize],
                        cannot_relay: matches!(answer, Answer::CannotRelay { .. }),
                        unanswered_ms: answer
                            .since_ms()
                            .map_or(0, |since| now.saturating_sub(since)),
                    });
                }
            }
            changes += u64::from(self.apply_role(row, new));
        }
        changes
    }

    /// `row`'s zone when it is a remote zone this node chooses a gateway in:
    /// both zones known and different, and the peer speaks this version.
    fn remote_zone(&self, row: usize) -> Option<ZoneHash> {
        let zone = self.link[row].zone?;
        let remote = self.local_zone.is_some_and(|local| local != zone);
        (remote && self.role[row] != Role::Incompatible).then_some(zone)
    }

    /// Give `row` role `new`; returns whether it changed. A stream the row
    /// starts receiving, or receives with a new kind of ack, rewinds to 0 as
    /// prompt work (an unheard row first heard included); a row left receiving
    /// nothing has nothing to wait for unless it is suspect.
    fn apply_role(&mut self, row: usize, new: Role) -> bool {
        let old = std::mem::replace(&mut self.role[row], new);
        if old == new {
            return false;
        }
        if new.gets_own() {
            self.tx(row, Stream::Own).reset();
        }
        if new == Role::Zone {
            self.tx(row, Stream::Relay).reset();
        }
        self.flags[row].set(PeerFlags::RESEND_OWN, new.gets_own());
        if !new.gets_own() && !self.suspect(row) {
            self.link[row].answer = Answer::Settled;
        }
        self.refresh_deadline(row);
        true
    }

    /// Whether any of `row`'s timers has fallen due at `now`.
    pub(super) fn due(&self, row: usize, now: u64) -> bool {
        self.deadline[row].0 <= now
    }

    /// When `row`'s own-stream timer falls due: while own data is in flight,
    /// and while an answer is awaited with no relay data in flight, whose own
    /// timer retries (a probe for a row that receives no stream).
    pub(super) fn own_timer(&self, row: usize) -> Option<u64> {
        let link = &self.link[row];
        let role = self.role[row];
        let runs = (role.gets_own() && link.out.own.acked < self.sent.own[row])
            || (link.answer != Answer::Settled && self.relay_timer(row).is_none());
        (runs && role != Role::Incompatible)
            .then(|| link.out.own.timer_ms.saturating_add(self.rto_ms))
    }

    /// When `row`'s relay stream times out, while relay data is in flight.
    pub(super) fn relay_timer(&self, row: usize) -> Option<u64> {
        let link = &self.link[row];
        (self.role[row] == Role::Zone && link.out.relay.acked < self.sent.relay[row])
            .then(|| link.out.relay.timer_ms.saturating_add(self.rto_ms))
    }

    /// Recompute `row`'s deadline after its timers changed: the stream
    /// timers, the reported marks' next gap reports, and when an awaited
    /// answer turns the row suspect.
    pub(super) fn refresh_deadline(&mut self, row: usize) {
        let suspect_at = (self.link[row].answer)
            .suspect_from(self.suspect_after_ms)
            .filter(|&at| at > self.judged_ms);
        let link = &mut self.link[row];
        let (own, relay) = (link.own_mark().report_at(), link.relay_in.report_at());
        let open = link.own_mark().is_open() || link.relay_in.is_open();
        self.deadline[row] = [
            self.own_timer(row),
            self.relay_timer(row),
            own,
            relay,
            suspect_at,
        ]
        .into_iter()
        .flatten()
        .min()
        .map_or(Deadline::NEVER, Deadline);
        self.flags[row].set(PeerFlags::GAP_OPEN, open);
    }

    /// The admin view of every row at `now`.
    pub(super) fn entries(&self, now: u64) -> impl Iterator<Item = PeerEntry> + '_ {
        (0..self.len).map(move |row| {
            let link = &self.link[row];
            let answer = match link.answer {
                Answer::Settled => AnswerState::Settled,
                Answer::CannotRelay { .. } => AnswerState::CannotRelay,
                Answer::Owed { .. } if self.suspect(row) => AnswerState::Suspect,
                Answer::Owed { .. } => AnswerState::Owed,
            };
            let zone = match link.zone {
                None => PeerZone::Unknown,
                Some(zone) if Some(zone) == self.local_zone => PeerZone::Same,
                Some(zone) => PeerZone::Other(zone),
            };
            PeerEntry {
                addr: self.addr[row],
                node_id: link.identity.map(|identity| identity.node_id),
                zone,
                role: self.role[row],
                answer,
                unanswered_ms: (link.answer.since_ms())
                    .map(|since| u32::try_from(now.saturating_sub(since)).unwrap_or(u32::MAX)),
                own_unacked: self.sent.own[row] - link.out.own.acked,
                relay_unacked: self.sent.relay[row] - link.out.relay.acked,
            }
        })
    }

    /// How many rows hold each role.
    pub(super) fn role_counts(&self) -> RoleCounts {
        let mut counts = RoleCounts::default();
        for &role in &self.role[..self.len] {
            *match role {
                Role::Unheard => &mut counts.unheard,
                Role::Zone => &mut counts.zone,
                Role::Gateway => &mut counts.gateway,
                Role::Remote => &mut counts.remote,
                Role::Incompatible => &mut counts.incompatible,
            } += 1;
        }
        counts
    }

    /// The peers a join sweep may ask, with their weights: the peers of this
    /// node's zone, or with `any_zone` every peer that speaks this version,
    /// but only once every peer has been heard, since an unheard one may be
    /// of this zone.
    pub(super) fn sweep_candidates(
        &self,
        any_zone: bool,
    ) -> impl Iterator<Item = (u64, SocketAddr)> + '_ {
        let any_zone = any_zone && !self.role[..self.len].contains(&Role::Unheard);
        (0..self.len)
            .filter(move |&row| match self.role[row] {
                Role::Zone => true,
                Role::Gateway | Role::Remote => any_zone,
                Role::Unheard | Role::Incompatible => false,
            })
            .map(|row| (self.link[row].weight, self.addr[row]))
    }

    fn lookup(&self, hash: u64, addr: SocketAddr) -> Option<u32> {
        self.by_addr
            .find(hash, |row| self.addr[row as usize] == addr)
    }
}

/// The address as one 128-bit key: an IPv4 address in its IPv6-mapped form,
/// with the port in bits 48..64, which are zero in that form.
fn addr_hash(addr: SocketAddr) -> u64 {
    let ip = match addr.ip() {
        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
        IpAddr::V6(ip) => ip,
    };
    hash_fingerprint(u128::from(ip) ^ (u128::from(addr.port()) << 48))
}

/// The rendezvous (highest-random-weight) weight of peer `addr` for the node
/// whose id hashes to `local_key`: each node ranks its peers differently, so
/// join sweeps and gateway roles spread over the cluster.
pub(super) fn rendezvous_weight(local_key: u64, addr: SocketAddr) -> u64 {
    mix64(local_key ^ addr_hash(addr).rotate_left(21))
}

#[cfg(test)]
mod tests;
