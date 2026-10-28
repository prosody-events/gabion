//! The runtime's peer table: one dense row per discovered peer, sized once to
//! `gossip.peer_capacity` and indexed by address. The columns every round
//! scans (role, flags, the own stream's `sent`, the earliest timer) are hot;
//! the stream state read only when a packet goes to or comes from one peer is
//! cold, in [`PeerLink`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;

use crate::crdt::{
    CellIndex, Incarnation, NodeIdentity, hash_fingerprint, mix64, pow2_index_capacity_for,
};
use crate::defaults::GOSSIP_GAP_REPORTS;

use super::admin::PeerEntry;

/// What [`PeerTable::insert`] did with an address.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Insert {
    Added,
    Present,
    Full,
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
    /// A peer this node sends its own stream to directly.
    Zone,
    /// A peer that speaks another wire version: it is sent nothing until a
    /// packet of this version arrives from it.
    Incompatible,
}

/// The row's owed work, one bit each.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct PeerFlags(u8);

impl PeerFlags {
    /// The peer sent stream data since this node last sent it a packet; a
    /// heartbeat sends a standalone ack.
    pub(super) const ACK_DUE: Self = Self(0x01);
    /// The peer's own stream has a gap open, whose report is prompt work.
    pub(super) const GAP_OPEN: Self = Self(0x08);
    /// The bits that make a row prompt work.
    pub(super) const PROMPT: Self = Self(0x02 | 0x04 | 0x08);
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

    /// Credit an ack. One above the head, or not above `acked`, changes
    /// nothing; progress that leaves data in flight restarts the timer.
    pub(super) fn note_ack(&mut self, ack: u64, head: u64, now: u64) {
        if ack > head || ack <= self.state.acked {
            return;
        }
        self.state.acked = ack;
        *self.sent = (*self.sent).max(ack);
        if ack < *self.sent {
            self.state.timer_ms = now;
        }
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

    /// Data has been in flight for a whole timeout without an ack.
    pub(super) fn timed_out(&self, now: u64, rto_ms: u64) -> bool {
        self.state.acked < *self.sent && now.saturating_sub(self.state.timer_ms) >= rto_ms
    }

    /// Resend everything above `acked`.
    pub(super) fn rewind(&mut self, now: u64) {
        *self.sent = self.state.acked;
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
    /// Reported [`GOSSIP_GAP_REPORTS`] times without progress; reports stop.
    Abandoned { stash: (u64, u64) },
}

/// What [`Inbound::take_report`] found due.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GapDue {
    Nothing,
    /// Send a gap report now.
    Report,
    /// The gap is abandoned; `missing` stream positions never arrived.
    Abandoned {
        missing: u64,
    },
}

/// One stream's receiver state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Inbound {
    /// The highest position held contiguously from 0.
    pub(super) have: u64,
    pub(super) gap: Gap,
}

impl Inbound {
    /// A packet closing `(lower, upper]` arrived. `have` moves only over
    /// contiguous ranges; a range above it is stashed and opens a gap.
    pub(super) fn receive(&mut self, lower: u64, upper: u64, now: u64, floor_ms: u64) {
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
                Gap::Abandoned { stash } => Gap::Abandoned {
                    stash: keep(stash, (lower, upper)),
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

/// A row's cold state.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PeerLink {
    /// The peer's rendezvous weight for this node; join sweeps ask the
    /// highest.
    weight: u64,
    /// From the peer's latest current packet; `None` until its first.
    pub(super) identity: Option<NodeIdentity>,
    /// The incarnation before the last restart; a late packet carrying it
    /// changes no stream state.
    prev_incarnation: Option<Incarnation>,
    own: Outbound,
    /// The peer's own stream as this node receives it.
    pub(super) own_in: Inbound,
}

// 128 bytes: `Option<NodeIdentity>` takes 48 for `u128` alignment, and the
// gap 40. E-Z10 states the per-row cost from it.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<PeerLink>() == 128);

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

/// Rows `0..len` hold the peers in no particular order: a removed row is
/// filled by the last. Every column is sized at construction and never grows.
pub(super) struct PeerTable {
    len: usize,
    /// The retransmission timeout: one heartbeat.
    rto_ms: u64,
    /// This node's id hashed, for the rendezvous weights.
    local_key: u64,
    // Hot: every round's scan.
    role: Box<[Role]>,
    flags: Box<[PeerFlags]>,
    /// The highest own-stream position sent to the peer.
    own_sent: Box<[u64]>,
    deadline: Box<[Deadline]>,
    // Cold: one packet to or from this peer.
    addr: Box<[SocketAddr]>,
    link: Box<[PeerLink]>,
    /// Address hash to row; `addr[row]` confirms a match.
    by_addr: CellIndex,
}

impl PeerTable {
    pub(super) fn with_capacity(capacity: NonZeroU16, local_key: u64, rto_ms: u64) -> Self {
        let rows = usize::from(capacity.get());
        Self {
            len: 0,
            rto_ms,
            local_key,
            role: vec![Role::Zone; rows].into_boxed_slice(),
            flags: vec![PeerFlags::default(); rows].into_boxed_slice(),
            own_sent: vec![0; rows].into_boxed_slice(),
            deadline: vec![Deadline::NEVER; rows].into_boxed_slice(),
            addr: vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)); rows].into_boxed_slice(),
            link: vec![PeerLink::default(); rows].into_boxed_slice(),
            by_addr: CellIndex::with_capacity(pow2_index_capacity_for(capacity.into())),
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
    /// re-announcing a peer is never a decline.
    pub(super) fn insert(&mut self, addr: SocketAddr) -> Insert {
        let hash = addr_hash(addr);
        if self.lookup(hash, addr).is_some() {
            return Insert::Present;
        }
        let row = self.len;
        if row == self.addr.len() {
            return Insert::Full;
        }
        self.role[row] = Role::Zone;
        self.flags[row] = PeerFlags::default();
        self.own_sent[row] = 0;
        self.deadline[row] = Deadline::NEVER;
        self.addr[row] = addr;
        self.link[row] = PeerLink {
            weight: rendezvous_weight(self.local_key, addr),
            ..PeerLink::default()
        };
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
        let (row, last) = (row as usize, self.len);
        let removed = self.link[row];
        if row != last {
            let moved = addr_hash(self.addr[last]);
            self.by_addr.remove(moved, last as u32);
            self.by_addr.insert_unchecked(moved, row as u32);
            self.role[row] = self.role[last];
            self.flags[row] = self.flags[last];
            self.own_sent[row] = self.own_sent[last];
            self.deadline[row] = self.deadline[last];
            self.addr[row] = self.addr[last];
            self.link[row] = self.link[last];
        }
        Some(removed)
    }

    /// Note the sender identity of a packet from `row`. A new incarnation
    /// resets the whole row: both directions of the stream start over.
    pub(super) fn contact(&mut self, row: usize, sender: NodeIdentity) -> Contact {
        let link = &mut self.link[row];
        let contact = match link.identity {
            None => Contact::First,
            Some(known) if known == sender => return Contact::Current,
            Some(known)
                if known.node_id == sender.node_id
                    && link.prev_incarnation == Some(sender.incarnation) =>
            {
                return Contact::Late;
            }
            Some(known) => {
                link.prev_incarnation = Some(known.incarnation);
                link.own = Outbound::default();
                link.own_in = Inbound::default();
                self.own_sent[row] = 0;
                self.flags[row] = PeerFlags::default();
                Contact::Restarted
            }
        };
        self.link[row].identity = Some(sender);
        self.refresh_deadline(row);
        contact
    }

    /// The peer speaks another wire version: send it nothing.
    pub(super) fn mute(&mut self, row: usize) {
        self.role[row] = Role::Incompatible;
        self.flags[row] = PeerFlags::default();
    }

    /// A packet of this version arrived from `row`. A muted row becomes a
    /// recipient again, its stream rewound to 0 as prompt work; returns
    /// whether it was muted.
    pub(super) fn restore(&mut self, row: usize) -> bool {
        if self.role[row] != Role::Incompatible {
            return false;
        }
        self.role[row] = Role::Zone;
        self.own_tx(row).reset();
        self.flags[row].set(PeerFlags::RESEND_OWN, true);
        self.refresh_deadline(row);
        true
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

    pub(super) fn own_sent(&self, row: usize) -> u64 {
        self.own_sent[row]
    }

    pub(super) fn own_tx(&mut self, row: usize) -> StreamTx<'_> {
        StreamTx {
            sent: &mut self.own_sent[row],
            state: &mut self.link[row].own,
        }
    }

    pub(super) fn link(&self, row: usize) -> &PeerLink {
        &self.link[row]
    }

    pub(super) fn own_in(&mut self, row: usize) -> &mut Inbound {
        &mut self.link[row].own_in
    }

    /// Whether any of `row`'s timers has fallen due at `now`.
    pub(super) fn due(&self, row: usize, now: u64) -> bool {
        self.deadline[row].0 <= now
    }

    /// Recompute `row`'s deadline after its timers changed: the own stream's
    /// timeout while data is in flight, and the open gap's next report.
    pub(super) fn refresh_deadline(&mut self, row: usize) {
        let link = &self.link[row];
        let timeout = (link.own.acked < self.own_sent[row])
            .then(|| link.own.timer_ms.saturating_add(self.rto_ms));
        let report = link.own_in.report_at();
        self.deadline[row] = Deadline(timeout.into_iter().chain(report).min().unwrap_or(u64::MAX));
        let open = matches!(link.own_in.gap, Gap::Open { .. });
        self.flags[row].set(PeerFlags::GAP_OPEN, open);
    }

    /// The admin view of every row.
    pub(super) fn entries(&self) -> impl Iterator<Item = PeerEntry> + '_ {
        (0..self.len).map(|row| PeerEntry {
            addr: self.addr[row],
            node_id: self.link[row].identity.map(|identity| identity.node_id),
            role: self.role[row],
            own_unacked: self.own_sent[row] - self.link[row].own.acked,
        })
    }

    /// The peers a join sweep may ask, with their weights: every peer heard
    /// from that speaks this version.
    pub(super) fn sweep_candidates(&self) -> impl Iterator<Item = (u64, SocketAddr)> + '_ {
        (0..self.len)
            .filter(|&row| self.role[row] == Role::Zone && self.link[row].identity.is_some())
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
/// join sweeps spread over the cluster.
fn rendezvous_weight(local_key: u64, addr: SocketAddr) -> u64 {
    mix64(local_key ^ addr_hash(addr).rotate_left(21))
}

#[cfg(test)]
mod tests;
