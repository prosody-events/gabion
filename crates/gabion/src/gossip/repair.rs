//! Repairing lost broadcasts.
//!
//! An owner numbers every broadcast packet it sends, 1, 2, 3, ..., and
//! remembers which key buckets the most recent ones carried ([`History`]).
//! A receiver tracks, per owner, the highest number it has seen and which
//! below it never arrived ([`Heard`]). Every sync it asks each owner for the
//! ones it missed (a NACK), and the owner re-sends those buckets' current
//! totals. A receiver that has not heard from an owner since it learned of
//! it, or fell further behind than it can track, asks for a snapshot of
//! every key the owner owns instead.

use std::num::NonZeroU64;

use crate::crdt::{BucketEpoch, Count, KeyHash, KeyTotal, RuleSlot};
use crate::wire::Nack;

/// Most packets one snapshot may span: a receiver tracks a snapshot's parts
/// in a `u128`.
pub(super) const SNAPSHOT_MAX_PARTS: usize = 128;

/// One key bucket a broadcast carried.
type Bucket = (RuleSlot, KeyHash, BucketEpoch);

#[derive(Clone, Copy, Default)]
struct Packet {
    seq: u64,
    /// Position of its first bucket in the stream of recorded buckets.
    start: u64,
    len: u32,
}

/// The key buckets of an owner's most recent broadcast packets, so it can
/// re-send what a receiver missed. Bounded twice: by packets (a ring indexed
/// by sequence number) and by buckets (a ring the packets' rows share). A
/// packet whose buckets have been overwritten is forgotten.
pub(super) struct History {
    packets: Box<[Packet]>,
    buckets: Box<[Bucket]>,
    /// Buckets ever recorded.
    written: u64,
}

impl History {
    pub(super) fn new(packets: usize, buckets: usize) -> Self {
        Self {
            packets: vec![Packet::default(); packets.max(1)].into_boxed_slice(),
            buckets: vec![(0, KeyHash(0), 0); buckets.max(1)].into_boxed_slice(),
            written: 0,
        }
    }

    /// Remember that broadcast packet `seq` carried `totals`.
    pub(super) fn record<C: Count>(&mut self, seq: u64, totals: &[KeyTotal<C>]) {
        let capacity = self.buckets.len() as u64;
        let slot = (seq % self.packets.len() as u64) as usize;
        if totals.len() as u64 > capacity {
            // Too large to keep; a NACK for it gets a snapshot instead.
            self.packets[slot] = Packet::default();
            return;
        }
        self.packets[slot] = Packet {
            seq,
            start: self.written,
            len: totals.len() as u32,
        };
        for total in totals {
            self.buckets[(self.written % capacity) as usize] =
                (total.rule, total.key_hash, total.bucket);
            self.written += 1;
        }
    }

    /// The buckets broadcast packet `seq` carried, if still remembered.
    pub(super) fn get(&self, seq: u64) -> Option<impl Iterator<Item = Bucket> + '_> {
        let packet = self.packets[(seq % self.packets.len() as u64) as usize];
        let capacity = self.buckets.len() as u64;
        let kept = seq != 0 && packet.seq == seq && self.written - packet.start <= capacity;
        kept.then(|| {
            (packet.start..packet.start + u64::from(packet.len))
                .map(move |position| self.buckets[(position % capacity) as usize])
        })
    }
}

/// What a receiver needs from an owner beyond its broadcasts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Snapshot {
    /// Nothing: every broadcast up to `latest` arrived, was repaired, or is
    /// tracked in `missing`.
    Current,
    /// A snapshot, because the receiver has not heard from the owner yet or
    /// lost track of what it missed.
    Wanted,
    /// A snapshot whose parts are arriving: `through` is the last broadcast
    /// it covers, `parts` has bit `i` set once part `i` arrived, and `last`
    /// is the index of the final part once it arrived.
    Receiving {
        through: u64,
        parts: u128,
        last: Option<u32>,
    },
}

/// What a receiver has heard of one owner's broadcasts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Heard {
    /// Highest broadcast sequence number seen or covered by a snapshot.
    latest: u64,
    /// Bit `i` set: broadcast `latest - 1 - i` has not arrived.
    missing: u64,
    snapshot: Snapshot,
}

impl Heard {
    /// A newly discovered owner: everything it broadcast so far is unknown.
    pub(super) fn new() -> Self {
        Self {
            latest: 0,
            missing: 0,
            snapshot: Snapshot::Wanted,
        }
    }

    /// Broadcast `seq` arrived.
    pub(super) fn broadcast(&mut self, seq: u64) {
        if seq <= self.latest {
            self.repaired(seq);
            return;
        }
        let shift = seq - self.latest;
        // Broadcasts `latest + 1 .. seq` are missing; the bits of those
        // already missing move up by `shift`. Any that fall off the top can
        // no longer be asked for.
        let gap = shift - 1;
        let fresh = if gap >= 64 { u64::MAX } else { (1 << gap) - 1 };
        let (kept, lost) = if shift >= 64 {
            (0, self.missing != 0 || gap > 64)
        } else {
            (self.missing << shift, self.missing >> (64 - shift) != 0)
        };
        self.missing = kept | fresh;
        self.latest = seq;
        if lost && self.snapshot == Snapshot::Current {
            self.snapshot = Snapshot::Wanted;
        }
    }

    /// A repair of broadcast `seq` arrived.
    pub(super) fn repaired(&mut self, seq: u64) {
        if seq < self.latest {
            let bit = self.latest - 1 - seq;
            if bit < 64 {
                self.missing &= !(1 << bit);
            }
        }
    }

    /// Part `part` of a snapshot covering broadcasts up to `through`
    /// arrived; `last` says it is the final part.
    pub(super) fn snapshot_part(&mut self, through: u64, part: u64, last: bool) {
        let Ok(part) = u32::try_from(part) else {
            return;
        };
        if part as usize >= SNAPSHOT_MAX_PARTS {
            return;
        }
        let (mut parts, mut final_part) = match self.snapshot {
            Snapshot::Receiving {
                through: t,
                parts,
                last,
            } if t == through => (parts, last),
            // A snapshot this node did not ask for, or a part of an older
            // one, still carries valid totals; it just does not count.
            Snapshot::Current => return,
            _ => (0, None),
        };
        parts |= 1 << part;
        if last {
            final_part = Some(part);
        }
        let complete = final_part.is_some_and(|f| parts == u128::MAX >> (127 - f));
        if !complete {
            self.snapshot = Snapshot::Receiving {
                through,
                parts,
                last: final_part,
            };
            return;
        }
        // Everything up to `through` is covered.
        if through >= self.latest {
            self.latest = through;
            self.missing = 0;
        } else {
            let newer = self.latest - 1 - through;
            if newer < 64 {
                self.missing &= (1 << newer) - 1;
            }
        }
        self.snapshot = Snapshot::Current;
    }

    /// What to ask the owner for at the next sync, if anything.
    pub(super) fn request(&self) -> Option<Nack> {
        match self.snapshot {
            Snapshot::Current => NonZeroU64::new(self.missing).map(|missing| Nack::Missed {
                latest: self.latest,
                missing,
            }),
            Snapshot::Wanted | Snapshot::Receiving { .. } => Some(Nack::Snapshot),
        }
    }
}

#[cfg(test)]
mod tests;
