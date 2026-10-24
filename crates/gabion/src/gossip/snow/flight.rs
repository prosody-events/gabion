//! Bounded tables of tree broadcasts in flight.
//!
//! [`Origins`] holds the broadcasts this node is the root of: the encoded
//! packet (so a rebroadcast resends exactly what the first send carried),
//! which children have acknowledged, and the timeout. [`Relays`] holds the
//! broadcasts this node forwards for someone else: just enough to
//! acknowledge the parent once the children have, and to recognise a
//! rebroadcast. Both are sized at construction; when full, the oldest entry
//! is overwritten, which at worst costs one redundant rebroadcast.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::wire::{FrameLimits, PacketBuf};

use super::ring::Children;

/// Most children one node can have (`k` is clamped to this).
pub(in crate::gossip) const MAX_CHILDREN: usize = u8::BITS as usize;

/// A broadcast this node roots.
pub(in crate::gossip) struct Origin {
    pub seq: u32,
    /// The cell frame without a control extension.
    pub packet: PacketBuf,
    pub children: Children,
    /// Bit `i` set once `children[i]` has acknowledged.
    pub acked: u8,
    pub started_ms: u64,
    pub deadline_ms: u64,
    pub rebroadcasts_left: u8,
    live: bool,
}

impl Origin {
    pub fn is_live(&self) -> bool {
        self.live
    }

    pub fn all_acked(&self) -> bool {
        self.acked == full_mask(self.children.len())
    }
}

pub(in crate::gossip) struct Origins {
    slots: Box<[Origin]>,
    next_seq: u32,
    live: usize,
}

impl Origins {
    /// `first_seq` seeds the sequence numbers so a restarted node does not
    /// reuse the numbers peers still remember.
    pub fn new(capacity: usize, limits: FrameLimits, first_seq: u32) -> Self {
        let slots = (0..capacity.max(1))
            .map(|_| Origin {
                seq: 0,
                packet: PacketBuf::for_limits(limits),
                children: Children::new(),
                acked: 0,
                started_ms: 0,
                deadline_ms: 0,
                rebroadcasts_left: 0,
                live: false,
            })
            .collect();
        Self {
            slots,
            next_seq: first_seq,
            live: 0,
        }
    }

    /// Number of broadcasts waiting for acknowledgements.
    pub fn live(&self) -> usize {
        self.live
    }

    /// Take a free slot, or the oldest live one, and give it a fresh
    /// sequence number. Returns the slot and whether a live broadcast was
    /// displaced.
    pub fn claim(&mut self, started_ms: u64) -> (usize, bool) {
        let free = self.slots.iter().position(|o| !o.live);
        let (idx, displaced) = match free {
            Some(idx) => (idx, false),
            None => {
                let oldest = (0..self.slots.len())
                    .min_by_key(|&i| self.slots[i].started_ms)
                    .unwrap_or(0);
                (oldest, true)
            }
        };
        let slot = &mut self.slots[idx];
        if !slot.live {
            self.live += 1;
        }
        self.next_seq = self.next_seq.wrapping_add(1);
        slot.seq = self.next_seq;
        slot.live = true;
        slot.acked = 0;
        slot.started_ms = started_ms;
        slot.children.clear();
        (idx, displaced)
    }

    pub fn release(&mut self, idx: usize) {
        if std::mem::take(&mut self.slots[idx].live) {
            self.live -= 1;
        }
    }

    pub fn find(&self, seq: u32) -> Option<usize> {
        self.slots.iter().position(|o| o.live && o.seq == seq)
    }

    pub fn get(&self, idx: usize) -> &Origin {
        &self.slots[idx]
    }

    pub fn get_mut(&mut self, idx: usize) -> &mut Origin {
        &mut self.slots[idx]
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }
}

/// A broadcast this node forwards.
#[derive(Clone, Debug)]
pub(in crate::gossip) struct Relay {
    pub root: SocketAddr,
    pub seq: u32,
    pub parent: SocketAddr,
    /// The children the first copy was forwarded to, with the ranges handed
    /// to them. Frozen: a view change while the broadcast is in flight does
    /// not change who must acknowledge.
    pub children: Children,
    /// Bit `i` set once `children[i]` acked.
    pub acked: u8,
    /// The parent has been acknowledged.
    pub done: bool,
    /// When the children were first sent the data.
    pub sent_ms: u64,
    /// The data went to the children a second time, so their acks no longer
    /// time one send.
    pub resent: bool,
}

pub(in crate::gossip) struct Relays {
    slots: Box<[Option<Relay>]>,
    /// `(root, seq)` to slot. Lookups only: nothing iterates it. Sized at
    /// twice the slot count so steady-state insert/remove never regrows it.
    index: HashMap<(SocketAddr, u32), u32>,
    next: usize,
}

impl Relays {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            slots: vec![None; capacity].into_boxed_slice(),
            index: HashMap::with_capacity(2 * capacity),
            next: 0,
        }
    }

    pub fn find(&self, root: SocketAddr, seq: u32) -> Option<usize> {
        self.index.get(&(root, seq)).map(|&i| i as usize)
    }

    pub fn get_mut(&mut self, idx: usize) -> Option<&mut Relay> {
        self.slots[idx].as_mut()
    }

    /// Record a new relay, overwriting the oldest one when full.
    pub fn insert(&mut self, relay: Relay) -> usize {
        let idx = self.next;
        self.next = (self.next + 1) % self.slots.len();
        if let Some(old) = &self.slots[idx] {
            self.index.remove(&(old.root, old.seq));
        }
        self.index.insert((relay.root, relay.seq), idx as u32);
        self.slots[idx] = Some(relay);
        idx
    }
}

/// Bit mask with the low `n` bits set.
pub(in crate::gossip) fn full_mask(n: usize) -> u8 {
    debug_assert!(n <= MAX_CHILDREN);
    u8::try_from((1_u16 << n) - 1).unwrap_or(u8::MAX)
}

/// The expected round trip of a whole-tree broadcast: the tree height times
/// the smoothed round trip of one hop, or the smoothed round trip of whole
/// broadcasts when that is longer. Both gain 1/8, as TCP's SRTT.
///
/// A whole-broadcast sample is taken only when a broadcast completes without
/// a rebroadcast, so a late ack is never mistaken for the answer to the
/// resend (Karn and Partridge). With loss nearly every broadcast over a
/// large tree needs its rebroadcast, so whole-broadcast samples stop. The
/// hop estimate keeps the timeout measured regardless: a leaf acknowledges
/// as soon as the data arrives, so a leaf's ack to a parent that sent once
/// is an unambiguous one-hop round trip, and every node sees many of them
/// as a relay. Until the first hop sample the hop time is the seed, and the
/// expected time doubles after each round in which a broadcast timed out
/// (up to [`Self::MAX_BACKOFF`] times), so a seed below the real round trip
/// cannot rebroadcast forever without ever being measured.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::gossip) struct RttEstimator {
    /// Smoothed whole-broadcast RTT in 1/8 ms; `0` until the first sample.
    tree_eighths: u64,
    /// Smoothed one-hop RTT in 1/8 ms; `0` until the first sample.
    hop_eighths: u64,
    /// Doublings applied to the expected time while nothing is measured.
    backoff: u8,
}

impl RttEstimator {
    /// Most doublings after consecutive timeouts.
    pub const MAX_BACKOFF: u8 = 3;

    /// A broadcast this node rooted completed without a rebroadcast.
    pub fn sample_tree(&mut self, rtt_ms: u64) {
        smooth(&mut self.tree_eighths, rtt_ms);
        self.backoff = 0;
    }

    /// A leaf acknowledged data this node sent it once.
    pub fn sample_hop(&mut self, rtt_ms: u64) {
        smooth(&mut self.hop_eighths, rtt_ms);
        self.backoff = 0;
    }

    /// A broadcast timed out: while the round trip is unmeasured, wait
    /// longer before the next one does.
    pub fn timed_out(&mut self) {
        if self.tree_eighths == 0 && self.hop_eighths == 0 {
            self.backoff = (self.backoff + 1).min(Self::MAX_BACKOFF);
        }
    }

    /// The expected whole-tree round trip in ms for a tree of `height`,
    /// with `seed_hop_ms` standing in for the hop time until it is measured.
    pub fn expected_ms(&self, height: u32, seed_hop_ms: u64) -> u64 {
        let hop = match self.hop_eighths {
            0 => seed_hop_ms,
            eighths => eighths.div_ceil(8),
        };
        let tree = self.tree_eighths.div_ceil(8);
        (u64::from(height) * hop).max(tree) << self.backoff
    }
}

fn smooth(eighths: &mut u64, rtt_ms: u64) {
    let sample = rtt_ms.max(1) * 8;
    *eighths = if *eighths == 0 {
        sample
    } else {
        *eighths - *eighths / 8 + sample / 8
    };
}
