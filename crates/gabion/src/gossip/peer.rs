//! Per-peer reliability state for direct mail.
//!
//! Each peer holds two cumulative sequence numbers, one per direction, plus the
//! timers that drive retransmission. Both are over *origin sequences*, the
//! per-origin counter every cell is stamped with when it changes:
//!
//! - `acked`: the highest of **our** sequences the peer has told us it merged.
//! - `have`: the highest of **the peer's** sequences we have merged
//!   contiguously, which we tell the peer in the ack fields of every packet we
//!   send it.
//!
//! Sequences of superseded cells are never sent, so a sequence number alone
//! cannot show that nothing below it is missing. Instead each packet states the
//! range it covers (`range_start`, `max_origin_sequence`] and carries every
//! live own cell in it; `have` advances to the packet's top only if it already
//! reached the packet's `range_start`.

use std::net::SocketAddr;

use crate::crdt::{Incarnation, NodeId, NodeIdentity};
use crate::wire;

/// One in-flight round-trip measurement: the highest sequence of a first
/// transmission and when it left. Cleared by a retransmission, whose ack would
/// be ambiguous (Karn's algorithm).
#[derive(Clone, Copy)]
struct Probe {
    sequence: u64,
    sent_ms: u64,
}

pub(super) struct Peer {
    pub(super) addr: SocketAddr,
    /// Learned from the peer's packets. A different identity at the same
    /// address means the peer restarted and all sequence state is void.
    pub(super) identity: Option<NodeIdentity>,
    /// Highest own sequence the peer has acknowledged. Never above `sent_upto`.
    acked: u64,
    /// Highest own sequence handed to the send queue for this peer.
    sent_upto: u64,
    /// Highest contiguous sequence of the peer's own cells merged here.
    have: u64,
    /// A data packet arrived since we last sent the peer our `have`.
    ack_owed: bool,
    /// When the oldest unacknowledged data is re-sent; `0` when nothing is
    /// outstanding.
    rto_deadline_ms: u64,
    /// Smoothed ack round trip, `0` before the first sample.
    srtt_ms: u64,
    probe: Option<Probe>,
}

impl Peer {
    pub(super) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            identity: None,
            acked: 0,
            sent_upto: 0,
            have: 0,
            ack_owed: false,
            rto_deadline_ms: 0,
            srtt_ms: 0,
            probe: None,
        }
    }

    pub(super) fn node_id(&self) -> Option<NodeId> {
        self.identity.map(|i| i.node_id)
    }

    pub(super) fn ack_owed(&self) -> bool {
        self.ack_owed
    }

    /// `(ack_sequence, ack_incarnation)` for the header of any packet to this
    /// peer.
    pub(super) fn ack_fields(&self) -> (u64, Incarnation) {
        (self.have, self.identity.map_or(0, |i| i.incarnation))
    }

    /// Retransmission timeout: twice the smoothed round trip, never below the
    /// configured floor.
    fn rto_ms(&self, min_rto_ms: u64) -> u64 {
        self.srtt_ms.saturating_mul(2).max(min_rto_ms)
    }

    /// Fold in the header of a packet from this peer. `merged_all` is false if
    /// the store refused any of the packet's cells, in which case `have` must
    /// not claim them.
    pub(super) fn receive(
        &mut self,
        header: &wire::Header,
        local_incarnation: Incarnation,
        merged_all: bool,
        now_ms: u64,
        min_rto_ms: u64,
    ) {
        let sender = NodeIdentity::new(NodeId(header.sender_node_id), header.sender_incarnation);
        match self.identity {
            Some(known) if known != sender => *self = Self::new(self.addr),
            _ => {}
        }
        self.identity = Some(sender);

        // The ack only counts if it is about this incarnation of us, and can
        // never cover more than we sent.
        let acked = header.ack_sequence.min(self.sent_upto);
        if header.ack_incarnation == local_incarnation && acked > self.acked {
            self.acked = acked;
            if let Some(probe) = self.probe
                && acked >= probe.sequence
            {
                let sample = now_ms.saturating_sub(probe.sent_ms).max(1);
                self.srtt_ms = if self.srtt_ms == 0 {
                    sample
                } else {
                    (self.srtt_ms * 7 + sample) / 8
                };
                self.probe = None;
            }
            self.rto_deadline_ms = if self.acked < self.sent_upto {
                now_ms + self.rto_ms(min_rto_ms)
            } else {
                0
            };
        }

        if header.cell_count > 0 {
            self.ack_owed = true;
            if merged_all && header.range_start <= self.have {
                self.have = self.have.max(header.max_origin_sequence);
            }
        }
    }

    /// Where this peer's next delta starts, and whether it is a
    /// retransmission: everything past the last ack once the timeout has
    /// expired, otherwise only what has not been sent yet.
    pub(super) fn delta_after(&self, now_ms: u64) -> (u64, bool) {
        if self.rto_deadline_ms != 0 && now_ms >= self.rto_deadline_ms {
            (self.acked, true)
        } else {
            (self.sent_upto, false)
        }
    }

    /// Record that data up to `top` was queued for the peer.
    pub(super) fn sent(&mut self, top: u64, retransmit: bool, now_ms: u64, min_rto_ms: u64) {
        self.ack_owed = false;
        self.sent_upto = self.sent_upto.max(top);
        if retransmit {
            self.probe = None;
            self.rto_deadline_ms = now_ms + self.rto_ms(min_rto_ms);
        } else {
            if self.probe.is_none() {
                self.probe = Some(Probe {
                    sequence: top,
                    sent_ms: now_ms,
                });
            }
            if self.rto_deadline_ms == 0 {
                self.rto_deadline_ms = now_ms + self.rto_ms(min_rto_ms);
            }
        }
    }

    /// A retransmission found no live cell above the ack: what was
    /// outstanding has expired, so nothing is owed.
    pub(super) fn nothing_outstanding(&mut self) {
        self.sent_upto = self.acked;
        self.rto_deadline_ms = 0;
        self.probe = None;
    }

    /// A standalone ack carrying `have` was queued.
    pub(super) fn ack_sent(&mut self) {
        self.ack_owed = false;
    }
}

#[cfg(test)]
mod tests;
