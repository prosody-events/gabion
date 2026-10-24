//! Per-peer protocol state: two outbound streams (own cells and relayed
//! cells), the cumulative acks held for the peer's streams, an RTT estimate,
//! and the gateway flags.
//!
//! A *stream* is a sequence space the sender stamps its cells with. A peer
//! acknowledges a stream cumulatively: "I hold everything through `n`".
//! [`Outbound`] is the sender's view of one stream towards one peer.

use std::net::SocketAddr;

use crate::crdt::{Incarnation, NodeId};

use super::topology::{Zone, rendezvous_weight};

/// The sender's state for one stream towards one peer.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Outbound {
    /// Highest sequence the peer has acknowledged.
    pub(super) acked: u64,
    /// Highest sequence sent to the peer; `sent > acked` means data in flight.
    pub(super) sent: u64,
    /// Highest sequence ever sent, kept across rewinds: a send at or below it
    /// is a retransmission.
    high: u64,
    /// When the oldest unacknowledged send went out; restarted on ack
    /// progress.
    timer_ms: u64,
    /// `(sequence, send time)` of one in-flight send whose ack yields an RTT
    /// sample. Dropped on retransmission and armed only by a send of new data
    /// (Karn's rule), so a sample never spans a resend.
    probe: Option<(u64, u64)>,
}

impl Outbound {
    /// Record that the stream was sent through `upper` at `now_ms`.
    pub(super) fn note_sent(&mut self, upper: u64, now_ms: u64) {
        if self.sent == self.acked {
            self.timer_ms = now_ms;
        }
        if self.sent >= self.high {
            self.probe.get_or_insert((upper, now_ms));
        }
        self.sent = self.sent.max(upper);
        self.high = self.high.max(upper);
    }

    /// Apply a cumulative ack. Returns an RTT sample, in milliseconds, when
    /// the ack covers the probe.
    pub(super) fn note_ack(&mut self, ack: u64, now_ms: u64) -> Option<u64> {
        if ack <= self.acked {
            return None;
        }
        self.acked = ack;
        self.sent = self.sent.max(ack);
        if self.acked < self.sent {
            self.timer_ms = now_ms;
        }
        match self.probe {
            Some((seq, sent_ms)) if ack >= seq => {
                self.probe = None;
                Some(now_ms.saturating_sub(sent_ms))
            }
            _ => None,
        }
    }

    /// Whether data has been in flight for at least `rto_ms` without an ack.
    pub(super) fn timed_out(&self, now_ms: u64, rto_ms: u64) -> bool {
        self.sent > self.acked && now_ms.saturating_sub(self.timer_ms) >= rto_ms
    }

    /// Resume sending from the last ack, so the next round resends what the
    /// peer has not confirmed.
    pub(super) fn rewind(&mut self, now_ms: u64) {
        self.sent = self.acked;
        self.timer_ms = now_ms;
        self.probe = None;
    }
}

pub(super) struct Peer {
    pub(super) addr: SocketAddr,
    pub(super) zone: Zone,
    /// Rendezvous weight of this peer as a gateway for the local node.
    weight: u64,
    /// Identity from the peer's first packet; a change means the process
    /// behind this address restarted and its stream state is void.
    pub(super) identity: Option<(NodeId, Incarnation)>,
    /// Our own-origin cells towards this peer.
    pub(super) own: Outbound,
    /// The cells we relay into our zone, towards this peer.
    pub(super) relay: Outbound,
    /// Highest sequence of the peer's own stream we hold contiguously.
    pub(super) have_own: u64,
    /// Highest sequence of the peer's relay stream we hold contiguously.
    pub(super) have_relay: u64,
    /// Data arrived from the peer since we last told it what we hold.
    pub(super) ack_due: bool,
    /// Smoothed round-trip time in milliseconds; `0` until the first sample.
    srtt_ms: u64,
    /// The peer missed an ack deadline while serving as our gateway, and we
    /// have not heard from it since.
    pub(super) suspect: bool,
    /// The peer is our current gateway into its (remote) zone.
    pub(super) gateway: bool,
}

impl Peer {
    pub(super) fn new(addr: SocketAddr, local: NodeId, zone: Zone) -> Self {
        Self {
            addr,
            zone,
            weight: rendezvous_weight(local, addr),
            identity: None,
            own: Outbound::default(),
            relay: Outbound::default(),
            have_own: 0,
            have_relay: 0,
            ack_due: false,
            srtt_ms: 0,
            suspect: false,
            gateway: false,
        }
    }

    /// Forget everything learned about the process behind this address.
    pub(super) fn reset_streams(&mut self) {
        self.own = Outbound::default();
        self.relay = Outbound::default();
        self.have_own = 0;
        self.have_relay = 0;
        self.ack_due = false;
        self.srtt_ms = 0;
        self.suspect = false;
    }

    pub(super) fn note_rtt(&mut self, sample_ms: u64) {
        self.srtt_ms = if self.srtt_ms == 0 {
            sample_ms.max(1)
        } else {
            (self.srtt_ms * 7 + sample_ms) / 8
        };
    }

    /// `max(2 * RTT_est, floor)`.
    pub(super) fn rto_ms(&self, floor_ms: u64) -> u64 {
        (2 * self.srtt_ms).max(floor_ms)
    }

    /// Gateway ranking within a zone: peers not suspected of being dead come
    /// first, then by descending rendezvous weight, then by address. When
    /// every member is suspect the order degrades to plain rendezvous, so a
    /// zone is never left without a gateway.
    fn rank(&self) -> (bool, u64, SocketAddr) {
        (!self.suspect, self.weight, self.addr)
    }
}

/// Mark each remote-zone peer that ranks first in its zone as a gateway.
pub(super) fn refresh_gateways(peers: &mut [Peer], local_zone: Zone) {
    for i in 0..peers.len() {
        let p = &peers[i];
        let best = p.zone != local_zone
            && !peers
                .iter()
                .any(|q| q.zone == p.zone && q.rank() > p.rank());
        peers[i].gateway = best;
    }
}

#[cfg(test)]
mod tests;
