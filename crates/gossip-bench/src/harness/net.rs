//! Datagram network model and whole-network byte accounting.
//!
//! Every datagram is accounted at send time, for the whole network, as its
//! UDP payload plus [`IP_UDP_HEADER_BYTES`]. Its fate is then decided in a
//! fixed order: partition, unknown destination, i.i.d. loss, Gilbert-Elliott
//! burst loss. Survivors arrive after a one-way latency of `base + U(0,
//! jitter_max)` (intra- or cross-availability-zone). On arrival a datagram
//! is dropped if the destination process has stopped or its receive buffer
//! would overflow; there is no backpressure toward the sender, exactly as
//! with a UDP socket.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::task::{Context, Poll, Waker};

use serde::{Deserialize, Serialize};

use super::plan::{NS_PER_S, Plan, node_addr};
use super::regime::{NetworkConfig, Partition};
use super::rng::{Rng, Stream};

/// IPv4 (20 B) plus UDP (8 B) header, charged on every datagram.
pub const IP_UDP_HEADER_BYTES: u64 = 28;
/// Largest UDP payload that fits a 1,500 B Ethernet MTU unfragmented.
pub const MAX_UDP_PAYLOAD_BYTES: usize = 1_472;

/// Datagram tallies. `bytes_*` include the IP/UDP header; `payload_bytes_*`
/// do not.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NetCounters {
    pub packets_sent: u64,
    pub bytes_sent: u64,
    pub payload_bytes_sent: u64,
    pub cross_az_packets: u64,
    pub cross_az_bytes: u64,
    pub packets_delivered: u64,
    pub bytes_delivered: u64,
    pub dropped_loss_packets: u64,
    pub dropped_loss_bytes: u64,
    pub dropped_partition_packets: u64,
    pub dropped_partition_bytes: u64,
    pub dropped_rcvbuf_packets: u64,
    pub dropped_rcvbuf_bytes: u64,
    pub dropped_no_receiver_packets: u64,
    pub dropped_no_receiver_bytes: u64,
    pub oversize_packets: u64,
}

#[derive(Clone, Copy, Debug)]
enum Drop {
    Loss,
    Partition,
    Rcvbuf,
    NoReceiver,
}

impl NetCounters {
    fn sent(&mut self, wire: u64, payload: u64, cross_az: bool, oversize: bool) {
        self.packets_sent += 1;
        self.bytes_sent += wire;
        self.payload_bytes_sent += payload;
        if cross_az {
            self.cross_az_packets += 1;
            self.cross_az_bytes += wire;
        }
        if oversize {
            self.oversize_packets += 1;
        }
    }

    fn dropped(&mut self, cause: Drop, wire: u64) {
        let (packets, bytes) = match cause {
            Drop::Loss => (&mut self.dropped_loss_packets, &mut self.dropped_loss_bytes),
            Drop::Partition => (
                &mut self.dropped_partition_packets,
                &mut self.dropped_partition_bytes,
            ),
            Drop::Rcvbuf => (
                &mut self.dropped_rcvbuf_packets,
                &mut self.dropped_rcvbuf_bytes,
            ),
            Drop::NoReceiver => (
                &mut self.dropped_no_receiver_packets,
                &mut self.dropped_no_receiver_bytes,
            ),
        };
        *packets += 1;
        *bytes += wire;
    }

    fn delivered(&mut self, wire: u64) {
        self.packets_delivered += 1;
        self.bytes_delivered += wire;
    }
}

#[derive(Debug)]
pub struct Datagram {
    pub src: SocketAddr,
    pub bytes: Box<[u8]>,
}

impl Datagram {
    fn wire_bytes(&self) -> u64 {
        self.bytes.len() as u64 + IP_UDP_HEADER_BYTES
    }
}

#[derive(Debug, Default)]
struct Inbox {
    queue: VecDeque<Datagram>,
    payload_bytes: u64,
    waker: Option<Waker>,
}

/// Where a sent datagram goes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Route {
    Deliver { dst: usize, latency_ns: u64 },
    Dropped,
}

pub struct Network {
    config: NetworkConfig,
    rng: Rng,
    by_addr: BTreeMap<SocketAddr, usize>,
    az: Vec<u8>,
    inboxes: Vec<Inbox>,
    running: Vec<bool>,
    /// Gilbert-Elliott state per directed link, `src * n + dst`; `true` is
    /// the bad state. Empty when burst loss is off.
    link_bad: Vec<bool>,
    partitions: Vec<Partition>,
    measure_from_ns: u64,
    pub run: NetCounters,
    pub window: NetCounters,
}

impl Network {
    pub fn new(
        config: NetworkConfig,
        partitions: Vec<Partition>,
        plan: &Plan,
        seed: u64,
        measure_from_ns: u64,
    ) -> Self {
        let n = plan.nodes.len();
        Self {
            config,
            rng: Rng::stream(seed, Stream::Network, 0),
            by_addr: (0..n).map(|i| (node_addr(i), i)).collect(),
            az: plan.nodes.iter().map(|p| p.az).collect(),
            inboxes: (0..n).map(|_| Inbox::default()).collect(),
            running: vec![false; n],
            link_bad: if config.burst_loss.is_some() {
                vec![false; n * n]
            } else {
                Vec::new()
            },
            partitions,
            measure_from_ns,
            run: NetCounters::default(),
            window: NetCounters::default(),
        }
    }

    fn tally(&mut self, now_ns: u64, f: impl Fn(&mut NetCounters)) {
        f(&mut self.run);
        if now_ns >= self.measure_from_ns {
            f(&mut self.window);
        }
    }

    fn partitioned(&self, now_ns: u64, a: u8, b: u8) -> bool {
        self.partitions.iter().any(|p| {
            now_ns >= p.at_s * NS_PER_S
                && now_ns < p.heal_s * NS_PER_S
                && (a == p.az) != (b == p.az)
        })
    }

    /// Account one datagram of `len` payload bytes from node `src` and decide
    /// its route.
    pub fn send(&mut self, now_ns: u64, src: usize, len: usize, dst: SocketAddr) -> Route {
        let wire = len as u64 + IP_UDP_HEADER_BYTES;
        let dst = self.by_addr.get(&dst).copied();
        let cross_az = dst.is_some_and(|d| self.az[d] != self.az[src]);
        let oversize = len > MAX_UDP_PAYLOAD_BYTES;
        self.tally(now_ns, |c| c.sent(wire, len as u64, cross_az, oversize));

        let Some(dst) = dst else {
            self.tally(now_ns, |c| c.dropped(Drop::NoReceiver, wire));
            return Route::Dropped;
        };
        if self.partitioned(now_ns, self.az[src], self.az[dst]) {
            self.tally(now_ns, |c| c.dropped(Drop::Partition, wire));
            return Route::Dropped;
        }
        if self.rng.chance(self.config.loss) || self.burst_lost(src, dst) {
            self.tally(now_ns, |c| c.dropped(Drop::Loss, wire));
            return Route::Dropped;
        }
        let (base, jitter) = if cross_az {
            (
                self.config.cross_az_base_ns,
                self.config.cross_az_jitter_max_ns,
            )
        } else {
            (
                self.config.intra_az_base_ns,
                self.config.intra_az_jitter_max_ns,
            )
        };
        Route::Deliver {
            dst,
            latency_ns: base + self.rng.up_to(jitter),
        }
    }

    fn burst_lost(&mut self, src: usize, dst: usize) -> bool {
        let Some(ge) = self.config.burst_loss else {
            return false;
        };
        let link = src * self.az.len() + dst;
        let bad = self.link_bad[link];
        let bad = if bad {
            !self.rng.chance(ge.p_bad_to_good)
        } else {
            self.rng.chance(ge.p_good_to_bad)
        };
        self.link_bad[link] = bad;
        bad && self.rng.chance(ge.loss_in_bad)
    }

    /// A datagram reaches node `dst`. Returns the waker to fire when it was
    /// queued for a waiting receiver.
    pub fn arrive(&mut self, now_ns: u64, dst: usize, datagram: Datagram) -> Option<Waker> {
        let wire = datagram.wire_bytes();
        if !self.running[dst] {
            self.tally(now_ns, |c| c.dropped(Drop::NoReceiver, wire));
            return None;
        }
        let inbox = &mut self.inboxes[dst];
        let len = datagram.bytes.len() as u64;
        if inbox.payload_bytes + len > self.config.rcvbuf_bytes {
            self.tally(now_ns, |c| c.dropped(Drop::Rcvbuf, wire));
            return None;
        }
        inbox.payload_bytes += len;
        inbox.queue.push_back(datagram);
        let waker = inbox.waker.take();
        self.tally(now_ns, |c| c.delivered(wire));
        waker
    }

    /// Hand the oldest queued datagram to node `idx`'s receive call.
    pub fn poll_recv(
        &mut self,
        idx: usize,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<(usize, SocketAddr)> {
        let inbox = &mut self.inboxes[idx];
        match inbox.queue.pop_front() {
            Some(datagram) => {
                inbox.payload_bytes -= datagram.bytes.len() as u64;
                let n = datagram.bytes.len().min(buf.len());
                buf[..n].copy_from_slice(&datagram.bytes[..n]);
                Poll::Ready((n, datagram.src))
            }
            None => {
                inbox.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    pub fn set_running(&mut self, now_ns: u64, idx: usize, running: bool) {
        self.running[idx] = running;
        if !running {
            let inbox = std::mem::take(&mut self.inboxes[idx]);
            for datagram in inbox.queue {
                let wire = datagram.wire_bytes();
                self.tally(now_ns, |c| c.dropped(Drop::NoReceiver, wire));
            }
        }
    }
}

#[cfg(test)]
mod tests;
