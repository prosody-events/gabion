//! Outbound datagrams the transport could not take yet.
//!
//! Packets are encoded once into a shared buffer and offered to the
//! transport straight away. Only a datagram the transport refuses with
//! `WouldBlock` is copied into a pool slot and parked until the transport
//! is writable again; with the pool exhausted it is dropped, as a full
//! kernel send buffer would drop it.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use crate::wire::{FrameLimits, PacketBuf};

use super::GossipError;
use super::transport::GossipTransport;

pub(super) struct SendQueue {
    pool: Vec<PacketBuf>,
    free: Vec<u32>,
    pending: VecDeque<(SocketAddr, u32)>,
    max_depth: usize,
    dropped: u64,
}

impl SendQueue {
    pub(super) fn new(slots: usize, limits: FrameLimits) -> Self {
        let slots = slots.max(1);
        Self {
            pool: (0..slots).map(|_| PacketBuf::for_limits(limits)).collect(),
            free: (0..slots as u32).rev().collect(),
            pending: VecDeque::with_capacity(slots),
            max_depth: 0,
            dropped: 0,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn depth(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Offer `packet` to the transport for `dst`.
    pub(super) fn transmit<T: GossipTransport>(
        &mut self,
        transport: &T,
        packet: &PacketBuf,
        dst: SocketAddr,
    ) -> Result<(), GossipError> {
        match transport.try_send_to(packet.as_bytes(), dst) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.park(packet, dst);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn park(&mut self, packet: &PacketBuf, dst: SocketAddr) {
        let Some(slot) = self.free.pop() else {
            self.dropped = self.dropped.saturating_add(1);
            if self.dropped.is_power_of_two() {
                tracing::warn!(
                    dropped_total = self.dropped,
                    capacity = self.pool.len(),
                    config_key = "gossip.send_queue_capacity",
                    "The network is not accepting gossip packets as fast as this node produces \
                     them. Packets are being dropped, so peers will under-count requests handled \
                     here until the next change to the affected keys (requests were still \
                     allowed). This usually means the kernel's UDP send buffer is full. Raise \
                     `gossip.send_queue_capacity` (currently {}) or the socket send buffer.",
                    self.pool.len(),
                );
            }
            return;
        };
        self.pool[slot as usize].copy_from(packet);
        self.pending.push_back((dst, slot));
        self.max_depth = self.max_depth.max(self.pending.len());
    }

    /// Retry the oldest parked datagram.
    pub(super) fn drain_one<T: GossipTransport>(
        &mut self,
        transport: &T,
    ) -> Result<(), GossipError> {
        let Some((dst, slot)) = self.pending.pop_front() else {
            return Ok(());
        };
        match transport.try_send_to(self.pool[slot as usize].as_bytes(), dst) {
            Ok(_) => self.free.push(slot),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.pending.push_front((dst, slot));
            }
            Err(e) => {
                self.free.push(slot);
                return Err(e.into());
            }
        }
        Ok(())
    }
}
