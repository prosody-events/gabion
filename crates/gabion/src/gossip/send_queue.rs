//! Outbound datagram queue.
//!
//! A fixed pool of pre-allocated packet buffers. One buffer may be queued to
//! several destinations (a message pushed to every mesh peer, an `IHAVE`
//! sent to every gossip target): it carries a reference count and returns to
//! the free list when its last destination has been sent.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;

use crate::wire::{FrameLimits, PacketBuf};

use super::GossipError;
use super::transport::GossipTransport;

#[cfg(test)]
mod tests;

pub(super) struct SendQueue {
    pool: Box<[PacketBuf]>,
    refs: Box<[u16]>,
    free: Vec<u8>,
    pending: VecDeque<(SocketAddr, u8)>,
    /// Datagrams skipped because every buffer was still queued.
    exhausted: u64,
}

impl SendQueue {
    /// `slots` is capped at 256, the range of a slot index.
    pub(super) fn new(slots: usize, limits: FrameLimits) -> Self {
        let slots = slots.clamp(1, usize::from(u8::MAX) + 1);
        Self {
            pool: (0..slots).map(|_| PacketBuf::for_limits(limits)).collect(),
            refs: vec![0; slots].into_boxed_slice(),
            free: (0..slots).map(|slot| (slots - 1 - slot) as u8).collect(),
            pending: VecDeque::with_capacity(slots),
            exhausted: 0,
        }
    }

    pub(super) fn has_free(&self) -> bool {
        !self.free.is_empty()
    }

    pub(super) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn exhausted(&self) -> u64 {
        self.exhausted
    }

    /// Take a free buffer to fill. Pair every claim with [`Self::settle`].
    /// With none free, counts `what` as skipped and warns on power-of-two
    /// totals.
    pub(super) fn claim(&mut self, what: &'static str) -> Option<u8> {
        let slot = self.free.pop();
        if slot.is_none() {
            self.exhausted = self.exhausted.saturating_add(1);
            if self.exhausted.is_power_of_two() {
                tracing::warn!(
                    skipped = what,
                    send_pool_exhausted_total = self.exhausted,
                    "Every gossip send buffer is still queued, so this node skipped a datagram; \
                     peers learn of the update late, through IHAVE/IWANT or a later message \
                     touching the same cells. Usually a burst of gossip traffic larger than the \
                     send pool, or a socket that cannot keep up. Raise \
                     `gossip.send_queue_capacity` if this repeats.",
                );
            }
        }
        slot
    }

    pub(super) fn buf_mut(&mut self, slot: u8) -> &mut PacketBuf {
        &mut self.pool[usize::from(slot)]
    }

    /// Queue the claimed buffer to one more destination.
    pub(super) fn send_to(&mut self, slot: u8, dst: SocketAddr) {
        self.refs[usize::from(slot)] += 1;
        self.pending.push_back((dst, slot));
    }

    /// Finish queueing a claimed buffer; frees it if nobody was queued.
    pub(super) fn settle(&mut self, slot: u8) {
        if self.refs[usize::from(slot)] == 0 {
            self.free.push(slot);
        }
    }

    /// Send the oldest queued datagram. A full socket (`WouldBlock`) leaves it
    /// at the head for the next writable wake-up.
    pub(super) fn drain_one<T: GossipTransport>(
        &mut self,
        transport: &T,
    ) -> Result<(), GossipError> {
        let Some((dst, slot)) = self.pending.pop_front() else {
            return Ok(());
        };
        match transport.try_send_to(self.pool[usize::from(slot)].as_bytes(), dst) {
            Ok(_) => self.release(slot),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.pending.push_front((dst, slot));
            }
            Err(e) => {
                self.release(slot);
                return Err(e.into());
            }
        }
        Ok(())
    }

    fn release(&mut self, slot: u8) {
        let refs = &mut self.refs[usize::from(slot)];
        *refs -= 1;
        if *refs == 0 {
            self.free.push(slot);
        }
    }
}
