//! Pulling missed broadcasts from their root.
//!
//! The tree and its one rebroadcast deliver most changes; a node that still
//! misses a broadcast learns so from the gap in the root's broadcast
//! numbering (see `snow/roots.rs`) and asks the root for the cells it
//! missed. The root answers from its local dirty ring, which lists every one
//! of its own cells under the origin sequence of its latest change, one
//! packet per request; the requester asks for the rest once an answer
//! arrives, and again after a timeout when none does.

use std::net::SocketAddr;

use crate::crdt::Count;
use crate::wire::{self, EXT_PREFIX_LEN, FrameLimits, Packets};

use super::{Ctrl, GossipRuntime};
use crate::gossip::clock::Clock;
use crate::gossip::snow::CTRL_MAX_LEN;
use crate::gossip::store::AggregateStore;
use crate::gossip::transport::GossipTransport;

/// Cells gathered for one answer; more than a packet holds, so a full
/// answer is cut by the frame budget rather than by this.
const ANSWER_CELLS: usize = 64;

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    /// How long a revealed gap waits before it is pulled: long enough for
    /// the root's rebroadcast, timed on the same expected round trip, to
    /// arrive first.
    pub(super) fn pull_grace_ms(&self) -> u64 {
        2 * self.ack_timeout_ms()
    }

    /// Send the pulls that are due, keeping half the send buffers free for
    /// the tree.
    pub(super) fn pull_due(&mut self, now_millis: u64) {
        if !self.roots.maybe_gaps() {
            return;
        }
        let timeout = self.ack_timeout_ms();
        let max_retry = self.config.tick_interval.as_millis() as u64;
        let header = self.sender_header();
        let (key, limits) = (self.config.auth_key.as_ref(), self.config.wire_limits);
        let Self {
            roots, ring, send, ..
        } = self;
        let mut budget = send.free.len() / 2;
        roots.pull_due(
            now_millis,
            timeout,
            max_retry,
            |root| ring.contains(root),
            |root, gap| {
                if budget == 0 {
                    return false;
                }
                let mut ext = [0_u8; CTRL_MAX_LEN];
                let ext = Ctrl::Pull {
                    after: gap.after,
                    before: gap.before,
                }
                .encode(&mut ext);
                let queued = send.enqueue(root, |out| {
                    wire::encode_ext_only::<C>(header, ext, key, limits, out).is_ok()
                });
                budget -= usize::from(queued);
                queued
            },
        );
    }

    /// Answer a pull with this node's own cells whose latest change has an
    /// origin sequence strictly between `after` and `before`: as many as fit
    /// one packet, oldest first, and how far the answer reaches.
    pub(super) fn serve_pull(&mut self, dst: SocketAddr, after: u64, before: u64) {
        if before <= after.saturating_add(1) {
            return;
        }
        self.frame_handles.clear();
        let handles = &mut self.frame_handles;
        let mut cut = false;
        self.store.visit_local_dirty_after(after, |handle, seq| {
            if seq >= before {
                return false;
            }
            if handles.len() == ANSWER_CELLS {
                cut = true;
                return false;
            }
            handles.push(handle);
            true
        });

        let header = self.sender_header();
        let (key, limits) = (self.config.auth_key.as_ref(), self.config.wire_limits);
        let mut through = before - 1;
        if !self.frame_handles.is_empty() {
            let budget = FrameLimits {
                max_payload_bytes: limits
                    .max_payload_bytes
                    .saturating_sub(EXT_PREFIX_LEN + CTRL_MAX_LEN),
                ..limits
            };
            let handles = &self.frame_handles;
            let packets = match key {
                Some(key) => {
                    Packets::<C>::auth(header, &self.store, handles, key, &mut self.scratch, budget)
                }
                None => {
                    Packets::<C>::unauth(header, &self.store, handles, &mut self.scratch, budget)
                }
            };
            let Ok(mut packets) = packets else {
                return;
            };
            if !matches!(packets.next_into(&mut self.pull_buf), Ok(Some(_))) {
                return;
            }
            let consumed = packets.finish();
            if consumed < handles.len() || cut {
                let last = handles[consumed.max(1) - 1];
                match self.store.get(last) {
                    Some(row) => through = row.origin_sequence,
                    None => return,
                }
            }
        }

        let mut ext = [0_u8; CTRL_MAX_LEN];
        let ext = Ctrl::Fill { after, through }.encode(&mut ext);
        let cells = !self.frame_handles.is_empty();
        let frame = self.pull_buf.as_bytes();
        let queued = self.send.enqueue(dst, |out| {
            if cells {
                wire::reframe(frame, header, ext, key, limits, out).is_ok()
            } else {
                wire::encode_ext_only::<C>(header, ext, key, limits, out).is_ok()
            }
        });
        if !queued {
            self.note_send_dropped(dst);
        }
    }

    /// The root answered a pull; ask for the rest at once if there is more.
    pub(super) fn on_fill(&mut self, src: SocketAddr, after: u64, through: u64) {
        let now = self.clock.now_millis();
        if self.roots.filled(src, after, through, now) {
            self.pull_due(now);
        }
    }
}
