//! Pulling what the tree did not deliver.
//!
//! The tree and its one rebroadcast deliver most changes; a node that still
//! misses a broadcast learns so from the gap in the root's broadcast
//! numbering (see `snow/roots.rs`) or from a peer's version digest, and asks
//! the root for the cells it missed. The root answers from its local dirty
//! ring, which lists every one of its own cells under the origin sequence of
//! its latest change, one packet per request; the requester asks for the
//! rest once an answer arrives, and again after a timeout when none does.
//!
//! A node that starts also sweeps the live window from two peers, page by
//! page, so it holds the cells of roots that left before it could pull from
//! them.

use std::net::SocketAddr;

use arrayvec::ArrayVec;

use crate::crdt::Count;
use crate::wire::{self, EXT_PREFIX_LEN, FrameLimits, Packets};

use super::{Ctrl, GossipRuntime};
use crate::gossip::clock::Clock;
use crate::gossip::snow::digest::{self, Entry};
use crate::gossip::snow::{AntiEntropy, CTRL_MAX_LEN, SWEEP_DONE};
use crate::gossip::store::AggregateStore;
use crate::gossip::transport::GossipTransport;

/// Cells gathered for one answer; more than a packet holds, so a full
/// answer is cut by the frame budget rather than by this.
const ANSWER_CELLS: usize = 64;

/// Peers a starting node sweeps the live window from (the study's
/// `pull-sweep-join`, `k = 2`).
const JOIN_SWEEP_PEERS: usize = 2;

/// Unanswered sweep requests after which a peer is given up.
const SWEEP_ATTEMPTS: u8 = 8;

/// The sweep a starting node makes of its peers' live windows.
pub(super) enum JoinSweep {
    /// No peer to sweep from yet.
    Waiting,
    Running(ArrayVec<Sweep, JOIN_SWEEP_PEERS>),
    Done,
}

impl JoinSweep {
    pub(super) fn is_running(&self) -> bool {
        matches!(self, JoinSweep::Running(_))
    }
}

/// One peer being swept: the store slot its next page starts at.
pub(super) struct Sweep {
    peer: SocketAddr,
    from: u32,
    due_ms: u64,
    attempts: u8,
}

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

        let mut through = before - 1;
        let Some(consumed) = self.encode_answer() else {
            return;
        };
        if consumed > 0 && (consumed < self.frame_handles.len() || cut) {
            match self.store.get(self.frame_handles[consumed - 1]) {
                Some(row) => through = row.origin_sequence,
                None => return,
            }
        }
        self.queue_answer(dst, Ctrl::Fill { after, through }, consumed > 0);
    }

    /// Encode as many of `frame_handles` as fit one answer packet, with room
    /// for its control header, into `pull_buf`. Returns the handles consumed
    /// (0 when there are none), or `None` if encoding failed.
    fn encode_answer(&mut self) -> Option<usize> {
        if self.frame_handles.is_empty() {
            return Some(0);
        }
        let header = self.sender_header();
        let limits = self.config.wire_limits;
        let budget = FrameLimits {
            max_payload_bytes: limits
                .max_payload_bytes
                .saturating_sub(EXT_PREFIX_LEN + CTRL_MAX_LEN),
            ..limits
        };
        let handles = &self.frame_handles;
        let mut packets = match self.config.auth_key.as_ref() {
            Some(key) => {
                Packets::<C>::auth(header, &self.store, handles, key, &mut self.scratch, budget)
            }
            None => Packets::<C>::unauth(header, &self.store, handles, &mut self.scratch, budget),
        }
        .ok()?;
        matches!(packets.next_into(&mut self.pull_buf), Ok(Some(_))).then(|| packets.finish())
    }

    /// Queue an answer: the cells in `pull_buf` when `cells`, under `ctrl`.
    fn queue_answer(&mut self, dst: SocketAddr, ctrl: Ctrl, cells: bool) {
        let header = self.sender_header();
        let (key, limits) = (self.config.auth_key.as_ref(), self.config.wire_limits);
        let mut ext = [0_u8; CTRL_MAX_LEN];
        let ext = ctrl.encode(&mut ext);
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

    // -- joining ------------------------------------------------------------

    /// On a node's first heartbeat with peers, start sweeping the live window
    /// from [`JOIN_SWEEP_PEERS`] random peers.
    pub(super) fn start_join_sweep(&mut self, now_millis: u64) {
        if !matches!(self.join_sweep, JoinSweep::Waiting) || self.ring.len() < 2 {
            return;
        }
        if self.tree.anti_entropy == AntiEntropy::Off {
            self.join_sweep = JoinSweep::Done;
            return;
        }
        let mut sweeps = ArrayVec::new();
        let peers = self.ring.len() - 1;
        let first = self.rng.next_u64() as usize % peers;
        for offset in 0..JOIN_SWEEP_PEERS.min(peers) {
            if let Some(peer) = self.ring.peers().nth((first + offset) % peers) {
                sweeps.push(Sweep {
                    peer,
                    from: 0,
                    due_ms: now_millis,
                    attempts: 0,
                });
            }
        }
        self.join_sweep = JoinSweep::Running(sweeps);
        self.sweep_due(now_millis);
    }

    /// Ask each peer being swept for its next page when due; give a peer up
    /// after [`SWEEP_ATTEMPTS`] unanswered requests.
    pub(super) fn sweep_due(&mut self, now_millis: u64) {
        let timeout = self.ack_timeout_ms();
        let max_retry = self.config.tick_interval.as_millis() as u64;
        let JoinSweep::Running(sweeps) = &mut self.join_sweep else {
            return;
        };
        sweeps.retain(|s| s.attempts < SWEEP_ATTEMPTS);
        let mut due = ArrayVec::<(SocketAddr, u32), JOIN_SWEEP_PEERS>::new();
        for sweep in sweeps.iter_mut().filter(|s| s.due_ms <= now_millis) {
            due.push((sweep.peer, sweep.from));
            sweep.due_ms =
                now_millis + (timeout << sweep.attempts.min(16)).min(max_retry.max(timeout));
            sweep.attempts += 1;
        }
        if sweeps.is_empty() {
            self.join_sweep = JoinSweep::Done;
        }
        for (peer, from) in due {
            self.queue_answer(peer, Ctrl::Sweep { from }, false);
        }
    }

    /// Answer a sweep with the next page of every live cell this node holds.
    pub(super) fn serve_sweep(&mut self, dst: SocketAddr, from: u32) {
        self.frame_handles.clear();
        let handles = &mut self.frame_handles;
        let resume = self
            .store
            .visit_active_from(from, ANSWER_CELLS, |handle| handles.push(handle));
        let Some(consumed) = self.encode_answer() else {
            return;
        };
        let next = match self.frame_handles.get(consumed) {
            Some(handle) => handle.index,
            None => resume.unwrap_or(SWEEP_DONE),
        };
        self.queue_answer(dst, Ctrl::Swept { from, next }, consumed > 0);
    }

    /// A peer answered a sweep; ask for its next page at once.
    pub(super) fn on_swept(&mut self, src: SocketAddr, from: u32, next: u32) {
        let now = self.clock.now_millis();
        let JoinSweep::Running(sweeps) = &mut self.join_sweep else {
            return;
        };
        let Some(at) = sweeps.iter().position(|s| s.peer == src && s.from == from) else {
            return;
        };
        if next == SWEEP_DONE || next <= from {
            sweeps.remove(at);
        } else {
            sweeps[at] = Sweep {
                peer: src,
                from: next,
                due_ms: now,
                attempts: 0,
            };
        }
        self.sweep_due(now);
    }

    /// Once per heartbeat, push this node's position on every root it has
    /// heard from, itself included, to one random peer.
    pub(super) fn push_digest(&mut self, now_millis: u64) {
        let interval = self.config.tick_interval.as_millis() as u64;
        if self.tree.anti_entropy == AntiEntropy::Off
            || now_millis.saturating_sub(self.last_digest_ms) < interval
        {
            return;
        }
        self.last_digest_ms = now_millis;
        let peers = self.ring.len() - 1;
        let pick = self.rng.next_u64() as usize % peers;
        let Some(peer) = self.ring.peers().nth(pick) else {
            return;
        };
        let header = self.sender_header();
        let (key, limits) = (self.config.auth_key.as_ref(), self.config.wire_limits);
        let capacity = wire::ext_capacity(limits, key.is_some());
        let (seq, high) = self.rooted;
        let me = (high > 0).then_some(Entry {
            root: self.ring.me(),
            seq,
            high,
        });
        let Self {
            roots,
            send,
            digest_buf: buf,
            ..
        } = self;
        let others = roots
            .positions()
            .map(|(root, seq, high)| Entry { root, seq, high });
        digest::begin(buf);
        for entry in me.into_iter().chain(others) {
            if buf.len() + digest::ENTRY_LEN_MAX > capacity {
                if !send.enqueue(peer, |out| {
                    wire::encode_ext_only::<C>(header, buf, key, limits, out).is_ok()
                }) {
                    return;
                }
                digest::begin(buf);
            }
            digest::push(buf, entry);
        }
        if digest::has_entries(buf) {
            send.enqueue(peer, |out| {
                wire::encode_ext_only::<C>(header, buf, key, limits, out).is_ok()
            });
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
