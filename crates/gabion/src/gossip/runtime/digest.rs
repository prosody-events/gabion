//! The repair lane: digests of settled cells.
//!
//! Thicket, like Plumtree, is a broadcast and moves no state: a change that
//! both its tree and the lazy IHAVE path missed (a packet lost past its NACKs,
//! an announcement lost, a partition, a parent that went silent) is never
//! sent again. Once per bucket, and at every heartbeat while repair keeps
//! bringing news, a node sends one random peer a digest of each rule's
//! settled buckets: per origin, the sum of the counts it holds in each. A
//! bucket is settled once it closed a heartbeat and a missing timeout ago, so
//! changes in flight do not make two up-to-date nodes disagree. The peer pushes
//! back the cells of each origin and bucket where it holds more, and of each
//! origin the digest's range leaves out, which the sender holds none of: the
//! sender is repaired. Where the sender holds more, the peer answers with its
//! own digest of the same range, marked as a reply, which the sender answers by
//! pushing: one exchange repairs both, as Scuttlebutt's does.
//!
//! This replaces the blind repair slice, which sent a random peer the next
//! 256 cells of the store whatever it held.

use crate::crdt::{BucketEpoch, Count, NodeSlot, RuleSlot};
use crate::defaults::{
    THICKET_DIGEST_BUCKETS, THICKET_DIGEST_PUSH_CELLS, THICKET_MISSING_TIMEOUT_MS,
};
use crate::wire::{DigestFrame, DigestScope, DigestWriter, decode_digest};

use super::super::clock::Clock;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::thicket::NONE;
use super::{GossipRuntime, Lane};

#[cfg(test)]
mod tests;

/// Scratch for building and answering digests, sized at construction.
pub(super) struct Digests {
    /// Sum over rules of one past the newest settled bucket when the last
    /// digest went out: a digest is due when it moves.
    sent_mark: u64,
    /// Cells from outside the trees changed the store since the last digest:
    /// repair is under way, so the next heartbeat sends another.
    pub repairing: bool,
    /// Count sums, `THICKET_DIGEST_BUCKETS` per row: a row per origin slot
    /// while building, per received group while answering.
    acc: Box<[u32]>,
    /// Origins with settled cells, while building: node id and slot.
    order: Vec<(u128, NodeSlot)>,
    listed: Box<[bool]>,
    /// While answering: the group of each origin slot, or `NONE`.
    group_of: Box<[u16]>,
    /// While answering: each group's origin slot, the sender's sums, and the
    /// buckets where this node holds more.
    groups: Vec<NodeSlot>,
    theirs: Vec<u32>,
    ahead: Vec<u64>,
}

const _: () = assert!(THICKET_DIGEST_BUCKETS <= u64::BITS as usize);
const _: () = assert!(THICKET_DIGEST_BUCKETS <= u8::MAX as usize);

impl Digests {
    pub(super) fn new(node_capacity: usize, max_payload_bytes: usize) -> Self {
        // A group is at least a 20-byte origin and one 4-byte sum.
        let groups = max_payload_bytes / 24 + 1;
        Self {
            sent_mark: 0,
            repairing: false,
            acc: vec![0; node_capacity.max(groups) * THICKET_DIGEST_BUCKETS].into_boxed_slice(),
            order: Vec::with_capacity(node_capacity),
            listed: vec![false; node_capacity].into_boxed_slice(),
            group_of: vec![NONE; node_capacity].into_boxed_slice(),
            groups: Vec::with_capacity(groups),
            theirs: Vec::with_capacity(max_payload_bytes / 4),
            ahead: Vec::with_capacity(groups),
        }
    }
}

/// A rule's settled buckets at some instant: `first ..= last`.
#[derive(Clone, Copy)]
struct Settled {
    first: BucketEpoch,
    last: BucketEpoch,
}

impl Settled {
    fn len(self) -> usize {
        (self.last - self.first) as usize + 1
    }

    fn offset(self, bucket: BucketEpoch) -> Option<usize> {
        (self.first..=self.last)
            .contains(&bucket)
            .then(|| (bucket - self.first) as usize)
    }
}

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    /// Rule `slot`'s settled buckets at `now`, at most the newest
    /// `THICKET_DIGEST_BUCKETS`: those still in the window that closed at
    /// least a heartbeat and a missing timeout ago, by when this node's own
    /// changes of them have left in a round and the trees and IHAVE have had
    /// their turn. `None` when there are none.
    fn settled(&self, slot: RuleSlot, now: u64) -> Option<Settled> {
        let rule = self.store.rule_dictionary().descriptor(slot)?;
        let bucket_ms = u64::from(rule.bucket_millis);
        let margin = self.config.tick_interval.as_millis() as u64 + THICKET_MISSING_TIMEOUT_MS;
        let closed = now.checked_sub(margin)?.checked_div(bucket_ms)?;
        let last = BucketEpoch::try_from(closed.checked_sub(1)?).ok()?;
        let oldest = rule.current_epoch(now).saturating_sub(rule.live_buckets());
        let first = oldest.max((last + 1).saturating_sub(THICKET_DIGEST_BUCKETS as u32));
        (first <= last).then_some(Settled { first, last })
    }

    /// Heartbeat: once a rule's newest settled bucket has moved since the
    /// last digest, or while repair keeps bringing news, send one random peer
    /// a digest of every rule.
    pub(super) fn digest_due(&mut self, now: u64) {
        for peer in &mut self.thicket.peers {
            peer.repair_sent = 0;
        }
        let rules = self.store.rule_dictionary().capacity() as RuleSlot;
        let mark = (0..rules)
            .filter_map(|r| self.settled(r, now))
            .map(|s| u64::from(s.last) + 1)
            .sum();
        if mark == self.digests.sent_mark && !self.digests.repairing {
            return;
        }
        self.digests.sent_mark = mark;
        self.digests.repairing = false;
        let Some(peer) = self.thicket.random_alive(&mut self.rng, |_| false) else {
            return;
        };
        for rule in 0..rules {
            if let Some(settled) = self.settled(rule, now) {
                self.send_digest(peer, rule, settled, 0..u128::MAX, false);
            }
        }
    }

    /// Sum this node's settled cells of `rule` per origin and bucket, for the
    /// origins whose node id is in `range`, and send them to `peer` in node id
    /// order, as many frames as they take. A `reply` asks to be answered by
    /// pushing only.
    fn send_digest(
        &mut self,
        peer: u16,
        rule: RuleSlot,
        settled: Settled,
        range: std::ops::Range<u128>,
        reply: bool,
    ) {
        let n = settled.len();
        let fingerprint = self
            .store
            .rule_dictionary()
            .descriptor(rule)
            .map_or(0, |r| r.fingerprint);
        let addr = self.thicket.peers[usize::from(peer)].addr;
        let header = self.frame_header();
        let d = &mut self.digests;
        for handle in self.store.active_handles() {
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            let Some(i) = settled
                .offset(row.key.bucket)
                .filter(|_| row.key.rule == rule)
            else {
                continue;
            };
            let origin = row.key.origin;
            if !d.listed[usize::from(origin)] {
                let Some(node) = self.store.node_dictionary().descriptor(origin) else {
                    continue;
                };
                if !range.contains(&node.node_id.0) {
                    continue;
                }
                d.listed[usize::from(origin)] = true;
                d.order.push((node.node_id.0, origin));
            }
            let sum = &mut d.acc[usize::from(origin) * THICKET_DIGEST_BUCKETS + i];
            *sum = sum.saturating_add(count_u32(row.count));
        }
        d.order.sort_unstable_by_key(|&(node_id, _)| node_id);
        // A digest of nothing says "send me everything": worth sending in
        // answer to a digest, or from a node that holds nothing at all, such
        // as one that started among peers that already hold state. Otherwise
        // a rule this node holds nothing of is left to the trees.
        if d.order.is_empty() && !reply && !self.store.is_empty() {
            return;
        }

        let key = self.config.auth_key.as_ref();
        let mut next = 0;
        let mut lo = range.start;
        let mut unsent = 0;
        loop {
            let Some(slot) = self.send_free.pop() else {
                unsent = d.order.len() - next;
                break;
            };
            let buf = &mut self.send_pool[usize::from(slot)];
            let scope = DigestScope {
                rule_fingerprint: fingerprint,
                first_bucket: settled.first,
                buckets: n as u8,
                reply,
            };
            let Ok(mut writer) =
                DigestWriter::begin(buf, scope, lo, key.is_some(), self.config.wire_limits)
            else {
                self.send_free.push(slot);
                break;
            };
            while let Some(&(_, origin)) = d.order.get(next)
                && writer.has_room()
            {
                let node = self.store.node_dictionary().descriptor(origin);
                let row = usize::from(origin) * THICKET_DIGEST_BUCKETS;
                writer.push(
                    node.map_or_else(Default::default, |n| n.node_id),
                    node.map_or(0, |n| n.incarnation),
                    d.acc[row..row + n].iter().copied(),
                );
                next += 1;
            }
            let hi = d.order.get(next).map_or(range.end, |&(node_id, _)| node_id);
            writer.finish(header, hi, key);
            self.send_pending.push_back((addr, slot));
            if next == d.order.len() {
                break;
            }
            lo = hi;
        }

        for &(_, origin) in &d.order {
            let row = usize::from(origin) * THICKET_DIGEST_BUCKETS;
            d.acc[row..row + n].fill(0);
            d.listed[usize::from(origin)] = false;
        }
        d.order.clear();
        if unsent > 0 {
            self.note_send_queue_full(addr, unsent);
        }
    }

    /// A digest frame from `peer`: push back what this node holds more of in
    /// the buckets it names, for the origins it lists and for those in its
    /// range it leaves out, and, unless it is a reply, answer with this
    /// node's own digest of that range where the peer holds more.
    pub(super) fn handle_digest(&mut self, peer: u16, len: usize) {
        let d = &mut self.digests;
        d.groups.clear();
        d.theirs.clear();
        let mut unknown = false;
        let store = &self.store;
        let decoded = decode_digest(
            &self.recv_buf[..len],
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
            |node_id, incarnation, sums| {
                let Some(origin) = store.find_node(node_id, incarnation) else {
                    // The peer holds an origin this node has never seen.
                    unknown |= (0..sums.len()).any(|i| sums.get(i) != 0);
                    return;
                };
                if d.group_of[usize::from(origin)] != NONE || d.groups.len() == d.groups.capacity()
                {
                    return;
                }
                d.group_of[usize::from(origin)] = d.groups.len() as u16;
                d.groups.push(origin);
                d.theirs.extend((0..sums.len()).map(|i| sums.get(i)));
            },
        );
        match decoded {
            Ok(frame) => self.answer_digest(peer, &frame, unknown),
            Err(err) => {
                let src = self.thicket.peers[usize::from(peer)].addr;
                self.note_decode_reject(src, &err);
            }
        }
        let d = &mut self.digests;
        for &origin in &d.groups {
            d.group_of[usize::from(origin)] = NONE;
        }
    }

    fn answer_digest(&mut self, peer: u16, frame: &DigestFrame, unknown: bool) {
        let scope = frame.scope;
        let n = usize::from(scope.buckets);
        if n == 0 || n > THICKET_DIGEST_BUCKETS {
            return;
        }
        let Some(settled) = scope
            .first_bucket
            .checked_add(n as u32 - 1)
            .map(|last| Settled {
                first: scope.first_bucket,
                last,
            })
        else {
            return;
        };
        let Some(rule) = self.store.find_rule(scope.rule_fingerprint) else {
            return;
        };
        let cap = THICKET_DIGEST_PUSH_CELLS
            .min(self.config.max_cells_per_tick)
            .saturating_sub(self.thicket.peers[usize::from(peer)].repair_sent);
        let mut push = std::mem::take(&mut self.frame_handles);
        push.clear();
        let d = &mut self.digests;
        d.acc[..d.groups.len() * n].fill(0);

        // What this node holds of each listed group; every cell of an origin
        // in range that is not listed, which the peer holds none of.
        for handle in self.store.active_handles() {
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            let Some(i) = settled
                .offset(row.key.bucket)
                .filter(|_| row.key.rule == rule)
            else {
                continue;
            };
            match d.group_of[usize::from(row.key.origin)] {
                NONE => {
                    let in_range = self
                        .store
                        .node_dictionary()
                        .descriptor(row.key.origin)
                        .is_some_and(|node| (frame.lo..frame.hi).contains(&node.node_id.0));
                    if in_range && push.len() < cap {
                        push.push(handle);
                    }
                }
                g => {
                    let sum = &mut d.acc[usize::from(g) * n + i];
                    *sum = sum.saturating_add(count_u32(row.count));
                }
            }
        }

        // The buckets of listed origins where this node holds more.
        d.ahead.clear();
        for g in 0..d.groups.len() {
            let ahead = (0..n)
                .filter(|&i| d.acc[g * n + i] > d.theirs[g * n + i])
                .fold(0u64, |bits, i| bits | (1 << i));
            d.ahead.push(ahead);
        }
        if d.ahead.iter().any(|&bits| bits != 0) {
            for handle in self.store.active_handles() {
                if push.len() >= cap {
                    break;
                }
                let Some(row) = self.store.get(handle) else {
                    continue;
                };
                let Some(i) = settled
                    .offset(row.key.bucket)
                    .filter(|_| row.key.rule == rule)
                else {
                    continue;
                };
                let g = d.group_of[usize::from(row.key.origin)];
                if g != NONE && d.ahead[usize::from(g)] & (1 << i) != 0 {
                    push.push(handle);
                }
            }
        }
        let behind = unknown || (0..d.groups.len() * n).any(|i| d.theirs[i] > d.acc[i]);
        // `send_digest` accumulates into the same scratch at another stride
        // and expects it zeroed.
        d.acc[..d.groups.len() * n].fill(0);
        if !push.is_empty() {
            self.thicket.peers[usize::from(peer)].repair_sent += push.len();
            self.send_cells(peer, &push, Lane::Pull);
        }
        self.frame_handles = push;
        if behind && !scope.reply {
            self.send_digest(peer, rule, settled, frame.lo..frame.hi, true);
        }
    }
}

/// A cell's count as a digest sums it, saturating.
fn count_u32<C: Count>(count: C) -> u32 {
    let count: u64 = count.into();
    u32::try_from(count).unwrap_or(u32::MAX)
}
