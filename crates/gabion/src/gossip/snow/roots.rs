//! What this node has of each root's changes, and what it knows it lacks.
//!
//! A root numbers its broadcasts consecutively, and each broadcast carries
//! the root's own cells, whose origin sequences rise from one broadcast to
//! the next. A node that receives broadcast `b` when the last one it saw
//! from that root was `b - 3` has missed two, and the cells they carried are
//! exactly the root's cells with an origin sequence between the highest it
//! had seen and the lowest in `b` (a cell updated since then went out again
//! in a later broadcast). That interval is a [`Gap`]. Once the root has had
//! time to rebroadcast, the node pulls the gap from the root, which holds
//! every one of its own cells. The first broadcast from a root opens the gap
//! below it, so a node that joins late pulls each root's live window from
//! the root itself.

use std::net::SocketAddr;

use arrayvec::ArrayVec;

/// Gaps tracked per root before neighbouring ones are merged.
pub(in crate::gossip) const MAX_GAPS: usize = 8;

/// Broadcast sequences further apart than this are not one root's run of
/// broadcasts: the root restarted (its numbering starts at a random value).
const MAX_SEQ_JUMP: u32 = 1 << 20;

/// The root's cells with an origin sequence strictly between `after` and
/// `before`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) struct Gap {
    pub after: u64,
    pub before: u64,
}

impl Gap {
    fn is_empty(&self) -> bool {
        self.before <= self.after.saturating_add(1)
    }
}

/// One root's log.
#[derive(Clone, Debug)]
pub(in crate::gossip) struct RootLog {
    pub root: SocketAddr,
    /// Highest broadcast sequence received from the root.
    seq: u32,
    /// Highest origin sequence of the root's cells received.
    high: u64,
    /// Missing origin-sequence intervals, ascending and disjoint.
    gaps: ArrayVec<Gap, MAX_GAPS>,
    /// Earliest time the next pull of `gaps[0]` may be sent.
    due_ms: u64,
    /// Pulls of `gaps[0]` sent without an answer.
    attempts: u8,
    /// Last time anything was heard of this root.
    heard_ms: u64,
}

impl RootLog {
    fn new(root: SocketAddr, seq: u32, high: u64, now_ms: u64) -> Self {
        Self {
            root,
            seq,
            high,
            gaps: ArrayVec::new(),
            due_ms: 0,
            attempts: 0,
            heard_ms: now_ms,
        }
    }

    /// Note a gap, pulled no earlier than `due_ms` (unless an older gap is
    /// already waiting).
    fn open(&mut self, gap: Gap, due_ms: u64) {
        if gap.is_empty() {
            return;
        }
        if self.gaps.is_empty() {
            self.due_ms = due_ms;
            self.attempts = 0;
        }
        let at = self.gaps.partition_point(|g| g.before <= gap.after);
        let mut merged = gap;
        let mut end = at;
        while end < self.gaps.len() && self.gaps[end].after < merged.before {
            merged.after = merged.after.min(self.gaps[end].after);
            merged.before = merged.before.max(self.gaps[end].before);
            end += 1;
        }
        self.gaps.drain(at..end);
        if self.gaps.is_full() {
            // Merge into the nearer neighbour; re-pulling what lies between
            // costs bytes, never correctness.
            let near = if at == self.gaps.len()
                || (at > 0
                    && merged.after - self.gaps[at - 1].before
                        <= self.gaps[at].after - merged.before)
            {
                at - 1
            } else {
                at
            };
            let g = &mut self.gaps[near];
            g.after = g.after.min(merged.after);
            g.before = g.before.max(merged.before);
            return;
        }
        self.gaps.insert(at, merged);
    }

    /// Cells with origin sequences `low..=high` arrived: take them out of
    /// the gaps.
    fn fill(&mut self, low: u64, high: u64) {
        let mut i = 0;
        while i < self.gaps.len() {
            let g = self.gaps[i];
            let covers_start = low <= g.after.saturating_add(1);
            let covers_end = high.saturating_add(1) >= g.before;
            if high <= g.after || low >= g.before {
                i += 1;
            } else if covers_start && covers_end {
                self.gaps.remove(i);
                if i == 0 {
                    self.attempts = 0;
                }
            } else if covers_start {
                self.gaps[i].after = high;
                i += 1;
            } else if covers_end {
                self.gaps[i].before = low;
                i += 1;
            } else if self.gaps.is_full() {
                i += 1;
            } else {
                self.gaps[i].before = low;
                self.gaps.insert(i + 1, Gap { after: high, ..g });
                i += 2;
            }
        }
    }
}

/// Per-root logs, sorted by root address and bounded at construction.
pub(in crate::gossip) struct Roots {
    logs: Vec<RootLog>,
    /// Some log may hold a gap. Set when one opens, recomputed by
    /// [`Self::pull_due`], so the event loop can test it without a scan.
    maybe_gaps: bool,
}

impl Roots {
    /// Unanswered pulls after which a root that is not a member is dropped.
    pub const MAX_ATTEMPTS: u8 = 8;

    pub fn new(capacity: usize) -> Self {
        Self {
            logs: Vec::with_capacity(capacity.max(1)),
            maybe_gaps: false,
        }
    }

    /// Whether a pull may be waiting; see [`Self::pull_due`].
    pub fn maybe_gaps(&self) -> bool {
        self.maybe_gaps
    }

    /// The first copy of broadcast `seq` from `root` arrived, carrying the
    /// root's cells with origin sequences `low..=high`. A gap it reveals is
    /// pulled no earlier than `due_ms`, giving the root's rebroadcast time
    /// to fill it.
    pub fn broadcast(
        &mut self,
        root: SocketAddr,
        seq: u32,
        (low, high): (u64, u64),
        now_ms: u64,
        due_ms: u64,
    ) {
        let first = Gap {
            after: 0,
            before: low,
        };
        let at = match self.logs.binary_search_by(|l| l.root.cmp(&root)) {
            Ok(at) => at,
            Err(_) => {
                let Some(at) = self.insert(RootLog::new(root, seq, high, now_ms)) else {
                    return;
                };
                self.logs[at].open(first, due_ms);
                self.maybe_gaps |= !self.logs[at].gaps.is_empty();
                return;
            }
        };
        let log = &mut self.logs[at];
        log.heard_ms = now_ms;
        let ahead = seq.wrapping_sub(log.seq);
        if ahead == 0 || ahead > u32::MAX - MAX_SEQ_JUMP {
            // A copy that arrived after a later broadcast: it fills what
            // that one revealed missing.
            log.fill(low, high);
        } else if ahead > MAX_SEQ_JUMP || low <= log.high {
            // Broadcast or origin sequences that do not continue the run:
            // the root restarted.
            *log = RootLog::new(root, seq, high, now_ms);
            log.open(first, due_ms);
        } else {
            if ahead > 1 {
                let gap = Gap {
                    after: log.high,
                    before: low,
                };
                log.open(gap, due_ms);
            }
            log.seq = seq;
            log.high = high;
        }
        self.maybe_gaps |= !log.gaps.is_empty();
    }

    /// A peer reports holding the root's broadcasts up to `seq` and its
    /// cells up to origin sequence `high`. If that is ahead of this node,
    /// the difference is a gap, pulled no earlier than `due_ms`.
    pub fn advertised(&mut self, root: SocketAddr, seq: u32, high: u64, now_ms: u64, due_ms: u64) {
        let everything = Gap {
            after: 0,
            before: high.saturating_add(1),
        };
        let at = match self.logs.binary_search_by(|l| l.root.cmp(&root)) {
            Ok(at) => at,
            Err(_) => {
                let Some(at) = self.insert(RootLog::new(root, seq, high, now_ms)) else {
                    return;
                };
                self.logs[at].open(everything, due_ms);
                self.maybe_gaps |= !self.logs[at].gaps.is_empty();
                return;
            }
        };
        let log = &mut self.logs[at];
        let ahead = seq.wrapping_sub(log.seq);
        if ahead == 0 || ahead > u32::MAX - MAX_SEQ_JUMP {
            return;
        }
        if ahead > MAX_SEQ_JUMP {
            // Not a continuation of the run this node holds: the root
            // restarted.
            *log = RootLog::new(root, seq, high, now_ms);
            log.open(everything, due_ms);
        } else if high > log.high {
            let gap = Gap {
                after: log.high,
                before: high.saturating_add(1),
            };
            log.open(gap, due_ms);
            log.seq = seq;
            log.high = high;
        }
        self.maybe_gaps |= !log.gaps.is_empty();
    }

    /// Each root's position as this node holds it.
    pub fn positions(&self) -> impl Iterator<Item = (SocketAddr, u32, u64)> + '_ {
        self.logs.iter().map(|l| (l.root, l.seq, l.high))
    }

    /// The root answered a pull of the gap starting after `after` with its
    /// cells up to and including `through`. Returns whether more remains to
    /// pull from it.
    pub fn filled(&mut self, root: SocketAddr, after: u64, through: u64, now_ms: u64) -> bool {
        let Ok(at) = self.logs.binary_search_by(|l| l.root.cmp(&root)) else {
            return false;
        };
        let log = &mut self.logs[at];
        log.heard_ms = now_ms;
        let Some(&first) = log.gaps.first() else {
            return false;
        };
        if first.after != after || through <= after {
            return false;
        }
        if through.saturating_add(1) >= first.before {
            log.gaps.remove(0);
        } else {
            log.gaps[0].after = through;
        }
        log.attempts = 0;
        log.due_ms = now_ms;
        !log.gaps.is_empty()
    }

    /// Forget a root that left the cluster.
    pub fn remove(&mut self, root: SocketAddr) {
        if let Ok(at) = self.logs.binary_search_by(|l| l.root.cmp(&root)) {
            self.logs.remove(at);
        }
    }

    /// Pull every gap that is due: `send(root, gap)` queues the request and
    /// returns `false` when nothing more can be queued now. A pull without
    /// an answer is retried after `timeout_ms`, doubling per attempt up to
    /// `max_retry_ms`. A root `known` does not report as a member is given
    /// up on after [`Self::MAX_ATTEMPTS`] unanswered pulls.
    pub fn pull_due(
        &mut self,
        now_ms: u64,
        timeout_ms: u64,
        max_retry_ms: u64,
        known: impl Fn(SocketAddr) -> bool,
        mut send: impl FnMut(SocketAddr, Gap) -> bool,
    ) {
        let mut any = false;
        for log in self.logs.iter_mut() {
            let Some(&gap) = log.gaps.first() else {
                continue;
            };
            if log.attempts >= Self::MAX_ATTEMPTS && !known(log.root) {
                log.gaps.clear();
                continue;
            }
            any = true;
            if log.due_ms > now_ms {
                continue;
            }
            if !send(log.root, gap) {
                self.maybe_gaps = true;
                return;
            }
            let wait = (timeout_ms << log.attempts.min(16)).min(max_retry_ms.max(timeout_ms));
            log.attempts = log.attempts.saturating_add(1);
            log.due_ms = now_ms + wait;
        }
        self.maybe_gaps = any;
    }

    #[cfg(test)]
    pub fn gaps(&self, root: SocketAddr) -> Vec<Gap> {
        self.logs
            .iter()
            .find(|l| l.root == root)
            .map(|l| l.gaps.to_vec())
            .unwrap_or_default()
    }

    /// Add a log, evicting the one heard from longest ago when full.
    /// Returns its index.
    fn insert(&mut self, log: RootLog) -> Option<usize> {
        if self.logs.len() == self.logs.capacity() {
            let oldest = (0..self.logs.len()).min_by_key(|&i| self.logs[i].heard_ms)?;
            self.logs.remove(oldest);
        }
        let at = self
            .logs
            .binary_search_by(|l| l.root.cmp(&log.root))
            .unwrap_or_else(|at| at);
        self.logs.insert(at, log);
        Some(at)
    }
}
