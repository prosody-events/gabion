//! The membership view as a ring, and the Snow child function.
//!
//! Every node sorts its view (itself plus the peers discovery reported) into
//! a ring. A node is responsible for a clockwise interval `[lo, hi)` of that
//! ring (`hi` excluded). It leaves itself out, splits what remains into the
//! part before itself and the part after itself, divides each part into
//! `k / 2` sub-ranges, and makes each sub-range's midpoint node a child that
//! is responsible for the whole sub-range. When at most `k` nodes remain they
//! are all children and leaves. A root is responsible for the whole ring but
//! itself: the nodes clockwise from its successor up to, excluding, itself,
//! split so that it sits at the midpoint. Intervals are positions on the
//! ring, not offsets into a view-sized sequence, so a view that differs from
//! its parent's by a member or two shifts nothing: it just counts one node
//! more or fewer inside the interval. Children are a pure function of the
//! local view and the carried range.

use std::net::SocketAddr;

use arrayvec::ArrayVec;

use super::flight::MAX_CHILDREN;

/// Children of one node.
pub(in crate::gossip) type Children = ArrayVec<Child, MAX_CHILDREN>;

/// One child to forward to, with the clockwise range `[lo, hi)` it becomes
/// responsible for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) struct Child {
    pub addr: SocketAddr,
    pub lo: SocketAddr,
    pub hi: SocketAddr,
}

/// Sorted membership view including the local node.
pub(in crate::gossip) struct Ring {
    me: SocketAddr,
    members: Vec<SocketAddr>,
    /// `members` plus the boundary nodes a carried range names that this
    /// node has not discovered yet. Capacity is kept at `members.len() + 3`
    /// so building it never allocates.
    scratch: Vec<SocketAddr>,
}

impl Ring {
    pub fn new(me: SocketAddr, capacity: usize) -> Self {
        let mut members = Vec::with_capacity(capacity + 1);
        members.push(me);
        Self {
            me,
            members,
            scratch: Vec::with_capacity(capacity + 4),
        }
    }

    pub fn me(&self) -> SocketAddr {
        self.me
    }

    /// Members including the local node.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Peers, excluding the local node, in ring order.
    pub fn peers(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        let me = self.me;
        self.members.iter().copied().filter(move |&a| a != me)
    }

    pub fn contains(&self, addr: SocketAddr) -> bool {
        self.members.binary_search(&addr).is_ok()
    }

    /// Add a member. Returns `false` if it was already known (or is the
    /// local node).
    pub fn insert(&mut self, addr: SocketAddr) -> bool {
        match self.members.binary_search(&addr) {
            Ok(_) => false,
            Err(at) => {
                self.members.insert(at, addr);
                self.scratch.clear();
                self.scratch.reserve(self.members.len() + 3);
                true
            }
        }
    }

    /// Drop a member. The local node cannot be removed.
    pub fn remove(&mut self, addr: SocketAddr) -> bool {
        if addr == self.me {
            return false;
        }
        match self.members.binary_search(&addr) {
            Ok(at) => {
                self.members.remove(at);
                true
            }
            Err(_) => false,
        }
    }

    /// Children of the local node for a broadcast it roots itself.
    pub fn root_children(&self, k: usize, out: &mut Children) {
        out.clear();
        let m = self.members.len();
        let Ok(me_at) = self.members.binary_search(&self.me) else {
            debug_assert!(false, "the local node is always in its view");
            return;
        };
        // The `m - 1` other nodes clockwise from the successor. The root sits
        // between the last `m / 2` of them and the first `m - 1 - m / 2`.
        let split = m - 1 - m / 2;
        fan_out(
            &self.members,
            (me_at + 1) % m,
            m - 1,
            (split, split),
            k,
            out,
        );
    }

    /// Children of the local node for a broadcast whose packet carried the
    /// range `[lo, hi)`. Boundary nodes missing from the local view are
    /// inserted for this computation only.
    pub fn children(&mut self, lo: SocketAddr, hi: SocketAddr, k: usize, out: &mut Children) {
        let missing = |a: SocketAddr| self.members.binary_search(&a).is_err();
        if !(missing(lo) || missing(hi)) {
            interval_children(&self.members, self.me, lo, hi, k, out);
            return;
        }
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.members);
        for a in [lo, hi] {
            if let Err(at) = self.scratch.binary_search(&a) {
                self.scratch.insert(at, a);
            }
        }
        interval_children(&self.scratch, self.me, lo, hi, k, out);
    }
}

/// Height of a Snow tree over `n` nodes with branching factor `k`:
/// `ceil(log_k((k - 1) n) + 1)`, at least 1.
pub(in crate::gossip) fn tree_height(n: usize, k: usize) -> u32 {
    let (n, k) = (n.max(1) as f64, k.max(2) as f64);
    (((k - 1.0) * n).log(k) + 1.0).ceil().max(1.0) as u32
}

/// The child function for a carried range. `view` is sorted and holds `me`,
/// `lo` and `hi`.
fn interval_children(
    view: &[SocketAddr],
    me: SocketAddr,
    lo: SocketAddr,
    hi: SocketAddr,
    k: usize,
    out: &mut Children,
) {
    out.clear();
    let m = view.len();
    let (Ok(me_at), Ok(start), Ok(end)) = (
        view.binary_search(&me),
        view.binary_search(&lo),
        view.binary_search(&hi),
    ) else {
        debug_assert!(false, "the local node and both boundaries are in the view");
        return;
    };
    let len = (end + m - start) % m;
    // The local node splits the interval into what precedes and what follows
    // it. A node outside the interval it was handed (views disagree) treats
    // the whole interval as one side.
    let me_off = (me_at + m - start) % m;
    let sides = if me_off < len {
        (me_off, me_off + 1)
    } else {
        (len, len)
    };
    fan_out(view, start, len, sides, k, out);
}

/// Split the `len` nodes starting at ring index `start` into children. The
/// nodes at offsets `[0, before_end)` and `[after_start, len)` are the two
/// sides; the nodes between them are not this node's to cover.
fn fan_out(
    view: &[SocketAddr],
    start: usize,
    len: usize,
    (before_end, after_start): (usize, usize),
    k: usize,
    out: &mut Children,
) {
    let m = view.len();
    let at = |offset: usize| view[(start + offset) % m];
    let mut push = |from: usize, to: usize| {
        let pushed = out.try_push(Child {
            addr: at(from + (to - from) / 2),
            lo: at(from),
            hi: at(to),
        });
        debug_assert!(pushed.is_ok(), "k is clamped to MAX_CHILDREN");
    };

    if before_end + (len - after_start) <= k {
        for j in (0..before_end).chain(after_start..len) {
            push(j, j + 1);
        }
        return;
    }
    let per_side = (k / 2).max(1);
    for (from, to) in [(0, before_end), (after_start, len)] {
        let side = to - from;
        let parts = per_side.min(side);
        for p in 0..parts {
            push(from + side * p / parts, from + side * (p + 1) / parts);
        }
    }
}
