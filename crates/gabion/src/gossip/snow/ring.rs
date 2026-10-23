//! The membership view as a ring, and the Snow child function.
//!
//! Every node sorts its view (itself plus the peers discovery reported) into
//! a ring. For a broadcast rooted at `r` the ring is read starting so that
//! `r` sits at the midpoint. A node responsible for a range of that rotated
//! sequence leaves itself out, splits what remains into the part before and
//! the part after itself, divides each part into `k / 2` sub-ranges, and
//! makes each sub-range's midpoint node a child that is responsible for the
//! whole sub-range. When at most `k` nodes remain they are all children and
//! leaves. Children are therefore a pure function of the local view, the
//! root and the carried range.

use std::net::SocketAddr;

/// One child to forward to, with the range it becomes responsible for:
/// `[lo, hi)` in the root-rotated order, `hi == None` meaning "to the end".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) struct Child {
    pub addr: SocketAddr,
    pub lo: SocketAddr,
    pub hi: Option<SocketAddr>,
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

    /// Children of the local node for a broadcast it roots itself: the
    /// range is the whole ring.
    pub fn root_children(&mut self, k: usize, out: &mut Vec<Child>) {
        let me = self.me;
        children(&self.members, me, me, None, None, k, out);
    }

    /// Children of the local node for a broadcast rooted at `root` whose
    /// packet carried the range `[lo, hi)`. Boundary nodes missing from the
    /// local view are inserted for this computation only.
    pub fn children(
        &mut self,
        root: SocketAddr,
        lo: SocketAddr,
        hi: Option<SocketAddr>,
        k: usize,
        out: &mut Vec<Child>,
    ) {
        let missing = |a: SocketAddr| self.members.binary_search(&a).is_err();
        if !(missing(root) || missing(lo) || hi.is_some_and(missing)) {
            children(&self.members, self.me, root, Some(lo), hi, k, out);
            return;
        }
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.members);
        for a in [Some(root), Some(lo), hi].into_iter().flatten() {
            if let Err(at) = self.scratch.binary_search(&a) {
                self.scratch.insert(at, a);
            }
        }
        children(&self.scratch, self.me, root, Some(lo), hi, k, out);
    }
}

/// Height of a Snow tree over `n` nodes with branching factor `k`:
/// `ceil(log_k((k - 1) n) + 1)`, at least 1.
pub(in crate::gossip) fn tree_height(n: usize, k: usize) -> u32 {
    let (n, k) = (n.max(1) as f64, k.max(2) as f64);
    (((k - 1.0) * n).log(k) + 1.0).ceil().max(1.0) as u32
}

/// The child function. `view` is sorted and holds `me`, `root` and any named
/// boundary; `lo == None` means the range starts at the first node of the
/// rotated order (the root's own range).
fn children(
    view: &[SocketAddr],
    me: SocketAddr,
    root: SocketAddr,
    lo: Option<SocketAddr>,
    hi: Option<SocketAddr>,
    k: usize,
    out: &mut Vec<Child>,
) {
    out.clear();
    let m = view.len();
    let position = |a: SocketAddr| view.binary_search(&a).ok();
    let (Some(root_at), Some(me_at)) = (position(root), position(me)) else {
        debug_assert!(false, "root and local node are always in the view");
        return;
    };
    let mid = m / 2;
    // Rotated index `j` is ring index `root_at - mid + j`, so the root sits
    // at `mid`.
    let rotated = |j: usize| view[(root_at + m - mid + j) % m];
    let index_of = |ring_at: usize| (ring_at + mid + m - root_at) % m;
    let start = match lo.map(position) {
        None => 0,
        Some(Some(at)) => index_of(at),
        Some(None) => return,
    };
    let end = match hi.map(position) {
        None => m,
        Some(Some(at)) => index_of(at),
        Some(None) => return,
    };
    if start >= end {
        return;
    }

    // The local node splits the range into what precedes and what follows
    // it. A node outside the range it was handed (views disagree) treats the
    // whole range as one side.
    let me_j = index_of(me_at);
    let (before_end, after_start) = if (start..end).contains(&me_j) {
        (me_j, me_j + 1)
    } else if me_j < start {
        (start, start)
    } else {
        (end, end)
    };

    let child = |from: usize, to: usize, out: &mut Vec<Child>| {
        out.push(Child {
            addr: rotated(from + (to - from) / 2),
            lo: rotated(from),
            hi: (to < m).then(|| rotated(to)),
        });
    };

    if (before_end - start) + (end - after_start) <= k {
        for j in (start..before_end).chain(after_start..end) {
            child(j, j + 1, out);
        }
        return;
    }
    let per_side = (k / 2).max(1);
    for (from, to) in [(start, before_end), (after_start, end)] {
        let len = to - from;
        let parts = per_side.min(len);
        for p in 0..parts {
            child(from + len * p / parts, from + len * (p + 1) / parts, out);
        }
    }
}
