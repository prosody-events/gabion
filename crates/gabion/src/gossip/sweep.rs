//! Join sweeps: a starting node asks its highest-weight heard peers for
//! their live cells, and a node answers at most `gossip.sweep_concurrency`
//! requesters at a time, one page per prompt round.

use std::net::SocketAddr;
use std::num::NonZeroU8;

use crate::defaults::{GOSSIP_GAP_REPORTS, GOSSIP_SWEEP_PEERS};

/// The requester's one want: the sources asked and not yet answered, and
/// those that answered. A source that answered is not asked again.
#[derive(Debug)]
pub(super) struct SweepWant {
    asking: [Option<Ask>; GOSSIP_SWEEP_PEERS],
    answered: [Option<SocketAddr>; GOSSIP_SWEEP_PEERS],
    /// The last source picked, so the next pick goes down the ranking.
    last_pick: Option<(u64, SocketAddr)>,
    /// A peer was first heard while the want waits: ask on the next round.
    pub(super) due: bool,
    /// When the want started waiting; after `gossip.zone_grace`, peers not
    /// yet heard count as absent.
    pub(super) since_ms: u64,
}

/// A source asked `tries` times without a page.
#[derive(Clone, Copy, Debug)]
pub(super) struct Ask {
    pub(super) addr: SocketAddr,
    pub(super) tries: u8,
}

impl SweepWant {
    /// A want that started waiting at `since_ms`, with nothing asked.
    pub(super) fn new(since_ms: u64) -> Self {
        Self {
            asking: [None; GOSSIP_SWEEP_PEERS],
            answered: [None; GOSSIP_SWEEP_PEERS],
            last_pick: None,
            due: false,
            since_ms,
        }
    }

    /// Advance the want against the current candidates, ranked by weight:
    /// `true` once pages have come from `min(sources, k)` sources with
    /// `k > 0`, `sources` at most [`GOSSIP_SWEEP_PEERS`]. Otherwise drop the
    /// sources asked [`GOSSIP_GAP_REPORTS`] times or no longer candidates,
    /// and ask the next ones down the ranking, wrapping to the top.
    pub(super) fn advance<I>(&mut self, candidates: impl Fn() -> I, sources: usize) -> bool
    where
        I: Iterator<Item = (u64, SocketAddr)>,
    {
        let k = candidates().count();
        let answered = self.answered.iter().flatten().count();
        let need = k.min(sources);
        if k > 0 && answered >= need {
            return true;
        }
        for slot in &mut self.asking {
            if slot.is_some_and(|ask| {
                ask.tries == GOSSIP_GAP_REPORTS || !candidates().any(|(_, addr)| addr == ask.addr)
            }) {
                *slot = None;
            }
        }
        while answered + self.asking.iter().flatten().count() < need {
            let open = |&(_, addr): &(u64, SocketAddr)| !self.answered(addr) && !self.asking(addr);
            let below = |pick: &(u64, SocketAddr)| self.last_pick.is_none_or(|last| *pick < last);
            let Some(pick) = candidates()
                .filter(|pick| open(pick) && below(pick))
                .max()
                .or_else(|| candidates().filter(open).max())
            else {
                break;
            };
            self.last_pick = Some(pick);
            if let Some(slot) = self.asking.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(Ask {
                    addr: pick.1,
                    tries: 0,
                });
            }
        }
        false
    }

    /// The sources being asked; the caller counts each try it sends.
    pub(super) fn asks(&mut self) -> impl Iterator<Item = &mut Ask> {
        self.asking.iter_mut().flatten()
    }

    /// A page arrived from `src`: any page, the empty one included, answers.
    pub(super) fn answered_by(&mut self, src: SocketAddr) {
        if self.answered(src) {
            return;
        }
        for slot in &mut self.asking {
            if slot.is_some_and(|ask| ask.addr == src) {
                *slot = None;
            }
        }
        if let Some(slot) = self.answered.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(src);
        }
    }

    fn answered(&self, addr: SocketAddr) -> bool {
        self.answered.contains(&Some(addr))
    }

    fn asking(&self, addr: SocketAddr) -> bool {
        self.asking.iter().flatten().any(|ask| ask.addr == addr)
    }
}

/// One sweep this node is answering: the requester and the slot its next page
/// starts at.
#[derive(Clone, Copy, Debug)]
pub(super) struct Sweep {
    pub(super) dst: SocketAddr,
    pub(super) next_slot: u32,
}

/// What [`Sweeps::accept`] did with a request.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Accept {
    Started,
    /// Already sweeping the requester: a retry, which changes nothing.
    Running,
    /// Every slot is busy.
    Full,
}

/// The sweeps this node answers, at most `gossip.sweep_concurrency`, sized
/// once.
pub(super) struct Sweeps(Box<[Option<Sweep>]>);

impl Sweeps {
    pub(super) fn new(concurrency: NonZeroU8) -> Self {
        Self(vec![None; usize::from(concurrency.get())].into_boxed_slice())
    }

    pub(super) fn accept(&mut self, dst: SocketAddr) -> Accept {
        if self.0.iter().flatten().any(|sweep| sweep.dst == dst) {
            return Accept::Running;
        }
        match self.0.iter_mut().find(|slot| slot.is_none()) {
            Some(slot) => {
                *slot = Some(Sweep { dst, next_slot: 0 });
                Accept::Started
            }
            None => Accept::Full,
        }
    }

    /// Forget `dst`'s sweep: a restarted requester is swept from the start.
    pub(super) fn end(&mut self, dst: SocketAddr) {
        for slot in &mut self.0 {
            if slot.is_some_and(|sweep| sweep.dst == dst) {
                *slot = None;
            }
        }
    }

    pub(super) fn active(&self) -> usize {
        self.0.iter().flatten().count()
    }

    pub(super) fn limit(&self) -> usize {
        self.0.len()
    }

    pub(super) fn get(&self, i: usize) -> Option<Sweep> {
        self.0.get(i).copied().flatten()
    }

    /// Move slot `i`'s sweep on to `next_slot`, or end it with `None`. The
    /// requester stays, so no requester holds two slots.
    pub(super) fn advance(&mut self, i: usize, next_slot: Option<u32>) {
        if let Some(slot) = self.0.get_mut(i) {
            *slot = slot
                .zip(next_slot)
                .map(|(sweep, next_slot)| Sweep { next_slot, ..sweep });
        }
    }
}
