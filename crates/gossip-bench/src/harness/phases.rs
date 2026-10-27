//! Per-phase metrics for a regime that schedules a network partition.
//!
//! The regime's partition (`churn.partitions`) cuts one availability zone
//! off from the rest during `[at, heal)`, which splits the measurement
//! window into three phases: **before** the cut, **during** it, and
//! **after** the heal. Pods fall on one of two *sides*: those inside the cut
//! zone and the rest. The oracle scores, per phase:
//!
//! * **Convergence** of each hit, split by relation: `same_side` is the time
//!   until every pod on the origin's own side covers the hit, `cross_side` the
//!   time until every pod on the other side does. Nodes report counts, not
//!   origins, so `cross_side` is a conservative bound: it requires the far pod
//!   to have caught up on its own side's hits too (see the oracle). A hit
//!   belongs to the phase in which it was admitted. Hits whose bucket leaves
//!   the window first count as never converged, as in the whole-run figures.
//! * **Staleness** of pods on each side, time-weighted over the part of each
//!   exposure interval inside the phase. A pod's view is not resolved by
//!   origin, so staleness is split by the observer's side only.
//! * **Time to heal**: from the heal instant until no pod that ran at the heal
//!   is missing a hit of any bucket that closed before it. Tolerance is none:
//!   every cross-cut count is exact. A hit that leaves the window unrepaired is
//!   never counted as repaired: the figure is then `None`, and
//!   `unrepaired_at_expiry_hits` says how much was lost. The driver probes
//!   every [`HEAL_PROBE_NS`], so the figure is quantized to it, and it is
//!   `None` when the run ends first.
//!
//! Everything is driven by the regime's partition schedule; nothing here
//! looks at a regime's name.

use super::oracle::{Class, WeightedHist};
use super::plan::NS_PER_S;
use super::regime::Regime;
use super::report::{
    ConvergenceReport, Dist, ObserverStaleness, PartitionReport, PhaseReport, SideConvergence,
    SideStaleness,
};

/// How often the driver probes the heal's progress.
pub const HEAL_PROBE_NS: u64 = 100_000_000;

/// The cut: zone `zone` is isolated during `[at_ns, heal_ns)`.
#[derive(Clone, Copy, Debug)]
pub struct Schedule {
    pub at_ns: u64,
    pub heal_ns: u64,
    pub zone: u8,
}

impl Schedule {
    /// The regime's partition (the first, if several: `validate` allows one).
    pub fn of(regime: &Regime) -> Option<Self> {
        regime.churn.partitions.first().map(|p| Self {
            at_ns: p.at_s * NS_PER_S,
            heal_ns: p.heal_s * NS_PER_S,
            zone: p.az,
        })
    }

    /// 1 for a pod inside the cut zone, 0 for the rest.
    pub fn side(&self, az: u8) -> u8 {
        u8::from(az == self.zone)
    }

    fn phase(&self, t_ns: u64) -> usize {
        usize::from(t_ns >= self.at_ns) + usize::from(t_ns >= self.heal_ns)
    }

    fn span(&self, phase: usize) -> (u64, u64) {
        [
            (0, self.at_ns),
            (self.at_ns, self.heal_ns),
            (self.heal_ns, u64::MAX),
        ][phase]
    }
}

/// Convergence samples of one `(phase, relation)`.
#[derive(Default)]
struct Samples {
    converged_ns: Vec<u64>,
    censored_ns: Vec<u64>,
    pending: u64,
}

pub struct PhaseStats {
    schedule: Schedule,
    /// `[phase][relation]`; relation 0 is same side, 1 cross side.
    convergence: [[Samples; 2]; 3],
    /// `[phase][observer side][class]`.
    staleness: [[[WeightedHist; 2]; 2]; 3],
    /// Mean missing hits per (pod, tracked key) at the heal instant.
    deficit_at_heal: Option<f64>,
    /// (pod, tracked key) pairs behind `deficit_at_heal`.
    pairs: u64,
    /// Hits some pod lacked of a cut-era bucket when it left the window.
    unrepaired_hits: u64,
    healed_ns: Option<u64>,
}

impl PhaseStats {
    pub fn new(schedule: Schedule) -> Self {
        Self {
            schedule,
            convergence: Default::default(),
            staleness: Default::default(),
            deficit_at_heal: None,
            pairs: 0,
            unrepaired_hits: 0,
            healed_ns: None,
        }
    }

    pub fn schedule(&self) -> Schedule {
        self.schedule
    }

    fn samples(&mut self, at_ns: u64, origin: u8, observers: usize) -> &mut Samples {
        let relation = usize::from(origin as usize != observers);
        &mut self.convergence[self.schedule.phase(at_ns)][relation]
    }

    /// The pods of side `observers` all cover the hit admitted at `at_ns` by
    /// a pod of side `origin`, `latency_ns` later.
    pub fn converged(&mut self, at_ns: u64, origin: u8, observers: usize, latency_ns: u64) {
        self.samples(at_ns, origin, observers)
            .converged_ns
            .push(latency_ns);
    }

    pub fn censored(&mut self, at_ns: u64, origin: u8, observers: usize, latency_ns: u64) {
        self.samples(at_ns, origin, observers)
            .censored_ns
            .push(latency_ns);
    }

    pub fn pending(&mut self, at_ns: u64, origin: u8, observers: usize) {
        self.samples(at_ns, origin, observers).pending += 1;
    }

    /// A pod of side `side` held error `error` over `[from_ns, to_ns)`, which
    /// may straddle phase boundaries.
    pub fn add_staleness(&mut self, class: usize, side: u8, error: i64, from_ns: u64, to_ns: u64) {
        for phase in 0..3 {
            let (lo, hi) = self.schedule.span(phase);
            let (from, to) = (from_ns.max(lo), to_ns.min(hi));
            if to > from {
                self.staleness[phase][side as usize][class].add(error, to - from);
            }
        }
    }

    /// One heal probe at `now_ns`: `missing` hits over `pairs` pods and keys.
    /// The heal completes when nothing is missing and no cut-era hit has
    /// expired unrepaired; expiry is never scored as repair.
    pub fn healed(&mut self, now_ns: u64, missing: u64, pairs: u64) {
        if self.deficit_at_heal.is_none() {
            self.deficit_at_heal = Some(missing as f64 / pairs.max(1) as f64);
            self.pairs = pairs;
        }
        if missing == 0 && self.unrepaired_hits == 0 && self.healed_ns.is_none() {
            self.healed_ns = Some(now_ns);
        }
    }

    /// A cut-era bucket left the window while pods still lacked `hits` of it.
    pub fn expired_unrepaired(&mut self, hits: u64) {
        self.unrepaired_hits += hits;
    }

    pub fn report(&mut self, limit: u64) -> PartitionReport {
        let schedule = self.schedule;
        let mut phase = |p: usize| {
            let mut convergence = |relation: usize| {
                let s = std::mem::take(&mut self.convergence[p][relation]);
                ConvergenceReport::new(s.converged_ns, &s.censored_ns, s.pending)
            };
            let convergence = SideConvergence {
                same_side: convergence(0),
                cross_side: convergence(1),
            };
            let staleness = |side: usize| ObserverStaleness {
                limited: Dist::from_hist(&self.staleness[p][side][Class::Limited as usize], limit),
                sampled: Dist::from_hist(&self.staleness[p][side][Class::Sampled as usize], limit),
            };
            PhaseReport {
                convergence,
                staleness: SideStaleness {
                    in_zone: staleness(1),
                    rest: staleness(0),
                },
            }
        };
        PartitionReport {
            zone_az: schedule.zone,
            at_s: schedule.at_ns / NS_PER_S,
            heal_s: schedule.heal_ns / NS_PER_S,
            before: phase(0),
            during: phase(1),
            after: phase(2),
            deficit_at_heal_hits: self.deficit_at_heal,
            unrepaired_at_expiry_hits: self
                .deficit_at_heal
                .map(|_| self.unrepaired_hits as f64 / self.pairs.max(1) as f64),
            time_to_heal_ms: self.healed_ns.map(|t| (t - schedule.heal_ns) as f64 / 1e6),
        }
    }
}

#[cfg(test)]
mod tests;
