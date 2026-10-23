//! Ground truth and the accuracy metrics computed against it. Nothing here
//! is visible to nodes.
//!
//! * **Truth** is every admitted hit, cluster-wide, per key and bucket.
//! * **Staleness error** for a (node, tracked key) pair is `truth window total
//!   − the node's window total`: positive is an under-count, the quantity that
//!   lets a node over-admit. It is piecewise constant and recomputed on every
//!   event that can change it (an admission, an aggregate update on that node,
//!   a bucket boundary), so its distribution is weighted by exact virtual time
//!   rather than sampled.
//! * **Convergence time** of an admitted hit is the time until every node that
//!   was running when it was admitted (and still runs) has a bucket count at
//!   least the truth count that hit produced. With a partition, counts are
//!   ranked within the origin's side (see [`ConvSlot`]), so a pod's own side's
//!   hits are never mistaken for hits that crossed the cut.
//! * **Residual error** counts only buckets that closed at least
//!   [`RESIDUAL_AGE_MS`] ago: the part of the error that is not in flight.

use std::collections::BTreeMap;
use std::rc::Rc;

use gabion::crdt::BucketEpoch;
use serde::Serialize;

use super::node::{KeyKind, KeySpace, NodeStore, Slot, ring_window_total};
use super::phases::{PhaseStats, Schedule};
use super::plan::NS_PER_MS;
use super::report::PartitionReport;

pub const RESIDUAL_AGE_MS: u64 = 1_500;

/// Tracked keys fall into two classes reported separately: keys whose demand
/// meets their limit (hot and burst keys) and ordinary sampled sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Class {
    Limited = 0,
    Sampled = 1,
}

/// Time-weighted histogram of an integer-valued signal.
#[derive(Clone, Debug, Default)]
pub struct WeightedHist {
    weights: BTreeMap<i64, u128>,
}

impl WeightedHist {
    pub fn add(&mut self, value: i64, weight: u64) {
        if weight > 0 {
            *self.weights.entry(value).or_insert(0) += u128::from(weight);
        }
    }

    pub fn total(&self) -> u128 {
        self.weights.values().sum()
    }

    /// Smallest value whose cumulative weight reaches `q` of the total.
    pub fn quantile(&self, q: f64) -> Option<i64> {
        let total = self.total();
        if total == 0 {
            return None;
        }
        let target = (q * total as f64).ceil().max(1.0) as u128;
        let mut seen = 0;
        for (&value, &weight) in &self.weights {
            seen += weight;
            if seen >= target {
                return Some(value);
            }
        }
        self.weights.keys().next_back().copied()
    }

    pub fn mean(&self) -> Option<f64> {
        self.mean_by(|v| v)
    }

    /// Weighted mean of `f(value)`.
    pub fn mean_by(&self, f: impl Fn(i64) -> i64) -> Option<f64> {
        let total = self.total();
        (total > 0).then(|| {
            self.weights
                .iter()
                .map(|(&v, &w)| f(v) as f64 * w as f64)
                .sum::<f64>()
                / total as f64
        })
    }

    /// Share of the weight on values satisfying `pred`.
    pub fn fraction(&self, pred: impl Fn(i64) -> bool) -> Option<f64> {
        let total = self.total();
        (total > 0).then(|| {
            self.weights
                .iter()
                .filter(|&(&v, _)| pred(v))
                .map(|(_, &w)| w as f64)
                .sum::<f64>()
                / total as f64
        })
    }

    pub fn min(&self) -> Option<i64> {
        self.weights.keys().next().copied()
    }

    pub fn max(&self) -> Option<i64> {
        self.weights.keys().next_back().copied()
    }
}

/// Admissions of one tracked key in one bucket, and how far convergence has
/// got through them. A node only reports a bucket *count*, never who
/// contributed it, so coverage is count-based. Admissions are kept per side
/// of the partition (side 0 throughout without one) in admission order, and
/// `covered[g][s]` is the number of leading admissions by side `s` that every
/// running pod of observer group `g` has reached.
///
/// Admission `j` by side `s` needs a count of `j + 1` from observers on `s`.
/// From observers on the other side it needs `j + 1` plus every admission
/// their own side has made so far, so a far-side pod's own hits can never
/// stand in for hits that crossed the cut. That makes cross-side convergence
/// a conservative bound: it also waits for the observer to catch up on its
/// own side's hits. A hit has converged once every group in use covers it.
#[derive(Clone, Debug, Default)]
struct ConvSlot {
    bucket: BucketEpoch,
    /// Admission times by origin side.
    admits: [Vec<u64>; GROUPS],
    covered: [[usize; GROUPS]; GROUPS],
}

impl ConvSlot {
    /// Count observers on `group` need to have reached admission `j` of side
    /// `side`.
    fn need(&self, group: usize, side: usize, j: usize) -> u32 {
        let from_observers_side = if group == side {
            0
        } else {
            self.admits[group].len()
        };
        (j + 1 + from_observers_side) as u32
    }

    /// Index of the first admission of `side` that some group in use has not
    /// covered.
    fn first_unconverged(&self, side: usize, groups: usize) -> usize {
        (0..groups)
            .map(|g| self.covered[g][side])
            .min()
            .unwrap_or(0)
    }

    /// Admissions at least one group has not covered, as `(side, time)`.
    fn unconverged(&self, groups: usize) -> impl Iterator<Item = u64> + '_ {
        (0..GROUPS).flat_map(move |s| {
            self.admits[s][self.first_unconverged(s, groups)..]
                .iter()
                .copied()
        })
    }
}

/// Observer groups: one, or two (either side of a partition).
const GROUPS: usize = 2;

/// A pod that joined after the start, followed for one window.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Joiner {
    pub node: usize,
    pub join_ns: u64,
    #[serde(skip)]
    until_ns: u64,
    #[serde(skip)]
    under: i64,
    #[serde(skip)]
    last_ns: u64,
    /// Integral of the summed under-count over tracked keys across the
    /// first window after joining, in hit-seconds.
    pub undercount_hit_s: f64,
    /// Time from joining until the node's tracked-key totals first reach
    /// 95 % / 99 % of truth.
    pub t95_ms: Option<f64>,
    pub t99_ms: Option<f64>,
    /// Time from joining until the node's summed under-count first falls to
    /// the mean over established pods: catch-up, net of the steady-state
    /// error every pod carries. Checked whenever the joiner's error or the
    /// truth changes.
    pub t_parity_ms: Option<f64>,
}

impl Joiner {
    fn integrate(&mut self, now_ns: u64) {
        let to = now_ns.min(self.until_ns);
        if to > self.last_ns {
            self.undercount_hit_s += self.under.max(0) as f64 * (to - self.last_ns) as f64 / 1e9;
        }
        self.last_ns = self.last_ns.max(to);
    }

    fn check(&mut self, now_ns: u64, truth: i64, parity: Option<f64>) {
        let elapsed = (now_ns - self.join_ns) as f64 / NS_PER_MS as f64;
        if self.t_parity_ms.is_none() && parity.is_some_and(|p| self.under as f64 <= p) {
            self.t_parity_ms = Some(elapsed);
        }
        if truth <= 0 {
            return;
        }
        let coverage = 1.0 - self.under as f64 / truth as f64;
        if coverage >= 0.95 && self.t95_ms.is_none() {
            self.t95_ms = Some(elapsed);
        }
        if coverage >= 0.99 && self.t99_ms.is_none() {
            self.t99_ms = Some(elapsed);
        }
    }
}

/// Over-admission tally for one limited key, from bucket-boundary samples
/// of the trailing window.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct WindowSamples {
    pub windows: u64,
    pub over_windows: u64,
    pub over_max: i64,
    pub over_sum: i64,
}

pub struct Oracle {
    keys: Rc<KeySpace>,
    nodes: usize,
    measure_from_ns: u64,
    truth: Vec<Slot>,
    class: Vec<Class>,
    truth_window: Vec<i64>,
    sum_truth: i64,
    // Per (tracked key, node): `t * nodes + v`.
    view: Vec<i64>,
    err: Vec<i64>,
    since_ns: Vec<u64>,
    // Per (tracked key, ring slot): `t * ring + s`; node views of that
    // bucket at `(t * ring + s) * nodes + v`.
    conv: Vec<ConvSlot>,
    bucket_view: Vec<u32>,
    /// Highest count each `bucket_view` cell has held since its bucket began:
    /// unlike the view, an expiry does not lower it, so a hit reached before
    /// the solution aged it out still counts as reached.
    peak_view: Vec<u32>,
    alive: Vec<bool>,
    /// Running pods per observer group.
    alive_in: [usize; GROUPS],
    /// Observer group of each pod, and how many groups are in use.
    side: Vec<u8>,
    groups: usize,
    /// Per-phase metrics, when the regime schedules a partition.
    phases: Option<PhaseStats>,
    started_ns: Vec<u64>,
    joiners: Vec<Option<Joiner>>,
    pub finished_joiners: Vec<Joiner>,
    /// Running pods that are not followed as joiners, and the sum of their
    /// summed under-counts: the parity reference for joiners.
    established: Vec<bool>,
    established_under: i64,
    established_count: i64,
    pub staleness: [WeightedHist; 2],
    pub residual: [Vec<i64>; 2],
    pub convergence_ns: Vec<u64>,
    /// Per never-converged hit, the time from admission until its bucket
    /// left the window (or its ring slot was reused).
    pub censored_ns: Vec<u64>,
    /// Indexed by tracked index; only limited keys are sampled.
    pub over_admission: Vec<WindowSamples>,
}

impl Oracle {
    pub fn new(keys: Rc<KeySpace>, nodes: usize, measure_from_ns: u64) -> Self {
        let tracked = keys.tracked.len();
        let ring = keys.ring_len as usize;
        let class = keys
            .tracked
            .iter()
            .map(|&k| match keys.kinds[k as usize] {
                KeyKind::Site => Class::Sampled,
                KeyKind::Hot | KeyKind::Burst => Class::Limited,
            })
            .collect();
        Self {
            truth: vec![Slot::default(); keys.len() * ring],
            class,
            truth_window: vec![0; tracked],
            sum_truth: 0,
            view: vec![0; tracked * nodes],
            err: vec![0; tracked * nodes],
            since_ns: vec![0; tracked * nodes],
            conv: vec![ConvSlot::default(); tracked * ring],
            bucket_view: vec![0; tracked * ring * nodes],
            peak_view: vec![0; tracked * ring * nodes],
            alive: vec![false; nodes],
            alive_in: [0; GROUPS],
            side: vec![0; nodes],
            groups: 1,
            phases: None,
            started_ns: vec![0; nodes],
            joiners: vec![None; nodes],
            finished_joiners: Vec::new(),
            established: vec![false; nodes],
            established_under: 0,
            established_count: 0,
            staleness: Default::default(),
            residual: Default::default(),
            convergence_ns: Vec::new(),
            censored_ns: Vec::new(),
            over_admission: vec![WindowSamples::default(); tracked],
            keys,
            nodes,
            measure_from_ns,
        }
    }

    /// Also score the phases of `schedule`'s partition; `sides[v]` is pod
    /// `v`'s side ([`Schedule::side`]).
    pub fn with_partition(mut self, schedule: Schedule, sides: Vec<u8>) -> Self {
        self.side = sides;
        self.groups = GROUPS;
        self.phases = Some(PhaseStats::new(schedule));
        self
    }

    fn ring(&self) -> usize {
        self.keys.ring_len as usize
    }

    fn conv_index(&self, t: usize, bucket: BucketEpoch) -> usize {
        t * self.ring() + (bucket % self.keys.ring_len) as usize
    }

    /// Cluster-wide admitted hits for `key` in the window ending at `bucket`.
    pub fn truth_window_total(&self, key: u32, bucket: BucketEpoch) -> u64 {
        ring_window_total(&self.keys, &self.truth, key, bucket)
    }

    fn set_err(&mut self, t: usize, v: usize, value: i64, now_ns: u64) {
        let i = t * self.nodes + v;
        let old = self.err[i];
        let from = self.since_ns[i].max(self.measure_from_ns);
        if now_ns > from {
            let class = self.class[t] as usize;
            self.staleness[class].add(old, now_ns - from);
            if let Some(phases) = self.phases.as_mut() {
                phases.add_staleness(class, self.side[v], old, from, now_ns);
            }
        }
        self.since_ns[i] = now_ns;
        self.err[i] = value;
        let delta = value.max(0) - old.max(0);
        if self.established[v] {
            self.established_under += delta;
        }
        let parity = self.parity();
        if let Some(joiner) = self.joiners[v].as_mut() {
            joiner.integrate(now_ns);
            joiner.under += delta;
            joiner.check(now_ns, self.sum_truth, parity);
        }
    }

    fn parity(&self) -> Option<f64> {
        (self.established_count > 0)
            .then(|| self.established_under as f64 / self.established_count as f64)
    }

    pub fn node_started(&mut self, v: usize, now_ns: u64, joined: bool) {
        self.alive[v] = true;
        self.alive_in[self.side[v] as usize] += 1;
        self.started_ns[v] = now_ns;
        for s in 0..self.conv.len() {
            self.bucket_view[s * self.nodes + v] = 0;
            self.peak_view[s * self.nodes + v] = 0;
        }
        for t in 0..self.truth_window.len() {
            let i = t * self.nodes + v;
            self.view[i] = 0;
            self.err[i] = self.truth_window[t];
            self.since_ns[i] = now_ns;
        }
        let under = self.truth_window.iter().map(|&e| e.max(0)).sum();
        if joined && now_ns >= self.measure_from_ns {
            let mut joiner = Joiner {
                node: v,
                join_ns: now_ns,
                until_ns: now_ns + self.keys.window_ms * NS_PER_MS,
                under,
                last_ns: now_ns,
                undercount_hit_s: 0.0,
                t95_ms: None,
                t99_ms: None,
                t_parity_ms: None,
            };
            joiner.check(now_ns, self.sum_truth, self.parity());
            self.joiners[v] = Some(joiner);
        } else {
            self.established[v] = true;
            self.established_under += under;
            self.established_count += 1;
        }
    }

    pub fn node_stopped(&mut self, v: usize, now_ns: u64) {
        if let Some(mut joiner) = self.joiners[v].take() {
            joiner.integrate(now_ns);
            self.finished_joiners.push(joiner);
        }
        for t in 0..self.truth_window.len() {
            self.set_err(t, v, 0, now_ns);
        }
        if self.established[v] {
            self.established[v] = false;
            self.established_count -= 1;
        }
        self.alive[v] = false;
        self.alive_in[self.side[v] as usize] -= 1;
        for c in 0..self.conv.len() {
            self.advance_all(c, now_ns);
        }
    }

    /// A hit for `key` in `bucket` was admitted by pod `origin`.
    pub fn admitted(&mut self, key: u32, bucket: BucketEpoch, now_ns: u64, origin: usize) {
        let slot = key as usize * self.ring() + (bucket % self.keys.ring_len) as usize;
        self.truth[slot].add(bucket, 1);
        let t = self.keys.tracked_of[key as usize];
        if t == super::node::NOT_TRACKED {
            return;
        }
        let t = t as usize;
        self.truth_window[t] += 1;
        self.sum_truth += 1;
        let c = self.conv_index(t, bucket);
        if self.conv[c].bucket != bucket {
            self.retire(c, now_ns);
            self.conv[c].bucket = bucket;
        }
        self.conv[c].admits[self.side[origin] as usize].push(now_ns);
        // A group with no running pod covers the hit vacuously.
        for g in 0..self.groups {
            if self.alive_in[g] == 0 {
                for s in 0..GROUPS {
                    self.advance(c, g, s, now_ns);
                }
            }
        }
        for v in 0..self.nodes {
            if self.alive[v] {
                let value = self.truth_window[t] - self.view[t * self.nodes + v];
                self.set_err(t, v, value, now_ns);
            }
        }
        self.check_joiners(now_ns);
    }

    fn check_joiners(&mut self, now_ns: u64) {
        let truth = self.sum_truth;
        let parity = self.parity();
        for joiner in self.joiners.iter_mut().flatten() {
            joiner.check(now_ns, truth, parity);
        }
    }

    /// Node `v`'s aggregate changed for tracked key `t`, bucket `bucket`.
    pub fn view_changed(
        &mut self,
        v: usize,
        t: usize,
        bucket: BucketEpoch,
        bucket_count: u32,
        window_total: u64,
        now_ns: u64,
    ) {
        let c = self.conv_index(t, bucket);
        if self.conv[c].bucket == bucket {
            let cell = c * self.nodes + v;
            self.bucket_view[cell] = bucket_count;
            self.peak_view[cell] = self.peak_view[cell].max(bucket_count);
            let group = self.side[v] as usize;
            for s in 0..GROUPS {
                let slot = &self.conv[c];
                let next = slot.covered[group][s];
                if next < slot.admits[s].len() && bucket_count >= slot.need(group, s, next) {
                    self.advance(c, group, s, now_ns);
                }
            }
        }
        self.view[t * self.nodes + v] = window_total as i64;
        let value = self.truth_window[t] - window_total as i64;
        self.set_err(t, v, value, now_ns);
    }

    fn advance_all(&mut self, c: usize, now_ns: u64) {
        for g in 0..self.groups {
            for s in 0..GROUPS {
                self.advance(c, g, s, now_ns);
            }
        }
    }

    /// Record, for every admission by side `side` in slot `c` that every
    /// running pod of `group` now covers, the time the group took; and the
    /// time the whole cluster took, once the last group has. Convergence is
    /// monotone in admission order within a side: covering count `j + 1`
    /// implies covering `j`.
    fn advance(&mut self, c: usize, group: usize, side: usize, now_ns: u64) {
        let base = c * self.nodes;
        while self.conv[c].covered[group][side] < self.conv[c].admits[side].len() {
            let j = self.conv[c].covered[group][side];
            let at_ns = self.conv[c].admits[side][j];
            let need = self.conv[c].need(group, side, j);
            let lagging = (0..self.nodes).any(|v| {
                self.side[v] as usize == group
                    && self.alive[v]
                    && self.started_ns[v] <= at_ns
                    && self.bucket_view[base + v] < need
            });
            if lagging {
                return;
            }
            self.conv[c].covered[group][side] += 1;
            if at_ns < self.measure_from_ns {
                continue;
            }
            let latency_ns = now_ns - at_ns;
            // A group with no running pod covers vacuously: no sample.
            if let Some(phases) = self.phases.as_mut().filter(|_| self.alive_in[group] > 0) {
                phases.converged(at_ns, side as u8, group, latency_ns);
            }
            if (0..self.groups).all(|g| self.conv[c].covered[g][side] > j) {
                self.convergence_ns.push(latency_ns);
            }
        }
    }

    /// Slot `c` is about to be reused or its bucket left the window: any
    /// admission not yet converged never will.
    fn retire(&mut self, c: usize, now_ns: u64) {
        let slot = &mut self.conv[c];
        let from = self.measure_from_ns;
        self.censored_ns.extend(
            slot.unconverged(self.groups)
                .filter(|&at| at >= from)
                .map(|at| now_ns - at),
        );
        if let Some(phases) = self.phases.as_mut() {
            for g in 0..self.groups {
                for s in 0..GROUPS {
                    for &at in slot.admits[s][slot.covered[g][s]..]
                        .iter()
                        .filter(|&&at| at >= from)
                    {
                        phases.censored(at, s as u8, g, now_ns - at);
                    }
                }
            }
        }
        slot.admits.iter_mut().for_each(Vec::clear);
        slot.covered = Default::default();
        let base = c * self.nodes;
        self.bucket_view[base..base + self.nodes].fill(0);
        self.peak_view[base..base + self.nodes].fill(0);
    }

    /// Admissions still unconverged at the end of the run, whose bucket had
    /// not yet left the window (right-censored, not failures).
    pub fn pending(&self) -> u64 {
        self.conv
            .iter()
            .map(|s| {
                s.unconverged(self.groups)
                    .filter(|&at| at >= self.measure_from_ns)
                    .count() as u64
            })
            .sum()
    }

    /// The bucket clock ticked over to `current`.
    pub fn bucket_boundary(
        &mut self,
        now_ns: u64,
        now_millis: u64,
        current: BucketEpoch,
        stores: &[Option<Rc<NodeStore>>],
    ) {
        let live = self.keys.live_buckets;
        let in_window = now_ns >= self.measure_from_ns;
        // The bucket that just left the live window.
        if let Some(gone) = current.checked_sub(live) {
            self.note_unrepaired_expiry(gone, now_ns, now_millis);
            for t in 0..self.truth_window.len() {
                let c = self.conv_index(t, gone);
                if self.conv[c].bucket == gone {
                    self.retire(c, now_ns);
                }
            }
        }
        // Over-admission of the trailing complete window.
        if in_window && let Some(last) = current.checked_sub(1) {
            for t in 0..self.truth_window.len() {
                if self.class[t] != Class::Limited {
                    continue;
                }
                let key = self.keys.tracked[t];
                let total = self.truth_window_total(key, last) as i64;
                let over = total - self.keys.limit as i64;
                let samples = &mut self.over_admission[t];
                samples.windows += 1;
                if over > 0 {
                    samples.over_windows += 1;
                    samples.over_sum += over;
                }
                samples.over_max = samples.over_max.max(over);
            }
        }
        // The window slid: refresh truth and every live view.
        self.sum_truth = 0;
        for t in 0..self.truth_window.len() {
            let key = self.keys.tracked[t];
            self.truth_window[t] = self.truth_window_total(key, current) as i64;
            self.sum_truth += self.truth_window[t];
        }
        for (v, store) in stores.iter().enumerate() {
            let Some(store) = store else { continue };
            if !self.alive[v] {
                continue;
            }
            for t in 0..self.truth_window.len() {
                let total = store.window_total(self.keys.tracked[t], now_millis) as i64;
                self.view[t * self.nodes + v] = total;
                self.set_err(t, v, self.truth_window[t] - total, now_ns);
            }
        }
        self.check_joiners(now_ns);
    }

    /// Sample the residual error at `now_millis`.
    pub fn residual_sample(&mut self, now_millis: u64) {
        let bucket_ms = self.keys.bucket_ms;
        let current = self.keys.bucket_of(now_millis);
        let Some(cutoff) = now_millis.checked_sub(RESIDUAL_AGE_MS) else {
            return;
        };
        let live = self.keys.live_buckets;
        // Buckets in the live window that closed at or before `cutoff`.
        let closed: Vec<BucketEpoch> = (0..live)
            .filter_map(|o| current.checked_sub(o))
            .filter(|&b| (u64::from(b) + 1) * bucket_ms <= cutoff)
            .collect();
        for t in 0..self.truth_window.len() {
            let key = self.keys.tracked[t];
            for v in 0..self.nodes {
                if !self.alive[v] {
                    continue;
                }
                let mut residual = 0_i64;
                for &b in &closed {
                    let truth = i64::from(
                        self.truth[key as usize * self.ring() + (b % self.keys.ring_len) as usize]
                            .count_for(b),
                    );
                    let c = self.conv_index(t, b);
                    let seen = if self.conv[c].bucket == b {
                        i64::from(self.bucket_view[c * self.nodes + v])
                    } else {
                        0
                    };
                    residual += truth - seen;
                }
                self.residual[self.class[t] as usize].push(residual);
            }
        }
    }

    /// The last bucket that closed before the heal, and the pods that ran at
    /// it: the repair a partition owes. `now_ns` is at or after the heal, so
    /// the heal's millisecond is exactly `(now_ns - heal_ns)` earlier than
    /// `now_millis`.
    fn heal_debt(
        &self,
        schedule: Schedule,
        now_ns: u64,
        now_millis: u64,
    ) -> Option<(BucketEpoch, Vec<usize>)> {
        let heal_millis = now_millis - (now_ns - schedule.heal_ns) / NS_PER_MS;
        let last = self.keys.bucket_of(heal_millis).checked_sub(1)?;
        let pods = (0..self.nodes)
            .filter(|&v| self.alive[v] && self.started_ns[v] <= schedule.heal_ns)
            .collect();
        Some((last, pods))
    }

    /// Hits of buckets that closed before the partition healed and are
    /// still in the window, which some pod that ran at the heal has not yet
    /// reached: `(missing hits summed over pods and tracked keys, pairs)`.
    /// Zero missing means every cross-cut count still in the window is
    /// repaired (tolerance: none).
    fn heal_deficit(&self, schedule: Schedule, now_ns: u64, now_millis: u64) -> (u64, u64) {
        let Some((last, pods)) = self.heal_debt(schedule, now_ns, now_millis) else {
            return (0, 0);
        };
        let oldest = self
            .keys
            .bucket_of(now_millis)
            .saturating_sub(self.keys.live_buckets - 1);
        let mut missing = 0;
        for t in 0..self.truth_window.len() {
            for b in oldest..=last {
                missing += self.missing_in(self.conv_index(t, b), b, &pods);
            }
        }
        (missing, (pods.len() * self.truth_window.len()) as u64)
    }

    /// Hits admitted in slot `c` (holding `bucket`) that `pods` have never
    /// reached.
    fn missing_in(&self, c: usize, bucket: BucketEpoch, pods: &[usize]) -> u64 {
        let slot = &self.conv[c];
        if slot.bucket != bucket {
            return 0;
        }
        let truth = slot.admits.iter().map(Vec::len).sum::<usize>() as u32;
        pods.iter()
            .map(|&v| u64::from(truth.saturating_sub(self.peak_view[c * self.nodes + v])))
            .sum()
    }

    /// `gone`, a bucket that closed before the heal, just left the window:
    /// whatever some pod still lacked of it was never repaired, and the heal
    /// can no longer count as complete. A bucket that leaves the window at
    /// the heal instant itself had no time to be repaired and is owed
    /// nothing (it is outside `heal_deficit`'s window too).
    fn note_unrepaired_expiry(&mut self, gone: BucketEpoch, now_ns: u64, now_millis: u64) {
        let Some(schedule) = self.phases.as_ref().map(PhaseStats::schedule) else {
            return;
        };
        if now_ns <= schedule.heal_ns {
            return;
        }
        let Some((last, pods)) = self.heal_debt(schedule, now_ns, now_millis) else {
            return;
        };
        if gone > last {
            return;
        }
        let lost = (0..self.truth_window.len())
            .map(|t| self.missing_in(self.conv_index(t, gone), gone, &pods))
            .sum();
        if let Some(phases) = self.phases.as_mut() {
            phases.expired_unrepaired(lost);
        }
    }

    /// Probe the partition's repair at `now_ns` (at or after the heal).
    /// Returns `true` once nothing in the window is missing, after which the
    /// driver stops probing.
    pub fn heal_sample(&mut self, now_ns: u64, now_millis: u64) -> bool {
        let Some(schedule) = self.phases.as_ref().map(PhaseStats::schedule) else {
            return true;
        };
        let (missing, pairs) = self.heal_deficit(schedule, now_ns, now_millis);
        if let Some(phases) = self.phases.as_mut() {
            phases.healed(now_ns, missing, pairs);
        }
        missing == 0
    }

    /// The per-phase report, when the regime has a partition.
    pub fn partition_report(&mut self, limit: u64) -> Option<PartitionReport> {
        self.phases.as_mut().map(|p| p.report(limit))
    }

    /// Close every open interval at `end_ns`.
    pub fn finish(&mut self, end_ns: u64) {
        for v in 0..self.nodes {
            if !self.alive[v] {
                continue;
            }
            if let Some(mut joiner) = self.joiners[v].take() {
                joiner.integrate(end_ns);
                self.finished_joiners.push(joiner);
            }
            for t in 0..self.truth_window.len() {
                let value = self.err[t * self.nodes + v];
                self.set_err(t, v, value, end_ns);
            }
        }
        self.finished_joiners.sort_by_key(|j| j.join_ns);
        if let Some(phases) = self.phases.as_mut() {
            for slot in &self.conv {
                for g in 0..self.groups {
                    for s in 0..GROUPS {
                        for &at in slot.admits[s][slot.covered[g][s]..]
                            .iter()
                            .filter(|&&at| at >= self.measure_from_ns)
                        {
                            phases.pending(at, s as u8, g);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
