use std::rc::Rc;

use super::*;
use crate::harness::fixtures;
use crate::harness::node::KeySpace;
use crate::harness::oracle::Oracle;
use crate::harness::plan::NS_PER_MS;

const MS: u64 = NS_PER_MS;
const S: u64 = NS_PER_S;
/// An arbitrary bucket; the quiet fixture's buckets are 1 s wide.
const B: u32 = 100;

fn cut() -> Schedule {
    Schedule {
        at_ns: 10 * S,
        heal_ns: 20 * S,
        zone: 1,
    }
}

/// Epoch milliseconds at `ms` into bucket `B + 1`, where the heal falls in
/// the tests below.
fn millis(ms: u64) -> u64 {
    (u64::from(B) + 1) * 1_000 + ms
}

#[test]
fn staleness_exposure_is_split_at_the_cut_and_the_heal() {
    let mut stats = PhaseStats::new(cut());
    // 3 hits behind for 5 s before the cut, all of the cut, and 5 s after.
    stats.add_staleness(Class::Limited as usize, 1, 3, 5 * S, 25 * S);
    let report = stats.report(10);
    let weights =
        [&report.before, &report.during, &report.after].map(|p| p.staleness.in_zone.limited.weight);
    assert_eq!(weights, [5.0, 10.0, 5.0]);
    assert_eq!(report.during.staleness.in_zone.limited.mean, Some(3.0));
    // Other observers and the other class saw nothing.
    assert_eq!(report.during.staleness.rest.limited.weight, 0.0);
    assert_eq!(report.during.staleness.in_zone.sampled.weight, 0.0);
}

#[test]
fn hits_belong_to_the_phase_they_were_admitted_in() {
    let mut stats = PhaseStats::new(cut());
    stats.converged(9 * S, 0, 0, 100 * MS);
    stats.converged(10 * S, 0, 1, 700 * MS);
    stats.censored(19 * S, 1, 0, 2 * S);
    stats.pending(20 * S, 1, 1);
    let report = stats.report(10);
    assert_eq!(report.before.convergence.same_side.converged, 1);
    assert_eq!(report.during.convergence.cross_side.converged, 1);
    assert_eq!(report.during.convergence.cross_side.never_converged, 1);
    assert_eq!(report.after.convergence.same_side.pending_at_end, 1);
    assert_eq!(report.during.convergence.same_side.converged, 0);
}

/// Two pods, one on each side. Pod 0 admits a hit mid-cut and covers it at
/// once; pod 1, across the cut, only after the heal.
#[test]
fn convergence_is_split_by_origin_and_observer_side() {
    let keys = Rc::new(KeySpace::build(&fixtures::quiet(2), 1));
    let mut oracle = Oracle::new(keys, 2, 0).with_partition(cut(), vec![0, 1]);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    oracle.admitted(0, B, 12 * S, 0);
    oracle.view_changed(0, 0, B, 1, 1, 12 * S);
    assert!(oracle.convergence_ns.is_empty(), "pod 1 has not seen it");
    oracle.view_changed(1, 0, B, 1, 1, 12 * S + 7 * S);
    assert_eq!(oracle.convergence_ns, vec![7 * S]);
    let report = oracle.partition_report(10).expect("partition");
    let during = &report.during.convergence;
    assert_eq!(during.same_side.p50_ms, Some(0.0));
    assert_eq!(during.cross_side.p50_ms, Some(7_000.0));
    assert_eq!(report.before.convergence.cross_side.converged, 0);
}

/// A side with no running pod covers every hit vacuously.
#[test]
fn an_empty_side_does_not_hold_back_convergence() {
    let keys = Rc::new(KeySpace::build(&fixtures::quiet(2), 1));
    let mut oracle = Oracle::new(keys, 2, 0).with_partition(cut(), vec![0, 1]);
    oracle.node_started(0, 0, false);
    oracle.admitted(0, B, S, 0);
    oracle.view_changed(0, 0, B, 1, 1, S + 50 * MS);
    assert_eq!(oracle.convergence_ns, vec![50 * MS]);
    let before = oracle.partition_report(10).expect("partition").before;
    assert_eq!(before.convergence.same_side.converged, 1);
    assert_eq!(before.convergence.cross_side.converged, 0);
}

/// Pod 1 misses a bucket that closed before the heal until 500 ms after it.
#[test]
fn time_to_heal_counts_from_the_heal_until_nothing_is_missing() {
    let keys = Rc::new(KeySpace::build(&fixtures::quiet(2), 1));
    let mut oracle = Oracle::new(keys, 2, 0).with_partition(cut(), vec![0, 1]);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    oracle.admitted(0, B, 12 * S, 0);
    oracle.view_changed(0, 0, B, 1, 1, 12 * S);

    // The heal falls 500 ms into bucket B + 1, so bucket B closed before it.
    assert!(!oracle.heal_sample(20 * S, millis(500)));
    assert!(!oracle.heal_sample(20 * S + 400 * MS, millis(900)));
    oracle.view_changed(1, 0, B, 1, 1, 20 * S + 500 * MS);
    assert!(oracle.heal_sample(20 * S + 500 * MS, millis(1_000)));
    oracle.finish(30 * S);

    let report = oracle.partition_report(10).expect("partition");
    assert_eq!(report.time_to_heal_ms, Some(500.0));
    // One hit missing at one of two pods, for one tracked key.
    assert_eq!(report.deficit_at_heal_hits, Some(0.5));
}

#[test]
fn a_regime_without_a_partition_has_no_phases() {
    assert!(Schedule::of(&fixtures::tiny()).is_none());
    let keys = Rc::new(KeySpace::build(&fixtures::quiet(1), 1));
    let mut oracle = Oracle::new(keys, 1, 0);
    assert!(oracle.partition_report(10).is_none());
    assert!(oracle.heal_sample(S, S), "nothing to heal");
}
