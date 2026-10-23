use super::*;
use crate::harness::fixtures;
use crate::harness::plan::NS_PER_S;

const MS: u64 = NS_PER_MS;
/// An arbitrary bucket; the quiet fixture's buckets are 1 s wide.
const B: BucketEpoch = 100;

fn quiet_keys() -> Rc<KeySpace> {
    Rc::new(KeySpace::build(&fixtures::quiet(2), 1))
}

#[test]
fn weighted_hist_quantiles_and_mean() {
    let mut hist = WeightedHist::default();
    hist.add(2, 1_000);
    hist.add(0, 3_000);
    hist.add(5, 0);
    assert_eq!(hist.total(), 4_000);
    assert_eq!(hist.mean(), Some(0.5));
    assert_eq!(hist.quantile(0.5), Some(0));
    assert_eq!(hist.quantile(0.75), Some(0));
    assert_eq!(hist.quantile(0.76), Some(2));
    assert_eq!(hist.quantile(0.99), Some(2));
    assert_eq!((hist.min(), hist.max()), (Some(0), Some(2)));
    assert_eq!(WeightedHist::default().quantile(0.5), None);
}

/// Two nodes, one hit. Node 0 sees it after 100 ms, node 1 after 500 ms.
/// Over 2 s of two nodes, the error is 1 for 0.6 s and 0 for 3.4 s, and the
/// hit converges in 500 ms.
#[test]
fn staleness_and_convergence_on_a_hand_checked_trace() {
    let keys = quiet_keys();
    assert_eq!(keys.tracked, vec![0]);
    let mut oracle = Oracle::new(keys, 2, 0);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    oracle.admitted(0, B, NS_PER_S, 0);
    oracle.view_changed(0, 0, B, 1, 1, 1_100 * MS);
    assert!(oracle.convergence_ns.is_empty());
    oracle.view_changed(1, 0, B, 1, 1, 1_500 * MS);
    oracle.finish(2 * NS_PER_S);

    let hist = &oracle.staleness[Class::Sampled as usize];
    assert_eq!(hist.total(), 4 * u128::from(NS_PER_S));
    assert_eq!(hist.mean(), Some(0.15));
    assert_eq!(hist.quantile(0.5), Some(0));
    assert_eq!(hist.quantile(0.99), Some(1));
    assert_eq!(oracle.staleness[Class::Limited as usize].total(), 0);
    assert_eq!(oracle.convergence_ns, vec![500 * MS]);
    assert_eq!((oracle.censored_ns.len(), oracle.pending()), (0, 0));
}

#[test]
fn measurement_window_excludes_warmup_exposure_and_hits() {
    let mut oracle = Oracle::new(quiet_keys(), 1, NS_PER_S);
    oracle.node_started(0, 0, false);
    oracle.admitted(0, B, 500 * MS, 0);
    oracle.view_changed(0, 0, B, 1, 1, 700 * MS);
    oracle.admitted(0, B, 1_200 * MS, 0);
    oracle.view_changed(0, 0, B, 2, 2, 1_500 * MS);
    oracle.finish(2 * NS_PER_S);
    // Only the second hit is measured; only [1 s, 2 s) is exposure.
    assert_eq!(oracle.convergence_ns, vec![300 * MS]);
    let hist = &oracle.staleness[Class::Sampled as usize];
    assert_eq!(hist.total(), u128::from(NS_PER_S));
    assert_eq!(hist.mean(), Some(0.3));
}

#[test]
fn a_hit_whose_bucket_leaves_the_window_unseen_never_converges() {
    let keys = quiet_keys();
    let live = keys.live_buckets;
    let mut oracle = Oracle::new(keys, 2, 0);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    oracle.admitted(0, B, NS_PER_S, 0);
    oracle.view_changed(0, 0, B, 1, 1, NS_PER_S);
    assert_eq!(oracle.pending(), 1);
    oracle.bucket_boundary(5 * NS_PER_S, 0, B + live, &[None, None]);
    // Censored at the boundary that retired its bucket, 4 s after admission.
    assert_eq!(
        (oracle.censored_ns.clone(), oracle.pending()),
        (vec![4 * NS_PER_S], 0)
    );
    assert!(oracle.convergence_ns.is_empty());
}

#[test]
fn a_stopped_laggard_no_longer_blocks_convergence() {
    let mut oracle = Oracle::new(quiet_keys(), 2, 0);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    oracle.admitted(0, B, NS_PER_S, 0);
    oracle.view_changed(0, 0, B, 1, 1, NS_PER_S);
    oracle.node_stopped(1, 2 * NS_PER_S);
    assert_eq!(oracle.convergence_ns, vec![NS_PER_S]);
}

#[test]
fn residual_counts_only_buckets_closed_long_enough() {
    let mut oracle = Oracle::new(quiet_keys(), 2, 0);
    oracle.node_started(0, 0, false);
    oracle.node_started(1, 0, false);
    for _ in 0..3 {
        oracle.admitted(0, B, NS_PER_S, 0);
    }
    oracle.view_changed(0, 0, B, 3, 3, NS_PER_S);
    oracle.view_changed(1, 0, B, 1, 1, NS_PER_S);
    let closed_at = (u64::from(B) + 1) * 1_000;
    // 1 ms short of the residual age: nothing counts yet.
    oracle.residual_sample(closed_at + RESIDUAL_AGE_MS - 1);
    assert_eq!(oracle.residual[Class::Sampled as usize], vec![0, 0]);
    oracle.residual_sample(closed_at + RESIDUAL_AGE_MS);
    assert_eq!(oracle.residual[Class::Sampled as usize], vec![0, 0, 0, 2]);
}

#[test]
fn joiner_time_to_coverage_and_undercount_integral() {
    let mut oracle = Oracle::new(quiet_keys(), 2, 0);
    oracle.node_started(0, 0, false);
    for _ in 0..4 {
        oracle.admitted(0, B, NS_PER_S, 0);
    }
    oracle.view_changed(0, 0, B, 4, 4, NS_PER_S);
    oracle.node_started(1, 2 * NS_PER_S, true);
    oracle.view_changed(1, 0, B, 3, 3, 2_100 * MS);
    oracle.view_changed(1, 0, B, 4, 4, 2_300 * MS);
    oracle.finish(3 * NS_PER_S);
    let joiner = oracle.finished_joiners[0];
    assert_eq!(joiner.node, 1);
    // 75 % covered at +100 ms, 100 % at +300 ms.
    assert_eq!(joiner.t95_ms, Some(300.0));
    assert_eq!(joiner.t99_ms, Some(300.0));
    // The established pod is exact, so parity means a zero under-count.
    assert_eq!(joiner.t_parity_ms, Some(300.0));
    // 4 hits for 100 ms, then 1 hit for 200 ms.
    assert!((joiner.undercount_hit_s - 0.6).abs() < 1e-9);
}

#[test]
fn joiner_parity_is_relative_to_established_pods() {
    let mut oracle = Oracle::new(quiet_keys(), 2, 0);
    oracle.node_started(0, 0, false);
    for _ in 0..4 {
        oracle.admitted(0, B, NS_PER_S, 0);
    }
    // The established pod lags by 2; the joiner reaches parity at a lag of 2
    // and never reaches 95 % of truth.
    oracle.view_changed(0, 0, B, 2, 2, NS_PER_S);
    oracle.node_started(1, 2 * NS_PER_S, true);
    oracle.view_changed(1, 0, B, 1, 1, 2_100 * MS);
    oracle.view_changed(1, 0, B, 2, 2, 2_400 * MS);
    oracle.finish(3 * NS_PER_S);
    let joiner = oracle.finished_joiners[0];
    assert_eq!(joiner.t_parity_ms, Some(400.0));
    assert_eq!(joiner.t95_ms, None);
}

#[test]
fn over_admission_samples_the_trailing_window_of_limited_keys() {
    let regime = fixtures::tiny();
    let keys = Rc::new(KeySpace::build(&regime, 1));
    let hot = regime.site_keys();
    assert_eq!(keys.kinds[hot as usize], KeyKind::Hot);
    let t = keys.tracked_of[hot as usize] as usize;
    let limit = keys.limit;
    let mut oracle = Oracle::new(keys, 1, 0);
    for _ in 0..limit + 3 {
        oracle.admitted(hot, B, NS_PER_S, 0);
    }
    oracle.bucket_boundary(2 * NS_PER_S, 0, B + 1, &[None]);
    let samples = oracle.over_admission[t];
    assert_eq!(samples.windows, 1);
    assert_eq!(samples.over_windows, 1);
    assert_eq!(samples.over_max, 3);
    assert_eq!(oracle.truth_window_total(hot, B), limit + 3);
}
