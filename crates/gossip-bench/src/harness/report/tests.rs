use super::*;

/// Error +2 for 1 s, −3 (an over-count) for 1 s, 0 for 2 s.
#[test]
fn dist_separates_under_and_over_count() {
    let mut hist = WeightedHist::default();
    hist.add(2, 1_000_000_000);
    hist.add(-3, 1_000_000_000);
    hist.add(0, 2_000_000_000);
    let dist = Dist::from_hist(&hist, 10);
    assert_eq!(dist.weight, 4.0);
    assert_eq!(dist.mean, Some(-0.25));
    assert_eq!(dist.under_mean, Some(0.5));
    assert_eq!(dist.over_mean, Some(0.75));
    assert_eq!(dist.over_max, Some(3.0));
    assert_eq!(dist.over_time_fraction, Some(0.25));
    assert_eq!(dist.max_pct_limit, Some(20.0));
}

/// 98 hits converge in 1..=98 ms; two never do, censored at 4 s and 5 s.
#[test]
fn convergence_ranks_never_converged_hits_at_their_censoring_time() {
    let latencies: Vec<u64> = (1..=98).map(|ms| ms * 1_000_000).collect();
    let report = ConvergenceReport::new(latencies, &[4_000_000_000, 5_000_000_000], 7);
    assert_eq!((report.converged, report.never_converged), (98, 2));
    assert_eq!(report.pending_at_end, 7);
    assert_eq!(report.never_fraction, Some(0.02));
    assert_eq!(report.p99_ms, Some(98.0));
    assert_eq!(report.max_ms, Some(98.0));
    assert_eq!(report.p50_incl_never_ms, Some(50.0));
    assert_eq!(report.p99_incl_never_ms, Some(4_000.0));
    let mean = (98.0 * 99.0 / 2.0 + 9_000.0) / 100.0;
    assert_eq!(report.mean_incl_never_ms, Some(mean));
}
