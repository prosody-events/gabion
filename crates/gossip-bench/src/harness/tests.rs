use super::*;

#[test]
fn identical_inputs_give_bit_identical_results() {
    let regime = fixtures::tiny();
    let first = serde_json::to_string(&run(&regime, 11).expect("first run")).expect("json");
    let second = serde_json::to_string(&run(&regime, 11).expect("second run")).expect("json");
    assert_eq!(first, second);
    let other = serde_json::to_string(&run(&regime, 12).expect("other seed")).expect("json");
    assert_ne!(first, other);
}

#[test]
fn tiny_cluster_run_is_healthy_and_internally_consistent() {
    let regime = fixtures::tiny();
    let result = run(&regime, 3).expect("run");
    assert!(result.valid, "{:?}", result.invalid_reasons);
    assert_eq!(result.sizing.total_nodes, 7);
    assert_eq!(result.churn.joiners.len(), 2);

    let traffic = result.traffic.run;
    assert_eq!(
        traffic.offered,
        traffic.admitted + traffic.rejected + traffic.unrouted
    );
    assert!(traffic.rejected > 0, "the hot key must hit its limit");
    // A node's view never exceeds truth, so no reject can be false.
    assert_eq!(traffic.false_rejects, 0);
    assert!(result.staleness.limited.min.is_some_and(|m| m >= 0.0));
    assert!(result.staleness.sampled.min.is_some_and(|m| m >= 0.0));
    assert!(result.convergence.converged > 0);
    assert_eq!(result.staleness.limited.over_max, Some(0.0));
    assert_eq!(result.staleness.sampled.over_time_fraction, Some(0.0));
    assert_eq!(result.health.snapshot_failures, 0);
    assert_eq!(result.health.nodes_never_snapshotted, 0);
    assert!(result.flags.is_empty(), "{:?}", result.flags);

    let net = result.network.run;
    assert!(net.packets_sent > 0);
    let dropped = net.dropped_loss_packets
        + net.dropped_partition_packets
        + net.dropped_rcvbuf_packets
        + net.dropped_no_receiver_packets;
    // Every datagram is delivered or dropped exactly once, except the few
    // still in flight at the end.
    assert!(net.packets_delivered + dropped <= net.packets_sent);
    assert!(net.packets_sent - net.packets_delivered - dropped < 100);
    let loss = net.dropped_loss_packets as f64 / net.packets_sent as f64;
    assert!((0.02..0.08).contains(&loss), "loss {loss}");
}

#[test]
fn offered_load_does_not_depend_on_the_network() {
    let lossless = {
        let mut regime = fixtures::tiny();
        regime.network.loss = 0.0;
        run(&regime, 5).expect("lossless")
    };
    let lossy = {
        let mut regime = fixtures::tiny();
        regime.network.loss = 0.3;
        run(&regime, 5).expect("lossy")
    };
    assert_eq!(lossless.traffic.run.offered, lossy.traffic.run.offered);
    assert_eq!(
        lossless.traffic.measure.offered,
        lossy.traffic.measure.offered
    );
    let per_key =
        |r: &StudyResult| -> Vec<u64> { r.accuracy.iter().map(|a| a.measure.offered).collect() };
    assert_eq!(per_key(&lossless), per_key(&lossy));
    assert_ne!(
        lossless.network.run.packets_sent,
        lossy.network.run.packets_sent
    );
}

#[test]
fn every_regime_file_parses() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("regimes");
    let mut count = 0;
    for entry in std::fs::read_dir(&dir).expect("regimes directory") {
        let path = entry.expect("entry").path();
        let regime = Regime::load(&path).expect("regime loads");
        assert_eq!(
            path.file_stem().and_then(|s| s.to_str()),
            Some(&*regime.id),
            "file name matches id"
        );
        count += 1;
    }
    assert!(count >= 11);
}

/// Rollout replacements must not share a tick phase: with starts on exact
/// multiples of a 500 ms interval, every joiner of a 500 ms-tick solution
/// would burst at the same instant.
#[test]
fn rollout_joiners_start_out_of_phase() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("regimes");
    let regime = Regime::load(&dir.join("churn-rollout.json")).expect("regime loads");
    let plan = plan::Plan::build(&regime, 1);
    let phases: std::collections::BTreeSet<u64> = plan
        .nodes
        .iter()
        .filter(|n| !n.initial)
        .map(|n| n.start_ns % (500 * plan::NS_PER_MS))
        .collect();
    assert_eq!(phases.len(), 45, "every joiner has its own tick phase");
}

/// Shrunken dirty rings overflow; the run stays valid but the overflow is
/// attributed to the nodes that logged it and flagged.
#[test]
fn gabion_saturation_warnings_are_captured_and_flagged() {
    let regime = fixtures::tiny();
    let seed = 3;
    let plan = plan::Plan::build(&regime, seed);
    let keys = Rc::new(node::KeySpace::build(&regime, seed));
    let mut sizing = node::Sizing::new(&regime, &plan, &keys);
    sizing.local_dirty_capacity = 2;
    sizing.forwarded_dirty_capacity = 2;
    let world = Rc::new(driver::world_for(&regime, seed, &plan));
    let cells = sizing.cell_store_config();
    let launch = {
        let world = Rc::clone(&world);
        let keys = Rc::clone(&keys);
        move |idx| node::launch_node(&world, &keys, cells, idx, seed)
    };
    let result = driver::Driver::new(&regime, seed, plan, sizing, keys, world, launch)
        .run()
        .expect("run");
    assert!(result.valid, "{:?}", result.invalid_reasons);
    let warnings = &result.health.gabion_warnings;
    assert!(warnings.dirty_overflow_nodes >= 5, "{warnings:?}");
    assert!(warnings.dirty_overflow_lower_bound > 0);
    assert!(
        warnings
            .kinds
            .iter()
            .all(|k| k.nodes > 0 && k.target.starts_with("gabion"))
    );
    assert!(
        result
            .flags
            .iter()
            .any(|f| f.starts_with("dirty_ring_overflow"))
    );
}

/// A run with a partition reports the cut's phases; the cut shows up as
/// cross-side hits that never converge and a heal that takes time.
#[test]
fn partition_run_reports_phases_and_heals() {
    let regime = fixtures::partitioned();
    let result = run(&regime, 4).expect("run");
    assert!(result.valid, "{:?}", result.invalid_reasons);
    let report = result.partition.expect("partition report");
    assert_eq!((report.zone_az, report.at_s, report.heal_s), (0, 14, 30));
    let never = |p: &report::PhaseReport| p.convergence.cross_side.never_fraction;
    assert!(
        never(&report.during) > Some(0.25),
        "{:?}",
        never(&report.during)
    );
    assert!(
        never(&report.before) < Some(0.1),
        "{:?}",
        never(&report.before)
    );
    assert!(
        never(&report.after) < Some(0.1),
        "{:?}",
        never(&report.after)
    );
    let during = &report.during.staleness;
    assert!(during.in_zone.limited.mean > report.before.staleness.in_zone.limited.mean);
    assert!(report.deficit_at_heal_hits > Some(0.0));
    let heal_ms = report.time_to_heal_ms.expect("healed before the run ended");
    assert!((0.0..4_000.0).contains(&heal_ms), "{heal_ms}");
    // The three phases cover every measured hit, once per side.
    let phases = [&report.before, &report.during, &report.after];
    let sides: u64 = phases
        .iter()
        .map(|p| p.convergence.same_side.converged + p.convergence.cross_side.converged)
        .sum();
    assert!(sides >= 2 * result.convergence.converged);
    assert!(run(&fixtures::tiny(), 4).expect("run").partition.is_none());
}

#[test]
fn a_regime_may_cut_the_network_once_and_heal_after_it_starts() {
    let mut regime = fixtures::partitioned();
    regime.churn.partitions.push(regime.churn.partitions[0]);
    assert!(regime.validate().is_err());
    regime.churn.partitions.truncate(1);
    regime.churn.partitions[0].heal_s = 10;
    assert!(regime.validate().is_err());
}
