//! Each node decides a burst of one-hit requests in order, from its own
//! window, with gabion's own comparison; a rejected request is counted and
//! never recorded.

use gabion::rules::RuleSpec;
use quickcheck::{Arbitrary, Gen, QuickCheck};

use super::super::{SubStep, decide_burst};
use super::*;
use crate::event::Admission;

/// A warmed, settled cluster after each `(node, requests)` send, settling
/// after each.
async fn filled(state: &mut EngineState, sends: &[(u32, u64)]) {
    ready(state).await;
    for &(node, n) in sends {
        send(state, node, n).await;
        settle(state).await;
    }
}

/// The one burst decided in `batch`.
fn decided(batch: &EventBatch) -> Burst {
    match decisions(&batch.events)[..] {
        [(_, burst)] => burst,
        ref other => panic!("one decision, not {other:?}"),
    }
}

/// A burst is gabion's own comparison per request, saturation included: over
/// an empty table, for limits and totals at and around every edge, it decides
/// exactly what calling [`RuleSpec::admits`] once per request, each against
/// the window plus the requests admitted before it, decides.
#[test]
fn burst_decides_each_request_with_admits() {
    with_state(rate_limiting(), |state| async move {
        let table = state.nodes[0].aggregate.table();
        let (key, now) = (WATCHED, state.virtual_ms);
        for limit in [0, 1, 99, 100, 101, u64::MAX] {
            let spec = RuleSpec {
                limit,
                ..state.valid.spec
            };
            let mut totals = [
                0,
                1,
                limit.saturating_sub(1),
                limit,
                limit.saturating_add(1),
                u64::MAX - 1,
                u64::MAX,
            ];
            totals.sort_unstable();
            let mut last = None;
            for total in totals {
                if last.replace(total) == Some(total) {
                    continue;
                }
                for n in [1, 2, 25, 250] {
                    let mut admitted = 0;
                    for _ in 0..n {
                        if spec.admits(total.saturating_add(admitted), 1) {
                            admitted += 1;
                        }
                    }
                    let got = decide_burst(spec, table, key, now, total, requests(n));
                    let at = format!("limit {limit}, total {total}, {n} requests: {got:?}");
                    match NonZeroU64::new(n - admitted) {
                        None => assert_eq!(
                            got,
                            Burst::Admitted {
                                requests: requests(n),
                                total_before: total
                            },
                            "{at}"
                        ),
                        Some(rejected) => assert!(
                            matches!(
                                got,
                                Burst::Rejected { admitted: a, rejected: r, total: t, .. }
                                    if a == admitted
                                        && r == rejected
                                        && t == total.saturating_add(admitted)
                            ),
                            "{at}"
                        ),
                    }
                }
            }
        }
        state
    });
}

/// At limit 100 a burst admits exactly what the window leaves and rejects
/// the rest: 250 into a fresh window admits 100, 25 over 90 admits 10, and 25
/// over a full window admits none. Only the admitted requests are recorded
/// and counted admitted; a burst rejected whole changes nothing else.
#[test]
fn a_burst_admits_what_the_window_leaves() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let bucket = state.virtual_ms / 1_000 + 1;
        state
            .step(bucket * 1_000 + 1 - state.virtual_ms, StepUntil::Time)
            .await;
        let now = state.virtual_ms;
        assert_eq!(
            decided(&send(&mut state, 0, 250).await),
            Burst::Rejected {
                admitted: 100,
                rejected: requests(150),
                total: 100,
                reopens_ms: (bucket + 10) * 1_000 - now,
            }
        );
        settle(&mut state).await;
        let snap = state.snapshot(None, WATCHED).await;
        assert!(snap.nodes.iter().all(|n| n.window_total == 100), "{snap:?}");
        assert_eq!(snap.oracle_total, 100);
        assert_eq!((snap.admitted_hits, snap.rejected_hits), (100, 150));
        let origin = &snap.nodes[0];
        assert_eq!((origin.admitted_hits, origin.rejected_hits), (100, 150));
        state
    });

    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 30), (1, 30), (2, 30)]).await;
        let split = send(&mut state, 0, 25).await;
        assert!(
            matches!(
                decided(&split),
                Burst::Rejected {
                    admitted: 10,
                    total: 100,
                    rejected,
                    ..
                } if rejected == requests(15)
            ),
            "{split:?}"
        );
        settle(&mut state).await;
        let snap = state.snapshot(None, WATCHED).await;
        assert_eq!(snap.oracle_total, 100);
        assert_eq!((snap.admitted_hits, snap.rejected_hits), (100, 15));
        state
    });

    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 25), (1, 25), (2, 25)]).await;
        let admitted = send(&mut state, 2, 25).await;
        assert_eq!(
            decided(&admitted),
            Burst::Admitted {
                requests: requests(25),
                total_before: 75
            }
        );
        settle(&mut state).await;

        let before = state.snapshot(None, WATCHED).await;
        let rejected = send(&mut state, 2, 25).await;
        assert!(
            matches!(
                decided(&rejected),
                Burst::Rejected {
                    admitted: 0,
                    total: 100,
                    rejected,
                    ..
                } if rejected == requests(25)
            ),
            "{rejected:?}"
        );
        assert_eq!(rejected.events.len(), 1, "only the decision: {rejected:?}");
        assert!(
            matches!(state.sub_step, SubStep::Closed),
            "no packet is held"
        );
        let after = state.snapshot(None, WATCHED).await;
        assert_eq!(after.oracle_total, before.oracle_total);
        for (a, b) in after.nodes.iter().zip(&before.nodes) {
            assert_eq!(a.window_total, b.window_total, "node {}", a.id);
        }
        assert_eq!((after.admitted_hits, after.rejected_hits), (100, 25));
        assert_eq!(after.nodes[2].rejected_hits, 25);
        state
    });
}

/// `brim` is the one-hit decision: at 99 every node admits one more, at 100
/// none does.
#[test]
fn brim_is_the_one_hit_decision() {
    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 33), (1, 33), (2, 33)]).await;
        let snap = state.snapshot(None, WATCHED).await;
        assert!(snap.nodes.iter().all(|n| n.window_total == 99));
        assert!(
            snap.nodes
                .iter()
                .all(|n| n.brim == Admission::Admitted { total_before: 99 }),
            "{snap:?}"
        );

        send(&mut state, 0, 1).await;
        settle(&mut state).await;
        let snap = state.snapshot(None, WATCHED).await;
        assert!(
            snap.nodes
                .iter()
                .all(|n| matches!(n.brim, Admission::Rejected { total: 100, .. })),
            "{snap:?}"
        );
        state
    });
}

/// A script of bursts against one limit.
#[derive(Clone, Debug)]
struct Script {
    limit: u64,
    /// `(advance ms, requests, node)` of each burst.
    ops: Vec<(u64, u64, u32)>,
}

impl Arbitrary for Script {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = 1 + usize::arbitrary(g) % 16;
        Self {
            limit: 1 + u64::arbitrary(g) % 200,
            ops: (0..len)
                .map(|_| {
                    (
                        u64::arbitrary(g) % 3_001,
                        1 + u64::arbitrary(g) % 60,
                        u32::arbitrary(g) % 12,
                    )
                })
                .collect(),
        }
    }
}

/// The sliding window written out: admitted `(bucket, requests)` pairs, a
/// total over the ten buckets up to now, and the fall-off of the oldest
/// buckets that cover the excess.
#[derive(Default)]
struct Model {
    admitted: Vec<(u64, u64)>,
}

impl Model {
    fn total(&self, now: u64) -> u64 {
        let current = now / 1_000;
        self.admitted
            .iter()
            .filter(|&&(bucket, _)| current - bucket < 10)
            .map(|&(_, n)| n)
            .sum()
    }

    /// A burst of `n` one-hit requests, each admitted while the window plus
    /// the requests admitted before it leave room for one more: the model's
    /// own arithmetic, deliberately not `RuleSpec::admits`.
    fn decide(&mut self, now: u64, limit: u64, n: u64) -> Burst {
        let total = self.total(now);
        let mut admitted = 0;
        while admitted < n && total + admitted < limit {
            admitted += 1;
        }
        let current = now / 1_000;
        if admitted > 0 {
            self.admitted.push((current, admitted));
        }
        let Some(rejected) = NonZeroU64::new(n - admitted) else {
            return Burst::Admitted {
                requests: requests(n),
                total_before: total,
            };
        };
        let need = total + admitted + 1 - limit;
        let mut aged = 0;
        let mut reopens_ms = None;
        for bucket in current.saturating_sub(9)..=current {
            aged += self
                .admitted
                .iter()
                .filter(|&&(b, _)| b == bucket)
                .map(|&(_, n)| n)
                .sum::<u64>();
            if aged >= need {
                reopens_ms = Some((bucket + 10) * 1_000 - now);
                break;
            }
        }
        Burst::Rejected {
            admitted,
            rejected,
            total: total + admitted,
            reopens_ms: reopens_ms.expect("the window covers the excess"),
        }
    }
}

/// Run `script` on an engine from `config`, waiting `extra_ms` more before
/// each burst, and return each decision with the deciding node's
/// `window_total` after it, beside the model's.
fn run_script(config: SimConfig, script: &Script, extra_ms: u64) -> Vec<[(Burst, u64); 2]> {
    let nodes = config.nodes as u32;
    let mut out = Vec::new();
    with_state(config, |mut state| {
        let out = &mut out;
        async move {
            if nodes > 1 {
                warm(&mut state).await;
            }
            let mut model = Model::default();
            for &(advance, n, node) in &script.ops {
                let node = node % nodes;
                state.step(advance + extra_ms, StepUntil::Time).await;
                let now = state.virtual_ms;
                let want = model.decide(now, script.limit, n);
                let got = decided(&send(&mut state, node, n).await);
                let snap = state.snapshot(None, WATCHED).await;
                let admitted: u64 = snap.nodes.iter().map(|n| n.admitted_hits).sum();
                let rejected: u64 = snap.nodes.iter().map(|n| n.rejected_hits).sum();
                assert_eq!(admitted, snap.admitted_hits, "admitted across the nodes");
                assert_eq!(rejected, snap.rejected_hits, "rejected across the nodes");
                let view = snap.nodes[node as usize].window_total;
                out.push([(got, view), (want, model.total(now))]);
            }
            state
        }
    });
    out
}

/// Random scripts of bursts on one node, where gossip cannot lag, give the
/// sliding-window model's decisions, reopening times and views exactly; the
/// same scripts on twelve nodes, each burst one period plus 10 ms after the
/// last so every node agrees first, give the model's decisions on that
/// timeline.
#[test]
fn admission_matches_a_sliding_window_model() {
    fn one_node(script: Script) -> bool {
        let config = SimConfig {
            nodes: 1,
            tick_interval_ms: P,
            rule_window_ms: 10_000,
            rule_limit: script.limit,
            ..SimConfig::default()
        };
        run_script(config, &script, 0)
            .iter()
            .all(|[got, want]| got == want)
    }
    fn twelve_nodes(script: Script) -> bool {
        let config = SimConfig {
            rule_limit: script.limit,
            ..rate_limiting()
        };
        run_script(config, &script, P + 10)
            .iter()
            .all(|[got, want]| got.0 == want.0)
    }
    let started = std::time::Instant::now();
    QuickCheck::new()
        .tests(100)
        .quickcheck(one_node as fn(Script) -> bool);
    eprintln!("one node: {:?}", started.elapsed());
    let started = std::time::Instant::now();
    QuickCheck::new()
        .tests(20)
        .quickcheck(twelve_nodes as fn(Script) -> bool);
    eprintln!("twelve nodes: {:?}", started.elapsed());
}

/// Two nodes decide alone, each from what it has heard: at one instant, a
/// node that has not yet heard the other's burst admits its own.
#[test]
fn two_nodes_decide_alone_at_one_instant() {
    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 25), (1, 25), (2, 25)]).await;
        let g = gateway_of(&state, 0, 2).await;
        let x = members(&state, 2)
            .into_iter()
            .find(|&x| x != g)
            .expect("a zone-c node other than the gateway");
        let first = send(&mut state, 0, 25).await;
        let second = send(&mut state, x, 25).await;
        let admitted = Burst::Admitted {
            requests: requests(25),
            total_before: 75,
        };
        assert_eq!(decided(&first), admitted);
        assert_eq!(decisions(&second.events), [(x, admitted)]);
        assert_eq!(state.snapshot(None, WATCHED).await.oracle_total, 125);
        state
    });
}

/// The Rate limiting script: four sends of 25 fill the window exactly, the
/// next two are rejected whole, and every node counts what it admitted and
/// rejected.
#[test]
fn rate_limiting_script_and_counters() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let mut all = Vec::new();
        for node in [0, 1, 2, 2] {
            all.extend(send(&mut state, node, 25).await.events);
            settle(&mut state).await;
        }
        let full = state.snapshot(None, WATCHED).await;
        assert!(
            full.nodes
                .iter()
                .all(|n| matches!(n.brim, Admission::Rejected { .. })),
            "{full:?}"
        );
        for node in [2, 3] {
            all.extend(send(&mut state, node, 25).await.events);
        }
        let decided = decisions(&all);
        let admitted = decided
            .iter()
            .map(|(_, b)| matches!(b, Burst::Admitted { .. }))
            .collect::<Vec<_>>();
        assert_eq!(admitted, [true, true, true, true, false, false]);
        for (_, refused) in &decided[4..] {
            assert!(
                matches!(
                    refused,
                    Burst::Rejected {
                        admitted: 0,
                        total: 100,
                        rejected,
                        ..
                    } if *rejected == requests(25)
                ),
                "{refused:?}"
            );
        }
        let snap = state.snapshot(None, WATCHED).await;
        assert_eq!((snap.admitted_hits, snap.rejected_hits), (100, 50));
        let counts: Vec<_> = snap.nodes[..4]
            .iter()
            .map(|n| (n.admitted_hits, n.rejected_hits))
            .collect();
        assert_eq!(counts, [(25, 0), (25, 0), (50, 25), (0, 25)]);
        state
    });
}

/// The cluster's counters outlive the node that counted: a removal never
/// makes them fall.
#[test]
fn cluster_counters_survive_a_removal() {
    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 25)]).await;
        send(&mut state, 0, 100).await;
        state.remove_node(0).await.unwrap();
        let snap = state.snapshot(None, WATCHED).await;
        assert_eq!((snap.admitted_hits, snap.rejected_hits), (100, 25));
        assert!(snap.nodes.iter().all(|n| n.id != 0));
        state
    });
}

/// A burst whose node's runtime has stopped is refused with an error, and
/// nothing reports it decided or counts it.
#[test]
fn a_stopped_runtime_reports_no_decision() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        state.nodes[0].join.abort();
        state.drain_pending().await;
        let refused = state
            .submit_request(0, KeyHash(WATCHED_KEY), requests(25), StepUntil::Time)
            .await;
        assert!(matches!(refused, Err(EngineError::Gossip { node: 0, .. })));
        assert!(
            matches!(state.sub_step, SubStep::Closed),
            "a refused record closes its sub-step"
        );
        let next = state.step(0, StepUntil::Time).await;
        assert_eq!(decisions(&next.events), []);
        let snap = state.snapshot(None, WATCHED).await;
        assert_eq!((snap.admitted_hits, snap.nodes[0].admitted_hits), (0, 0));
        state
    });
}
