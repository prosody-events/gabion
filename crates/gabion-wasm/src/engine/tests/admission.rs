//! Each node admits or rejects a request whole, from its own window, with
//! gabion's own comparison; a rejected request is counted and never recorded.

use quickcheck::{Arbitrary, Gen, QuickCheck};

use super::super::SubStep;
use super::*;

/// A warmed, settled cluster after each `(node, hits)` send, settling after
/// each.
async fn filled(state: &mut EngineState, sends: &[(u32, u64)]) {
    ready(state).await;
    for &(node, hits) in sends {
        send(state, node, hits).await;
        settle(state).await;
    }
}

/// The one decision in `batch`.
fn decided(batch: &EventBatch) -> Admission {
    match decisions(&batch.events)[..] {
        [(_, admission)] => admission,
        ref other => panic!("one decision, not {other:?}"),
    }
}

/// At limit 100, 75 + 25 is admitted, 100 + 25 and 90 + 25 are rejected
/// whole, and a rejection counts the hits rejected and changes nothing else.
#[test]
fn admission_boundaries_at_limit_100() {
    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 25), (1, 25), (2, 25)]).await;
        let admitted = send(&mut state, 2, 25).await;
        assert_eq!(decided(&admitted), Admission::Admitted { total_before: 75 });
        settle(&mut state).await;

        let before = state.snapshot(None, WATCH).await;
        let rejected = send(&mut state, 2, 25).await;
        assert!(
            matches!(
                decided(&rejected),
                Admission::Rejected {
                    total: 100,
                    reopens: Reopens::In { .. }
                }
            ),
            "{rejected:?}"
        );
        assert_eq!(rejected.events.len(), 1, "only the decision: {rejected:?}");
        assert!(
            matches!(state.sub_step, SubStep::Closed),
            "no packet is held"
        );
        let after = state.snapshot(None, WATCH).await;
        assert_eq!(after.oracle_total, before.oracle_total);
        for (a, b) in after.nodes.iter().zip(&before.nodes) {
            assert_eq!(a.window_total, b.window_total, "node {}", a.id);
        }
        assert_eq!((after.admitted_hits, after.rejected_hits), (100, 25));
        assert_eq!(after.nodes[2].rejected_hits, 25);
        state
    });

    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 30), (1, 30), (2, 30)]).await;
        let rejected = send(&mut state, 0, 25).await;
        assert!(
            matches!(decided(&rejected), Admission::Rejected { total: 90, .. }),
            "{rejected:?}"
        );
        assert_eq!(state.snapshot(None, WATCH).await.oracle_total, 90);
        state
    });
}

/// With every hit in one bucket, a rejected request reopens at that bucket's
/// fall-off; a request heavier than the limit never does.
#[test]
fn reopening_time_and_never() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let bucket = state.virtual_ms / 1_000 + 1;
        state
            .step(bucket * 1_000 + 1 - state.virtual_ms, StepUntil::Time)
            .await;
        for node in [0, 1, 2, 3] {
            send(&mut state, node, 25).await;
            state.step(10, StepUntil::Time).await;
        }
        let now = state.virtual_ms;
        assert_eq!(now / 1_000, bucket, "every hit in one bucket");
        let rejected = send(&mut state, 2, 25).await;
        assert_eq!(
            decided(&rejected),
            Admission::Rejected {
                total: 100,
                reopens: Reopens::In {
                    ms: (bucket + 10) * 1_000 - now
                }
            }
        );
        state
    });

    with_state(rate_limiting(), |mut state| async move {
        warm(&mut state).await;
        let never = Admission::Rejected {
            total: 0,
            reopens: Reopens::Never,
        };
        assert_eq!(decided(&send(&mut state, 0, 150).await), never);
        state.step(30_000, StepUntil::Time).await;
        assert_eq!(decided(&send(&mut state, 0, 150).await), never);
        state
    });
}

/// `brim` is the one-hit decision, distinct from the inspected node's
/// decision on the probe's weight: at 99 every node admits one more hit but
/// not 25, and at 100 none admits even one.
#[test]
fn brim_is_the_one_hit_decision() {
    with_state(rate_limiting(), |mut state| async move {
        filled(&mut state, &[(0, 33), (1, 33), (2, 33)]).await;
        let snap = state.snapshot(Some(4), watch(25)).await;
        assert!(snap.nodes.iter().all(|n| n.window_total == 99));
        assert!(
            snap.nodes
                .iter()
                .all(|n| n.brim == Admission::Admitted { total_before: 99 }),
            "{snap:?}"
        );
        let next = snap.inspected.expect("node 4 is live").next_send;
        assert!(
            matches!(next, Admission::Rejected { total: 99, .. }),
            "{next:?}"
        );

        send(&mut state, 0, 1).await;
        settle(&mut state).await;
        let snap = state.snapshot(Some(4), watch(25)).await;
        assert!(
            snap.nodes
                .iter()
                .all(|n| matches!(n.brim, Admission::Rejected { total: 100, .. })),
            "{snap:?}"
        );
        state
    });
}

/// A script of requests against one limit.
#[derive(Clone, Debug)]
struct Script {
    limit: u64,
    /// `(advance ms, weight, node)` of each request.
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

/// The sliding window written out: admitted `(bucket, hits)` pairs, a total
/// over the ten buckets up to now, and the fall-off of the oldest buckets
/// that cover the excess.
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
            .map(|&(_, hits)| hits)
            .sum()
    }

    fn decide(&mut self, now: u64, limit: u64, hits: u64) -> Admission {
        let total = self.total(now);
        if total + hits <= limit {
            self.admitted.push((now / 1_000, hits));
            return Admission::Admitted {
                total_before: total,
            };
        }
        let reopens = if hits > limit {
            Reopens::Never
        } else {
            let need = total + hits - limit;
            let current = now / 1_000;
            let mut aged = 0;
            let mut ms = None;
            for bucket in current.saturating_sub(9)..=current {
                aged += self
                    .admitted
                    .iter()
                    .filter(|&&(b, _)| b == bucket)
                    .map(|&(_, hits)| hits)
                    .sum::<u64>();
                if aged >= need {
                    ms = Some((bucket + 10) * 1_000 - now);
                    break;
                }
            }
            Reopens::In {
                ms: ms.expect("the window covers the excess"),
            }
        };
        Admission::Rejected { total, reopens }
    }
}

/// Run `script` on an engine from `config`, waiting `extra_ms` more before
/// each request, and return each decision with the deciding node's
/// `window_total` after it, beside the model's.
fn run_script(config: SimConfig, script: &Script, extra_ms: u64) -> Vec<[(Admission, u64); 2]> {
    let nodes = config.nodes as u32;
    let mut out = Vec::new();
    with_state(config, |mut state| {
        let out = &mut out;
        async move {
            if nodes > 1 {
                warm(&mut state).await;
            }
            let mut model = Model::default();
            for &(advance, hits, node) in &script.ops {
                let node = node % nodes;
                state.step(advance + extra_ms, StepUntil::Time).await;
                let now = state.virtual_ms;
                let want = model.decide(now, script.limit, hits);
                let got = decided(&send(&mut state, node, hits).await);
                let snap = state.snapshot(None, WATCH).await;
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

/// Random scripts of requests on one node, where gossip cannot lag, give the
/// sliding-window model's decisions, reopening times and views exactly; the
/// same scripts on twelve nodes, each request one period plus 10 ms after the
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
/// node that has not yet heard the other's request admits its own.
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
        let admitted = Admission::Admitted { total_before: 75 };
        assert_eq!(decided(&first), admitted);
        assert_eq!(decisions(&second.events), [(x, admitted)]);
        assert_eq!(state.snapshot(None, WATCH).await.oracle_total, 125);
        state
    });
}

/// The Rate limiting script: four sends of 25 fill the window, the next two
/// are rejected, and every node counts what it admitted and rejected.
#[test]
fn rate_limiting_script_and_counters() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let mut all = Vec::new();
        for node in [0, 1, 2, 2] {
            all.extend(send(&mut state, node, 25).await.events);
            settle(&mut state).await;
        }
        let full = state.snapshot(None, WATCH).await;
        assert!(
            full.nodes
                .iter()
                .all(|n| matches!(n.brim, Admission::Rejected { .. })),
            "{full:?}"
        );
        for node in [2, 3] {
            all.extend(send(&mut state, node, 25).await.events);
        }
        let admitted = decisions(&all)
            .iter()
            .map(|(_, a)| matches!(a, Admission::Admitted { .. }))
            .collect::<Vec<_>>();
        assert_eq!(admitted, [true, true, true, true, false, false]);
        let snap = state.snapshot(None, WATCH).await;
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
    with_state(propagation(), |mut state| async move {
        filled(&mut state, &[(0, 25)]).await;
        let limit = state.spec.limit;
        send(&mut state, 0, limit + 1).await;
        state.remove_node(0).await.unwrap();
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.admitted_hits, 25);
        assert_eq!(snap.rejected_hits, limit + 1);
        assert!(snap.nodes.iter().all(|n| n.id != 0));
        state
    });
}

/// A request whose node's runtime has stopped is refused with an error, and
/// nothing reports it decided or counts it.
#[test]
fn a_stopped_runtime_reports_no_decision() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        state.nodes[0].join.abort();
        state.drain_pending().await;
        let refused = state
            .submit_request(0, KeyHash(WATCHED_KEY), hits(25))
            .await;
        assert!(matches!(refused, Err(EngineError::Gossip { node: 0, .. })));
        assert!(
            matches!(state.sub_step, SubStep::Closed),
            "a refused record closes its sub-step"
        );
        let next = state.step(0, StepUntil::Time).await;
        assert_eq!(decisions(&next.events), []);
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!((snap.admitted_hits, snap.nodes[0].admitted_hits), (0, 0));
        state
    });
}
