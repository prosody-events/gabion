//! Membership timeline: which pods exist when, and when each pod learns of
//! each other pod. Computed once per run from the regime and the churn
//! stream, before any node runs, so it is identical for every solution.
//!
//! Three intervals matter per pod:
//! * **process**: `[start, stop)` — the gossip task runs.
//! * **load balancer**: the same interval — requests are routed to the pod only
//!   while its process runs. A crashed pod is pulled from rotation at the crash
//!   (requests are re-routed rather than failed).
//! * **membership**: `[0 or start, stop + detection)` — the pod is listed by
//!   discovery. Initial pods are listed from time zero even before their
//!   process starts; a crashed pod stays listed for [`CRASH_DETECTION_NS`].

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use super::regime::Regime;
use super::rng::{Rng, Stream};

pub const NS_PER_MS: u64 = 1_000_000;
pub const NS_PER_S: u64 = 1_000_000_000;
/// Initial pods start uniformly within this interval so their gossip ticks
/// are not phase-aligned.
pub const INITIAL_START_SPREAD_NS: u64 = NS_PER_S;
/// Each pod learns of a membership change after `U(0, this)`.
pub const DISCOVERY_JITTER_NS: u64 = 2 * NS_PER_S;
/// A silently crashed pod stays in discovery this long.
pub const CRASH_DETECTION_NS: u64 = 10 * NS_PER_S;
pub const AVAILABILITY_ZONES: u8 = 3;
const GOSSIP_PORT: u16 = 7946;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopKind {
    Graceful,
    Crash,
}

#[derive(Clone, Copy, Debug)]
pub struct Stop {
    pub at_ns: u64,
    pub kind: StopKind,
}

#[derive(Clone, Copy, Debug)]
pub struct NodePlan {
    pub start_ns: u64,
    pub stop: Option<Stop>,
    pub az: u8,
    pub initial: bool,
}

impl NodePlan {
    fn member_from(&self) -> u64 {
        if self.initial { 0 } else { self.start_ns }
    }

    fn member_until(&self) -> u64 {
        match self.stop {
            None => u64::MAX,
            Some(Stop {
                at_ns,
                kind: StopKind::Graceful,
            }) => at_ns,
            Some(Stop {
                at_ns,
                kind: StopKind::Crash,
            }) => at_ns.saturating_add(CRASH_DETECTION_NS),
        }
    }

    fn is_member_at(&self, t: u64) -> bool {
        self.member_from() <= t && t < self.member_until()
    }

    fn is_running_at(&self, t: u64) -> bool {
        self.start_ns <= t && self.stop.is_none_or(|s| t < s.at_ns)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanEvent {
    Start(usize),
    Stop(usize, StopKind),
    /// Tell pod `to` that pod `about` was added (`true`) or removed.
    Discover {
        to: usize,
        about: usize,
        added: bool,
    },
}

#[derive(Debug)]
pub struct Plan {
    pub nodes: Vec<NodePlan>,
    /// Sorted by time; ties keep generation order.
    pub events: Vec<(u64, PlanEvent)>,
    pub max_running: usize,
}

pub fn node_addr(index: usize) -> SocketAddr {
    let az = (index % AVAILABILITY_ZONES as usize) as u8;
    let ip = Ipv4Addr::new(10, az, (index >> 8) as u8, (index & 0xFF) as u8);
    SocketAddr::V4(SocketAddrV4::new(ip, GOSSIP_PORT))
}

impl Plan {
    pub fn build(regime: &Regime, seed: u64) -> Self {
        let mut rng = Rng::stream(seed, Stream::Churn, 0);
        let mut nodes: Vec<NodePlan> = (0..regime.nodes as usize)
            .map(|i| NodePlan {
                start_ns: rng.below(INITIAL_START_SPREAD_NS),
                stop: None,
                az: (i % AVAILABILITY_ZONES as usize) as u8,
                initial: true,
            })
            .collect();

        for rollout in &regime.churn.rollouts {
            for i in 0..u64::from(rollout.count) {
                let start_ns = rollout.at_s * NS_PER_S + i * rollout.interval_ms * NS_PER_MS;
                let index = nodes.len();
                nodes.push(NodePlan {
                    start_ns,
                    stop: None,
                    az: (index % AVAILABILITY_ZONES as usize) as u8,
                    initial: false,
                });
                if let Some(victim) = nodes.iter_mut().find(|n| n.initial && n.stop.is_none()) {
                    victim.stop = Some(Stop {
                        at_ns: start_ns + rollout.overlap_ms * NS_PER_MS,
                        kind: StopKind::Graceful,
                    });
                }
            }
        }

        for crash in &regime.churn.crashes {
            let at_ns = crash.at_s * NS_PER_S;
            let candidates: Vec<usize> = (0..nodes.len())
                .filter(|&i| nodes[i].start_ns <= at_ns && nodes[i].stop.is_none())
                .collect();
            if candidates.is_empty() {
                continue;
            }
            let victim = candidates[rng.below(candidates.len() as u64) as usize];
            nodes[victim].stop = Some(Stop {
                at_ns,
                kind: StopKind::Crash,
            });
        }

        let mut events = Vec::new();
        for (j, plan) in nodes.iter().enumerate() {
            events.push((plan.start_ns, PlanEvent::Start(j)));
            if let Some(stop) = plan.stop {
                events.push((stop.at_ns, PlanEvent::Stop(j, stop.kind)));
            }
        }
        // Discovery. A starting pod lists every current member at once; the
        // others learn of a joiner, and everyone learns of a departure, after
        // independent jitter.
        for (j, plan) in nodes.iter().enumerate() {
            let t = plan.start_ns;
            for (i, other) in nodes.iter().enumerate() {
                if i != j && other.is_member_at(t) {
                    events.push((
                        t,
                        PlanEvent::Discover {
                            to: j,
                            about: i,
                            added: true,
                        },
                    ));
                }
            }
            if !plan.initial {
                for (i, other) in nodes.iter().enumerate() {
                    if i != j && other.is_member_at(t) {
                        let at = t + rng.below(DISCOVERY_JITTER_NS);
                        events.push((
                            at,
                            PlanEvent::Discover {
                                to: i,
                                about: j,
                                added: true,
                            },
                        ));
                    }
                }
            }
            if plan.stop.is_some() {
                let t = plan.member_until();
                for (i, other) in nodes.iter().enumerate() {
                    if i != j && other.is_member_at(t) {
                        let at = t + rng.below(DISCOVERY_JITTER_NS);
                        events.push((
                            at,
                            PlanEvent::Discover {
                                to: i,
                                about: j,
                                added: false,
                            },
                        ));
                    }
                }
            }
        }
        events.sort_by_key(|(t, _)| *t);

        // The running count only rises at a start, so sampling there suffices.
        let max_running = nodes
            .iter()
            .map(|n| nodes.iter().filter(|m| m.is_running_at(n.start_ns)).count())
            .max()
            .unwrap_or(0);

        Self {
            nodes,
            events,
            max_running,
        }
    }

    /// Integral of the running-pod count over `[from, to)`, in pod-seconds.
    pub fn node_seconds(&self, from: u64, to: u64) -> f64 {
        self.nodes
            .iter()
            .map(|n| {
                let start = n.start_ns.max(from);
                let stop = n.stop.map_or(to, |s| s.at_ns.min(to));
                stop.saturating_sub(start) as f64 / NS_PER_S as f64
            })
            .sum()
    }
}
