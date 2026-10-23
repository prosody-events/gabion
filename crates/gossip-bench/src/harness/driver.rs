//! The event loop: pops the next event, applies it, then polls every node
//! it woke until the cluster is quiescent at that instant.

use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use anyhow::Result;
use gabion::crdt::BucketEpoch;
use gabion::gossip::{AdminCommand, GossipClient, GossipError};
use tokio::sync::{mpsc, oneshot};

use super::net::Network;
use super::node::{KeySpace, Launched, NodeStore, Sizing};
use super::oracle::{Class, Oracle};
use super::plan::{NS_PER_MS, NS_PER_S, Plan, PlanEvent, node_addr};
use super::regime::Regime;
use super::report::{
    ChurnReport, ConvergenceReport, Dist, Health, KeyAccuracy, NetworkReport, SCHEMA, SimStats,
    StalenessReport, StudyResult, TrafficCounts, TrafficReport, Windows,
};
use super::warnings::{NO_NODE, Warnings};
use super::workload::KeyProcess;
use super::world::{Event, Tasks, World, peer_event};

const RESIDUAL_PERIOD_NS: u64 = NS_PER_S;
const RESIDUAL_PHASE_NS: u64 = 500 * NS_PER_MS;
const SNAPSHOT_PERIOD_NS: u64 = 5 * NS_PER_S;
const SNAPSHOT_PHASE_NS: u64 = 250 * NS_PER_MS;
/// Polls of a record future before the hit counts as lost. The first poll
/// enqueues the request, the node then applies it, the second poll sees the
/// acknowledgement.
const RECORD_ATTEMPTS: usize = 4;

struct NodeHandle {
    client: GossipClient<u32>,
    admin: mpsc::Sender<AdminCommand>,
}

/// Health counters reported by one node's latest admin snapshot.
#[derive(Clone, Copy, Default)]
struct NodeHealth {
    /// A snapshot was requested from this node.
    asked: bool,
    /// The node answered at least one snapshot.
    answered: bool,
    cell_store_full_rejects: u64,
    rule_dictionary_full_rejects: u64,
    node_dictionary_full_rejects: u64,
    decode_rejects: u64,
}

pub struct Driver<'r, F, L> {
    regime: &'r Regime,
    seed: u64,
    plan: Plan,
    sizing: Sizing,
    keys: Rc<KeySpace>,
    world: Rc<World>,
    launch: L,
    tasks: Tasks<F>,
    handles: Vec<Option<NodeHandle>>,
    stores: Vec<Option<Rc<NodeStore>>>,
    stopped: Vec<bool>,
    /// Running pods in index order: the load balancer's rotation.
    rotation: Vec<usize>,
    processes: Vec<KeyProcess>,
    oracle: Oracle,
    measure_from_ns: u64,
    end_ns: u64,
    traffic_run: TrafficCounts,
    traffic_measure: TrafficCounts,
    key_traffic: Vec<TrafficCounts>,
    node_health: Vec<NodeHealth>,
    health: Health,
    touched: Vec<(u32, BucketEpoch)>,
    warnings: Warnings,
    events: u64,
    polls: u64,
}

impl<'r, F, L> Driver<'r, F, L>
where
    F: Future<Output = Result<(), GossipError>>,
    L: FnMut(usize) -> Result<Launched<F>>,
{
    pub fn new(
        regime: &'r Regime,
        seed: u64,
        plan: Plan,
        sizing: Sizing,
        keys: Rc<KeySpace>,
        world: Rc<World>,
        launch: L,
    ) -> Self {
        let n = plan.nodes.len();
        let measure_from_ns = regime.warmup_s * NS_PER_S;
        let processes = KeyProcess::for_regime(regime, &keys.kinds, seed);
        let oracle = Oracle::new(Rc::clone(&keys), n, measure_from_ns);
        let key_traffic = vec![TrafficCounts::default(); keys.len()];
        Self {
            regime,
            seed,
            sizing,
            world,
            launch,
            tasks: Tasks::new(n),
            handles: (0..n).map(|_| None).collect(),
            stores: vec![None; n],
            stopped: vec![false; n],
            rotation: Vec::new(),
            processes,
            oracle,
            measure_from_ns,
            end_ns: regime.duration_s * NS_PER_S,
            traffic_run: TrafficCounts::default(),
            traffic_measure: TrafficCounts::default(),
            key_traffic,
            node_health: vec![NodeHealth::default(); n],
            health: Health {
                cell_capacity: sizing.cell_capacity,
                ..Health::default()
            },
            touched: Vec::new(),
            warnings: Warnings::new(),
            events: 0,
            polls: 0,
            keys,
            plan,
        }
    }

    /// Run to the end with gabion's warnings captured into the result.
    pub fn run(self) -> Result<StudyResult> {
        let warnings = self.warnings.clone();
        tracing::subscriber::with_default(warnings.subscriber(), move || self.simulate())
    }

    fn simulate(mut self) -> Result<StudyResult> {
        self.schedule_initial();
        loop {
            self.drain_ready();
            let next = self.world.events.borrow_mut().pop();
            let Some((at_ns, event)) = next else { break };
            if at_ns > self.end_ns {
                break;
            }
            self.world.set_now_ns(at_ns);
            self.events += 1;
            self.handle(event)?;
        }
        self.world.set_now_ns(self.end_ns);
        self.drain_ready();
        for v in 0..self.plan.nodes.len() {
            if self.tasks.is_running(v) {
                self.snapshot(v);
            }
        }
        Ok(self.finish())
    }

    fn schedule_initial(&mut self) {
        for &(at, event) in &self.plan.events {
            self.world.schedule(at, Event::Plan(event));
        }
        for (key, process) in self.processes.iter_mut().enumerate() {
            if let Some(at) = process.first() {
                self.world.schedule(at, Event::Request(key as u32));
            }
        }
        let bucket_ns = self.keys.bucket_ms * NS_PER_MS;
        self.world.schedule(bucket_ns, Event::BucketBoundary);
        self.world
            .schedule(RESIDUAL_PHASE_NS, Event::ResidualSample);
        self.world
            .schedule(SNAPSHOT_PHASE_NS, Event::HealthSnapshot);
    }

    fn handle(&mut self, event: Event) -> Result<()> {
        let now = self.world.now_ns();
        match event {
            Event::Wake(waker) => waker.wake(),
            Event::Arrive { dst, datagram } => {
                let waker = self.world.net.borrow_mut().arrive(now, dst, datagram);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            Event::Plan(PlanEvent::Start(v)) => self.start(v)?,
            Event::Plan(PlanEvent::Stop(v, _)) => self.stop(v),
            Event::Plan(PlanEvent::Discover { to, about, added }) => {
                if !self.stopped[to] {
                    let waker = self.world.discover(to, peer_event(node_addr(about), added));
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
            Event::Request(key) => self.request(key),
            Event::BucketBoundary => {
                let now_millis = self.world.now_millis();
                let current = self.keys.bucket_of(now_millis);
                self.oracle
                    .bucket_boundary(now, now_millis, current, &self.stores);
                self.world
                    .schedule(now + self.keys.bucket_ms * NS_PER_MS, Event::BucketBoundary);
            }
            Event::ResidualSample => {
                if now >= self.measure_from_ns {
                    self.oracle.residual_sample(self.world.now_millis());
                }
                self.world
                    .schedule(now + RESIDUAL_PERIOD_NS, Event::ResidualSample);
            }
            Event::HealthSnapshot => {
                for v in 0..self.plan.nodes.len() {
                    if self.tasks.is_running(v) {
                        self.snapshot(v);
                    }
                }
                self.world
                    .schedule(now + SNAPSHOT_PERIOD_NS, Event::HealthSnapshot);
            }
        }
        Ok(())
    }

    /// Poll woken nodes until none is ready, then hand the oracle every
    /// tracked-key change their aggregates absorbed.
    fn drain_ready(&mut self) {
        while let Some(v) = self.tasks.next_ready() {
            self.polls += 1;
            self.warnings.set_node(v);
            let exit = self.tasks.poll(v);
            self.warnings.set_node(NO_NODE);
            if let Some(exit) = exit {
                self.health.runtime_exits += 1;
                if self.health.runtime_exit_errors.len() < 8 {
                    self.health.runtime_exit_errors.push(match exit {
                        Ok(()) => format!("node {v}: runtime returned"),
                        Err(error) => format!("node {v}: {error}"),
                    });
                }
                self.stop(v);
                continue;
            }
            self.absorb(v);
        }
    }

    fn absorb(&mut self, v: usize) {
        let Some(store) = self.stores[v].as_ref() else {
            return;
        };
        store.take_touched(&mut self.touched);
        if self.touched.is_empty() {
            return;
        }
        let now = self.world.now_ns();
        let now_millis = self.world.now_millis();
        for &(t, bucket) in &self.touched {
            let key = self.keys.tracked[t as usize];
            self.oracle.view_changed(
                v,
                t as usize,
                bucket,
                store.bucket_count(key, bucket),
                store.window_total(key, now_millis),
                now,
            );
        }
    }

    fn start(&mut self, v: usize) -> Result<()> {
        let now = self.world.now_ns();
        self.warnings.set_node(v);
        let launched = (self.launch)(v);
        self.warnings.set_node(NO_NODE);
        let launched = launched?;
        self.tasks.spawn(v, launched.future);
        self.handles[v] = Some(NodeHandle {
            client: launched.client,
            admin: launched.admin,
        });
        self.stores[v] = Some(launched.store);
        self.world.net.borrow_mut().set_running(now, v, true);
        if let Err(pos) = self.rotation.binary_search(&v) {
            self.rotation.insert(pos, v);
        }
        self.oracle
            .node_started(v, now, !self.plan.nodes[v].initial);
        Ok(())
    }

    fn stop(&mut self, v: usize) {
        if self.stopped[v] {
            return;
        }
        let now = self.world.now_ns();
        if self.tasks.is_running(v) {
            self.snapshot(v);
        }
        self.stopped[v] = true;
        self.tasks.remove(v);
        self.handles[v] = None;
        if let Some(store) = self.stores[v].take() {
            self.health.foreign_rows += store.foreign_rows.get();
        }
        self.world.net.borrow_mut().set_running(now, v, false);
        self.world.clear_discovery(v);
        if let Ok(pos) = self.rotation.binary_search(&v) {
            self.rotation.remove(pos);
        }
        self.oracle.node_stopped(v, now);
    }

    fn snapshot(&mut self, v: usize) {
        let Some(handle) = self.handles[v].as_ref() else {
            return;
        };
        self.node_health[v].asked = true;
        let (reply, mut rx) = oneshot::channel();
        if handle
            .admin
            .try_send(AdminCommand::Snapshot { reply })
            .is_err()
        {
            self.health.snapshot_failures += 1;
            return;
        }
        self.drain_ready();
        let Ok(snapshot) = rx.try_recv() else {
            self.health.snapshot_failures += 1;
            return;
        };
        let stats = snapshot.store_stats;
        self.health.peak_active_cells = self.health.peak_active_cells.max(stats.active_cells);
        self.health.max_send_pending_depth = self
            .health
            .max_send_pending_depth
            .max(snapshot.max_send_pending_depth);
        self.node_health[v] = NodeHealth {
            asked: true,
            answered: true,
            cell_store_full_rejects: stats.cell_store_full_rejects,
            rule_dictionary_full_rejects: stats.rule_dictionary_full_rejects,
            node_dictionary_full_rejects: stats.node_dictionary_full_rejects,
            decode_rejects: snapshot.decode_reject_count,
        };
    }

    /// One request arrives: the load balancer picks a running pod, the pod
    /// admits against its aggregate exactly as `SharedLimiter::evaluate`
    /// does (`crates/server/src/lib.rs`: reject iff `total + hits > limit`,
    /// otherwise `GossipClient::record` the hit), and the next arrival of
    /// the key is scheduled.
    fn request(&mut self, key: u32) {
        let now = self.world.now_ns();
        let measuring = now >= self.measure_from_ns;
        let k = key as usize;
        let running = self.rotation.len();
        let picked = (running > 0).then(|| self.rotation[self.processes[k].pick(running)]);
        if let Some(next) = self.processes[k].next(now) {
            self.world.schedule(next, Event::Request(key));
        }
        self.count(k, measuring, |c| c.offered += 1);
        let Some((v, store)) = picked.and_then(|v| Some((v, self.stores[v].clone()?))) else {
            self.count(k, measuring, |c| c.unrouted += 1);
            return;
        };
        let now_millis = self.world.now_millis();
        let bucket = self.keys.bucket_of(now_millis);
        let limit = self.keys.limit;
        if store.window_total(key, now_millis).saturating_add(1) > limit {
            self.count(k, measuring, |c| c.rejected += 1);
            if self.oracle.truth_window_total(key, bucket) < limit {
                self.count(k, measuring, |c| c.false_rejects += 1);
            }
            return;
        }
        self.count(k, measuring, |c| c.admitted += 1);
        self.oracle.admitted(key, bucket, now);
        self.record(v, key, bucket, now_millis);
    }

    fn count(&mut self, key: usize, measuring: bool, f: impl Fn(&mut TrafficCounts)) {
        f(&mut self.traffic_run);
        if measuring {
            f(&mut self.traffic_measure);
            f(&mut self.key_traffic[key]);
        }
    }

    fn record(&mut self, v: usize, key: u32, bucket: BucketEpoch, now_millis: u64) {
        let Some(handle) = self.handles[v].as_ref() else {
            return;
        };
        let client = handle.client.clone();
        let mut record = pin!(client.record(
            self.keys.fingerprint,
            self.keys.hashes[key as usize],
            bucket,
            1,
            self.keys.limit,
            now_millis,
        ));
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..RECORD_ATTEMPTS {
            match record.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(())) => return,
                Poll::Ready(Err(_)) => break,
                Poll::Pending => self.drain_ready(),
            }
        }
        self.health.record_failures += 1;
    }

    fn finish(mut self) -> StudyResult {
        let end = self.end_ns;
        self.oracle.finish(end);
        for store in self.stores.iter().flatten() {
            self.health.foreign_rows += store.foreign_rows.get();
        }
        for h in &self.node_health {
            self.health.cell_store_full_rejects += h.cell_store_full_rejects;
            self.health.rule_dictionary_full_rejects += h.rule_dictionary_full_rejects;
            self.health.node_dictionary_full_rejects += h.node_dictionary_full_rejects;
            self.health.decode_rejects += h.decode_rejects;
            self.health.nodes_never_snapshotted += u64::from(h.asked && !h.answered);
        }
        self.health.gabion_warnings = self.warnings.report();
        let net = self.world.net.borrow();
        self.health.oversize_packets = net.run.oversize_packets;
        let invalid_reasons = self.health.invalid_reasons();
        let flags = self.health.flags();

        let measure_s = (end - self.measure_from_ns) as f64 / 1e9;
        let node_seconds_measure = self.plan.node_seconds(self.measure_from_ns, end);
        let node_seconds_run = self.plan.node_seconds(0, end);
        let per = |x: u64, d: f64| if d > 0.0 { x as f64 / d } else { 0.0 };
        let ratio = |x: u64, d: u64| (d > 0).then(|| x as f64 / d as f64);
        let network = NetworkReport {
            run: net.run,
            measure: net.window,
            measure_bytes_per_node_per_s: per(net.window.bytes_sent, node_seconds_measure),
            measure_packets_per_node_per_s: per(net.window.packets_sent, node_seconds_measure),
            measure_bytes_per_s: per(net.window.bytes_sent, measure_s),
            measure_bytes_per_admitted: ratio(net.window.bytes_sent, self.traffic_measure.admitted),
            measure_bytes_per_offered: ratio(net.window.bytes_sent, self.traffic_measure.offered),
            run_bytes_per_node_per_s: per(net.run.bytes_sent, node_seconds_run),
        };
        drop(net);

        let limit = self.keys.limit;
        let accuracy = self
            .keys
            .tracked
            .iter()
            .enumerate()
            .filter(|&(t, _)| {
                self.keys.kinds[self.keys.tracked[t] as usize] != super::node::KeyKind::Site
            })
            .map(|(t, &key)| {
                let windows = self.oracle.over_admission[t];
                let pct = |x: f64| 100.0 * x / limit as f64;
                KeyAccuracy {
                    key,
                    kind: self.keys.kinds[key as usize],
                    limit,
                    measure: self.key_traffic[key as usize],
                    windows,
                    over_max_pct_limit: pct(windows.over_max.max(0) as f64),
                    over_mean_pct_limit: pct(if windows.windows > 0 {
                        windows.over_sum as f64 / windows.windows as f64
                    } else {
                        0.0
                    }),
                }
            })
            .collect();

        let limited = Class::Limited as usize;
        let sampled = Class::Sampled as usize;
        let staleness = StalenessReport {
            limited: Dist::from_hist(&self.oracle.staleness[limited], limit),
            sampled: Dist::from_hist(&self.oracle.staleness[sampled], limit),
            residual_limited: Dist::from_samples(&self.oracle.residual[limited], limit),
            residual_sampled: Dist::from_samples(&self.oracle.residual[sampled], limit),
        };
        let convergence = ConvergenceReport::new(
            std::mem::take(&mut self.oracle.convergence_ns),
            &self.oracle.censored_ns,
            self.oracle.pending(),
        );
        let churn = ChurnReport::new(std::mem::take(&mut self.oracle.finished_joiners));

        StudyResult {
            schema: SCHEMA,
            regime_id: self.regime.id.clone(),
            seed: self.seed,
            valid: invalid_reasons.is_empty(),
            invalid_reasons,
            flags,
            regime: self.regime.clone(),
            sizing: self.sizing,
            windows: Windows {
                warmup_s: self.regime.warmup_s,
                measure_s: self.regime.duration_s - self.regime.warmup_s,
                node_seconds_measure,
                node_seconds_run,
                mean_nodes_measure: node_seconds_measure / measure_s,
            },
            traffic: TrafficReport {
                run: self.traffic_run,
                measure: self.traffic_measure,
            },
            network,
            staleness,
            convergence,
            accuracy,
            churn,
            health: self.health,
            sim: SimStats {
                virtual_s: end as f64 / 1e9,
                events: self.events,
                polls: self.polls,
            },
        }
    }
}

/// Build the world for `regime` and `seed`.
pub fn world_for(regime: &Regime, seed: u64, plan: &Plan) -> World {
    let net = Network::new(
        regime.network,
        regime.churn.partitions.clone(),
        plan,
        seed,
        regime.warmup_s * NS_PER_S,
    );
    World::new(net, plan.nodes.len())
}
