use super::*;

use std::cell::Cell;
use std::sync::mpsc as std_mpsc;
use std::time::Instant;

use gabion::crdt::{DeltaSink, ExpirationSink};
use gabion::gossip::AggregateStore;
use tokio::time::timeout;

use crate::mock::with_logs;
use crate::rules::mock::rules;
use crate::shm::mock::TestZone;
use crate::shm::queue::QueueEvent;

const LEASE_TICK: Duration = Duration::from_millis(10);
const DEADLINE: Duration = Duration::from_secs(5);

/// A region over a leaked allocation, so a leader thread that outlives a
/// failed assertion never reads freed memory.
fn leaked_region() -> ShmRegion {
    let (zone, region) = TestZone::new(128, 64);
    std::mem::forget(zone);
    region
}

fn spawn_worker(region: ShmRegion, worker_id: u32) -> LeaderHandle {
    let rules = rules(&["per_tenant tenant:$http_x_tenant rate=2r/s"]);
    let (gossip, cell_store) = crate::mock::validated(|_| {});
    let config = LeaderConfig {
        worker_id,
        gossip,
        cell_store,
        lease_tick: LEASE_TICK,
    };
    spawn(region, Arc::new(rules), config).expect("spawn leader thread")
}

fn wait_for_owner(region: ShmRegion, worker_id: u32) {
    let deadline = Instant::now() + DEADLINE;
    while region.lease().snapshot().owner_worker != worker_id {
        assert!(
            Instant::now() < deadline,
            "worker {worker_id} never took the lease: {:?}",
            region.lease().snapshot(),
        );
        std::thread::sleep(LEASE_TICK);
    }
}

/// Stop `handle`, failing instead of hanging if the thread never exits.
fn stop_within_deadline(handle: LeaderHandle) {
    let (done_tx, done_rx) = std_mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(handle.stop().is_ok());
    });
    let stopped = done_rx
        .recv_timeout(DEADLINE)
        .expect("leader thread did not stop");
    assert!(stopped, "leader thread panicked");
}

#[test]
#[cfg_attr(miri, ignore = "binds a UDP socket")]
fn stopping_a_follower_leaves_the_leader_in_place() {
    let region = leaked_region();
    let leader = spawn_worker(region, 1);
    wait_for_owner(region, 1);
    let follower = spawn_worker(region, 2);
    std::thread::sleep(LEASE_TICK * 10);

    stop_within_deadline(follower);
    assert_eq!(region.lease().snapshot().owner_worker, 1);
    stop_within_deadline(leader);
}

#[test]
#[cfg_attr(miri, ignore = "binds a UDP socket")]
fn stopping_the_leader_releases_the_lease_to_a_follower() {
    let region = leaked_region();
    let leader = spawn_worker(region, 1);
    wait_for_owner(region, 1);
    let follower = spawn_worker(region, 2);
    std::thread::sleep(LEASE_TICK * 10);
    assert_eq!(region.lease().snapshot().owner_worker, 1);
    let leader_incarnation = region.header().identity.load_incarnation();

    // The leader releases the lease on its way out, so the follower takes
    // over within a tick rather than after the TTL.
    stop_within_deadline(leader);
    assert_ne!(region.lease().snapshot().owner_worker, 1);
    wait_for_owner(region, 2);
    stop_within_deadline(follower);
    // The takeover lands within the same second, so a seconds-based
    // incarnation alone would hand the follower the leader's identity.
    assert!(region.header().identity.load_incarnation() > leader_incarnation);
}

/// Counts the hits the gossip runtime applies, one delta row each.
#[derive(Default)]
struct AppliedHits(Cell<usize>);

impl AggregateStore for AppliedHits {
    fn apply(&self, deltas: &DeltaSink, _: &ExpirationSink) {
        self.0.set(self.0.get() + deltas.len());
    }
}

#[tokio::test(start_paused = true)]
#[cfg_attr(miri, ignore = "binds a UDP socket")]
async fn drain_hands_each_event_to_the_runtime_once_it_has_room() {
    const EVENTS: usize = DRAIN_BATCH.get() + 8;
    let region = leaked_region();
    let event = QueueEvent {
        hits: 1,
        ..QueueEvent::default()
    };
    (0..EVENTS)
        .try_for_each(|_| region.queue().push(event))
        .expect("queue has room");
    let identity = NodeIdentity::new(gabion::crdt::NodeId(1), 1);
    let applied = Rc::new(AppliedHits::default());
    let (gossip, cell_store) = crate::mock::validated(|_| {});
    let gossip = gossip.with_shallow_limit_queue();
    let transport = UdpTransport::bind(LONE_NODE_BIND, &gossip)
        .await
        .expect("bind the gossip socket");
    let (runtime, client) = GossipRuntime::from_parts(
        transport,
        TokioClock::new(),
        gossip,
        CellStore::new(cell_store, identity, rules(&[]).table()),
        applied.clone(),
    );
    let drained = || region.stats().snapshot().queue_drained as usize;
    let mut drain = std::pin::pin!(drain_loop(client, region, 1));
    assert!(timeout(DEADLINE, drain.as_mut()).await.is_err());
    assert_eq!(drained(), DRAIN_BATCH.get());
    let both = async { tokio::join!(drain.as_mut(), runtime.run(futures::stream::empty())) };
    assert!(timeout(DEADLINE, both).await.is_err());
    assert_eq!((applied.0.get(), drained()), (EVENTS, EVENTS));
    // `both` dropped the runtime, closing its queue.
    assert_eq!(drain.await.to_string(), "gossip runtime shut down");
}

/// Whether `line` carries the structured field `field`, as `name=value`.
fn has_field(line: &str, field: &str) -> bool {
    line.split(' ').any(|token| token == field)
}

/// `logs`' lines whose `code` field is `code`.
fn coded<'a>(logs: &'a str, code: &str) -> impl Iterator<Item = &'a str> {
    let field = format!("code=\"{code}\"");
    logs.lines().filter(move |line| has_field(line, &field))
}

/// The value of `field` on each of `logs`' lines whose `code` is `code`.
fn logged_totals(logs: &str, code: &str, field: &str) -> Vec<u64> {
    coded(logs, code)
        .map(|line| {
            line.split(' ')
                .find_map(|token| token.strip_prefix(field)?.strip_prefix('=')?.parse().ok())
                .unwrap_or_else(|| panic!("{field} missing from {line}"))
        })
        .collect()
}

/// Workers bump the four counters at different rates between ticks; each
/// tick the leader logs a counter only when its total passed a power of two
/// since a leader last logged it: a steady one at 1, 2, 4 and 8, a faster one
/// once per tick that passes one or more, and nothing on a tick without a
/// rise.
#[test]
#[cfg_attr(
    miri,
    ignore = "tracing-subscriber's registry is outside the unsafe boundary Miri checks here"
)]
fn leader_logs_shm_counters_at_powers_of_two() {
    let (_zone, region) = TestZone::new(8, 16);
    let (_, storage) = crate::mock::validated(|_| {});
    let stats = region.stats();
    let ((), logs) = with_logs(|| {
        for _ in 0..9 {
            stats.record_matched_rule_overflow();
            stats.record_queue_drop(2);
            stats
                .declines_invalid_descriptor
                .fetch_add(3, Ordering::Relaxed);
            stats.rejected_cardinality.fetch_add(5, Ordering::Relaxed);
            log_bypasses(region, &storage);
        }
        log_bypasses(region, &storage);
    });
    let totals = |code, field| logged_totals(&logs, code, field);
    assert_eq!(
        totals("matched_rule_overflow", "overflowed_total"),
        [1, 2, 4, 8],
        "{logs}"
    );
    assert_eq!(
        totals("queue_dropped", "dropped_total"),
        [2, 4, 8, 16],
        "{logs}"
    );
    assert_eq!(
        totals("invalid_descriptor_declined", "declined_total"),
        [3, 6, 9, 18],
        "{logs}"
    );
    assert_eq!(
        totals("cardinality_rejected", "rejected_total"),
        [5, 10, 20, 35],
        "{logs}"
    );
    for (code, fields, texts) in [
        (
            "matched_rule_overflow",
            &["cap=16", "config_key=\"storage.max_matched_rules\""][..],
            &["`gabion_storage_max_matched_rules`", "64 bytes per worker"][..],
        ),
        (
            "queue_dropped",
            &["capacity=8", "config_key=\"gossip.limit_queue_capacity\""][..],
            &[
                "`gabion_gossip_limit_queue_capacity`",
                "72 bytes",
                "about 1.3s",
            ][..],
        ),
        (
            "invalid_descriptor_declined",
            &[][..],
            &["not valid UTF-8", "`map`"][..],
        ),
        (
            "cardinality_rejected",
            &[
                "max=512",
                "limit=\"bytes\"",
                "config_key=\"storage.max_descriptor_bytes\"",
            ][..],
            &["`gabion_storage_max_descriptor_bytes`"][..],
        ),
    ] {
        let line = coded(&logs, code)
            .next()
            .unwrap_or_else(|| panic!("no {code} line in {logs}"));
        assert!(line.contains(" WARN "), "{line}");
        for field in fields {
            assert!(has_field(line, field), "{field} missing from {line}");
        }
        for text in texts {
            assert!(line.contains(text), "{text} missing from {line}");
        }
    }
}

/// A leadership term reads the bypass counters on its own lease-tick arm: a
/// drop a worker counted moves the zone's mark within a few ticks.
#[test]
#[cfg_attr(miri, ignore = "binds a UDP socket")]
fn the_leader_logs_the_bypass_counters_every_lease_tick() {
    let region = leaked_region();
    let leader = spawn_worker(region, 1);
    wait_for_owner(region, 1);
    region.stats().record_queue_drop(1);
    let deadline = Instant::now() + DEADLINE;
    while region.stats().logged.queue_dropped.load(Ordering::Relaxed) != 1 {
        assert!(
            Instant::now() < deadline,
            "the leader never read the bypass counters"
        );
        std::thread::sleep(LEASE_TICK);
    }
    stop_within_deadline(leader);
}
