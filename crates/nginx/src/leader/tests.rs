use super::*;

#[test]
fn defaults_match_shared_production_defaults() {
    let gossip = GossipSettings::default();
    assert_eq!(
        gossip.tick_interval,
        Duration::from_millis(defaults::GOSSIP_TICK_INTERVAL_MILLIS)
    );
    assert_eq!(gossip.fanout, defaults::GOSSIP_FANOUT);
    assert_eq!(gossip.max_payload_bytes, defaults::GOSSIP_MAX_PAYLOAD_BYTES);
    assert_eq!(
        gossip.max_cells_per_frame,
        defaults::GOSSIP_MAX_CELLS_PER_FRAME
    );
    assert_eq!(
        gossip.max_cells_per_tick,
        defaults::GOSSIP_MAX_CELLS_PER_TICK
    );
    assert_eq!(
        gossip.send_queue_capacity,
        defaults::GOSSIP_SEND_QUEUE_CAPACITY
    );
    assert_eq!(
        gossip.limit_queue_capacity,
        defaults::GOSSIP_LIMIT_QUEUE_CAPACITY
    );
    assert_eq!(gossip.cluster_id_hash, defaults::GOSSIP_CLUSTER_ID_HASH);
    assert_eq!(gossip.target_err_bps, defaults::GOSSIP_TARGET_ERR_BPS);
    assert_eq!(
        gossip.min_emit_interval,
        Duration::from_millis(defaults::GOSSIP_MIN_EMIT_INTERVAL_MS)
    );

    let cell_store = production_cell_store_config();
    assert_eq!(cell_store.cell_capacity, defaults::STORAGE_MAX_CELLS as u32);
    assert_eq!(
        cell_store.rule_dictionary_capacity,
        defaults::STORAGE_RULE_DICTIONARY_CAPACITY
    );
    assert_eq!(
        cell_store.node_dictionary_capacity,
        defaults::STORAGE_NODE_DICTIONARY_CAPACITY
    );
    assert_eq!(
        cell_store.local_dirty_capacity,
        defaults::STORAGE_LOCAL_DIRTY_CAPACITY
    );
    assert_eq!(
        cell_store.forwarded_dirty_capacity,
        defaults::STORAGE_FORWARDED_DIRTY_CAPACITY
    );
    assert_eq!(cell_store.peer_capacity, defaults::STORAGE_PEER_CAPACITY);

    let leader = LeaderConfig::default();
    assert_eq!(
        leader.cell_store.cell_capacity,
        defaults::STORAGE_MAX_CELLS as u32
    );
}

use std::sync::mpsc as std_mpsc;
use std::time::Instant;

use gabion::rules::EnforcementMode;

use crate::rules::{DEFAULT_DOMAIN, DescriptorBinding, RuleConfig};
use crate::shm::Layout;

const LEASE_TICK: Duration = Duration::from_millis(10);
const DEADLINE: Duration = Duration::from_secs(5);

/// A region over a leaked allocation, so a leader thread that outlives a
/// failed assertion never reads freed memory.
fn leaked_region() -> ShmRegion {
    let layout = Layout::new(64, 64).expect("layout");
    let words: &'static mut [u64] =
        Box::leak(vec![0_u64; layout.total_bytes.div_ceil(8)].into_boxed_slice());
    // SAFETY: `ShmRegion::initialize`'s preconditions hold: `words` is a
    // fresh, writable, 8-byte-aligned allocation of at least
    // `layout.total_bytes`, `initialize` is its first user, and the leak
    // keeps it alive for every thread that reads the region.
    unsafe { ShmRegion::initialize(words.as_mut_ptr().cast(), layout) }
}

fn spawn_worker(region: ShmRegion, worker_id: u32) -> LeaderHandle {
    let rules = CompiledRules::compile(&[RuleConfig {
        name: "per_tenant".into(),
        domain: DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 2,
        window: Duration::from_secs(1),
        bucket: Duration::from_secs(1),
        mode: EnforcementMode::Enforce,
        except_if: None,
    }])
    .expect("rules");
    let config = LeaderConfig {
        worker_id,
        gossip_bind: "127.0.0.1:0".parse().expect("addr"),
        lease_tick: LEASE_TICK,
        ..LeaderConfig::default()
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
