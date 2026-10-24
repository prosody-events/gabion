use std::net::SocketAddr;
use std::time::Duration;

use crate::crdt::{
    CellStore, CellStoreConfig, Count, DeltaSink, ExpirationSink, KeyHash, NodeId, NodeIdentity,
    RuleDescriptor,
};
use crate::gossip::client::LimitRequest;
use crate::gossip::sim::{SimRouter, SimTransport};
use crate::gossip::{AggregateStore, Clock, FixedClock, GossipConfig, GossipRuntime};
use crate::wire::FrameLimits;

use super::super::Round;
use super::PendingHits;

const RULE: u128 = 0xC0DE;
// One peer at the default 100 bps: ε = 1000 × 100 / (10_000 × 1) = 10.
const LIMIT: u64 = 1_000;
const EPSILON: u64 = 10;
const FLOOR_MS: u64 = 5;

struct NoAggregates;

impl<C: Count> AggregateStore<C> for NoAggregates {
    fn apply(&self, _deltas: &DeltaSink<C>, _expirations: &ExpirationSink<C>) {}
}

type Runtime = GossipRuntime<u32, NoAggregates, SimTransport, FixedClock>;

fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// A runtime with one bootstrap peer in its zone and `RULE` interned,
/// driven by hand: tests call the arm handlers directly instead of running
/// the loop.
fn runtime(max_cells_per_tick: usize) -> Runtime {
    runtime_with(
        &SimRouter::new(),
        max_cells_per_tick,
        1_024,
        FrameLimits::default(),
    )
}

fn runtime_with(
    router: &SimRouter,
    max_cells_per_tick: usize,
    cell_capacity: u32,
    wire_limits: FrameLimits,
) -> Runtime {
    let identity = NodeIdentity::new(NodeId(0xA1), 1);
    let config = GossipConfig {
        local_identity: identity,
        cluster_id_hash: 0xC1,
        bootstrap_peers: vec![sock(2)],
        fanout: 1,
        target_err_bps: 100,
        min_emit_interval: Duration::from_millis(FLOOR_MS),
        max_cells_per_tick,
        wire_limits,
        ..GossipConfig::default()
    };
    let mut store = CellStore::new(
        CellStoreConfig {
            cell_capacity,
            local_dirty_capacity: 1_024,
            ..CellStoreConfig::default()
        },
        identity,
    );
    assert!(
        store
            .intern_rule(RuleDescriptor {
                fingerprint: RULE,
                ..RuleDescriptor::default()
            })
            .is_some()
    );
    let (rt, _client) = GossipRuntime::from_parts(
        router.bind(sock(1)),
        FixedClock::new(1_000),
        config,
        store,
        NoAggregates,
    );
    rt
}

/// One heartbeat round.
fn round(rt: &mut Runtime) {
    rt.handle_gossip_round(Round::Heartbeat)
        .expect("the simulated socket never fails");
}

/// Record `hits` on `key` and report whether the request asked for an
/// immediate flush, consuming the flag the way the run loop does.
fn hit(rt: &mut Runtime, key: u128, hits: u64) -> bool {
    rt.handle_limit_request(LimitRequest {
        rule_fingerprint: RULE,
        key_hash: KeyHash(key),
        bucket: 0,
        hits,
        rule_limit: LIMIT,
        now_millis: rt.clock.now_millis(),
        reply: None,
    });
    std::mem::take(&mut rt.want_immediate_flush)
}

#[test]
fn many_keys_of_one_rule_do_not_fire_per_hit() {
    let mut rt = runtime(4_096);
    // 500 keys each brought to exactly ε: 5,000 hits on the rule, none of
    // them over any one key's budget.
    for _ in 0..EPSILON {
        for key in 0..500 {
            assert!(!hit(&mut rt, key, 1), "key {key} fired below its budget");
        }
    }
    assert_eq!(rt.last_error_budget, EPSILON);
}

#[test]
fn one_hot_key_fires_once_past_its_budget() {
    let mut rt = runtime(4_096);
    for _ in 0..EPSILON {
        assert!(!hit(&mut rt, 7, 1));
        assert!(!hit(&mut rt, 8, 1));
    }
    assert!(
        hit(&mut rt, 7, 1),
        "the hit that takes key 7 past ε must fire"
    );
}

#[test]
fn floor_holds_back_a_second_fire() {
    let mut rt = runtime(4_096);
    assert!(hit(&mut rt, 7, EPSILON + 1));
    round(&mut rt);

    rt.clock.advance(FLOOR_MS - 1);
    assert!(
        !hit(&mut rt, 7, EPSILON + 1),
        "fired inside min_emit_interval"
    );
    rt.clock.advance(1);
    assert!(
        hit(&mut rt, 7, 1),
        "the floor elapsed with key 7 still over ε"
    );
}

#[test]
fn emit_clears_only_the_keys_it_carried() {
    // One cell per frame: key 7 (dirty first) goes out, key 8 does not.
    let mut rt = runtime(1);
    assert!(!hit(&mut rt, 7, EPSILON));
    assert!(!hit(&mut rt, 8, EPSILON));
    round(&mut rt);
    rt.clock.advance(FLOOR_MS);

    assert!(
        !hit(&mut rt, 7, EPSILON),
        "key 7 was emitted; its count restarts"
    );
    assert!(
        hit(&mut rt, 8, 1),
        "key 8 was not emitted; its count carries over"
    );
}

#[test]
fn floor_holds_when_the_firing_key_is_not_framed() {
    // One cell per frame: key 7 goes out, key 8 never makes a frame.
    let mut rt = runtime(1);
    assert!(!hit(&mut rt, 7, EPSILON));
    assert!(!hit(&mut rt, 8, EPSILON));
    round(&mut rt);
    rt.clock.advance(FLOOR_MS);

    assert!(hit(&mut rt, 8, 1), "the floor had elapsed");
    for _ in 0..100 {
        assert!(!hit(&mut rt, 8, 1), "fired twice inside min_emit_interval");
        round(&mut rt);
    }
}

#[test]
fn floor_holds_when_the_key_cannot_be_stored() {
    let mut rt = runtime_with(&SimRouter::new(), 4_096, 2, FrameLimits::default());
    assert!(!hit(&mut rt, 1, 1));
    assert!(!hit(&mut rt, 2, 1));

    // The store is full, so key 3 never gets a cell for a frame to carry.
    assert!(hit(&mut rt, 3, EPSILON + 1));
    for _ in 0..100 {
        assert!(!hit(&mut rt, 3, 1), "fired twice inside min_emit_interval");
        round(&mut rt);
    }
}

#[test]
fn a_frame_cut_short_keeps_its_counts() {
    // 512-byte packets hold a few dozen cells, so 300 cells span several.
    // The peer's socket buffers one packet and is never read, so the socket
    // pushes back once the one send slot holds a second.
    let router = SimRouter::with_channel_capacity(1);
    let _peer = router.bind(sock(2));
    let mut rt = runtime_with(
        &router,
        4_096,
        1_024,
        FrameLimits {
            max_payload_bytes: 512,
            ..FrameLimits::default()
        },
    );
    for key in 0..300 {
        assert!(!hit(&mut rt, key, 1));
    }
    rt.send_free.truncate(1);
    round(&mut rt);
    rt.clock.advance(FLOOR_MS);

    assert!(
        hit(&mut rt, 0, EPSILON),
        "the frame did not fit the send queue; key 0 keeps its count"
    );
}

#[test]
fn colliding_keys_share_a_count() {
    let mut pending = PendingHits::with_capacity(1);
    assert_eq!(pending.add(0, KeyHash(1), 3), 3);
    assert_eq!(pending.add(0, KeyHash(2), 4), 7);
    assert!(pending.clear(0, KeyHash(1)));
    assert!(pending.is_empty());
    assert!(!pending.clear(0, KeyHash(2)));
}
