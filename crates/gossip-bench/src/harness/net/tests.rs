use std::rc::Rc;

use gabion::gossip::GossipTransport;

use super::*;
use crate::harness::fixtures;
use crate::harness::plan::{NS_PER_MS, Plan, node_addr};
use crate::harness::world::{Event, SimSocket, World};

fn network(config: NetworkConfig, nodes: u32) -> (Network, Plan) {
    let plan = Plan::build(&fixtures::quiet(nodes), 7);
    let mut net = Network::new(config, Vec::new(), &plan, 7, 0);
    for i in 0..plan.nodes.len() {
        net.set_running(0, i, true);
    }
    (net, plan)
}

fn latencies(net: &mut Network, src: usize, dst: usize, n: usize) -> Vec<u64> {
    (0..n)
        .filter_map(|_| match net.send(0, src, 100, node_addr(dst)) {
            Route::Deliver { latency_ns, .. } => Some(latency_ns),
            Route::Dropped => None,
        })
        .collect()
}

/// `jitter_max` is inclusive: a 1 ns jitter delivers at `base` and `base + 1`.
#[test]
fn the_largest_jitter_is_reachable() {
    let config = NetworkConfig {
        intra_az_base_ns: 10,
        intra_az_jitter_max_ns: 1,
        ..NetworkConfig::default()
    };
    let (mut net, _) = network(config, 6);
    let seen: std::collections::BTreeSet<u64> =
        latencies(&mut net, 0, 3, 100).into_iter().collect();
    assert_eq!(seen, [10, 11].into());
}

#[test]
fn receive_buffer_overflow_drops_without_backpressure() {
    let (mut net, _) = network(
        NetworkConfig {
            rcvbuf_bytes: 3_000,
            ..NetworkConfig::default()
        },
        3,
    );
    let datagram = || Datagram {
        src: node_addr(0),
        bytes: vec![0_u8; 1_000].into_boxed_slice(),
    };
    for _ in 0..5 {
        net.arrive(0, 1, datagram());
    }
    // Delivery is counted when the node reads, not when the buffer queues.
    assert_eq!(net.run.packets_delivered, 0);
    assert_eq!(net.run.dropped_rcvbuf_packets, 2);
    assert_eq!(net.run.dropped_rcvbuf_bytes, 2 * 1_028);

    // Reading one frees room for exactly one more.
    let mut cx = Context::from_waker(Waker::noop());
    let mut buf = [0_u8; 1_500];
    assert_eq!(
        net.poll_recv(1, 0, &mut cx, &mut buf),
        Recv::Datagram(1_000, node_addr(0))
    );
    net.arrive(0, 1, datagram());
    net.arrive(0, 1, datagram());
    assert_eq!(net.run.packets_delivered, 1);
    assert_eq!(net.run.dropped_rcvbuf_packets, 3);

    // A stopped node receives nothing; its unread queue is discarded and
    // counted once, as undelivered.
    net.set_running(0, 1, false);
    assert_eq!(net.run.dropped_no_receiver_packets, 3);
    net.arrive(0, 1, datagram());
    assert_eq!(net.run.dropped_no_receiver_packets, 4);
    let accounted = net.run.packets_delivered
        + net.run.dropped_rcvbuf_packets
        + net.run.dropped_no_receiver_packets;
    assert_eq!(
        accounted, 8,
        "each of the 8 arrivals is counted exactly once"
    );
}

/// Reads are paced by the receive path's service time; arrivals during it
/// queue in the buffer and overflow it, and each busy spell schedules one
/// wake-up.
#[test]
fn receive_service_time_paces_reads_and_fills_the_buffer() {
    let (mut net, _) = network(
        NetworkConfig {
            rcvbuf_bytes: 3_000,
            rx_ns_per_datagram: 1_000,
            rx_ns_per_byte: 1,
            ..NetworkConfig::default()
        },
        3,
    );
    let datagram = || Datagram {
        src: node_addr(0),
        bytes: vec![0_u8; 1_000].into_boxed_slice(),
    };
    let mut cx = Context::from_waker(Waker::noop());
    let mut buf = [0_u8; 1_500];
    assert_eq!(net.poll_recv(1, 0, &mut cx, &mut buf), Recv::Empty);
    for _ in 0..3 {
        net.arrive(0, 1, datagram());
    }
    let read = Recv::Datagram(1_000, node_addr(0));
    assert_eq!(net.poll_recv(1, 0, &mut cx, &mut buf), read);
    // Serving 1,000 B takes 1 µs + 1 µs.
    assert_eq!(
        net.poll_recv(1, 0, &mut cx, &mut buf),
        Recv::Busy(Some(2_000))
    );
    assert_eq!(net.poll_recv(1, 1_999, &mut cx, &mut buf), Recv::Busy(None));
    // Two still queued: room for one more, the next overflows.
    net.arrive(500, 1, datagram());
    net.arrive(500, 1, datagram());
    assert_eq!(net.run.dropped_rcvbuf_packets, 1);
    for at in [2_000, 4_000, 6_000] {
        assert_eq!(net.poll_recv(1, at, &mut cx, &mut buf), read);
        assert_eq!(
            net.poll_recv(1, at, &mut cx, &mut buf),
            if at < 6_000 {
                Recv::Busy(Some(at + 2_000))
            } else {
                Recv::Empty
            }
        );
    }
    assert_eq!(net.run.packets_delivered, 4);
}

#[test]
fn byte_accounting_is_exact_through_the_transport() {
    let plan = Plan::build(&fixtures::quiet(3), 1);
    // Measurement starts at 5 ms: earlier sends count only in `run`.
    let net = Network::new(
        NetworkConfig::default(),
        Vec::new(),
        &plan,
        1,
        5 * NS_PER_MS,
    );
    let world = Rc::new(World::new(net, plan.nodes.len()));
    for i in 0..3 {
        world.net.borrow_mut().set_running(0, i, true);
    }
    let socket = SimSocket {
        world: Rc::clone(&world),
        idx: 0,
        addr: node_addr(0),
    };
    let unknown: SocketAddr = "192.0.2.1:7946".parse().expect("address");
    // Nodes 1 and 2 sit in other availability zones than node 0.
    let sends: [(usize, SocketAddr); 5] = [
        (100, node_addr(1)),
        (1_400, node_addr(2)),
        (1_473, node_addr(1)),
        (0, node_addr(2)),
        (64, unknown),
    ];
    for (len, dst) in sends {
        assert_eq!(socket.try_send_to(&vec![7_u8; len], dst).ok(), Some(len));
    }
    world.set_now_ns(10 * NS_PER_MS);
    assert_eq!(socket.try_send_to(&[1_u8; 10], node_addr(1)).ok(), Some(10));

    let routed_wire = (100 + 28) + (1_400 + 28) + (1_473 + 28) + 28 + (10 + 28);
    let net = world.net.borrow();
    assert_eq!(net.run.packets_sent, 6);
    assert_eq!(net.run.bytes_sent, routed_wire + 64 + 28);
    assert_eq!(net.run.payload_bytes_sent, 100 + 1_400 + 1_473 + 64 + 10);
    assert_eq!(net.run.oversize_packets, 1);
    assert_eq!(net.run.cross_az_packets, 5);
    assert_eq!(net.run.cross_az_bytes, routed_wire);
    assert_eq!(net.run.dropped_no_receiver_packets, 1);
    assert_eq!(net.run.dropped_no_receiver_bytes, 92);
    assert_eq!(net.window.packets_sent, 1);
    assert_eq!(net.window.bytes_sent, 38);
    drop(net);

    // Every routed datagram is scheduled after a cross-AZ latency.
    let mut arrivals = 0;
    while let Some((at, event)) = world.events.borrow_mut().pop() {
        if let Event::Arrive { .. } = event {
            assert!(at >= 800_000);
            arrivals += 1;
        }
    }
    assert_eq!(arrivals, 5);
}

#[test]
fn partition_drops_only_traffic_crossing_the_cut() {
    let plan = Plan::build(&fixtures::quiet(6), 1);
    let partition = Partition {
        at_s: 1,
        heal_s: 2,
        az: 0,
    };
    let mut net = Network::new(NetworkConfig::default(), vec![partition], &plan, 1, 0);
    let s = NS_PER_S;
    assert_ne!(net.send(s / 2, 0, 10, node_addr(1)), Route::Dropped);
    assert_eq!(net.send(s, 0, 10, node_addr(1)), Route::Dropped);
    assert_eq!(net.send(s, 1, 10, node_addr(0)), Route::Dropped);
    assert_ne!(net.send(s, 0, 10, node_addr(3)), Route::Dropped);
    assert_ne!(net.send(s, 1, 10, node_addr(2)), Route::Dropped);
    assert_ne!(net.send(2 * s, 0, 10, node_addr(1)), Route::Dropped);
    assert_eq!(net.run.dropped_partition_packets, 2);
}
