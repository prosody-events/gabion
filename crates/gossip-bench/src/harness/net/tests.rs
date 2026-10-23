use std::rc::Rc;

use gabion::gossip::GossipTransport;

use super::*;
use crate::harness::fixtures;
use crate::harness::plan::{NS_PER_MS, Plan, node_addr};
use crate::harness::regime::GilbertElliott;
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

fn mean(v: &[u64]) -> f64 {
    v.iter().sum::<u64>() as f64 / v.len() as f64
}

#[test]
fn latency_is_base_plus_bounded_jitter_and_cross_az_is_slower() {
    let config = NetworkConfig::default();
    let (mut net, plan) = network(config, 6);
    assert_eq!(plan.nodes[0].az, plan.nodes[3].az);
    assert_ne!(plan.nodes[0].az, plan.nodes[1].az);

    let intra = latencies(&mut net, 0, 3, 10_000);
    let cross = latencies(&mut net, 0, 1, 10_000);
    assert_eq!((intra.len(), cross.len()), (10_000, 10_000));
    let intra_max = config.intra_az_base_ns + config.intra_az_jitter_max_ns;
    let cross_max = config.cross_az_base_ns + config.cross_az_jitter_max_ns;
    assert!(
        intra
            .iter()
            .all(|l| (config.intra_az_base_ns..=intra_max).contains(l))
    );
    assert!(
        cross
            .iter()
            .all(|l| (config.cross_az_base_ns..=cross_max).contains(l))
    );
    assert!(intra.iter().max() < cross.iter().min());
    // Uniform jitter: means of 0.25 ms intra-AZ and 1.0 ms cross-AZ.
    assert!(
        (mean(&intra) - 250_000.0).abs() < 2_500.0,
        "{}",
        mean(&intra)
    );
    assert!(
        (mean(&cross) - 1_000_000.0).abs() < 10_000.0,
        "{}",
        mean(&cross)
    );
}

#[test]
fn bernoulli_loss_rate_is_within_four_sigma() {
    let p = 0.05;
    let (mut net, _) = network(
        NetworkConfig {
            loss: p,
            ..NetworkConfig::default()
        },
        3,
    );
    let n = 200_000;
    for _ in 0..n {
        net.send(0, 0, 100, node_addr(1));
    }
    let observed = net.run.dropped_loss_packets as f64 / n as f64;
    let sigma = (p * (1.0 - p) / n as f64).sqrt();
    assert!(
        (observed - p).abs() < 4.0 * sigma,
        "observed {observed}, expected {p} ± {}",
        4.0 * sigma
    );
    assert_eq!(net.run.packets_sent, n);
}

#[test]
fn gilbert_elliott_matches_stationary_loss_and_burst_length() {
    let ge = GilbertElliott {
        p_good_to_bad: 0.01,
        p_bad_to_good: 0.19,
        loss_in_bad: 1.0,
    };
    let (mut net, _) = network(
        NetworkConfig {
            burst_loss: Some(ge),
            ..NetworkConfig::default()
        },
        3,
    );
    let n = 400_000;
    let mut runs = Vec::new();
    let mut run = 0_u64;
    for _ in 0..n {
        if net.send(0, 0, 100, node_addr(1)) == Route::Dropped {
            run += 1;
        } else if run > 0 {
            runs.push(run);
            run = 0;
        }
    }
    let observed = net.run.dropped_loss_packets as f64 / n as f64;
    assert!((observed - 0.05).abs() < 0.006, "mean loss {observed}");
    let mean_burst = mean(&runs);
    assert!(
        (mean_burst - 1.0 / 0.19).abs() < 0.5,
        "mean burst length {mean_burst}"
    );
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
    assert_eq!(net.run.packets_delivered, 3);
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
    assert_eq!(net.run.packets_delivered, 4);
    assert_eq!(net.run.dropped_rcvbuf_packets, 3);

    // A stopped node receives nothing; its queue is discarded.
    net.set_running(0, 1, false);
    assert_eq!(net.run.dropped_no_receiver_packets, 3);
    net.arrive(0, 1, datagram());
    assert_eq!(net.run.dropped_no_receiver_packets, 4);
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
