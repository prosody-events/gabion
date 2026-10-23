//! Each test checks the simulator against a known result and prints the
//! measured and expected figures the README's Validation section records
//! (`cargo nextest run --release -p gossip-bench validation --no-capture`).
//! Seeds are fixed, so every run is bit-identical.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Context, Waker};
use std::time::Duration;

use gabion::gossip::GossipTransport;

use super::reference::{self, Arrival, Epidemic, Probes, epidemic_node, probe_node};
use crate::harness::driver::{self, Driver};
use crate::harness::net::{Datagram, NetCounters, Network, Recv};
use crate::harness::plan::{NS_PER_S, Plan, node_addr};
use crate::harness::regime::{GilbertElliott, NetworkConfig};
use crate::harness::rng::{Rng, Stream};
use crate::harness::world::SimSocket;
use crate::harness::{fixtures, node};

/// Standard-normal quantile used for every confidence interval below: a
/// two-sided 4-sigma band, which a correct simulator misses about once in
/// 16,000 independent checks (the epidemic check's expected value is itself
/// approximate, so its rate is nearer 1 in 1,000).
const SIGMAS: f64 = 4.0;

/// Run `count` probes from node 0 to node `to` through a two-or-more-node
/// quiet cluster and return what the receiver read and the network's tally.
fn probe_run(
    seed: u64,
    config: NetworkConfig,
    to: usize,
    count: u32,
    period: Duration,
    len: usize,
) -> (Vec<Arrival>, NetCounters) {
    let nodes = to.max(1) + 1;
    let plan = Plan::build(&crate::harness::fixtures::quiet(nodes as u32), 1);
    let world = reference::world(&plan, config, seed);
    let log = Rc::new(RefCell::new(Vec::new()));
    let probes = Probes {
        to,
        count,
        period,
        len,
    };
    reference::run(&world, nodes, |idx| {
        let send = (idx == 0).then_some(probes);
        probe_node(Rc::clone(&world), idx, send, Rc::clone(&log))
    });
    let arrivals = log.borrow().clone();
    let counters = world.net.borrow().run;
    (arrivals, counters)
}

// ---------------------------------------------------------------------------
// 1. Epidemic reach
// ---------------------------------------------------------------------------

/// Independent runs per `(n, c)`: at most 0.0071 standard error.
const EPIDEMIC_TRIALS: u64 = 5_000;

/// Fraction of `EPIDEMIC_TRIALS` runs in which every node received the
/// message, for fanout `ln(n) + c`.
fn reach_probability(nodes: usize, c: f64) -> f64 {
    let plan = Plan::build(&fixtures::quiet(nodes as u32), 1);
    let fanout = (nodes as f64).ln() + c;
    let complete = (1..=EPIDEMIC_TRIALS)
        .filter(|&seed| {
            let world = reference::world(&plan, NetworkConfig::default(), seed);
            let reached = Rc::new(RefCell::new(vec![false; nodes]));
            let spec = Epidemic {
                nodes,
                fanout,
                seed,
            };
            reference::run(&world, nodes, |idx| {
                epidemic_node(Rc::clone(&world), idx, spec, Rc::clone(&reached))
            });
            reached.borrow().iter().all(|&r| r)
        })
        .count();
    complete as f64 / EPIDEMIC_TRIALS as f64
}

/// How far from the limit the harness may land at these sizes.
const LIMIT_TOLERANCE: f64 = 0.05;

/// Kermarrec, Massoulié and Ganesh, "Probabilistic Reliable Dissemination in
/// Large-Scale Systems", IEEE TPDS 14(3), 2003: with fanout `ln(n) + c`,
/// the probability that every node is reached tends to `exp(-exp(-c))`.
///
/// At finite `n` the same Poisson argument, with a node missed by one of the
/// other `n - 1` with probability `1 - f/(n-1)` (targets are distinct), gives
/// `exp(-(n-1) (1 - f/(n-1))^(n-1))`, which is closer to the measurement.
/// The harness must match that within 4 standard errors, and the limit
/// within [`LIMIT_TOLERANCE`].
fn epidemic_reach_matches_theory(nodes: usize) {
    for c in [0.0_f64, 1.0, 2.0] {
        let limit = (-(-c).exp()).exp();
        let others = (nodes - 1) as f64;
        let fanout = (nodes as f64).ln() + c;
        let finite = (-others * (1.0 - fanout / others).powf(others)).exp();
        let measured = reach_probability(nodes, c);
        let sigma = (finite * (1.0 - finite) / EPIDEMIC_TRIALS as f64).sqrt();
        println!(
            "validation epidemic n={nodes} c={c}: measured {measured:.4}, KMG limit {limit:.4}, \
             finite-n {finite:.4} ± {:.4}",
            SIGMAS * sigma
        );
        assert!(
            (measured - finite).abs() <= SIGMAS * sigma,
            "n={nodes} c={c}: measured {measured}, finite-n {finite} ± {}",
            SIGMAS * sigma
        );
        assert!(
            (measured - limit).abs() <= LIMIT_TOLERANCE,
            "n={nodes} c={c}: measured {measured}, KMG limit {limit} ± {LIMIT_TOLERANCE}"
        );
    }
}

#[test]
fn epidemic_reach_with_100_nodes() {
    epidemic_reach_matches_theory(100);
}

#[test]
fn epidemic_reach_with_300_nodes() {
    epidemic_reach_matches_theory(300);
}

// ---------------------------------------------------------------------------
// 2. Loss
// ---------------------------------------------------------------------------

const LOSS_PROBES: u32 = 1_000_000;
/// Probes one millisecond apart never queue behind each other at the
/// receiver or overtake one another (cross-zone latency is below 1.2 ms).
const PROBE_PERIOD: Duration = Duration::from_millis(1);

/// Lost probes, from the sequence numbers the receiver read.
fn lost_runs(arrivals: &[Arrival], sent: u32) -> Vec<u64> {
    assert!(
        arrivals.windows(2).all(|w| w[0].seq < w[1].seq),
        "probes arrive in order"
    );
    let mut runs = Vec::new();
    let mut next = 0;
    for a in arrivals.iter().map(|a| a.seq).chain([sent]) {
        if a > next {
            runs.push(u64::from(a - next));
        }
        next = a + 1;
    }
    runs
}

#[test]
fn bernoulli_loss_rate_equals_the_configured_rate() {
    for p in [0.01, 0.05, 0.20] {
        let config = NetworkConfig {
            loss: p,
            ..NetworkConfig::default()
        };
        let (arrivals, net) = probe_run(1, config, 1, LOSS_PROBES, PROBE_PERIOD, 100);
        let lost = u64::from(LOSS_PROBES) - arrivals.len() as u64;
        let measured = lost as f64 / f64::from(LOSS_PROBES);
        let sigma = (p * (1.0 - p) / f64::from(LOSS_PROBES)).sqrt();
        println!(
            "validation bernoulli p={p}: measured {measured:.5}, tolerance {:.5}",
            SIGMAS * sigma
        );
        assert_eq!(net.packets_sent, u64::from(LOSS_PROBES));
        assert_eq!(net.dropped_loss_packets, lost, "counter matches receiver");
        assert_eq!(net.packets_delivered, arrivals.len() as u64);
        assert!(
            (measured - p).abs() <= SIGMAS * sigma,
            "p={p}: measured {measured}"
        );
    }
}

/// Gilbert (1960) / Elliott (1963): a two-state chain with `P(good->bad) =
/// p` and `P(bad->good) = r` has stationary `P(bad) = p / (p + r)`; with
/// loss probability `q` in the bad state the mean loss is `q p / (p + r)`,
/// and with `q = 1` a burst of consecutive losses is a bad sojourn, of
/// geometric length with mean `1 / r`.
#[test]
fn gilbert_elliott_matches_the_closed_forms() {
    // The first is the `loss-burst` regime's chain; the second loses only
    // half the datagrams sent while bad.
    for (p, r, q) in [(0.01, 0.19, 1.0), (0.02, 0.10, 0.5)] {
        let config = NetworkConfig {
            burst_loss: Some(GilbertElliott {
                p_good_to_bad: p,
                p_bad_to_good: r,
                loss_in_bad: q,
            }),
            ..NetworkConfig::default()
        };
        let (arrivals, net) = probe_run(1, config, 1, LOSS_PROBES, PROBE_PERIOD, 100);
        let n = f64::from(LOSS_PROBES);
        let lost = LOSS_PROBES as u64 - arrivals.len() as u64;
        assert_eq!(net.dropped_loss_packets, lost);

        let bad = p / (p + r);
        let expected = q * bad;
        let measured = lost as f64 / n;
        // Asymptotic variance of the mean of a loss indicator that is
        // Bernoulli(q) while the chain is bad: the within-state noise plus
        // the chain's autocorrelation, whose lag-k correlation is (1-p-r)^k.
        let rho = 1.0 - p - r;
        let variance = bad * q * (1.0 - q) + q * q * bad * (1.0 - bad) * (1.0 + rho) / (1.0 - rho);
        let sigma = (variance / n).sqrt();
        println!(
            "validation gilbert-elliott p={p} r={r} q={q}: loss measured {measured:.5}, expected \
             {expected:.5}, tolerance {:.5}",
            SIGMAS * sigma
        );
        assert!(
            (measured - expected).abs() <= SIGMAS * sigma,
            "mean loss {measured}, expected {expected} ± {}",
            SIGMAS * sigma
        );

        if q == 1.0 {
            let bursts = lost_runs(&arrivals, LOSS_PROBES);
            let mean = bursts.iter().sum::<u64>() as f64 / bursts.len() as f64;
            // Geometric on 1, 2, ...: variance (1 - r) / r^2.
            let sigma = ((1.0 - r).sqrt() / r) / (bursts.len() as f64).sqrt();
            println!(
                "validation gilbert-elliott p={p} r={r}: {} bursts, mean length {mean:.3}, \
                 expected {:.3}, tolerance {:.3}",
                bursts.len(),
                1.0 / r,
                SIGMAS * sigma
            );
            assert!(
                (mean - 1.0 / r).abs() <= SIGMAS * sigma,
                "mean burst {mean}, expected {}",
                1.0 / r
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Latency
// ---------------------------------------------------------------------------

const LATENCY_PROBES: u32 = 200_000;

/// Kolmogorov-Smirnov distance between `delays` and the discrete uniform
/// distribution on `base..=base + jitter`, the model's `base + U(0, jitter)`.
fn ks_distance_to_uniform(mut delays: Vec<u64>, base: u64, jitter: u64) -> f64 {
    delays.sort_unstable();
    let n = delays.len() as f64;
    let cdf = |x: u64| (x.saturating_sub(base - 1)).min(jitter + 1) as f64 / (jitter + 1) as f64;
    delays
        .iter()
        .enumerate()
        .map(|(i, &x)| {
            let above = (i + 1) as f64 / n - cdf(x);
            let below = cdf(x - 1) - i as f64 / n;
            above.max(below)
        })
        .fold(0.0, f64::max)
}

#[test]
fn delivered_delay_follows_the_configured_latency_model() {
    // No receive-path service time, so a datagram is read the instant it
    // arrives and the delay is the network's alone.
    let instant_rx = NetworkConfig {
        rx_ns_per_datagram: 0,
        rx_ns_per_byte: 0,
        ..NetworkConfig::default()
    };
    let slow_links = NetworkConfig {
        intra_az_base_ns: 2_000_000,
        intra_az_jitter_max_ns: 3_000_000,
        cross_az_base_ns: 15_000_000,
        cross_az_jitter_max_ns: 8_000_000,
        ..instant_rx
    };
    for (i, (name, config)) in [("default", instant_rx), ("slow", slow_links)]
        .into_iter()
        .enumerate()
    {
        // Node 3 shares node 0's zone; node 1 does not.
        for (path, to, base, jitter) in [
            (
                "intra-AZ",
                3,
                config.intra_az_base_ns,
                config.intra_az_jitter_max_ns,
            ),
            (
                "cross-AZ",
                1,
                config.cross_az_base_ns,
                config.cross_az_jitter_max_ns,
            ),
        ] {
            // A distinct seed per path, so no two share a draw sequence.
            let seed = (10 * i + to) as u64;
            let (arrivals, _) = probe_run(seed, config, to, LATENCY_PROBES, PROBE_PERIOD, 100);
            assert_eq!(
                arrivals.len(),
                LATENCY_PROBES as usize,
                "no loss configured"
            );
            let delays: Vec<u64> = arrivals.iter().map(|a| a.delay_ns).collect();
            let (min, max) = (delays.iter().min().copied(), delays.iter().max().copied());
            assert!(min >= Some(base) && max <= Some(base + jitter));

            let n = delays.len() as f64;
            let mean = delays.iter().sum::<u64>() as f64 / n;
            let expected_mean = base as f64 + jitter as f64 / 2.0;
            let mean_tolerance = SIGMAS * (jitter as f64 / 12.0_f64.sqrt()) / n.sqrt();
            let ks = ks_distance_to_uniform(delays, base, jitter);
            // Kolmogorov distribution: P(sqrt(n) D > 1.95) = 0.001.
            let ks_critical = 1.95 / n.sqrt();
            println!(
                "validation latency {name} {path}: mean {:.1} us, expected {:.1} us ± {:.2}; \
                 range {}..{} ns, configured {base}..{} ns; KS D {ks:.5}, critical \
                 {ks_critical:.5}",
                mean / 1e3,
                expected_mean / 1e3,
                mean_tolerance / 1e3,
                min.unwrap_or(0),
                max.unwrap_or(0),
                base + jitter,
            );
            assert!(
                (mean - expected_mean).abs() <= mean_tolerance,
                "{name} {path} mean"
            );
            assert!(ks <= ks_critical, "{name} {path}: KS distance {ks}");
        }
    }
}

// ---------------------------------------------------------------------------
// 4. Byte accounting
// ---------------------------------------------------------------------------

/// IPv4 plus UDP header, written out here rather than imported so the tally
/// does not share a constant with the code under test.
const HEADER: u64 = 28;

#[derive(Default)]
struct Tally {
    sent_packets: Cell<u64>,
    sent_payload: Cell<u64>,
    sent_wire: Cell<u64>,
    window_packets: Cell<u64>,
    window_wire: Cell<u64>,
    read_packets: Cell<u64>,
    read_wire: Cell<u64>,
}

/// A node's socket that tallies every datagram handed to it and every one
/// read from it, independently of [`Network`]'s own counters.
struct TallySocket {
    inner: SimSocket,
    tally: Rc<Tally>,
    measure_from_ns: u64,
}

impl GossipTransport for TallySocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        self.inner.writable()
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let sent = self.inner.try_send_to(buf, dst)?;
        let wire = buf.len() as u64 + HEADER;
        let t = &self.tally;
        t.sent_packets.set(t.sent_packets.get() + 1);
        t.sent_payload.set(t.sent_payload.get() + buf.len() as u64);
        t.sent_wire.set(t.sent_wire.get() + wire);
        if self.inner.world.now_ns() >= self.measure_from_ns {
            t.window_packets.set(t.window_packets.get() + 1);
            t.window_wire.set(t.window_wire.get() + wire);
        }
        Ok(sent)
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (len, src) = self.inner.recv_from(buf).await?;
        let t = &self.tally;
        t.read_packets.set(t.read_packets.get() + 1);
        t.read_wire.set(t.read_wire.get() + len as u64 + HEADER);
        Ok((len, src))
    }
}

/// `tiny` is a real gabion cluster with loss, a rollout and a crash, so
/// datagrams are lost, dropped at stopped pods, and still in flight at the
/// end, as well as delivered.
#[test]
fn byte_totals_equal_an_independent_tally_on_a_real_run() {
    let regime = fixtures::tiny();
    let seed = 3;
    let measure_from_ns = regime.warmup_s * NS_PER_S;
    let plan = Plan::build(&regime, seed);
    let keys = Rc::new(node::KeySpace::build(&regime, seed));
    let sizing = node::Sizing::new(&regime, &plan, &keys);
    let world = Rc::new(driver::world_for(&regime, seed, &plan));
    let tally = Rc::new(Tally::default());
    let cells = sizing.cell_store_config();
    let launch = {
        let world = Rc::clone(&world);
        let keys = Rc::clone(&keys);
        let tally = Rc::clone(&tally);
        move |idx| {
            let socket = TallySocket {
                inner: SimSocket {
                    world: Rc::clone(&world),
                    idx,
                    addr: node_addr(idx),
                },
                tally: Rc::clone(&tally),
                measure_from_ns,
            };
            node::launch_on(socket, &world, &keys, cells, idx, seed)
        }
    };
    let result = Driver::new(&regime, seed, plan, sizing, keys, world, launch)
        .run()
        .expect("run");
    assert!(result.valid, "{:?}", result.invalid_reasons);

    let (run, window) = (result.network.run, result.network.measure);
    println!(
        "validation bytes: {} datagrams, {} B on the wire in the run (tally {}), {} B in the \
         measurement window (tally {}); {} B read (tally {})",
        run.packets_sent,
        run.bytes_sent,
        tally.sent_wire.get(),
        window.bytes_sent,
        tally.window_wire.get(),
        run.bytes_delivered,
        tally.read_wire.get(),
    );
    assert!(run.packets_sent > 1_000);
    assert!(run.dropped_loss_packets > 0 && run.dropped_no_receiver_packets > 0);
    assert_eq!(run.packets_sent, tally.sent_packets.get());
    assert_eq!(run.payload_bytes_sent, tally.sent_payload.get());
    assert_eq!(run.bytes_sent, tally.sent_wire.get());
    assert_eq!(
        run.bytes_sent,
        run.payload_bytes_sent + HEADER * run.packets_sent
    );
    assert_eq!(window.packets_sent, tally.window_packets.get());
    assert_eq!(window.bytes_sent, tally.window_wire.get());
    assert_eq!(run.packets_delivered, tally.read_packets.get());
    assert_eq!(run.bytes_delivered, tally.read_wire.get());
}

// ---------------------------------------------------------------------------
// 5. Receive buffer
// ---------------------------------------------------------------------------

fn datagram(len: usize) -> Datagram {
    Datagram {
        src: node_addr(0),
        bytes: vec![0; len].into_boxed_slice(),
    }
}

fn two_node_network(config: NetworkConfig) -> Network {
    let plan = Plan::build(&fixtures::quiet(2), 1);
    let mut net = Network::new(config, Vec::new(), &plan, 1, 0);
    net.set_running(0, 1, true);
    net
}

/// A datagram is dropped exactly when the bytes already queued plus its own
/// would exceed the buffer: checked on every one of 300,000 random arrivals
/// and reads against a plain queue-of-lengths model.
#[test]
fn receive_buffer_drops_exactly_when_the_queue_would_overflow() {
    const CAPACITY: u64 = 8_192;
    let mut net = two_node_network(NetworkConfig {
        rcvbuf_bytes: CAPACITY,
        rx_ns_per_datagram: 0,
        rx_ns_per_byte: 0,
        ..NetworkConfig::default()
    });
    let mut rng = Rng::stream(1, Stream::Oracle, 0);
    let mut model: VecDeque<usize> = VecDeque::new();
    let mut queued = 0_u64;
    let mut cx = Context::from_waker(Waker::noop());
    let mut buf = [0_u8; 1_500];
    let (mut arrivals, mut drops, mut at_the_brink) = (0_u64, 0_u64, 0_u64);
    for _ in 0..300_000 {
        if rng.chance(0.55) {
            let len = 1 + rng.below(1_472) as usize;
            let before = net.run.dropped_rcvbuf_packets;
            net.arrive(0, 1, datagram(len));
            let dropped = net.run.dropped_rcvbuf_packets > before;
            let would_overflow = queued + len as u64 > CAPACITY;
            assert_eq!(
                dropped, would_overflow,
                "queued {queued} B, arrival {len} B"
            );
            arrivals += 1;
            drops += u64::from(dropped);
            at_the_brink += u64::from(queued + len as u64 == CAPACITY);
            if !dropped {
                model.push_back(len);
                queued += len as u64;
            }
        } else {
            let read = net.poll_recv(1, 0, &mut cx, &mut buf);
            match model.pop_front() {
                Some(len) => {
                    assert_eq!(read, Recv::Datagram(len, node_addr(0)));
                    queued -= len as u64;
                }
                None => assert_eq!(read, Recv::Empty),
            }
        }
    }
    println!(
        "validation rcvbuf model: {arrivals} arrivals, {drops} dropped, {at_the_brink} filled the \
         buffer exactly and were accepted; 0 disagreements"
    );
    assert!(drops > 10_000 && arrivals - drops > 10_000);
    assert!(at_the_brink > 0, "the boundary itself is exercised");
}

/// The boundary, by hand: filling the buffer to the byte is accepted, one
/// byte over is dropped, and a read frees exactly what it read.
#[test]
fn receive_buffer_boundary_is_exact() {
    let mut net = two_node_network(NetworkConfig {
        rcvbuf_bytes: 3_000,
        rx_ns_per_datagram: 0,
        rx_ns_per_byte: 0,
        ..NetworkConfig::default()
    });
    let dropped = |net: &Network| net.run.dropped_rcvbuf_packets;
    for _ in 0..3 {
        net.arrive(0, 1, datagram(1_000));
    }
    assert_eq!(dropped(&net), 0, "3,000 B in a 3,000 B buffer");
    net.arrive(0, 1, datagram(1));
    assert_eq!(dropped(&net), 1, "one byte over");
    let mut cx = Context::from_waker(Waker::noop());
    let mut buf = [0_u8; 1_500];
    assert_eq!(
        net.poll_recv(1, 0, &mut cx, &mut buf),
        Recv::Datagram(1_000, node_addr(0))
    );
    net.arrive(0, 1, datagram(1_001));
    assert_eq!(
        dropped(&net),
        2,
        "1,001 B does not fit in the 1,000 B freed"
    );
    net.arrive(0, 1, datagram(1_000));
    assert_eq!(dropped(&net), 2, "1,000 B fits exactly");
    net.arrive(0, 1, datagram(3_001));
    assert_eq!(dropped(&net), 3, "larger than the whole buffer");
}

/// The same rule through the event loop: 50 probes arrive within 50 ns of
/// each other at a node that needs 5 us to read one. The first is read on
/// arrival, the next ten fill the buffer to the byte, and the other 39
/// overflow it.
#[test]
fn receive_buffer_overflow_through_the_event_loop() {
    let config = NetworkConfig {
        rcvbuf_bytes: 10_000,
        cross_az_base_ns: 1_000,
        cross_az_jitter_max_ns: 0,
        rx_ns_per_datagram: 5_000,
        rx_ns_per_byte: 0,
        ..NetworkConfig::default()
    };
    let (arrivals, net) = probe_run(1, config, 1, 50, Duration::from_nanos(1), 1_000);
    let read: Vec<u32> = arrivals.iter().map(|a| a.seq).collect();
    println!(
        "validation rcvbuf burst: 50 sent, {} read (seqs {:?}..{:?}), {} dropped, expected 11 \
         read and 39 dropped",
        read.len(),
        read.first(),
        read.last(),
        net.dropped_rcvbuf_packets
    );
    assert_eq!(read, (0..=10).collect::<Vec<u32>>());
    assert_eq!(net.dropped_rcvbuf_packets, 39);
    assert_eq!(net.dropped_rcvbuf_bytes, 39 * (1_000 + HEADER));
    assert_eq!(net.packets_delivered, 11);
}
