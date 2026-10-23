//! Reference test nodes and a minimal driver over the harness's own network
//! model ([`Network`]), event queue and executor ([`World`], [`Tasks`]), the
//! machinery [`Driver`](crate::harness::driver::Driver) runs gabion on.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use gabion::gossip::{Clock, GossipTransport, Ticker};

use crate::harness::net::Network;
use crate::harness::plan::{Plan, node_addr};
use crate::harness::regime::NetworkConfig;
use crate::harness::rng::{Rng, Stream};
use crate::harness::world::{Event, SimClock, SimSocket, Tasks, World};

/// Bytes of a probe's header: sequence number and send time.
const PROBE_HEADER: usize = 12;

pub fn world(plan: &Plan, config: NetworkConfig, seed: u64) -> Rc<World> {
    let net = Network::new(config, Vec::new(), plan, seed, 0);
    Rc::new(World::new(net, plan.nodes.len()))
}

fn socket(world: &Rc<World>, idx: usize) -> SimSocket {
    SimSocket {
        world: Rc::clone(world),
        idx,
        addr: node_addr(idx),
    }
}

/// Run one task per node, `node(idx)`, until the event queue drains: pop the
/// next event, apply it, poll every task it woke.
pub fn run<F: Future<Output = ()>>(world: &Rc<World>, n: usize, mut node: impl FnMut(usize) -> F) {
    let mut tasks = Tasks::new(n);
    for idx in 0..n {
        world.net.borrow_mut().set_running(0, idx, true);
        tasks.spawn(idx, node(idx));
    }
    loop {
        while let Some(idx) = tasks.next_ready() {
            tasks.poll(idx);
        }
        let Some((at_ns, event)) = world.events.borrow_mut().pop() else {
            return;
        };
        world.set_now_ns(at_ns);
        match event {
            Event::Wake(waker) => waker.wake(),
            Event::Arrive { dst, datagram } => {
                let waker = world.net.borrow_mut().arrive(at_ns, dst, datagram);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            _ => {}
        }
    }
}

/// A node's schedule of probes to send: `count` datagrams of `len` bytes to
/// node `to`, one per `period`.
#[derive(Clone, Copy)]
pub struct Probes {
    pub to: usize,
    pub count: u32,
    pub period: Duration,
    pub len: usize,
}

/// One probe as its receiver read it.
#[derive(Clone, Copy)]
pub struct Arrival {
    pub seq: u32,
    /// Virtual time from the send to the read.
    pub delay_ns: u64,
}

pub type Log = Rc<RefCell<Vec<Arrival>>>;

/// A node that sends `probes`, if it has any, and otherwise logs every probe
/// it reads.
pub async fn probe_node(world: Rc<World>, idx: usize, probes: Option<Probes>, log: Log) {
    let socket = socket(&world, idx);
    if let Some(probes) = probes {
        let mut ticker = SimClock {
            world: Rc::clone(&world),
        }
        .ticker(probes.period);
        let mut payload = vec![0_u8; probes.len.max(PROBE_HEADER)];
        for seq in 0..probes.count {
            ticker.tick().await;
            payload[..4].copy_from_slice(&seq.to_le_bytes());
            payload[4..PROBE_HEADER].copy_from_slice(&world.now_ns().to_le_bytes());
            let sent = socket.try_send_to(&payload, node_addr(probes.to));
            assert!(sent.is_ok(), "the simulated socket accepts every datagram");
        }
        return;
    }
    let mut buf = [0_u8; 1_500];
    loop {
        let read = socket.recv_from(&mut buf).await;
        let Ok((len, _)) = read else { return };
        assert!(len >= PROBE_HEADER, "a probe carries its header");
        let seq = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let mut sent = [0_u8; 8];
        sent.copy_from_slice(&buf[4..PROBE_HEADER]);
        log.borrow_mut().push(Arrival {
            seq,
            delay_ns: world.now_ns() - u64::from_le_bytes(sent),
        });
    }
}

/// Infect-and-forward-once push gossip, the protocol Kermarrec, Massoulié
/// and Ganesh (2003) analyse: on first receipt a node sends the message to
/// `fanout` distinct nodes chosen uniformly from the others, then falls
/// silent. A non-integer fanout is realised by randomised rounding, so the
/// mean fanout is exact.
#[derive(Clone, Copy)]
pub struct Epidemic {
    pub nodes: usize,
    pub fanout: f64,
    pub seed: u64,
}

/// The node that holds the message at the start.
const ORIGIN: usize = 0;

pub async fn epidemic_node(
    world: Rc<World>,
    idx: usize,
    spec: Epidemic,
    reached: Rc<RefCell<Vec<bool>>>,
) {
    let socket = socket(&world, idx);
    if idx != ORIGIN {
        let mut buf = [0_u8; 1];
        let read = socket.recv_from(&mut buf).await;
        assert!(read.is_ok(), "the simulated socket delivers");
    }
    reached.borrow_mut()[idx] = true;
    let mut rng = Rng::stream(spec.seed, Stream::Gossip, idx as u64);
    let whole = spec.fanout.floor();
    let fanout = whole as usize + usize::from(rng.chance(spec.fanout - whole));
    let mut targets = Vec::with_capacity(fanout);
    while targets.len() < fanout {
        let other = rng.below(spec.nodes as u64 - 1) as usize;
        let target = other + usize::from(other >= idx);
        if !targets.contains(&target) {
            targets.push(target);
            let sent = socket.try_send_to(&[1], node_addr(target));
            assert!(sent.is_ok(), "the simulated socket accepts every datagram");
        }
    }
}
