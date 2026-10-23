//! Discrete-event core: the virtual clock, the event queue, and the
//! implementations of gabion's [`GossipTransport`], [`Clock`], [`Ticker`]
//! and discovery stream that connect each node's runtime to them.
//!
//! Time is a `u64` count of virtual nanoseconds. It advances only when the
//! driver pops the next event, so results do not depend on how fast the host
//! runs. Nodes process instantly: no virtual time passes while a runtime is
//! polled. The runtime sees milliseconds through [`Clock::now_millis`], as it
//! does in production; ticks and datagram arrivals keep nanosecond precision
//! with no quantization.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures::Stream;
use gabion::discovery::{Peer, PeerEvent};
use gabion::gossip::{Clock, GossipTransport, Ticker};

use super::net::{Datagram, Network, Recv, Route};
use super::plan::{NS_PER_MS, PlanEvent};

/// Unix-epoch milliseconds at virtual time zero: 2026-01-01T00:00:00Z, a
/// multiple of every bucket width the regimes use, so virtual bucket
/// boundaries fall on whole multiples of the bucket.
pub const BASE_EPOCH_MS: u64 = 1_767_225_600_000;

pub enum Event {
    /// A timer registered by a [`SimTicker`] fired.
    Wake(Waker),
    Arrive {
        dst: usize,
        datagram: Datagram,
    },
    Plan(PlanEvent),
    /// The next request of key `.0` arrives.
    Request(u32),
    BucketBoundary,
    ResidualSample,
    HealthSnapshot,
}

struct Entry {
    at_ns: u64,
    seq: u64,
    event: Event,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    /// Reversed so [`BinaryHeap`] pops the earliest event; ties pop in
    /// insertion order.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at_ns, other.seq).cmp(&(self.at_ns, self.seq))
    }
}

#[derive(Default)]
pub struct EventQueue {
    heap: BinaryHeap<Entry>,
    seq: u64,
}

impl EventQueue {
    pub fn push(&mut self, at_ns: u64, event: Event) {
        self.seq += 1;
        self.heap.push(Entry {
            at_ns,
            seq: self.seq,
            event,
        });
    }

    pub fn pop(&mut self) -> Option<(u64, Event)> {
        self.heap.pop().map(|e| (e.at_ns, e.event))
    }
}

#[derive(Default)]
struct DiscoveryInbox {
    queue: VecDeque<PeerEvent>,
    waker: Option<Waker>,
}

/// State shared between the driver and every node's transport, clock and
/// discovery stream. Single-threaded; borrows never span a poll.
pub struct World {
    now_ns: Cell<u64>,
    pub events: RefCell<EventQueue>,
    pub net: RefCell<Network>,
    discovery: RefCell<Vec<DiscoveryInbox>>,
}

impl World {
    pub fn new(net: Network, nodes: usize) -> Self {
        Self {
            now_ns: Cell::new(0),
            events: RefCell::new(EventQueue::default()),
            net: RefCell::new(net),
            discovery: RefCell::new((0..nodes).map(|_| DiscoveryInbox::default()).collect()),
        }
    }

    pub fn now_ns(&self) -> u64 {
        self.now_ns.get()
    }

    pub fn set_now_ns(&self, now_ns: u64) {
        debug_assert!(now_ns >= self.now_ns.get(), "virtual time went backwards");
        self.now_ns.set(now_ns);
    }

    pub fn now_millis(&self) -> u64 {
        BASE_EPOCH_MS + self.now_ns.get() / NS_PER_MS
    }

    pub fn schedule(&self, at_ns: u64, event: Event) {
        self.events.borrow_mut().push(at_ns, event);
    }

    /// Queue a discovery event for node `idx`; returns the waker to fire.
    pub fn discover(&self, idx: usize, event: PeerEvent) -> Option<Waker> {
        let mut inboxes = self.discovery.borrow_mut();
        let inbox = &mut inboxes[idx];
        inbox.queue.push_back(event);
        inbox.waker.take()
    }

    pub fn clear_discovery(&self, idx: usize) {
        self.discovery.borrow_mut()[idx] = DiscoveryInbox::default();
    }
}

/// One node's UDP socket.
pub struct SimSocket {
    pub world: Rc<World>,
    pub idx: usize,
    pub addr: SocketAddr,
}

impl GossipTransport for SimSocket {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    /// The kernel send buffer never fills: a datagram is either queued on
    /// the wire or dropped, never refused.
    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        std::future::ready(Ok(()))
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let now_ns = self.world.now_ns();
        let route = self
            .world
            .net
            .borrow_mut()
            .send(now_ns, self.idx, buf.len(), dst);
        if let Route::Deliver { dst, latency_ns } = route {
            self.world.schedule(
                now_ns + latency_ns,
                Event::Arrive {
                    dst,
                    datagram: Datagram {
                        src: self.addr,
                        bytes: buf.into(),
                    },
                },
            );
        }
        Ok(buf.len())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let received = std::future::poll_fn(|cx| {
            let now_ns = self.world.now_ns();
            let recv = self
                .world
                .net
                .borrow_mut()
                .poll_recv(self.idx, now_ns, cx, buf);
            match recv {
                Recv::Datagram(n, src) => Poll::Ready((n, src)),
                Recv::Empty => Poll::Pending,
                Recv::Busy(wake_at) => {
                    if let Some(at) = wake_at {
                        self.world.schedule(at, Event::Wake(cx.waker().clone()));
                    }
                    Poll::Pending
                }
            }
        })
        .await;
        Ok(received)
    }
}

/// The node's wall clock: virtual time offset to [`BASE_EPOCH_MS`]. All
/// nodes share it (no clock skew).
pub struct SimClock {
    pub world: Rc<World>,
}

impl Clock for SimClock {
    type Ticker = SimTicker;

    fn now_millis(&self) -> u64 {
        self.world.now_millis()
    }

    fn ticker(&self, interval: Duration) -> SimTicker {
        SimTicker {
            world: Rc::clone(&self.world),
            interval_ns: (interval.as_nanos() as u64).max(1),
            due_ns: None,
            armed_ns: None,
        }
    }
}

/// Mirrors `tokio::time::interval` with `MissedTickBehavior::Delay`, the
/// production ticker: the first tick resolves immediately; afterwards a
/// tick is due every `interval`, and a tick consumed late pushes the
/// cadence back to `now + interval`.
pub struct SimTicker {
    world: Rc<World>,
    interval_ns: u64,
    due_ns: Option<u64>,
    armed_ns: Option<u64>,
}

impl SimTicker {
    fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let now = self.world.now_ns();
        let due = *self.due_ns.get_or_insert(now);
        if now >= due {
            let next = if now > due { now } else { due };
            self.due_ns = Some(next + self.interval_ns);
            return Poll::Ready(());
        }
        if self.armed_ns != Some(due) {
            self.world.schedule(due, Event::Wake(cx.waker().clone()));
            self.armed_ns = Some(due);
        }
        Poll::Pending
    }
}

impl Ticker for SimTicker {
    async fn tick(&mut self) {
        std::future::poll_fn(|cx| self.poll_tick(cx)).await;
    }
}

/// The node's discovery feed. Never ends; a stopped node's stream is
/// dropped with its runtime.
pub struct PeerStream {
    pub world: Rc<World>,
    pub idx: usize,
}

impl Stream for PeerStream {
    type Item = PeerEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<PeerEvent>> {
        let mut inboxes = self.world.discovery.borrow_mut();
        let inbox = &mut inboxes[self.idx];
        match inbox.queue.pop_front() {
            Some(event) => Poll::Ready(Some(event)),
            None => {
                inbox.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub fn peer_event(addr: SocketAddr, added: bool) -> PeerEvent {
    let peer = Peer::new(addr);
    if added {
        PeerEvent::Added(peer)
    } else {
        PeerEvent::Removed(peer)
    }
}

/// Nodes whose task was woken, in wake order, without duplicates.
#[derive(Default)]
struct ReadyQueue {
    order: VecDeque<usize>,
    queued: Vec<bool>,
}

struct TaskWaker {
    idx: usize,
    ready: Arc<Mutex<ReadyQueue>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut ready = self.ready.lock().unwrap_or_else(PoisonError::into_inner);
        if !ready.queued[self.idx] {
            ready.queued[self.idx] = true;
            ready.order.push_back(self.idx);
        }
    }
}

/// A minimal single-threaded executor: one task per node, polled when its
/// waker fires, in deterministic wake order.
pub struct Tasks<F> {
    futures: Vec<Option<Pin<Box<F>>>>,
    wakers: Vec<Waker>,
    ready: Arc<Mutex<ReadyQueue>>,
}

impl<F: Future> Tasks<F> {
    pub fn new(nodes: usize) -> Self {
        let ready = Arc::new(Mutex::new(ReadyQueue {
            order: VecDeque::new(),
            queued: vec![false; nodes],
        }));
        let wakers = (0..nodes)
            .map(|idx| {
                Waker::from(Arc::new(TaskWaker {
                    idx,
                    ready: Arc::clone(&ready),
                }))
            })
            .collect();
        Self {
            futures: (0..nodes).map(|_| None).collect(),
            wakers,
            ready,
        }
    }

    pub fn spawn(&mut self, idx: usize, future: F) {
        self.futures[idx] = Some(Box::pin(future));
        self.wakers[idx].wake_by_ref();
    }

    pub fn remove(&mut self, idx: usize) {
        self.futures[idx] = None;
    }

    pub fn is_running(&self, idx: usize) -> bool {
        self.futures[idx].is_some()
    }

    pub fn next_ready(&self) -> Option<usize> {
        let mut ready = self.ready.lock().unwrap_or_else(PoisonError::into_inner);
        let idx = ready.order.pop_front()?;
        ready.queued[idx] = false;
        Some(idx)
    }

    /// Poll node `idx` once. `Some` carries the output of a task that
    /// finished; the task is removed.
    pub fn poll(&mut self, idx: usize) -> Option<F::Output> {
        let future = self.futures[idx].as_mut()?;
        let mut cx = Context::from_waker(&self.wakers[idx]);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => {
                self.futures[idx] = None;
                Some(output)
            }
            Poll::Pending => None,
        }
    }
}
