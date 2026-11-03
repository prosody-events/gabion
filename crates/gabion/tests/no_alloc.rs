//! Steady-state gossip allocates nothing. Three runtimes on one paused
//! current-thread runtime, A in one zone and B and C in another, exchange
//! own-stream packets, acks, threshold and heartbeat rounds and a batched
//! expiry of more than one sink's worth of cells; A's gateway G relays A's
//! cells to the other member M. Every tenth datagram A sends G is lost, so
//! gap reports, timeout resends and prompt rounds run too, and a cut from A
//! to G brings header-only probes and fails A over to M and back.
//! A counting global allocator, armed on this thread for the whole window,
//! counts every allocation any of them makes.
//!
//! The runtimes talk through `RingTransport`: fixed per-address rings of
//! pre-sized slots that copy each datagram once, as the kernel would. The
//! warm-up fills the store, cycles the request channel's blocks and lets
//! every join sweep end, outside the counted window.
//!
//! Miri exemption: the allocator only forwards to `System`, and Miri cannot
//! usefully run a tokio runtime under a counting allocator, so this binary
//! runs under nextest only.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, KeyHash, NodeId, NodeIdentity};
use gabion::discovery::Membership;
use gabion::gossip::Role;
use gabion::gossip::sim::sim_advance_ticks;
use gabion::gossip::{
    AdminCommand, AdminSnapshot, GossipClient, GossipRuntime, GossipTransport, TokioClock,
};
use gabion::rules::{EnforcementMode, RuleDef, RuleTable};
use gabion::settings::Settings;
use gabion::zone::ZoneName;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

thread_local! {
    /// Set on this thread while the counted window runs.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds `GlobalAlloc`'s contract; the bookkeeping touches only
// const-initialised thread-locals with no destructor, so it never allocates
// or re-enters the allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        // SAFETY: the caller's guarantees for `layout` pass straight through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` through `alloc` or `realloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note();
        // SAFETY: the caller's guarantees pass straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn note() {
    if COUNTING.get() {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
    }
}

const PAYLOAD: usize = 1400;

// The warm-up's 5,000 cells expire in more than one batch.
const _: () = assert!(5_000 > gabion::defaults::GOSSIP_EXPIRATION_BATCH);
const SLOTS: usize = 512;

/// One address's inbound ring: datagrams copied into fixed slots, oldest
/// first, and the receiver's waker.
struct Ring {
    slots: Box<[Box<[u8]>]>,
    lens: Box<[usize]>,
    srcs: Box<[SocketAddr]>,
    head: usize,
    len: usize,
    waker: Option<Waker>,
}

impl Ring {
    fn new() -> RefCell<Self> {
        RefCell::new(Self {
            slots: (0..SLOTS)
                .map(|_| vec![0; PAYLOAD].into_boxed_slice())
                .collect(),
            lens: vec![0; SLOTS].into_boxed_slice(),
            srcs: vec![SocketAddr::from(([0, 0, 0, 0], 0)); SLOTS].into_boxed_slice(),
            head: 0,
            len: 0,
            waker: None,
        })
    }
}

/// The nodes' addresses and inbound rings.
struct Net {
    addrs: [SocketAddr; 3],
    rings: [RefCell<Ring>; 3],
    /// A's gateway, once known: in the counted window every tenth datagram
    /// A sends it is lost, and while `cut` is set every one.
    gateway: Cell<Option<usize>>,
    cut: Cell<bool>,
    a_to_g: Cell<u32>,
}

struct RingTransport {
    net: Rc<Net>,
    me: usize,
}

impl GossipTransport for RingTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.net.addrs[self.me])
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let Some(to) = self.net.addrs.iter().position(|&a| a == dst) else {
            return Ok(buf.len());
        };
        if self.me == 0 && Some(to) == self.net.gateway.get() && COUNTING.get() {
            let sent = self.net.a_to_g.get() + 1;
            self.net.a_to_g.set(sent);
            if sent.is_multiple_of(10) || self.net.cut.get() {
                return Ok(buf.len());
            }
        }
        let mut ring = self.net.rings[to].borrow_mut();
        if ring.len == SLOTS {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let slot = (ring.head + ring.len) % SLOTS;
        ring.slots[slot][..buf.len()].copy_from_slice(buf);
        ring.lens[slot] = buf.len();
        ring.srcs[slot] = self.net.addrs[self.me];
        ring.len += 1;
        if let Some(waker) = &ring.waker {
            waker.wake_by_ref();
        }
        Ok(buf.len())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| {
            let mut ring = self.net.rings[self.me].borrow_mut();
            if ring.len == 0 {
                if !ring.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                    ring.waker = Some(cx.waker().clone());
                }
                return Poll::Pending;
            }
            let slot = ring.head;
            let n = ring.lens[slot];
            buf[..n].copy_from_slice(&ring.slots[slot][..n]);
            ring.head = (slot + 1) % SLOTS;
            ring.len -= 1;
            Poll::Ready(Ok((n, ring.srcs[slot])))
        })
        .await
    }
}

/// Record `hits` on `key` in the bucket of `now`, letting the runtime drain
/// its queue when it is full.
async fn record(client: &GossipClient, fingerprint: u128, key: u128, hits: u64, now: u64) {
    while client
        .try_record(
            fingerprint,
            KeyHash(key),
            (now / 100) as u32,
            hits,
            100,
            now,
        )
        .is_err()
    {
        tokio::task::yield_now().await;
    }
}

async fn snapshot(admin: &mpsc::Sender<AdminCommand>) -> AdminSnapshot {
    let (reply, snapshot) = oneshot::channel();
    admin.send(AdminCommand::Snapshot { reply }).await.unwrap();
    snapshot.await.unwrap()
}

#[test]
fn steady_state_gossip_never_allocates() {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("tokio runtime");
    let allocations = tokio.block_on(LocalSet::new().run_until(async {
        let settings = Settings {
            max_payload_bytes: PAYLOAD,
            max_cells: 16_384.try_into().unwrap(),
            tick_interval: gabion::settings::Millis(100.try_into().unwrap()),
            ..Settings::default()
        };
        let (gossip, storage) = settings.validate().expect("settings validate");
        // One rule of a one-second window in 100 ms buckets.
        let rules = RuleTable::new(
            Box::new([RuleDef {
                name: "site",
                domain: "n1",
                patterns: &[("site", "*")],
                rate: "100r/s",
                window: None,
                bucket: Some(Duration::from_millis(100)),
                mode: EnforcementMode::Enforce,
            }
            .build(&storage.cardinality)
            .expect("rule builds")]),
            storage.rule_dictionary_capacity(),
        )
        .expect("rules fit");
        let fingerprint = rules.rules()[0].fingerprint();
        let net = Rc::new(Net {
            addrs: [1, 2, 3].map(|port| SocketAddr::from(([127, 0, 0, 1], port))),
            rings: [Ring::new(), Ring::new(), Ring::new()],
            gateway: Cell::new(None),
            cut: Cell::new(false),
            a_to_g: Cell::new(0),
        });
        let start = |me: usize| {
            let mut config = gossip.clone();
            let peers = (0..3).filter(|&i| i != me).map(|i| net.addrs[i]);
            config.membership = Membership::Static(peers.collect());
            let zone = if me == 0 { "a" } else { "b" };
            config.zone = Some(ZoneName::parse(zone).unwrap());
            let identity = NodeIdentity::new(NodeId(0xA0 + me as u128), 1);
            let (admin, admin_rx) = mpsc::channel(1);
            let (runtime, client) = GossipRuntime::from_parts_with_admin(
                RingTransport {
                    net: Rc::clone(&net),
                    me,
                },
                TokioClock::from_millis(0),
                config,
                CellStore::new(storage, identity, &rules),
                Aggregate::new(&storage),
                Some(admin_rx),
            );
            tokio::task::spawn_local(runtime.run(futures::stream::empty()));
            (client, admin)
        };
        let (a, a_admin) = start(0);
        let (b, b_admin) = start(1);
        let (c, c_admin) = start(2);

        // Warm-up: 5,000 cells at A in bucket 0, B's own cell, and time for
        // every zone to be heard and every stream, relay, ack and join sweep
        // to settle.
        for key in 0..5_000 {
            record(&a, fingerprint, key, 1, 0).await;
        }
        record(&b, fingerprint, 1 << 40, 1, 0).await;
        let mut now = 1_000;
        sim_advance_ticks(Duration::from_millis(1), 1_000).await;
        let a_before = snapshot(&a_admin).await;
        let gateway = a_before
            .peers
            .iter()
            .find(|p| p.role == Role::Gateway)
            .unwrap();
        let g = net
            .addrs
            .iter()
            .position(|&addr| addr == gateway.addr)
            .unwrap();
        net.gateway.set(Some(g));
        let (g_admin, m_admin) = if g == 1 {
            (&b_admin, &c_admin)
        } else {
            (&c_admin, &b_admin)
        };
        let (g_before, m_before) = (snapshot(g_admin).await, snapshot(m_admin).await);
        for zone_b in [&g_before, &m_before] {
            assert_eq!(
                zone_b.store_stats.active_cells, 5_001,
                "B and C hold A's cells"
            );
        }

        // Counted: 1,000 hits, 95 % on live keys and 5 % on new ones, each
        // crossing the error budget past the emit floor, so each runs a
        // threshold round whose flagged packet G acks and relays to M; ten
        // heartbeats a second; at 1,100 ms, the expiry of bucket 0's 5,000
        // cells; from hit 400 to hit 800 A cannot reach G, so A fails over
        // to M with a full rewind and back once G answers.
        COUNTING.set(true);
        for i in 0..1_000_u128 {
            let key = if i % 20 == 0 {
                10_000 + i
            } else {
                i * 7 % 5_000
            };
            if i == 399 {
                // Only G's relay reaches M, which is `Remote` at A.
                COUNTING.set(false);
                let m_now = snapshot(m_admin).await;
                let merged = m_now.counters.cells_merged - m_before.counters.cells_merged;
                assert!(merged >= 300, "{merged} cells relayed");
                COUNTING.set(true);
            }
            net.cut.set((400..800).contains(&i));
            record(&a, fingerprint, key, 2, now).await;
            sim_advance_ticks(Duration::from_millis(1), 6).await;
            now += 6;
        }
        sim_advance_ticks(Duration::from_millis(1), 300).await;
        COUNTING.set(false);

        // The window did what it claims: a threshold round per hit, G
        // receiving all but the lost tenth and A receiving acks, G reporting
        // the gaps the losses open and A resending in prompt rounds, a
        // failover and its way back, probes, and bucket 0's cells expired.
        let (a_after, g_after) = (snapshot(&a_admin).await, snapshot(g_admin).await);
        let delta = |after: u64, before: u64| after - before;
        let (a_c, a_b) = (a_after.counters, a_before.counters);
        assert!(delta(a_c.rounds_threshold, a_b.rounds_threshold) >= 990);
        assert!(delta(a_c.rounds_heartbeat, a_b.rounds_heartbeat) >= 59);
        assert!(delta(a_c.rounds_prompt, a_b.rounds_prompt) > 0);
        let (g_c, g_b) = (g_after.counters, g_before.counters);
        assert!(delta(g_c.packets_received, g_b.packets_received) >= 500);
        assert!(delta(g_c.gap_reports_sent, g_b.gap_reports_sent) > 0);
        assert!(delta(a_c.packets_received, a_b.packets_received) >= 50);
        assert!(a_c.gateway_failovers >= 1);
        assert!(delta(a_c.liveness_probes, a_b.liveness_probes) > 0);
        assert!(delta(a_c.role_changes, a_b.role_changes) >= 2);
        assert!(a_after.store_stats.active_cells < 1_000);
        drop((a, b, c));
        ALLOCATIONS.get()
    }));
    assert_eq!(allocations, 0);
}
