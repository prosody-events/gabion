//! In-process simulator for the gossip event loop.
//!
//! [`SimTransport`] implements [`crate::gossip::GossipTransport`] over mpsc
//! channels routed by a shared [`SimRouter`]. Combined with
//! `tokio::time::pause()` and a [`crate::gossip::TokioClock`] anchored at
//! a known base, property tests can drive many runtimes in one process,
//! deliver packets through memory, and advance virtual time deterministically.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};

use super::transport::GossipTransport;

/// Per-direction link policy for property tests modelling lossy networks.
#[derive(Clone, Copy, Debug, Default)]
pub enum LinkPolicy {
    /// Always deliver.
    #[default]
    Pass,
    /// Drop every packet on this link.
    Block,
    /// Drop the first `count` packets, then deliver normally. Useful for
    /// "warm-up" scenarios where a link is unreachable for a known
    /// number of attempts before recovering.
    DropFirst { count: u32 },
    /// i.i.d. Bernoulli drop: each packet is dropped independently with
    /// probability `p` (clamped to `[0.0, 1.0]`). Uses a deterministic
    /// per-link PRNG seeded from the link's `(src, dst)` so repeated
    /// runs of the same scenario hit the same drop pattern. Matches the
    /// loss model used by Birman et al. (Bimodal Multicast, TOCS 1999).
    DropProb { p: f64 },
}

/// One packet in flight on the sim network. The byte buffer is owned (one
/// copy per send) — production stays zero-copy via the UDP transport.
#[derive(Debug)]
struct SimPacket {
    src: SocketAddr,
    bytes: Vec<u8>,
}

struct SimRouterInner {
    /// `addr -> inbound sender` lookups.
    senders: RefCell<HashMap<SocketAddr, mpsc::Sender<SimPacket>>>,
    /// Per-direction policy. Defaults to `Pass` for unknown pairs.
    policies: RefCell<HashMap<(SocketAddr, SocketAddr), LinkPolicy>>,
    /// Per-direction counters used by `DropFirst`.
    drop_counters: RefCell<HashMap<(SocketAddr, SocketAddr), u32>>,
    /// Per-recipient packet counters incremented after policy evaluation
    /// decides the packet is delivered. Used by property tests that check
    /// peer-sampling distribution.
    received_counts: RefCell<HashMap<SocketAddr, u64>>,
    /// Per-receiver send-channel capacity. Bounded queue applies backpressure.
    channel_capacity: usize,
}

/// In-memory packet router shared by all sim runtimes in one process.
/// Cheap to clone — internally an `Rc<SimRouterInner>`.
#[derive(Clone)]
pub struct SimRouter {
    inner: Rc<SimRouterInner>,
}

impl Default for SimRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl SimRouter {
    pub fn new() -> Self {
        Self::with_channel_capacity(64)
    }

    pub fn with_channel_capacity(channel_capacity: usize) -> Self {
        Self {
            inner: Rc::new(SimRouterInner {
                senders: RefCell::new(HashMap::new()),
                policies: RefCell::new(HashMap::new()),
                drop_counters: RefCell::new(HashMap::new()),
                received_counts: RefCell::new(HashMap::new()),
                channel_capacity,
            }),
        }
    }

    /// Number of packets that were *delivered* to `addr` (i.e. the policy did
    /// not drop them and the destination had a receiver registered). Used by
    /// property tests that need per-recipient stats.
    pub fn received_count(&self, addr: SocketAddr) -> u64 {
        self.inner
            .received_counts
            .borrow()
            .get(&addr)
            .copied()
            .unwrap_or(0)
    }

    /// Register a transport at `addr` and return its handle. The transport
    /// holds the inbound receiver; the router keeps the matching sender.
    pub fn bind(&self, addr: SocketAddr) -> SimTransport {
        let (tx, rx) = mpsc::channel(self.inner.channel_capacity);
        self.inner.senders.borrow_mut().insert(addr, tx);
        SimTransport {
            addr,
            router: self.clone(),
            inbound_rx: Mutex::new(rx),
            blocked: RefCell::new(false),
        }
    }

    /// Drop the sender registered at `addr`, the symmetric inverse of
    /// [`SimRouter::bind`]; returns whether one was bound. Sim/test-only — it
    /// lets a simulation remove a node at runtime without leaving a dead sender
    /// in the routing table (otherwise the map would accumulate one stale entry
    /// per removal). After unbinding, sends to `addr` see no receiver and land
    /// on the floor, exactly as production UDP to a departed peer does.
    pub fn unbind(&self, addr: SocketAddr) -> bool {
        self.inner.senders.borrow_mut().remove(&addr).is_some()
    }

    /// Install a per-(src, dst) link policy. Default is [`LinkPolicy::Pass`].
    pub fn set_link_policy(&self, src: SocketAddr, dst: SocketAddr, policy: LinkPolicy) {
        self.inner.policies.borrow_mut().insert((src, dst), policy);
        self.inner.drop_counters.borrow_mut().remove(&(src, dst));
    }

    /// Look up the policy for `(src, dst)`. Mutates the counter for
    /// `DropFirst`/`DropProb`; returns the effective decision: `true` ⇒ drop.
    fn should_drop(&self, src: SocketAddr, dst: SocketAddr) -> bool {
        let policies = self.inner.policies.borrow();
        let Some(policy) = policies.get(&(src, dst)).copied() else {
            return false;
        };
        drop(policies);
        match policy {
            LinkPolicy::Pass => false,
            LinkPolicy::Block => true,
            LinkPolicy::DropFirst { count } => {
                let mut counters = self.inner.drop_counters.borrow_mut();
                let entry = counters.entry((src, dst)).or_insert(0);
                if *entry < count {
                    *entry += 1;
                    true
                } else {
                    false
                }
            }
            LinkPolicy::DropProb { p } => {
                // Deterministic per-link splitmix64 — same seed → same
                // drop pattern across re-runs. We reuse `drop_counters`
                // as the per-link splitmix state since we only need a
                // single u64 of state per link.
                let p = p.clamp(0.0, 1.0);
                if p <= 0.0 {
                    return false;
                }
                if p >= 1.0 {
                    return true;
                }
                let key = link_seed(src, dst);
                let mut counters = self.inner.drop_counters.borrow_mut();
                let state = counters.entry((src, dst)).or_insert_with(|| key as u32);
                // 32-bit splitmix. Drop iff next sample < p * u32::MAX.
                let mut z = (*state as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                *state = (*state).wrapping_add(1);
                let threshold = (p * (u32::MAX as f64)) as u32;
                (z as u32) < threshold
            }
        }
    }

    fn sender_for(&self, addr: SocketAddr) -> Option<mpsc::Sender<SimPacket>> {
        self.inner.senders.borrow().get(&addr).cloned()
    }

    fn record_delivery(&self, dst: SocketAddr) {
        *self
            .inner
            .received_counts
            .borrow_mut()
            .entry(dst)
            .or_insert(0) += 1;
    }
}

fn link_seed(src: SocketAddr, dst: SocketAddr) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for octet in src.to_string().bytes().chain(dst.to_string().bytes()) {
        h ^= octet as u64;
        h = h.wrapping_mul(0x1000_0000_01B3);
    }
    h
}

/// In-memory bidirectional transport. Single-threaded — the inbound receiver
/// lives in a `tokio::sync::Mutex` so `recv_from(&self, ...)` can hold the
/// lock across `.await` without tripping clippy's
/// `await_holding_refcell_ref` lint. In single-task use the mutex is never
/// contended; it's purely a borrow-across-await mechanism.
pub struct SimTransport {
    addr: SocketAddr,
    router: SimRouter,
    inbound_rx: Mutex<mpsc::Receiver<SimPacket>>,
    /// Set whenever `try_send_to` returns `WouldBlock`; cleared on the next
    /// successful send. Used by `writable` to insert a yield only while the
    /// loop would otherwise hot-spin retrying a blocked send, so other
    /// `select!` arms (notably `tick`) get a chance to run. Models the
    /// realtime UDP behavior where `writable` waits on socket state.
    blocked: RefCell<bool>,
}

impl SimTransport {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl GossipTransport for SimTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.addr)
    }

    async fn writable(&self) -> io::Result<()> {
        // Only yield when the last `try_send_to` returned `WouldBlock`. The
        // normal case (no backpressure) resolves immediately — preserves the
        // timing characteristics existing sim tests were written against.
        // The yield only matters under saturation, where without it the
        // writable arm hot-spins and starves `tick`.
        if *self.blocked.borrow() {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        if self.router.should_drop(self.addr, dst) {
            // Drop silently — caller observes "sent" successfully.
            return Ok(buf.len());
        }
        let Some(sender) = self.router.sender_for(dst) else {
            // No receiver bound at dst — treat as a delivered packet on the
            // floor. UDP semantics: no error surface for the sender.
            return Ok(buf.len());
        };
        let packet = SimPacket {
            src: self.addr,
            bytes: buf.to_vec(),
        };
        match sender.try_send(packet) {
            Ok(()) => {
                self.router.record_delivery(dst);
                *self.blocked.borrow_mut() = false;
                Ok(buf.len())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                *self.blocked.borrow_mut() = true;
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                *self.blocked.borrow_mut() = false;
                Ok(buf.len())
            }
        }
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        // `tokio::sync::Mutex` so the guard can be held across `.await`;
        // single-task ownership of `SimTransport` means the mutex is never
        // contended in tests.
        let mut rx = self.inbound_rx.lock().await;
        let packet = rx
            .recv()
            .await
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        drop(rx);
        let n = packet.bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&packet.bytes[..n]);
        Ok((n, packet.src))
    }
}

/// Tick the runtime forward by `period`, then yield so the runtime task can
/// observe the elapsed timer. Combine with `tokio::time::pause()`.
pub async fn sim_advance(period: Duration) {
    tokio::time::advance(period).await;
    tokio::task::yield_now().await;
}

/// Convenience: advance the clock in `n` increments of `period`, yielding
/// between each step so the runtime processes its tick.
pub async fn sim_advance_ticks(period: Duration, n: u32) {
    for _ in 0..n {
        sim_advance(period).await;
    }
}
