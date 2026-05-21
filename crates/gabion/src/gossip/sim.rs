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
    /// Drop the first `count` packets, then deliver normally.
    DropFirst { count: u32 },
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
                channel_capacity,
            }),
        }
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
        }
    }

    /// Install a per-(src, dst) link policy. Default is [`LinkPolicy::Pass`].
    pub fn set_link_policy(&self, src: SocketAddr, dst: SocketAddr, policy: LinkPolicy) {
        self.inner.policies.borrow_mut().insert((src, dst), policy);
        self.inner.drop_counters.borrow_mut().remove(&(src, dst));
    }

    /// Look up the policy for `(src, dst)`. Mutates the counter for
    /// `DropFirst`; returns the effective decision: `true` ⇒ drop.
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
        }
    }

    fn sender_for(&self, addr: SocketAddr) -> Option<mpsc::Sender<SimPacket>> {
        self.inner.senders.borrow().get(&addr).cloned()
    }
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
        // Sim has no kernel backpressure that we need to drain; capacity is
        // governed by `try_send` returning `WouldBlock`. Resolve immediately.
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
            Ok(()) => Ok(buf.len()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(buf.len()),
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

