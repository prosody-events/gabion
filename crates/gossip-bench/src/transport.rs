//! Wraps `gabion::gossip::sim::SimTransport` with byte / packet
//! counters. Required for any bandwidth / network-cost metric — the sim
//! transport itself is intentionally instrumentation-free.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

use gabion::gossip::GossipTransport;
use gabion::gossip::sim::SimTransport;

#[derive(Default)]
struct Counters {
    bytes_sent: Cell<u64>,
    packets_sent: Cell<u64>,
    packets_received: Cell<u64>,
}

/// Reference-counted, single-threaded counter pair. `Rc` because the
/// simulator runs on a `LocalSet` and we want both the transport and the
/// scenario runner to hold a handle.
#[derive(Clone, Default)]
pub struct CountingHandle {
    inner: Rc<Counters>,
}

impl CountingHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bytes_sent(&self) -> u64 {
        self.inner.bytes_sent.get()
    }

    pub fn packets_sent(&self) -> u64 {
        self.inner.packets_sent.get()
    }

    pub fn packets_received(&self) -> u64 {
        self.inner.packets_received.get()
    }

    fn record_send(&self, bytes: usize) {
        self.inner
            .bytes_sent
            .set(self.inner.bytes_sent.get().saturating_add(bytes as u64));
        self.inner
            .packets_sent
            .set(self.inner.packets_sent.get().saturating_add(1));
    }

    fn record_recv(&self) {
        self.inner
            .packets_received
            .set(self.inner.packets_received.get().saturating_add(1));
    }
}

/// Drop-in wrapper around `SimTransport` (or any `GossipTransport`).
/// Increments the per-node counters on every successful send/recv.
pub struct CountingTransport {
    inner: SimTransport,
    counters: CountingHandle,
}

impl CountingTransport {
    pub fn new(inner: SimTransport) -> (Self, CountingHandle) {
        let counters = CountingHandle::new();
        (
            Self {
                inner,
                counters: counters.clone(),
            },
            counters,
        )
    }
}

impl GossipTransport for CountingTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        self.inner.writable()
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let result = self.inner.try_send_to(buf, dst);
        if let Ok(n) = result {
            self.counters.record_send(n);
        }
        result
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let result = self.inner.recv_from(buf).await;
        if result.is_ok() {
            self.counters.record_recv();
        }
        result
    }
}
