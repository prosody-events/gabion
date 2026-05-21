//! Bidirectional datagram transport contract.
//!
//! Production uses [`UdpTransport`] over a `tokio::net::UdpSocket`; tests and
//! simulators use [`crate::gossip::sim::SimTransport`] over in-process mpsc
//! channels. The API mirrors `tokio::net::UdpSocket` exactly so the
//! production wrapper is a thin newtype.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;

/// Bidirectional datagram transport. The runtime borrows `&self.transport`
/// (not `&mut`) on every arm so disjoint-field borrows compose cleanly inside
/// `tokio::select!` with `&mut self.recv_buf`.
///
/// The async methods desugar to `impl Future + 'a` (rather than `async fn`)
/// so callers don't pay the `Send` auto-trait constraint dance — the runtime
/// is single-threaded and never crosses a thread boundary. Impls are free
/// to write `async fn`; the compiler matches the trait signature.
pub trait GossipTransport {
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Resolves when the transport has TX capacity (kernel buffer room for
    /// UDP; channel slot for sim). Used as a `select!` arm guarded by
    /// `if !self.send_pending.is_empty()` so it is polled only when we have
    /// something pending.
    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a;

    /// Non-blocking send. Returns `io::ErrorKind::WouldBlock` when capacity
    /// is exhausted; the runtime re-queues and waits on [`Self::writable`].
    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize>;

    /// Await one inbound packet. Writes into the caller-owned buffer.
    /// Production is zero-copy (the kernel writes directly into `buf`); the
    /// sim impl performs exactly one byte-copy per packet.
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + 'a;
}

/// Thin newtype over [`UdpSocket`]. Every method delegates to the inner
/// socket — zero overhead in production.
#[derive(Debug)]
pub struct UdpTransport(pub UdpSocket);

impl UdpTransport {
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self(UdpSocket::bind(addr).await?))
    }

    pub fn from_socket(socket: UdpSocket) -> Self {
        Self(socket)
    }
}

impl GossipTransport for UdpTransport {
    #[inline]
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    #[inline]
    async fn writable(&self) -> io::Result<()> {
        self.0.writable().await
    }

    #[inline]
    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        self.0.try_send_to(buf, dst)
    }

    #[inline]
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }
}
