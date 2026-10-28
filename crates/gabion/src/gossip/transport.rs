//! Bidirectional datagram transport contract.
//!
//! Production uses [`UdpTransport`] over a `tokio::net::UdpSocket`; tests and
//! simulators use [`crate::gossip::sim::SimTransport`] over in-process mpsc
//! channels. The API mirrors `tokio::net::UdpSocket` so the production
//! wrapper is a thin newtype.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

#[cfg(feature = "transport-udp")]
use std::num::NonZeroU32;

#[cfg(feature = "transport-udp")]
use tokio::net::UdpSocket;

#[cfg(feature = "transport-udp")]
use super::ValidGossipConfig;
#[cfg(feature = "transport-udp")]
use crate::settings::Setting;

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

    /// Resolves when the transport has room to send. The runtime does not
    /// poll it (a refused packet waits for the next round); it stays part of
    /// the trait the study harness implements.
    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a;

    /// Non-blocking send. Returns `io::ErrorKind::WouldBlock` when the kernel
    /// refuses the datagram for lack of buffer space; [`UdpTransport`]
    /// reports `ENOBUFS` the same way. The runtime holds the refused packet
    /// and retries it first on its next round.
    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize>;

    /// The send buffer the kernel granted, in bytes, for the `send_blocked`
    /// warning; `None` for a transport without a kernel socket.
    fn send_buffer_bytes(&self) -> Option<usize> {
        None
    }

    /// Await one inbound packet. Writes into the caller-owned buffer.
    /// Production is zero-copy (the kernel writes directly into `buf`).
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + 'a;
}

/// Thin newtype over [`UdpSocket`]. Every method delegates to the inner
/// socket — zero overhead in production.
#[cfg(feature = "transport-udp")]
#[derive(Debug)]
pub struct UdpTransport(pub(crate) UdpSocket);

#[cfg(feature = "transport-udp")]
impl UdpTransport {
    /// Bind `addr` and ask the kernel for the configured socket buffers,
    /// warning once per buffer it grants less of.
    pub async fn bind(addr: SocketAddr, config: &ValidGossipConfig) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let buffers = socket2::SockRef::from(&socket);
        // A request above the host's cap is clamped or refused; either way
        // the size read back is what the kernel granted.
        let _ = buffers.set_recv_buffer_size(config.recv_buffer_bytes.get() as usize);
        let _ = buffers.set_send_buffer_size(config.send_buffer_bytes.get() as usize);
        warn_if_capped(
            "receive",
            "net.core.rmem_max",
            Setting::GossipRecvBufferBytes,
            config.recv_buffer_bytes,
            buffers.recv_buffer_size()?,
        );
        warn_if_capped(
            "send",
            "net.core.wmem_max",
            Setting::GossipSendBufferBytes,
            config.send_buffer_bytes,
            buffers.send_buffer_size()?,
        );
        Ok(Self(socket))
    }
}

/// Warn that the kernel granted a socket buffer smaller than `setting` asked
/// for.
#[cfg(feature = "transport-udp")]
pub(crate) fn warn_if_capped(
    buffer: &'static str,
    sysctl: &'static str,
    setting: Setting,
    requested: NonZeroU32,
    granted: usize,
) {
    if granted >= requested.get() as usize {
        return;
    }
    tracing::warn!(
        code = "socket_buffer_capped",
        buffer,
        requested_bytes = requested.get(),
        granted_bytes = granted,
        sysctl,
        config_key = setting.yaml_key(),
        "The kernel gave the gossip socket a {buffer} buffer of {granted} bytes, less than the \
         {requested} bytes gabion asked for, because the host setting `{sysctl}` caps it. If this \
         node pauses (for example under CPU throttling) or receives a burst, gossip packets that \
         do not fit are dropped, and counts arrive late until a later round sends them again. \
         gabion keeps running. Raise `{sysctl}` on the host to at least {requested} (`sysctl -w \
         {sysctl}={requested}`; in Kubernetes a node setting, not a pod sysctl). If the host \
         cannot change, set {setting} to {granted} to accept the smaller buffer and silence this \
         warning. See \"Socket buffers\" in the gabion README.",
    );
}

/// A full interface queue refuses a datagram just as a full socket buffer
/// does, so `ENOBUFS` becomes `WouldBlock`.
#[cfg(feature = "transport-udp")]
pub(crate) fn refusal(error: io::Error) -> io::Error {
    if error.raw_os_error() == Some(libc::ENOBUFS) {
        io::ErrorKind::WouldBlock.into()
    } else {
        error
    }
}

#[cfg(feature = "transport-udp")]
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
        self.0.try_send_to(buf, dst).map_err(refusal)
    }

    fn send_buffer_bytes(&self) -> Option<usize> {
        socket2::SockRef::from(&self.0).send_buffer_size().ok()
    }

    #[inline]
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }
}
