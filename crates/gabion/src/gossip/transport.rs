//! Bidirectional datagram transport contract, and the `Outbox` the runtime
//! sends through.
//!
//! Production uses [`UdpTransport`] over a `tokio::net::UdpSocket`; tests and
//! simulators use [`crate::gossip::sim::SimTransport`] over in-process mpsc
//! channels.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::pin::pin;
use std::task::{Context, Poll};

#[cfg(feature = "transport-udp")]
use tokio::net::UdpSocket;

use super::ValidGossipConfig;
use super::admin::GossipCounters;
use crate::settings::Setting;
use crate::throttle;
use crate::wire::{CellPayload, Header, PacketBuf, Packets, Payload};

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

    /// Resolves at once unless overridden. The runtime never polls it; it
    /// stays so the study harness's transports build against every arm.
    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        async { Ok(()) }
    }

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

    /// Take one datagram already waiting, or fail with `WouldBlock`. The
    /// default polls [`Self::recv_from`] once with the runtime task's
    /// context, so a transport that arms a wake-up there wakes the runtime; a
    /// round reads the waiting datagrams this way before it services its
    /// timers.
    fn try_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> io::Result<(usize, SocketAddr)> {
        match pin!(self.recv_from(buf)).poll(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }

    /// The receive buffer the kernel granted, in bytes, which bounds the
    /// datagrams one round reads; `None` for a transport whose finite queue a
    /// round may read to empty.
    fn recv_buffer_bytes(&self) -> Option<usize> {
        None
    }
}

/// A payload this node sends, and whether its packet starts a wait for an
/// answer once it leaves: by default exactly when it asks for one.
pub(super) trait Outgoing: Copy + Into<Payload> {
    fn starts_wait(self) -> bool {
        self.into().asks_answer()
    }
}

impl Outgoing for Payload {}

impl Outgoing for CellPayload {}

/// The `PROBE` a peer not yet heard from is greeted with: its answer names
/// the peer's zone at once, but nothing waits on it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Greeting;

impl From<Greeting> for Payload {
    fn from(_: Greeting) -> Self {
        Self::Ack { probe: true }
    }
}

impl Outgoing for Greeting {
    fn starts_wait(self) -> bool {
        false
    }
}

/// A packet the kernel refused: where it goes, when it was refused, and
/// whether it starts a wait for an answer once it leaves.
#[derive(Clone, Copy, Debug)]
pub(super) struct Held {
    pub(super) dst: SocketAddr,
    pub(super) since_ms: u64,
    asks: bool,
}

/// What a send did: the bytes the socket accepted, and whether a packet that
/// starts a wait left this node, accepted or failed with an error other than
/// back-pressure; the addressee can answer neither a refused packet nor,
/// until it is retried, a held one.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Sent {
    pub(super) bytes: u64,
    pub(super) waits: bool,
}

/// The one send buffer. A packet the kernel refuses stays in it, with its
/// destination, until a retry sends it; nothing encodes meanwhile, so the
/// held packet is never overwritten.
pub(super) struct Outbox {
    packet: PacketBuf,
    held: Option<Held>,
    /// The send buffer gabion asked for, named by `send_blocked`.
    send_buffer: NonZeroU32,
}

impl Outbox {
    pub(super) fn new(config: &ValidGossipConfig) -> Self {
        Self {
            packet: PacketBuf::for_limits(config.wire_limits),
            held: None,
            send_buffer: config.send_buffer_bytes,
        }
    }

    pub(super) fn is_held(&self) -> bool {
        self.held.is_some()
    }

    /// Encode `packets` for `dst`, handing each packet to the socket as it
    /// is encoded and stopping at the first the kernel refuses. Returns the
    /// stream position the packets that left (or are held) close through;
    /// the cells above it stay above the peer's `sent` and go next round.
    pub(super) fn send_frame<T: GossipTransport>(
        &mut self,
        mut packets: Packets<'_>,
        dst: SocketAddr,
        now: u64,
        transport: &T,
        counters: &mut GossipCounters,
    ) -> (u64, Sent) {
        let mut total = Sent::default();
        let asks = packets.payload().starts_wait();
        while self.held.is_none() && packets.next_into(&mut self.packet).is_some() {
            let sent = self.send(dst, now, asks, transport, counters);
            total = Sent {
                bytes: total.bytes + sent.bytes,
                waits: total.waits || sent.waits,
            };
        }
        (packets.closed_through(), total)
    }

    /// Send a packet of `header` alone. Call it only while nothing is held.
    pub(super) fn send_header<T: GossipTransport, P: Outgoing>(
        &mut self,
        header: &Header<P>,
        dst: SocketAddr,
        now: u64,
        transport: &T,
        counters: &mut GossipCounters,
    ) -> Sent {
        debug_assert!(self.held.is_none(), "a held packet is never overwritten");
        self.packet.header_only(header);
        self.send(dst, now, header.payload.starts_wait(), transport, counters)
    }

    /// Send the held packet, if any; a refusal holds it again, still dated
    /// from the first refusal. Returns the packet once it is no longer held.
    pub(super) fn retry<T: GossipTransport>(
        &mut self,
        transport: &T,
        counters: &mut GossipCounters,
    ) -> Option<(Held, Sent)> {
        let held = self.held.take()?;
        let sent = self.send(held.dst, held.since_ms, held.asks, transport, counters);
        self.held.is_none().then_some((held, sent))
    }

    /// Hand the packet to the socket. A refusal for lack of buffer space
    /// holds it; any other error drops it, and gossip goes on.
    fn send<T: GossipTransport>(
        &mut self,
        dst: SocketAddr,
        now: u64,
        asks: bool,
        transport: &T,
        counters: &mut GossipCounters,
    ) -> Sent {
        match transport.try_send_to(self.packet.as_bytes(), dst) {
            Ok(bytes) => {
                counters.packets_sent += 1;
                counters.bytes_sent += bytes as u64;
                Sent {
                    bytes: bytes as u64,
                    waits: asks,
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Some(send_blocked_total) = throttle::bump(&mut counters.send_blocked) {
                    tracing::warn!(
                        code = "send_blocked",
                        send_blocked_total,
                        requested_send_buffer_bytes = self.send_buffer.get(),
                        granted_send_buffer_bytes = transport.send_buffer_bytes(),
                        sysctl = "net.core.wmem_max",
                        config_key = Setting::GossipSendBufferBytes.yaml_key(),
                        "The kernel's send buffer for the gossip socket was full, so this node \
                         stopped its gossip round early; it retries the refused packet first on \
                         each later round and sends the rest once the kernel accepts sends again. \
                         Counts reach some peers late, so limits there briefly under-count and \
                         allow requests over the limit. Usually the send buffer is smaller than \
                         gabion asked for (look for an earlier warning naming \
                         `net.core.wmem_max`), or the host's network is saturated. Raise \
                         `net.core.wmem_max` on the host to at least {0} bytes, the size {1} asks \
                         for. If the host already grants that much, raise {1} and \
                         `net.core.wmem_max` together.",
                        self.send_buffer,
                        Setting::GossipSendBufferBytes,
                    );
                }
                self.held = Some(Held {
                    dst,
                    since_ms: now,
                    asks,
                });
                Sent::default()
            }
            Err(error) => {
                if let Some(send_errors_total) = throttle::bump(&mut counters.send_errors) {
                    tracing::warn!(
                        code = "send_error",
                        peer = %dst,
                        error = %error,
                        send_errors_total,
                        "Could not send a gossip packet to this peer, so gabion dropped that packet \
                         and kept gossiping with every other peer. The peer gets the same counts on a \
                         later round once sends succeed; until then, limits enforced there \
                         under-count. If the peer is this node's gateway into its zone and sends \
                         to it keep failing, another member of that zone takes over after \
                         `gossip.suspect_after`. Usually the peer has just left the cluster (a \
                         stale address), or a firewall or NetworkPolicy blocks UDP from this node \
                         to it. If it repeats for a peer that is running, check the policy and the \
                         route between the two.",
                    );
                }
                Sent {
                    bytes: 0,
                    waits: asks,
                }
            }
        }
    }
}

/// Newtype over [`UdpSocket`]: `bind` sizes its kernel buffers, and
/// `try_send_to` reports `ENOBUFS` as `WouldBlock`.
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
        // the size read back shows what the kernel granted.
        let _ = buffers.set_recv_buffer_size(config.recv_buffer_bytes.get() as usize);
        let _ = buffers.set_send_buffer_size(config.send_buffer_bytes.get() as usize);
        warn_if_capped(
            "receive",
            "net.core.rmem_max",
            Setting::GossipRecvBufferBytes,
            config.recv_buffer_bytes,
            grant(buffers.recv_buffer_size()?),
        );
        warn_if_capped(
            "send",
            "net.core.wmem_max",
            Setting::GossipSendBufferBytes,
            config.send_buffer_bytes,
            grant(buffers.send_buffer_size()?),
        );
        Ok(Self(socket))
    }
}

/// The buffer the kernel granted, from the size it reports: Linux reports
/// double the grant, the extra covering its own bookkeeping.
#[cfg(feature = "transport-udp")]
pub(super) fn grant(reported: usize) -> usize {
    if cfg!(target_os = "linux") {
        reported / 2
    } else {
        reported
    }
}

/// Warn that the kernel granted a socket buffer smaller than `setting` asked
/// for.
#[cfg(feature = "transport-udp")]
pub(super) fn warn_if_capped(
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
pub(super) fn refusal(error: io::Error) -> io::Error {
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
    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        self.0.try_send_to(buf, dst).map_err(refusal)
    }

    fn send_buffer_bytes(&self) -> Option<usize> {
        socket2::SockRef::from(&self.0)
            .send_buffer_size()
            .ok()
            .map(grant)
    }

    #[inline]
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buf).await
    }

    #[inline]
    fn try_recv_from(
        &self,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> io::Result<(usize, SocketAddr)> {
        self.0.try_recv_from(buf)
    }

    fn recv_buffer_bytes(&self) -> Option<usize> {
        socket2::SockRef::from(&self.0)
            .recv_buffer_size()
            .ok()
            .map(grant)
    }
}
