//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick. After each
//! iteration the runtime calls [`AggregateStore::apply`] once with whatever
//! rows the arm produced. The runtime owns the [`CellStore`], pre-allocated
//! scratch buffers, the outbound send pool, and the peer table; nothing in
//! the loop allocates after construction.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, NodeId, Observation,
    ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, PacketBuf, Packets, WireScratch};

use super::client::{GossipClient, LimitRequest, Request};
use super::clock::{Clock, TokioClock};
use super::store::AggregateStore;
use super::transport::{GossipTransport, UdpTransport};
use super::{GossipConfig, GossipError};

/// One peer the runtime gossips with. `node_id` is `None` until we receive
/// our first inbound packet from this peer; `peer_slot` caches the
/// `PeerFrontierTable` slot once interned.
struct Peer {
    addr: SocketAddr,
    node_id: Option<NodeId>,
    peer_slot: Option<u16>,
}

/// Single-threaded gossip event loop. `!Send + !Sync`.
pub struct GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    config: GossipConfig,
    store: CellStore<C>,
    transport: T,
    clock: K,
    requests_rx: mpsc::Receiver<Request>,

    // Downstream count store (write-only).
    aggregates: S,

    // Pre-allocated reusable buffers.
    recv_buf: Box<[u8]>,
    scratch: WireScratch,
    obs_buf: ObservationBatch<C>,
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,
    frame_handles: Vec<CellHandle>,

    // Outbound send pool.
    send_pool: Vec<PacketBuf>,
    send_pending: VecDeque<(SocketAddr, u8)>,
    send_free: Vec<u8>,

    peers: Vec<Peer>,
    rng: SplitMix64,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    _not_send: PhantomData<*const ()>,
}

impl<C, S> GossipRuntime<C, S, UdpTransport, TokioClock>
where
    C: Count,
    S: AggregateStore<C>,
{
    /// Bind a UDP socket on `bind_addr` and assemble the runtime with the
    /// production transport and a wall-clock-anchored `TokioClock`.
    pub async fn bind(
        bind_addr: SocketAddr,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
    ) -> Result<(Self, GossipClient<C>), GossipError> {
        let transport = UdpTransport::bind(bind_addr).await?;
        Ok(Self::from_parts(transport, TokioClock::new(), config, store, aggregates))
    }
}

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    /// Generic entry point used by tests, simulators, and any non-UDP setup.
    pub fn from_parts(
        transport: T,
        clock: K,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
    ) -> (Self, GossipClient<C>) {
        let (req_tx, req_rx) = mpsc::channel(config.limit_queue_capacity);

        let recv_buf = vec![0u8; config.wire_limits.max_payload_bytes].into_boxed_slice();
        let scratch = WireScratch::for_store(&store);
        let obs_buf = ObservationBatch::<C>::with_capacity(config.max_cells_per_tick);
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);

        let send_pool_size = config.send_queue_capacity.max(1);
        let send_pool: Vec<PacketBuf> = (0..send_pool_size)
            .map(|_| PacketBuf::for_limits(config.wire_limits))
            .collect();
        let send_free: Vec<u8> = (0..send_pool_size as u8).rev().collect();
        let send_pending = VecDeque::with_capacity(send_pool_size);

        let peers: Vec<Peer> = config
            .bootstrap_peers
            .iter()
            .copied()
            .map(|addr| Peer {
                addr,
                node_id: None,
                peer_slot: None,
            })
            .collect();
        let rng = SplitMix64::new(config.rng_seed);

        let runtime = Self {
            config,
            store,
            transport,
            clock,
            requests_rx: req_rx,
            aggregates,
            recv_buf,
            scratch,
            obs_buf,
            sink_buf,
            expiration_buf,
            frame_handles,
            send_pool,
            send_pending,
            send_free,
            peers,
            rng,
            pending_reply: None,
            _not_send: PhantomData,
        };
        let client = GossipClient::new(req_tx);
        (runtime, client)
    }

    /// Local transport address (after binding). Useful for tests that need
    /// the port the kernel chose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    /// Run the event loop until shutdown is requested. `peer_events` is the
    /// fourth select arm; pass `futures::stream::empty()` if discovery is
    /// not yet wired up.
    pub async fn run<St>(mut self, peer_events: St) -> Result<(), GossipError>
    where
        St: Stream<Item = PeerEvent>,
    {
        let mut peer_events = std::pin::pin!(peer_events);
        let mut tick = make_tick(&self.config);
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            // Decide outside the `select!` whether the send arm has work to do.
            // Reading `self.send_pending.is_empty()` inside the `if` guard would
            // re-borrow `self` after we've already split-borrowed for the I/O
            // futures below.
            let have_send = !self.send_pending.is_empty();
            let watch_peers = !peer_events_done;

            let outcome = {
                // Split-borrow scope: we hand the I/O futures non-overlapping
                // pieces of `self` so they compose inside `select!`.
                let Self {
                    transport,
                    recv_buf,
                    requests_rx,
                    ..
                } = &mut self;

                tokio::select! {
                    biased;

                    req = requests_rx.recv() => ArmOutcome::Request(req),
                    recv = transport.recv_from(recv_buf) => ArmOutcome::Inbound(recv),
                    _ = transport.writable(), if have_send => ArmOutcome::Writable,
                    evt = peer_events.next(), if watch_peers => ArmOutcome::PeerEvent(evt),
                    _ = tick.tick() => ArmOutcome::Tick,
                }
            };

            match outcome {
                ArmOutcome::Request(None) | ArmOutcome::Request(Some(Request::Shutdown)) => {
                    return Ok(());
                }
                ArmOutcome::Request(Some(Request::Limit(r))) => self.handle_limit_request(r),
                ArmOutcome::Inbound(Ok((n, src))) => self.handle_inbound(n, src),
                ArmOutcome::Inbound(Err(e)) => return Err(e.into()),
                ArmOutcome::Writable => self.drain_one_send()?,
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Tick => self.handle_gossip_tick(),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, drain, idle ticks) leave both empty; the
            // branch short-circuits then.
            if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
                self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
                self.sink_buf.clear();
                self.expiration_buf.clear();
            }

            // Ack any pending limit-request AFTER the apply ran — callers
            // observe "ack ⇒ AggregateStore reflects the increment".
            // Receiver may have dropped; that is not an error.
            if let Some(reply) = self.pending_reply.take() {
                let _ = reply.send(());
            }
        }
    }

    // -- per-arm handlers ---------------------------------------------------

    fn handle_limit_request(&mut self, req: LimitRequest) {
        self.obs_buf.clear();
        self.sink_buf.clear();
        self.obs_buf.push(Observation {
            rule_fingerprint: req.rule_fingerprint,
            key_hash: req.key_hash,
            bucket: req.bucket,
            // ingest_local ignores origin/incarnation and forces local identity.
            origin: NodeId(0),
            incarnation: 0,
            count: C::saturating_from_u64(req.hits),
            last_update_millis: req.now_millis,
        });
        self.store.ingest_local(&self.obs_buf, &mut self.sink_buf);
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = Some(req.reply);
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        self.obs_buf.clear();
        self.sink_buf.clear();
        let bytes = &self.recv_buf[..n];
        let decoded = match self.config.auth_key.as_ref() {
            Some(key) => {
                wire::decode_auth::<C>(bytes, key, self.config.wire_limits, &mut self.obs_buf)
            }
            None => wire::decode_unauth::<C>(bytes, self.config.wire_limits, &mut self.obs_buf),
        };
        let summary = match decoded {
            Ok(s) => s,
            Err(_) => return,
        };

        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);

        // Frontier update — latency optimization. Best-effort: missing slot
        // or missing node entry just means we don't get the skip.
        let sender_id = NodeId(summary.header.sender_node_id);
        let sender_inc = summary.header.sender_incarnation;
        let max_origin_sequence = summary.header.max_origin_sequence;
        if let Some(peer_slot) = self.store.peer_frontiers_mut().intern_peer(sender_id) {
            if let Some(node_slot) = self.store.node_dictionary().find(sender_id, sender_inc) {
                self.store.peer_frontiers_mut().record_acked(
                    peer_slot,
                    node_slot,
                    max_origin_sequence,
                );
            }
            // Cache the peer_slot on the matching peer entry so future ticks
            // can prune without re-interning. `node_id` and `peer_slot` are
            // always set together — see `handle_gossip_tick`, which relies
            // on this invariant.
            if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == src) {
                peer.node_id = Some(sender_id);
                peer.peer_slot = Some(peer_slot);
            }
        }
    }

    fn drain_one_send(&mut self) -> Result<(), GossipError> {
        let Some((dst, slot)) = self.send_pending.pop_front() else {
            return Ok(());
        };
        let buf = &self.send_pool[slot as usize];
        match self.transport.try_send_to(buf.as_bytes(), dst) {
            Ok(_) => self.send_free.push(slot),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.send_pending.push_front((dst, slot));
            }
            Err(e) => {
                self.send_free.push(slot);
                return Err(e.into());
            }
        }
        Ok(())
    }

    fn handle_peer_event(&mut self, evt: PeerEvent) {
        match evt {
            PeerEvent::Added(p) => {
                if !self.peers.iter().any(|x| x.addr == p.addr) {
                    self.peers.push(Peer {
                        addr: p.addr,
                        node_id: None,
                        peer_slot: None,
                    });
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|x| x.addr == p.addr) {
                    let removed = self.peers.remove(pos);
                    if let Some(node_id) = removed.node_id {
                        self.store.peer_frontiers_mut().remove_peer(node_id);
                    }
                }
            }
        }
    }

    fn handle_gossip_tick(&mut self) {
        // Step 1: expire aged-out cells so we don't gossip them.
        let now_millis = self.clock.now_millis();
        self.expiration_buf.clear();
        self.store.expire_at(now_millis, &mut self.expiration_buf);

        if self.store.is_empty() || self.peers.is_empty() || self.send_free.is_empty() {
            return;
        }

        // Step 2: pick `fanout` distinct peers via a partial Fisher-Yates
        // shuffle of `self.peers` in place. With-replacement sampling would
        // silently break Demers' O(log N) convergence bound — selecting the
        // same peer twice in one tick burns a send-pool slot encoding the
        // same frame for the same peer. `self.peers` has no ordering
        // contract elsewhere (lookups are by `addr`), so shuffling is free.
        let n = self.peers.len();
        let pick_count = self.config.fanout.min(n);
        for i in 0..pick_count {
            let j = i + (self.rng.next_u64() as usize) % (n - i);
            self.peers.swap(i, j);
        }

        for peer_idx in 0..pick_count {
            if self.send_free.is_empty() {
                break;
            }
            let peer_addr = self.peers[peer_idx].addr;
            // `handle_inbound` writes `node_id` and `peer_slot` together —
            // when one is `Some`, the other is too. So a single read of
            // `peer_slot` is the full pruning decision.
            let peer_slot = self.peers[peer_idx].peer_slot;

            self.frame_handles.clear();
            match peer_slot {
                Some(slot) => {
                    self.store.fill_gossip_frame_for_peer(
                        self.config.max_cells_per_tick,
                        slot,
                        &mut self.frame_handles,
                    );
                }
                None => {
                    // Bootstrap fallback: we haven't heard from this peer
                    // yet, so we don't know what to prune. Send an unpruned
                    // frame; the next inbound from them caches the slot.
                    self.store.fill_gossip_frame(
                        self.config.max_cells_per_tick,
                        &mut self.frame_handles,
                    );
                }
            }
            if self.frame_handles.is_empty() {
                continue;
            }
            self.encode_packets_for(peer_addr);
        }
    }

    fn encode_packets_for(&mut self, dst: SocketAddr) {
        let header = wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            count_width: 0,
            cell_count: 0,
            body_len: 0,
            min_origin_sequence: 0,
            max_origin_sequence: 0,
            flags: 0,
        };

        let packets_result = match self.config.auth_key.as_ref() {
            Some(key) => Packets::<C>::auth(
                header,
                &self.store,
                &self.frame_handles,
                key,
                &mut self.scratch,
                self.config.wire_limits,
            ),
            None => Packets::<C>::unauth(
                header,
                &self.store,
                &self.frame_handles,
                &mut self.scratch,
                self.config.wire_limits,
            ),
        };
        let mut packets = match packets_result {
            Ok(p) => p,
            Err(_) => return,
        };

        while let Some(slot) = self.send_free.pop() {
            let buf = &mut self.send_pool[slot as usize];
            match packets.next_into(buf) {
                Ok(Some(_)) => self.send_pending.push_back((dst, slot)),
                Ok(None) => {
                    self.send_free.push(slot);
                    break;
                }
                Err(_) => {
                    self.send_free.push(slot);
                    break;
                }
            }
        }
        drop(packets);
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Tick,
}

fn make_tick(config: &GossipConfig) -> Interval {
    let mut tick = tokio::time::interval(config.tick_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick
}

/// SplitMix64 — small, fast, deterministic. Used only for peer sampling so
/// no security properties are required.
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
