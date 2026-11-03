//! Benchmarks of the gossip runtime in the production states it sits in,
//! driven through the public `GossipRuntime` as the adapters run it.
//!
//! One node runs on a paused current-thread tokio runtime with a
//! `TokioClock`, sized as `prod-peak` (182 peers, 3 zones of 61 with the
//! node, 20,070 live cells) and as `prod-4x` (299 peers, zones of 100, 79,440
//! live cells). The bench plays every peer: outside the timed section each
//! one answers what the node sent it, as a peer that receives it all would,
//! so streams, answers and roles stay in steady state between iterations.
//!
//! * `round/steady`           — one heartbeat interval: 2 raised own cells, and
//!   2 flagged 2-cell packets from remote origins, relayed into the zone by a
//!   prompt round or with the heartbeat, as the timers fall.
//! * `round/gateway_failover` — a gateway says it cannot relay; the prompt
//!   round picks the next one and sends it every own cell from 0. The next
//!   iteration's recovery fails back the same way.
//! * `peers/refresh`          — a remote row that is not a gateway says it
//!   cannot relay, then recovers: each prompt round re-ranks every row and
//!   changes no role.
//! * `sweep/page`             — a prompt round sending a running join sweep its
//!   next page of `q` cells (1,024 at the default store); the requester asks
//!   again once it ends.
//! * `inbound/zone_peer`      — a fresh 2-cell own-stream packet from each zone
//!   peer in turn, from bytes to delta rows.
//! * `inbound/relay_fresh`    — the same from each cross-zone peer, flagged for
//!   relay, so the merge also marks the relay list.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::poll_fn;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU128;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};

use gabion::aggregate::Aggregate;
use gabion::crdt::{
    CellHandle, CellStore, DeltaSink, KeyHash, NodeId, NodeIdentity, Observation, ObservationBatch,
    RelayMark, Stream,
};
use gabion::defaults::{GOSSIP_MIN_EMIT_INTERVAL_MS, GOSSIP_TICK_INTERVAL_MILLIS};
use gabion::gossip::sim::sim_advance;
use gabion::gossip::{GossipClient, GossipRuntime, GossipTransport, TokioClock};
use gabion::rules::{EnforcementMode, RuleDef, RuleTable};
use gabion::settings::Settings;
use gabion::wire::{
    AckInfo, CellPayload, FrameLimits, Header, OwnAck, PacketBuf, Packets, Payload, StreamRange,
    WireScratch, decode,
};
use gabion::zone::{ZoneHash, ZoneName};
use tokio::task::LocalSet;

/// `(name, peers, live cells)`; the node and its peers fill 3 equal zones.
type Scenario = (&'static str, u16, u32);
const SCENARIOS: [Scenario; 2] = [("prod-peak", 182, 20_070), ("prod-4x", 299, 79_440)];
const ZONES: usize = 3;
const INCARNATION: u32 = 1;
const HEARTBEAT: Duration = Duration::from_millis(GOSSIP_TICK_INTERVAL_MILLIS);
const PROMPT: Duration = Duration::from_millis(GOSSIP_MIN_EMIT_INTERVAL_MS);
/// Peers 0 and 1, of zones 1 and 2: the remote origins `round/steady` relays.
const ORIGINS: [usize; 2] = [0, 1];
/// The node's own keys and the other origins' live cells; the keys the peers
/// raise sit below both.
const OWN_KEYS: u128 = 1 << 64;
const LIVE_KEYS: u128 = 1 << 32;

fn addr(peer: usize) -> SocketAddr {
    SocketAddr::from(([10, 0, (peer >> 8) as u8, peer as u8], 9000))
}

fn peer(addr: SocketAddr) -> usize {
    let IpAddr::V4(ip) = addr.ip() else {
        unreachable!("the node sends only to its peers")
    };
    let [_, _, hi, lo] = ip.octets();
    usize::from(u16::from_be_bytes([hi, lo]))
}

/// Zone 0 is the node's.
fn zone(peer: usize) -> usize {
    (peer + 1) % ZONES
}

/// The datagrams the node sent since the peers last answered, back to back,
/// and those waiting for it.
#[derive(Default)]
struct Net {
    sent: RefCell<Vec<u8>>,
    /// Each sent datagram's addressee and end in `sent`.
    sends: RefCell<Vec<(SocketAddr, usize)>>,
    inbox: RefCell<VecDeque<(SocketAddr, Box<[u8]>)>>,
    waker: Cell<Option<Waker>>,
}

struct Wire(Rc<Net>);

impl GossipTransport for Wire {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::from(([10, 1, 0, 0], 9000)))
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let mut sent = self.0.sent.borrow_mut();
        sent.extend_from_slice(buf);
        self.0.sends.borrow_mut().push((dst, sent.len()));
        Ok(buf.len())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| {
            let Some((src, packet)) = self.0.inbox.borrow_mut().pop_front() else {
                self.0.waker.set(Some(cx.waker().clone()));
                return Poll::Pending;
            };
            buf[..packet.len()].copy_from_slice(&packet);
            Poll::Ready(Ok((packet.len(), src)))
        })
        .await
    }
}

/// A peer as it answers the node.
struct Peer {
    identity: NodeIdentity,
    zone: ZoneHash,
    /// The node's streams as this peer holds them.
    own: OwnAck,
    relay: u64,
    /// The head of this peer's own stream.
    next: u64,
    cannot_relay: bool,
}

/// The node's peers, and the store their cells come from.
struct World {
    net: Rc<Net>,
    client: GossipClient,
    cluster_id_hash: NonZeroU128,
    limits: FrameLimits,
    fingerprint: u128,
    peers: Box<[Peer]>,
    /// Each zone's gateway, from the node's flagged sends.
    gateways: [Option<usize>; ZONES],
    /// The packets and own-cell raises the next iteration delivers.
    staged: Vec<(SocketAddr, Box<[u8]>)>,
    raises: Vec<u128>,
    store: CellStore,
    scratch: WireScratch,
    buf: PacketBuf,
    handles: Vec<CellHandle>,
    obs: ObservationBatch,
    sink: DeltaSink,
}

impl World {
    fn header<P>(&self, peer: usize, payload: P, probe_reply: bool) -> Header<P> {
        let p = &self.peers[peer];
        Header {
            cluster_id_hash: self.cluster_id_hash,
            sender: p.identity,
            sender_zone: Some(p.zone),
            acks: AckInfo {
                incarnation: INCARNATION,
                own: p.own,
                relay: p.relay,
                cannot_relay: p.cannot_relay,
                probe_reply,
                ..AckInfo::default()
            },
            payload,
        }
    }

    /// Stage the packet in `buf` from `peer`. One the node rejected would
    /// time the reject path.
    fn stage(&mut self, peer: usize) {
        let packet = self.buf.as_bytes();
        decode(packet, self.limits, self.cluster_id_hash, |_| {})
            .expect("the node accepts the packet");
        self.staged.push((addr(peer), packet.into()));
    }

    fn stage_header(&mut self, peer: usize, payload: Payload, probe_reply: bool) {
        self.buf
            .header_only(&self.header(peer, payload, probe_reply));
        self.stage(peer);
    }

    /// Stage a fresh 2-cell packet of `peer`'s own stream, raising two of its
    /// cells.
    fn stage_own(&mut self, peer: usize, relay_into_zone: bool) {
        let head = self.store.head(Stream::Own);
        self.obs.clear();
        self.sink.clear();
        for key in [2 * peer as u128, 2 * peer as u128 + 1] {
            self.obs.push(cell(self.fingerprint, key, NodeId(0)));
        }
        self.store.ingest_local(&self.obs, &mut self.sink);
        self.store
            .stream_after(Stream::Own, head, 2, &mut self.handles);
        let next = self.peers[peer].next;
        self.peers[peer].next += 2;
        let payload = CellPayload::Own {
            range: StreamRange::new(next, next + 2).expect("a non-empty range"),
            relay_into_zone,
            probe: false,
        };
        let header = self.header(peer, payload, false);
        let mut packets = Packets::new(
            header,
            &self.store,
            &self.handles,
            &mut self.scratch,
            self.limits,
        );
        assert_eq!(packets.next_into(&mut self.buf), Some(2));
        self.stage(peer);
    }

    /// Flip whether `peer` says it cannot relay, and tell the node; the
    /// recovery carries a probe answer, which clears the node's suspicion.
    fn toggle_cannot_relay(&mut self, peer: usize) {
        let cannot = !self.peers[peer].cannot_relay;
        self.peers[peer].cannot_relay = cannot;
        self.stage_header(peer, Payload::Ack { probe: false }, !cannot);
    }

    /// Hand the node the staged packets and raises.
    fn deliver(&mut self) {
        self.net.inbox.borrow_mut().extend(self.staged.drain(..));
        for key in self.raises.drain(..) {
            self.client
                .try_record(self.fingerprint, KeyHash(key), 0, 1, u64::MAX, 0)
                .expect("the request queue has room");
        }
        if let Some(waker) = self.net.waker.take() {
            waker.wake();
        }
    }

    /// Answer every packet that asks for an answer, as its addressee would,
    /// and wait until the node has read the answers.
    async fn answer(&mut self) {
        self.stage_answers();
        self.deliver();
        while !self.net.inbox.borrow().is_empty() {
            tokio::task::yield_now().await;
        }
    }

    fn stage_answers(&mut self) {
        let net = Rc::clone(&self.net);
        let sent = net.sent.borrow();
        let mut start = 0;
        for &(dst, end) in net.sends.borrow().iter() {
            let header = decode(&sent[start..end], self.limits, self.cluster_id_hash, |_| {})
                .expect("the node sends valid packets");
            start = end;
            let p = peer(dst);
            let probe = match header.payload {
                Payload::Cells(CellPayload::Own {
                    range,
                    relay_into_zone,
                    probe,
                }) => {
                    self.peers[p].own = if relay_into_zone {
                        self.gateways[zone(p)] = Some(p);
                        OwnAck::AsGateway(range.upper())
                    } else {
                        OwnAck::Direct(range.upper())
                    };
                    probe
                }
                Payload::Cells(CellPayload::Relay { range, probe }) => {
                    self.peers[p].relay = range.upper();
                    probe
                }
                Payload::Ack { probe } => probe,
                Payload::SweepRequest => {
                    self.stage_header(p, CellPayload::Sweep.into(), false);
                    continue;
                }
                Payload::Cells(CellPayload::Sweep) => false,
            };
            if header.payload.asks_answer() {
                self.stage_header(p, Payload::Ack { probe: false }, probe);
            }
        }
        drop(sent);
        net.sent.borrow_mut().clear();
        net.sends.borrow_mut().clear();
    }
}

fn cell(rule_fingerprint: u128, key: u128, origin: NodeId) -> Observation {
    Observation {
        rule_fingerprint,
        key_hash: KeyHash(key),
        bucket: 0,
        origin,
        incarnation: INCARNATION,
        count: 1,
        last_update_millis: 0,
    }
}

struct Bench {
    tokio: tokio::runtime::Runtime,
    local: LocalSet,
    world: World,
}

impl Bench {
    /// The node with `peers` peers that have each been heard, `live` live
    /// cells of which its share is its own, and every stream answered.
    fn new(peers: u16, live: u32) -> Self {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("tokio runtime");
        let context = tokio.enter();
        let peers = usize::from(peers);
        let zone_name = |zone: usize| ZoneName::parse(&format!("az-{zone}")).expect("a zone name");
        let settings = Settings {
            // This node's own address, outside the peers' range.
            gossip_bind: Some(SocketAddr::from(([10, 255, 0, 0], 9000))),
            bootstrap_peers: (0..peers).map(addr).collect(),
            zone: Some(zone_name(0)),
            ..Settings::default()
        };
        let (gossip, storage) = settings.validate().expect("the bench settings validate");
        // A month-long window: no cell expires while the bench runs.
        let rules = RuleTable::new(
            Box::new([RuleDef {
                name: "site",
                domain: "bench",
                patterns: &[("site", "*")],
                rate: "1000000r/30d",
                window: None,
                bucket: Some(Duration::from_secs(86_400)),
                mode: EnforcementMode::Enforce,
            }
            .build(&storage.cardinality)
            .expect("the rule builds")]),
            storage.rule_dictionary_capacity(),
        )
        .expect("the rule fits");
        let fingerprint = rules.rules()[0].fingerprint();
        let net = Rc::new(Net::default());
        let (runtime, client) = GossipRuntime::from_parts(
            Wire(Rc::clone(&net)),
            TokioClock::from_millis(0),
            gossip,
            CellStore::new(storage, NodeIdentity::new(NodeId(1), INCARNATION), &rules),
            Aggregate::new(&storage),
        );
        let local = LocalSet::new();
        local.spawn_local(runtime.run(futures::stream::empty()));

        let limits = FrameLimits::new(settings.max_payload_bytes).expect("default payload");
        let store = CellStore::new(storage, NodeIdentity::new(NodeId(2), INCARNATION), &rules);
        let mut world = World {
            net,
            client,
            cluster_id_hash: settings.cluster_id_hash,
            limits,
            fingerprint,
            peers: (0..peers)
                .map(|p| Peer {
                    identity: NodeIdentity::new(NodeId(0xBEEF << 64 | p as u128), INCARNATION),
                    zone: zone_name(zone(p)).hash(),
                    own: OwnAck::Direct(0),
                    relay: 0,
                    next: 0,
                    cannot_relay: false,
                })
                .collect(),
            gateways: [None; ZONES],
            staged: Vec::with_capacity(4 * peers),
            raises: Vec::with_capacity(peers),
            scratch: WireScratch::for_store(&store),
            store,
            buf: PacketBuf::for_limits(limits),
            handles: Vec::with_capacity(2),
            obs: ObservationBatch::with_capacity(live as usize),
            sink: DeltaSink::with_capacity(live as usize),
        };

        // Every peer is heard; the cells of every origin but the node arrive
        // as sweep pages; the node raises its own share.
        for p in 0..peers {
            world.stage_header(p, Payload::Ack { probe: false }, false);
        }
        let own = live / (peers as u32 + 1);
        for key in 0..live - own {
            let origin = world.peers[key as usize % peers].identity.node_id;
            world
                .obs
                .push(cell(fingerprint, LIVE_KEYS + u128::from(key), origin));
        }
        world
            .store
            .merge_remote(&world.obs, &mut world.sink, RelayMark::Clear);
        let pages: Vec<CellHandle> = world.store.active_handles(0).collect();
        let mut packets = Packets::new(
            world.header(0, CellPayload::Sweep, false),
            &world.store,
            &pages,
            &mut world.scratch,
            limits,
        );
        while packets.next_into(&mut world.buf).is_some() {
            world.staged.push((addr(0), world.buf.as_bytes().into()));
        }
        world
            .raises
            .extend((0..own).map(|key| OWN_KEYS + u128::from(key)));
        tokio.block_on(local.run_until(async {
            world.deliver();
            for _ in 0..8 {
                sim_advance(HEARTBEAT).await;
                world.answer().await;
            }
        }));
        assert!(world.gateways[1..].iter().all(Option::is_some));
        drop(context);
        Self {
            tokio,
            local,
            world,
        }
    }

    /// Time `iters` iterations, each delivering what `stage` staged and then
    /// advancing the clock `by`; the answers run outside the timed section.
    fn measure(
        &mut self,
        iters: u64,
        by: Duration,
        mut stage: impl FnMut(&mut World, u64),
    ) -> Duration {
        let Self {
            tokio,
            local,
            world,
        } = self;
        tokio.block_on(local.run_until(async {
            let mut total = Duration::ZERO;
            for i in 0..iters {
                stage(world, i);
                let start = Instant::now();
                world.deliver();
                sim_advance(by).await;
                total += start.elapsed();
                world.answer().await;
            }
            total
        }))
    }
}

/// Bench `id` on a node of `scenario`'s size, staging each iteration with
/// `stage` and then advancing the clock `by`.
fn time(
    c: &mut Criterion,
    id: &str,
    (name, peers, live): Scenario,
    by: Duration,
    mut stage: impl FnMut(&mut World, u64),
) {
    let mut bench = Bench::new(peers, live);
    c.bench_function(&format!("{id}/{name}"), |b| {
        b.iter_custom(|iters| bench.measure(iters, by, &mut stage))
    });
}

fn benches(c: &mut Criterion) {
    for scenario @ (_, peers, live) in SCENARIOS {
        let own = live / (u32::from(peers) + 1);
        let (zone_peers, remote_peers): (Vec<usize>, Vec<usize>) =
            (0..usize::from(peers)).partition(|&p| zone(p) == 0);
        time(c, "round/steady", scenario, HEARTBEAT, |world, i| {
            let key = OWN_KEYS + u128::from(2 * (i % u64::from(own / 2)));
            world.raises.extend([key, key + 1]);
            for origin in ORIGINS {
                world.stage_own(origin, true);
            }
        });
        let mut down = None;
        time(c, "round/gateway_failover", scenario, PROMPT, |world, _| {
            let gateway = *down.get_or_insert(world.gateways[1].expect("a gateway"));
            world.toggle_cannot_relay(gateway);
            if !world.peers[gateway].cannot_relay {
                down = None;
            }
        });
        let mut row = None;
        time(c, "peers/refresh", scenario, PROMPT, |world, _| {
            let gateway = world.gateways[1];
            let row = *row.get_or_insert_with(|| {
                (0..)
                    .find(|&p| zone(p) == 1 && Some(p) != gateway)
                    .expect("a remote row")
            });
            world.toggle_cannot_relay(row);
        });
        time(c, "sweep/page", scenario, PROMPT, |world, _| {
            world.stage_header(zone_peers[0], Payload::SweepRequest, false)
        });
        for (id, senders, flagged) in [
            ("inbound/zone_peer", &zone_peers, false),
            ("inbound/relay_fresh", &remote_peers, true),
        ] {
            time(c, id, scenario, Duration::ZERO, |world, i| {
                world.stage_own(senders[i as usize % senders.len()], flagged)
            });
        }
    }
}

criterion_group!(gossip, benches);
criterion_main!(gossip);
