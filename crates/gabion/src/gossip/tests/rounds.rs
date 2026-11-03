//! Streams, acks, gaps, timeouts and join sweeps across simulated clusters
//! of one zone. Heartbeats are 100 ms and the relay floor 5 ms; time moves
//! 1 ms at a time so every prompt round runs when it falls due.

use std::task::{Context, Waker};

use futures::channel::mpsc::{UnboundedSender, unbounded};

use super::*;
use crate::defaults::GOSSIP_GAP_REPORTS;
use crate::gossip::PeerEntry;

/// One runtime on the simulated network, with its admin channel and the
/// discovery stream the test drives.
pub(super) struct Node {
    pub(super) addr: SocketAddr,
    pub(super) client: GossipClient,
    admin: mpsc::Sender<AdminCommand>,
    pub(super) agg: Rc<InMemoryAggregateStore>,
    events: UnboundedSender<PeerEvent>,
    handle: JoinHandle<Result<(), GossipError>>,
}

impl Node {
    pub(super) fn start(
        router: &SimRouter,
        addr: SocketAddr,
        store: CellStore,
        config: ValidGossipConfig,
    ) -> Self {
        Self::start_on(router.bind(addr), addr, store, config)
    }

    /// A node on any transport bound to `addr`.
    pub(super) fn start_on<T: GossipTransport + 'static>(
        transport: T,
        addr: SocketAddr,
        store: CellStore,
        config: ValidGossipConfig,
    ) -> Self {
        let (admin, admin_rx) = mpsc::channel(4);
        let (events, events_rx) = unbounded();
        let agg = Rc::new(InMemoryAggregateStore::new());
        let (rt, client) = GossipRuntime::from_parts_with_admin(
            transport,
            TokioClock::from_millis(0),
            config,
            store,
            agg.clone(),
            Some(admin_rx),
        );
        let handle = tokio::task::spawn_local(rt.run(events_rx));
        Self {
            addr,
            client,
            admin,
            agg,
            events,
            handle,
        }
    }

    pub(super) async fn snapshot(&self) -> AdminSnapshot {
        admin_snapshot(&self.admin).await
    }

    pub(super) async fn counters(&self) -> GossipCounters {
        self.snapshot().await.counters
    }

    /// This node's view of the peer at `addr`.
    pub(super) async fn peer(&self, addr: SocketAddr) -> PeerEntry {
        let peers = self.snapshot().await.peers;
        *peers.iter().find(|p| p.addr == addr).expect("a peer")
    }

    pub(super) fn tell(&self, event: PeerEvent) {
        self.events.unbounded_send(event).expect("the runtime runs");
    }

    pub(super) async fn hits(&self, keys: std::ops::Range<u128>) {
        for key in keys {
            hit(&self.client, key).await;
        }
    }

    pub(super) async fn stop(self) {
        let _ = self.client.shutdown().await;
        let _ = self.handle.await;
    }
}

pub(super) fn addrs(base: u16, n: usize) -> Vec<SocketAddr> {
    (0..n).map(|i| sock(base + i as u16)).collect()
}

/// Node `i` of a test cluster, incarnation `incarnation`.
pub(super) fn id(i: usize, incarnation: u32) -> NodeIdentity {
    NodeIdentity::new(NodeId(0xD000 + i as u128), incarnation)
}

/// Start a node at `addrs[i]` that knows every other address.
fn member(router: &SimRouter, addrs: &[SocketAddr], i: usize) -> Node {
    Node::start(
        router,
        addrs[i],
        store_for(id(i, 1)),
        sim_config(others(addrs, addrs[i])),
    )
}

pub(super) async fn run_for(ms: u64) {
    sim_advance_ticks(Duration::from_millis(1), ms as u32).await;
}

/// Restart the node at `addr` as a new process: the old runtime stops, and a
/// new one with an empty store and `identity` binds the address.
async fn restart(
    router: &SimRouter,
    node: Node,
    identity: NodeIdentity,
    config: ValidGossipConfig,
) -> Node {
    let addr = node.addr;
    node.stop().await;
    Node::start(router, addr, store_for(identity), config)
}

/// Every node's aggregate holds `total` hits.
pub(super) fn assert_totals(nodes: &[&Node], total: u64) {
    for node in nodes {
        assert_eq!(node.agg.total(), total, "{}", node.addr);
    }
}

/// One heartbeat sends a new count straight to every peer.
#[test]
fn one_zone_direct_mail_converges() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_000, 12);
        let nodes: Vec<_> = (0..12).map(|i| member(&router, &addrs, i)).collect();
        for key in [1, 2, 3, 1, 2] {
            hit(&nodes[0].client, key).await;
        }
        run_for(100 + 4 * 5).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 5);
        for node in nodes {
            node.stop().await;
        }
    });
}

/// A partition heals through the streams' retransmission timeouts: every join
/// sweep ended while the partition stood, and none runs after the heal, so no
/// sweep carried the counts across it.
#[test]
fn partition_then_heal() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(44_800, 3);
        router.set_link_policy(addrs[0], addrs[2], LinkPolicy::Block);
        router.set_link_policy(addrs[2], addrs[0], LinkPolicy::Block);
        let nodes: Vec<_> = (0..3).map(|i| member(&router, &addrs, i)).collect();
        for (node, hits) in nodes.iter().zip([1, 2, 4]) {
            node.hits(0..hits).await;
        }
        run_for(1_000).await;
        let mut served = Vec::new();
        for node in &nodes {
            served.push(node.counters().await.sweeps_served);
        }
        router.set_link_policy(addrs[0], addrs[2], LinkPolicy::Pass);
        router.set_link_policy(addrs[2], addrs[0], LinkPolicy::Pass);
        run_for(300).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 7);
        for (node, before) in nodes.iter().zip(served) {
            assert_eq!(node.counters().await.sweeps_served, before, "{}", node.addr);
        }
        for node in nodes {
            node.stop().await;
        }
    });
}

/// The last packet of a burst, lost with nothing after it to open a gap, is
/// resent one heartbeat later with `PROBE`, and the peer answers it at once
/// rather than at its own heartbeat half a heartbeat later.
#[test]
fn last_packet_of_a_burst_is_repaired_by_rto() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_100, 2);
        let a = member(&router, &addrs, 0);
        run_for(50).await;
        let b = member(&router, &addrs, 1);
        // Warm-up: B hears A, and every join sweep ends.
        hit(&a.client, 1).await;
        run_for(300).await;
        assert_eq!(b.agg.total(), 1);
        let requested = b.counters().await.sweeps_requested;
        run_for(200).await;
        assert_eq!(b.counters().await.sweeps_requested, requested);

        // t = 550; A's next heartbeat h is at 600.
        router.set_link_policy(a.addr, b.addr, LinkPolicy::DropFirst { count: 1 });
        a.hits(2..5).await;
        run_for(50 + 100 + 5).await;
        assert_eq!(b.agg.total(), 4);
        assert_eq!(a.peer(b.addr).await.own_unacked, 0);
        a.stop().await;
        b.stop().await;
    });
}

/// A lost packet followed by a delivered one opens a gap; the receiver reports
/// it after one relay floor and the sender resends at once, well before its
/// timeout. One cell fills a packet.
#[test]
fn gap_is_reported_and_resent_within_floors() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_200, 2);
        let config = |me| {
            tuned_config(others(&addrs, me), |s| {
                s.max_payload_bytes = MIN_PAYLOAD_BYTES
            })
        };
        let a = Node::start(&router, addrs[0], store_for(id(0, 1)), config(addrs[0]));
        let b = Node::start(&router, addrs[1], store_for(id(1, 1)), config(addrs[1]));
        hit(&a.client, 1).await;
        run_for(350).await;
        assert_eq!(b.agg.total(), 1);

        // t = 350; A's heartbeat at 400 sends (1, 2], dropped, then (2, 3].
        router.set_link_policy(a.addr, b.addr, LinkPolicy::DropFirst { count: 1 });
        a.hits(2..4).await;
        run_for(50 + 3 * 5).await;
        assert_eq!(b.counters().await.gap_reports_sent, 1);
        assert_eq!(b.agg.total(), 3);
        a.stop().await;
        b.stop().await;
    });
}

/// A gap whose sender never answers is reported `GOSSIP_GAP_REPORTS` times,
/// then abandoned once with `gap_abandoned`; the round that abandons it sends
/// nothing, yet the row stops being due and the prompt rounds stop.
#[test]
fn abandoned_gap_stops_prompt_rounds() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let x = sock(43_251);
            let b = Node::start(
                &router,
                sock(43_250),
                store_for(id(1, 1)),
                sim_config(vec![x]),
            );
            let raw = router.bind(x);
            let own = CellPayload::Own {
                range: wire::StreamRange::new(1, 2).unwrap(),
                relay_into_zone: false,
                probe: false,
            };
            let packet = header_bytes(id(9, 1), None, AckInfo::default(), Payload::Cells(own));
            raw.try_send_to(&packet, b.addr).unwrap();
            // Reports at 5, 25, 45 and 65 ms; abandoned at 85.
            run_for(100).await;
            let abandoned = b.counters().await;
            assert_eq!(abandoned.gap_reports_sent, u64::from(GOSSIP_GAP_REPORTS));
            assert_eq!(abandoned.gaps_abandoned, 1);
            run_for(300).await;
            let rounds = b.counters().await.rounds_prompt;
            assert_eq!(
                rounds, abandoned.rounds_prompt,
                "the abandoned gap stays prompt"
            );
            b.stop().await;
        })
    });
    assert_logged(&logs, "gap_abandoned", 1, &["missing=1", "reports=4"]);
}

/// A joiner learns the cells of an origin that left before it joined, which no
/// stream carries any more, from a join sweep.
#[test]
fn joiner_learns_a_departed_origins_cells_by_sweep() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_300, 5);
        let mut nodes: Vec<_> = (0..4).map(|i| member(&router, &addrs[..4], i)).collect();
        nodes[0].hits(1..6).await;
        run_for(300).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 5);

        let origin = nodes.remove(0);
        let gone = origin.addr;
        origin.stop().await;
        router.unbind(gone);
        for node in &nodes {
            node.tell(PeerEvent::Removed(Peer::new(gone)));
            hit(&node.client, 100 + node.addr.port() as u128).await;
        }
        let joiner = Node::start(
            &router,
            addrs[4],
            store_for(id(4, 1)),
            sim_config(addrs[1..4].to_vec()),
        );
        for node in &nodes {
            node.tell(PeerEvent::Added(Peer::new(joiner.addr)));
        }
        run_for(300).await;
        assert_eq!(joiner.agg.total(), 5 + 3);
        joiner.stop().await;
        for node in nodes {
            node.stop().await;
        }
    });
}

/// A node with no peers runs heartbeats and nothing else; its join sweep waits
/// without keeping the prompt ticker running.
#[test]
fn lone_node_runs_only_heartbeats() {
    run_paused(async {
        let router = SimRouter::new();
        let node = Node::start(
            &router,
            sock(43_400),
            store_for(id(0, 1)),
            sim_config(vec![]),
        );
        hit(&node.client, 1).await;
        run_for(1_000).await;
        let counters = node.counters().await;
        assert_eq!((counters.rounds_prompt, counters.rounds_heartbeat), (0, 11));
        node.stop().await;
    });
}

/// A sweep request from an address outside the peer table is counted and
/// answered with nothing, so a forged request cannot reflect the store.
#[test]
fn sweep_request_from_unknown_address_is_ignored() {
    run_paused(async {
        let router = SimRouter::new();
        let a = Node::start(
            &router,
            sock(43_500),
            store_for(id(0, 1)),
            sim_config(vec![]),
        );
        a.hits(1..4).await;
        let x = sock(43_501);
        let raw = router.bind(x);
        let request = header_bytes(id(9, 1), None, AckInfo::default(), Payload::SweepRequest);
        raw.try_send_to(&request, a.addr).unwrap();
        run_for(200).await;
        assert_eq!(router.received_count(x), 0);
        assert_eq!(a.counters().await.sweeps_ignored, 1);
        a.stop().await;
    });
}

/// A restarted node starts new streams with its peers and gets its previous
/// incarnation's cells back from a join sweep.
#[test]
fn restart_resets_streams_and_requests_a_sweep() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_600, 2);
        let r = member(&router, &addrs, 0);
        let p = member(&router, &addrs, 1);
        r.hits(1..4).await;
        p.hits(10..12).await;
        run_for(300).await;
        assert_totals(&[&r, &p], 5);

        let r = restart(&router, r, id(0, 2), sim_config(vec![p.addr])).await;
        r.hits(1..3).await;
        p.hits(12..13).await;
        run_for(300).await;
        assert_totals(&[&r, &p], 8);
        r.stop().await;
        p.stop().await;
    });
}

/// A node restarted into an idle cluster, at a new address or its old one,
/// hears every peer through the answers to its greetings, so it asks them for
/// a join sweep and gets its previous incarnation's cells back, although no
/// peer has anything to send.
#[test]
fn restarted_node_sweeps_from_idle_peers() {
    for same_address in [false, true] {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(43_650, 4);
            let r = member(&router, &addrs[..3], 0);
            let peers: Vec<_> = (1..3).map(|i| member(&router, &addrs[..3], i)).collect();
            r.hits(1..6).await;
            run_for(500).await;
            assert_totals(&[&peers[0], &peers[1]], 5);
            let new_addr = if same_address { addrs[0] } else { addrs[3] };
            r.stop().await;
            if !same_address {
                for peer in &peers {
                    peer.tell(PeerEvent::Removed(Peer::new(addrs[0])));
                    peer.tell(PeerEvent::Added(Peer::new(new_addr)));
                }
            }
            run_for(1_000).await;
            let config = sim_config(addrs[1..3].to_vec());
            let r = Node::start(&router, new_addr, store_for(id(0, 2)), config);
            run_for(1_000).await;
            assert_eq!(r.agg.total(), 5);
            r.stop().await;
            for peer in peers {
                peer.stop().await;
            }
        });
    }
}

/// One static list, naming every node and so each node itself, serves every
/// node: each drops its own address on its own first packet and keeps the
/// other two, every one heard.
#[test]
fn static_list_shared_by_every_node_drops_self() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(45_000, 3);
        let nodes: Vec<_> = (0..3)
            .map(|i| {
                let config = static_config(addrs[i], &addrs);
                Node::start(&router, addrs[i], store_for(id(i, 1)), config)
            })
            .collect();
        nodes[0].hits(1..2).await;
        run_for(300).await;
        for node in &nodes {
            let snapshot = node.snapshot().await;
            assert_eq!(
                snapshot.peers.len(),
                2,
                "{}: {:?}",
                node.addr,
                snapshot.peers
            );
            assert!(snapshot.peers.iter().all(|p| p.addr != node.addr));
            assert_eq!(snapshot.peers_by_role.unheard, 0, "{}", node.addr);
        }
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 1);
        for node in nodes {
            node.stop().await;
        }
    });
}

/// With a static list, a node that joins at an address the list named
/// all along, into peers with no cells of their own, is answered, asks them
/// for a join sweep and gets its previous incarnation's cells back.
#[test]
fn static_joiner_at_a_listed_address_sweeps_from_idle_peers() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(45_100, 4);
        let start = |i: usize, at: usize, incarnation| {
            let config = static_config(addrs[at], &addrs);
            Node::start(&router, addrs[at], store_for(id(i, incarnation)), config)
        };
        let r = start(0, 0, 1);
        let peers = [start(1, 1, 1), start(2, 2, 1)];
        r.hits(1..6).await;
        run_for(500).await;
        assert_totals(&[&peers[0], &peers[1]], 5);
        r.stop().await;
        run_for(1_000).await;
        let r = start(0, 3, 2);
        run_for(1_000).await;
        assert_eq!(r.agg.total(), 5);
        r.stop().await;
        for peer in peers {
            peer.stop().await;
        }
    });
}

/// With a static list, a node restarted at its own address, a new
/// incarnation in rows its idle peers already hold, gets its previous
/// incarnation's cells back by a join sweep.
#[test]
fn static_node_restarted_at_its_address_sweeps_from_idle_peers() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(45_200, 3);
        let start = |i: usize| {
            let config = static_config(addrs[i], &addrs);
            Node::start(&router, addrs[i], store_for(id(i, 1)), config)
        };
        let r = start(0);
        let peers = [start(1), start(2)];
        r.hits(1..6).await;
        run_for(500).await;
        assert_totals(&[&peers[0], &peers[1]], 5);
        let r = restart(&router, r, id(0, 2), static_config(addrs[0], &addrs)).await;
        run_for(1_000).await;
        assert_eq!(r.agg.total(), 5);
        r.stop().await;
        for peer in peers {
            peer.stop().await;
        }
    });
}

/// A peer speaking another wire version is sent nothing, not even the rest of a
/// sweep it asked for; its first packet of this version makes it a recipient
/// again, rewound to 0.
#[test]
fn other_version_peer_is_muted() {
    run_paused(async {
        let router = SimRouter::new();
        let v = sock(43_701);
        let a = Node::start(
            &router,
            sock(43_700),
            store_for(id(0, 1)),
            sim_config(vec![v]),
        );
        let raw = router.bind(v);
        a.hits(1..40).await;
        let request = header_bytes(id(1, 1), None, AckInfo::default(), Payload::SweepRequest);
        raw.try_send_to(&request, a.addr).unwrap();
        raw.try_send_to(&v1_packet(), a.addr).unwrap();
        run_for(1).await;
        let received = router.received_count(v);
        run_for(300).await;
        assert_eq!(router.received_count(v), received);
        assert_eq!(a.peer(v).await.role, Role::Incompatible);

        // The version packet is read before any round serves the sweep, so
        // no page may have gone out even before the snapshot above.
        let limits = wire::FrameLimits::new(wire::MAX_PAYLOAD_BYTES).unwrap();
        let mut buf = vec![0; wire::MAX_PAYLOAD_BYTES];
        let mut cx = Context::from_waker(Waker::noop());
        while let Ok((n, _)) = raw.try_recv_from(&mut cx, &mut buf) {
            let header = wire::decode(&buf[..n], limits, 0xC1.try_into().unwrap(), |_| {});
            let sweep = Payload::Cells(CellPayload::Sweep);
            assert_ne!(
                header.unwrap().payload,
                sweep,
                "a muted peer was sent a page"
            );
        }
        let ack = header_bytes(
            id(1, 1),
            None,
            AckInfo::default(),
            Payload::Ack { probe: false },
        );
        raw.try_send_to(&ack, a.addr).unwrap();
        run_for(100).await;
        assert_eq!(a.peer(v).await.role, Role::Zone);
        let mut own_cells = 0;
        while let Ok((n, _)) = raw.try_recv_from(&mut cx, &mut buf) {
            wire::decode(&buf[..n], limits, 0xC1.try_into().unwrap(), |_| {
                own_cells += 1
            })
            .unwrap();
        }
        assert_eq!(own_cells, 39);
        a.stop().await;
    });
}

/// Two restarts within one wall-clock second get distinct incarnations, so no
/// peer merges a new process's counts into the old one's: every node ends exact
/// although each restart's first packet to every peer is lost.
#[cfg(feature = "transport-udp")]
#[test]
fn restart_within_one_second_ends_exact() {
    use crate::identity::incarnation_at;
    run_paused(async {
        const BASE: u64 = 1_700_000_000_000;
        let router = SimRouter::new();
        let addrs = addrs(43_800, 3);
        let start = |i: usize, identity| {
            let config = sim_config(others(&addrs, addrs[i]));
            Node::start(&router, addrs[i], store_for(identity), config)
        };
        let mut incarnation = incarnation_at(BASE, None);
        let mut r = start(0, id(0, incarnation));
        let peers = [start(1, id(1, 1)), start(2, id(2, 1))];
        r.hits(1..4).await;
        run_for(100).await;
        let mut total = 3;
        for _ in 0..2 {
            incarnation = incarnation_at(BASE + 100, Some(incarnation));
            for peer in &peers {
                router.set_link_policy(r.addr, peer.addr, LinkPolicy::DropFirst { count: 1 });
            }
            let config = sim_config(others(&addrs, addrs[0]));
            r = restart(&router, r, id(0, incarnation), config).await;
            r.hits(1..3).await;
            for peer in &peers {
                hit(&peer.client, 200 + total).await;
                total += 1;
            }
            total += 2;
            run_for(100).await;
        }
        run_for(500).await;
        assert_totals(&[&r, &peers[0], &peers[1]], total as u64);
        r.stop().await;
        for peer in peers {
            peer.stop().await;
        }
    });
}

/// After a restart, an ack the peer wrote for the old process credits nothing
/// in the new one's stream, so R's resend starts from 0, P never sees a hole to
/// report, and every node ends exact.
#[test]
fn stale_ack_after_restart_is_ignored() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(43_900, 2);
        let r = member(&router, &addrs, 0);
        let p = member(&router, &addrs, 1);
        r.hits(1..11).await;
        run_for(300).await;
        // P has acknowledged R's old stream through 10.
        assert_eq!(r.peer(p.addr).await.own_unacked, 0);
        let reports = p.counters().await.gap_reports_sent;

        router.set_link_policy(r.addr, p.addr, LinkPolicy::DropFirst { count: 2 });
        let r = restart(&router, r, id(0, 2), sim_config(vec![p.addr])).await;
        r.hits(1..13).await;
        p.hits(50..51).await;
        run_for(300).await;
        assert_totals(&[&r, &p], 10 + 12 + 1);
        assert_eq!(p.counters().await.gap_reports_sent, reports);
        r.stop().await;
        p.stop().await;
    });
}

/// A receiver that forgot a sender's stream (discovery removed and re-added it,
/// with no restart) reports a gap from 0; the sender rewinds below its `acked`
/// and resends, so the gap closes instead of being abandoned.
#[test]
fn receiver_that_forgot_the_stream_is_resynced() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(44_000, 2);
            let o = member(&router, &addrs, 0);
            let z = member(&router, &addrs, 1);
            o.hits(1..4).await;
            run_for(300).await;
            z.tell(PeerEvent::Removed(Peer::new(o.addr)));
            z.tell(PeerEvent::Added(Peer::new(o.addr)));
            run_for(1).await;
            o.hits(4..6).await;
            run_for(300).await;
            let counters = z.counters().await;
            assert_eq!(counters.gaps_abandoned, 0);
            assert!(counters.gap_reports_sent >= 1);
            assert_eq!(o.peer(z.addr).await.own_unacked, 0);
            assert_totals(&[&o, &z], 5);
            o.stop().await;
            z.stop().await;
        })
    });
    assert_logged(&logs, "gap_abandoned", 0, &[]);
}

/// A cluster of a restarting node N and `sources` peers that hold N's old
/// cells: N has restarted, and every source has taken one hit since, so its
/// stream makes N hear it.
struct Takeover {
    router: SimRouter,
    n: Node,
    sources: Vec<Node>,
    /// Each source's `sweeps_served` before N restarted.
    served_before: Vec<u64>,
}

/// N takes `old_cells` hits and every node converges; then N restarts, after
/// `prepare` sets link policies, and each source takes one hit.
async fn takeover(
    base: u16,
    sources: usize,
    old_cells: u128,
    tune: impl Fn(&mut Settings) + Copy,
    store: impl Fn(NodeIdentity) -> CellStore,
    prepare: impl FnOnce(&SimRouter, &[SocketAddr]),
) -> Takeover {
    let router = SimRouter::with_channel_capacity(1024);
    let addrs = addrs(base, sources + 1);
    let config = |i: usize| tuned_config(others(&addrs, addrs[i]), tune);
    let n = Node::start(&router, addrs[0], store(id(0, 1)), config(0));
    let sources: Vec<_> = (1..=sources)
        .map(|i| Node::start(&router, addrs[i], store(id(i, 1)), config(i)))
        .collect();
    n.hits(0..old_cells).await;
    run_for(500).await;
    for source in &sources {
        assert_eq!(
            source.agg.total(),
            old_cells as u64,
            "the source holds N's old cells"
        );
    }
    let mut served_before = Vec::with_capacity(sources.len());
    for source in &sources {
        served_before.push(source.counters().await.sweeps_served);
    }
    n.stop().await;
    prepare(&router, &addrs);
    let n = Node::start(&router, addrs[0], store(id(0, 2)), config(0));
    for source in &sources {
        hit(&source.client, 1_000_000 + source.addr.port() as u128).await;
    }
    Takeover {
        router,
        n,
        sources,
        served_before,
    }
}

impl Takeover {
    /// The sweeps each source served since N restarted.
    async fn served(&self) -> Vec<u64> {
        let mut served = Vec::with_capacity(self.sources.len());
        for (source, before) in self.sources.iter().zip(&self.served_before) {
            served.push(source.counters().await.sweeps_served - before);
        }
        served
    }

    async fn stop(self) {
        self.n.stop().await;
        for source in self.sources {
            source.stop().await;
        }
    }
}

/// N's first sweep request is lost; it asks again at its next heartbeat and
/// gets its old cells.
#[test]
fn takeover_with_lost_sweep_request_ends_exact() {
    run_paused(async {
        let t = takeover(
            44_100,
            1,
            5,
            |_| {},
            store_for,
            |router, addrs| {
                router.set_link_policy(addrs[0], addrs[1], LinkPolicy::Block);
            },
        )
        .await;
        while t.n.counters().await.sweeps_requested == 0 {
            run_for(1).await;
        }
        let (n, s) = (t.n.addr, t.sources[0].addr);
        t.router.set_link_policy(n, s, LinkPolicy::Pass);
        run_for(300).await;
        assert_eq!(t.n.counters().await.sweeps_requested, 2);
        assert_eq!(t.served().await, [1]);
        assert_eq!(t.n.agg.total(), 5 + 1);
        t.stop().await;
    });
}

/// A retry that reaches the source while it is still paging N's sweep neither
/// starts a second sweep nor restarts the pages, so pages lost in between stay
/// lost (sweep pages are never acked).
#[test]
fn takeover_retry_during_a_paged_sweep_is_a_no_op() {
    run_paused(async {
        // A 2,048-cell store makes the quantum and the page one packet, 33
        // cells.
        let tune = |s: &mut Settings| {
            s.max_payload_bytes = 1400;
            s.max_cells = 2048.try_into().unwrap();
        };
        let store = |identity| CellStore::new(storage(2048, 32), identity, &rules(32));
        let t = takeover(44_200, 1, 1_000, tune, store, |_, _| {}).await;
        let (n, s) = (&t.n, &t.sources[0]);
        let paged = s.counters().await.sweep_cells_sent;
        // The source's hit cannot go out yet: give it a 34-cell burst, so its
        // stream to N runs a prompt round after the heartbeat and the first
        // page waits one relay floor behind N's request.
        s.hits(2_000_000..2_000_033).await;
        let mut waited = 0;
        while t.served().await == [0] {
            run_for(1).await;
            waited += 1;
            assert!(waited < 200, "N never asked S for a sweep");
        }
        // Every packet to N for over a heartbeat, a page each prompt round
        // among them: N asks again at its next heartbeat while S still pages.
        t.router
            .set_link_policy(s.addr, n.addr, LinkPolicy::DropFirst { count: 25 });
        run_for(400).await;
        assert_eq!(
            n.counters().await.sweeps_requested,
            2,
            "N retried while S paged"
        );
        assert_eq!(t.served().await, [1]);
        let snapshot = s.snapshot().await;
        assert_eq!(
            snapshot.counters.sweep_cells_sent - paged,
            snapshot.store_stats.active_cells as u64,
            "S paged its store once"
        );
        let total = n.agg.total();
        assert!(
            total < 1_000 + 34,
            "the lost pages came from nowhere else: {total}"
        );
        t.stop().await;
    });
}

/// A join sweep pages a quantum `q` of cells per prompt round, as a
/// full-store rewind drains, not a share of one per concurrent sweep.
#[test]
fn a_sweep_sends_a_quantum_per_prompt_round() {
    run_paused(async {
        // `q` is 1,024 at the default `max_cells`, 33 cells to a packet.
        let tune = |s: &mut Settings| s.max_payload_bytes = 1400;
        let store = |identity| CellStore::new(storage(32_768, 32), identity, &rules(32));
        let t = takeover(45_100, 1, 1_500, tune, store, |_, _| {}).await;
        let s = &t.sources[0];
        let before = s.counters().await.sweep_cells_sent;
        let mut paged = 0;
        for _ in 0..1_000 {
            run_for(1).await;
            paged = s.counters().await.sweep_cells_sent - before;
            if paged > 0 {
                break;
            }
        }
        assert_eq!(paged, 1_024);
        t.stop().await;
    });
}

/// N restarts again while its source pages the sweep the previous incarnation
/// asked for. A threshold round sends the source's next hit at once, so the new
/// incarnation hears the source and asks before a tail page reaches it; that
/// request starts the sweep over, so N gets its old cells from the first page
/// on.
#[test]
fn takeover_restarted_mid_sweep_is_swept_from_the_start() {
    run_paused(async {
        let tune = |s: &mut Settings| {
            s.max_payload_bytes = 1400;
            s.max_cells = 2048.try_into().unwrap();
        };
        let store = |identity| CellStore::new(storage(2048, 32), identity, &rules(32));
        let t = takeover(44_900, 1, 1_000, tune, store, |_, _| {}).await;
        while t.served().await == [0] {
            run_for(1).await;
        }
        run_for(10).await;
        let (addr, s) = (t.n.addr, t.sources[0].addr);
        t.n.stop().await;
        let n = Node::start(
            &t.router,
            addr,
            store(id(0, 3)),
            tuned_config(vec![s], tune),
        );
        let t = Takeover { n, ..t };
        // Two hits against a limit of 1 cross the error budget.
        let source = &t.sources[0].client;
        source
            .record(0xBEE, KeyHash(3_000_000), 0, 2, 1, 0)
            .await
            .unwrap();
        run_for(600).await;
        assert_eq!(t.served().await, [2]);
        assert_eq!(t.n.agg.total(), 1_000 + 1 + 2);
        t.stop().await;
    });
}

/// When every candidate is unreachable, N keeps rotating through them, wrapping
/// past the last, and ends with pages from two sources once two answer; then it
/// stops asking.
#[test]
fn takeover_with_every_source_unreachable_wraps_and_ends() {
    run_paused(async {
        let t = takeover(
            44_300,
            3,
            5,
            |_| {},
            store_for,
            |router, addrs| {
                for &source in &addrs[1..] {
                    router.set_link_policy(addrs[0], source, LinkPolicy::Block);
                }
            },
        )
        .await;
        run_for(2_000).await;
        for source in &t.sources[..2] {
            t.router
                .set_link_policy(t.n.addr, source.addr, LinkPolicy::Pass);
        }
        run_for(4_000).await;
        assert_eq!(t.served().await, [1, 1, 0]);
        let requested = t.n.counters().await.sweeps_requested;
        run_for(500).await;
        assert_eq!(t.n.counters().await.sweeps_requested, requested);
        assert_eq!(t.n.agg.total(), 5 + 3);
        t.stop().await;
    });
}

/// In a zone of two, N takes exactly one sweep from its one peer and then stops
/// asking.
#[test]
fn takeover_in_a_zone_of_two_takes_one_sweep() {
    run_paused(async {
        let t = takeover(44_400, 1, 5, |_| {}, store_for, |_, _| {}).await;
        run_for(300).await;
        assert_eq!(t.served().await, [1]);
        let requested = t.n.counters().await.sweeps_requested;
        run_for(500).await;
        assert_eq!(t.n.counters().await.sweeps_requested, requested);
        assert_eq!(t.n.agg.total(), 5 + 1);
        t.stop().await;
    });
}

/// A node that starts with no peers keeps its want until one is added and
/// heard, then takes one sweep.
#[test]
fn want_waits_for_a_first_peer() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(44_500, 2);
        let s = member(&router, &addrs, 1);
        s.hits(1..4).await;
        run_for(100).await;
        let n = Node::start(&router, addrs[0], store_for(id(0, 1)), sim_config(vec![]));
        run_for(200).await;
        n.tell(PeerEvent::Added(Peer::new(s.addr)));
        hit(&s.client, 4).await;
        run_for(300).await;
        assert_eq!(s.counters().await.sweeps_served, 1);
        assert_eq!(n.agg.total(), 4);
        n.stop().await;
        s.stop().await;
    });
}

/// A resend whose cells all expired in flight is one closing packet without
/// cells, in flight through the head like any other: lost, it times out and
/// goes again with `PROBE`. The receiver acks across the range, so nothing
/// stays in flight, the stream goes quiet and no gap is abandoned.
#[test]
fn expired_cells_are_closed_not_abandoned() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(44_600, 2);
            let o = Node::start(
                &router,
                addrs[0],
                store_for_expiring_rule(id(0, 1)),
                sim_config(vec![addrs[1]]),
            );
            let z = Node::start(
                &router,
                addrs[1],
                store_for_expiring_rule(id(1, 1)),
                sim_config(vec![addrs[0]]),
            );
            run_for(50).await;
            router.set_link_policy(o.addr, z.addr, LinkPolicy::DropFirst { count: 2 });
            // Two hits in bucket 0 of a 100 ms window, sent at the heartbeat
            // at 100 and expired by the retry at 200, whose closing packet is
            // lost too; the retry at 300 delivers it.
            for key in 1..3 {
                o.client
                    .record(rule_fp(0), KeyHash(key), 0, 1, 1_000, 50)
                    .await
                    .unwrap();
            }
            run_for(151).await;
            assert_eq!(o.snapshot().await.store_stats.active_cells, 0);
            run_for(105).await;
            assert_eq!(o.peer(z.addr).await.own_unacked, 0);
            let counters = o.counters().await;
            assert_eq!(counters.rto_rewinds_own, 2);
            run_for(500).await;
            let quiet = o.counters().await;
            assert_eq!(quiet.packets_sent, counters.packets_sent);
            assert_eq!(quiet.rounds_prompt, counters.rounds_prompt);
            o.stop().await;
            z.stop().await;
        })
    });
    assert_logged(&logs, "gap_abandoned", 0, &[]);
}

/// A peer that becomes a recipient with the whole store owed drains it on
/// prompt rounds, a quantum each, long before the next heartbeat, and not
/// through a sweep.
fn full_store_rewind_drains_on_prompt_rounds(max_cells: u32, max_rounds: u64) {
    run_paused(async {
        const CELLS: u128 = 20_000;
        let router = SimRouter::with_channel_capacity(4096);
        let (a_addr, b_addr, c_addr) = (sock(44_700), sock(44_701), sock(44_702));
        let a_config = tuned_config(vec![b_addr], |s| {
            s.max_payload_bytes = 1400;
            s.max_cells = max_cells.try_into().unwrap();
            s.tick_interval = ms(500);
        });
        let big = |identity| CellStore::new(storage(32_768, 32), identity, &rules(32));
        let a = Node::start(&router, a_addr, big(id(0, 1)), a_config);
        let raw = router.bind(b_addr);
        raw.try_send_to(&v1_packet(), a_addr).unwrap();
        run_for(1).await;
        assert_eq!(a.peer(b_addr).await.role, Role::Incompatible);
        a.hits(0..CELLS).await;
        drop(raw);

        // B's join sweep ends on C before A hears B (its greetings to A are
        // lost), so only A's stream can bring A's cells.
        router.set_link_policy(b_addr, a_addr, LinkPolicy::Block);
        let c = Node::start(
            &router,
            c_addr,
            store_for(id(2, 1)),
            sim_config(vec![b_addr]),
        );
        let b = Node::start(
            &router,
            b_addr,
            big(id(1, 1)),
            sim_config(vec![a_addr, c_addr]),
        );
        hit(&c.client, 1_000_000).await;
        run_for(400).await;
        assert_eq!(b.agg.total(), 1);
        assert!(b.counters().await.sweeps_requested > 0);
        run_for(110).await;
        // t = 511; B's heartbeat at 601 is its first packet to A, and A's
        // next heartbeat is at 1,000.
        router.set_link_policy(b_addr, a_addr, LinkPolicy::Pass);
        hit(&b.client, 1_000_001).await;
        let before = a.counters().await;
        run_for(480).await;
        assert_eq!(b.agg.total(), CELLS as u64 + 2);
        let after = a.counters().await;
        let rounds = after.rounds_prompt - before.rounds_prompt;
        assert!(rounds <= max_rounds, "{rounds} prompt rounds");
        assert_eq!(after.sweeps_served, 0);
        assert_eq!(after.rounds_heartbeat, before.rounds_heartbeat);
        a.stop().await;
        b.stop().await;
        c.stop().await;
    });
}

#[test]
fn full_store_rewind_drains_on_prompt_rounds_at_the_default_store() {
    // q = ⌈131,072 / 128⌉ = 1,024: ⌈20,000 / 1,024⌉ = 20 rounds.
    full_store_rewind_drains_on_prompt_rounds(131_072, 20);
}

#[test]
fn full_store_rewind_drains_on_prompt_rounds_at_twice_the_store() {
    // q = 2,048: 10 rounds.
    full_store_rewind_drains_on_prompt_rounds(262_144, 10);
}

/// A round reads at most `B_r` waiting datagrams, so a flood queued before the
/// first round neither delays it nor stops later heartbeats.
#[test]
fn inbound_flood_does_not_starve_rounds() {
    run_paused(async {
        let p = sock(2);
        let wire = Rc::new(Wire::default());
        wire.recv_buffer
            .set(Some(64 * crate::defaults::GOSSIP_MIN_DATAGRAM_CHARGE));
        for _ in 0..2_000 {
            inject(
                &wire,
                p,
                node(0xB),
                AckInfo::default(),
                Payload::Ack { probe: false },
            );
        }
        let (admin_tx, admin_rx) = mpsc::channel(4);
        let (rt, client) = GossipRuntime::from_parts_with_admin(
            MockTransport(wire.clone()),
            TokioClock::from_millis(0),
            sim_config(vec![p]),
            store_for(node(0xA)),
            Rc::new(InMemoryAggregateStore::new()),
            Some(admin_rx),
        );
        let _runtime = tokio::task::spawn_local(rt.run(futures::stream::empty()));
        hit(&client, 1).await;
        run_for(1).await;
        let first = wire
            .attempts
            .borrow()
            .iter()
            .find(|a| {
                matches!(
                    payload_of(&a.bytes),
                    Payload::Cells(CellPayload::Own { .. })
                )
            })
            .cloned()
            .expect("an own packet");
        assert_eq!(first.inbound_left, 2_000 - 64);
        assert!(wire.inbound.borrow().is_empty());
        let before = admin_snapshot(&admin_tx).await.counters.rounds_heartbeat;
        heartbeats(5).await;
        let after = admin_snapshot(&admin_tx).await.counters.rounds_heartbeat;
        assert_eq!(after - before, 5);
    });
}
