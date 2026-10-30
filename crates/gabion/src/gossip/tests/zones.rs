//! Two tiers across simulated clusters: direct mail inside a zone, one
//! rendezvous gateway per remote zone relaying into it, suspicion, failover
//! and the cannot-relay backstop. Heartbeats are 100 ms and the relay floor
//! 5 ms; time moves 1 ms at a time.

use std::cell::RefCell;

use super::rounds::{Node, addrs, assert_totals, id, run_for};
use super::*;
use crate::crdt::hash_fingerprint;
use crate::gossip::peer::rendezvous_weight;
use crate::gossip::{AnswerState, PeerZone, RoleCounts};
use crate::zone::{ZoneHash, ZoneName};

/// A transport that records every packet its node hands it, with when.
struct Tap<T> {
    inner: T,
    sent: Taps,
}

impl<T: GossipTransport> GossipTransport for Tap<T> {
    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> std::io::Result<usize> {
        let sent = self.inner.try_send_to(buf, dst)?;
        let at = tokio::time::Instant::now();
        self.sent.borrow_mut().push((at, dst, buf.to_vec()));
        Ok(sent)
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + 'a {
        self.inner.recv_from(buf)
    }
}

type Taps = Rc<RefCell<Vec<(tokio::time::Instant, SocketAddr, Vec<u8>)>>>;

/// The packets a tap recorded from `from` on: destination and header.
fn tapped(taps: &Taps, from: usize) -> Vec<(SocketAddr, Header)> {
    taps.borrow()[from..]
        .iter()
        .map(|(_, dst, bytes)| (*dst, header_of(bytes)))
        .collect()
}

fn zone(name: &str) -> ZoneHash {
    ZoneName::parse(name).unwrap().hash()
}

/// The test settings with `tune`, in zone `name` (none for `""`).
fn zoned_config(
    peers: Vec<SocketAddr>,
    name: &str,
    tune: impl FnOnce(&mut Settings),
) -> ValidGossipConfig {
    let mut config = tuned_config(peers, tune);
    config.zone = ZoneName::parse(name).ok();
    config
}

/// Node `i` in zone `zones[i]`, knowing every other address.
fn zoned_node(router: &SimRouter, addrs: &[SocketAddr], zones: &[&str], i: usize) -> Node {
    zoned_node_with(router, addrs, zones, i, |_| {})
}

fn zoned_node_with(
    router: &SimRouter,
    addrs: &[SocketAddr],
    zones: &[&str],
    i: usize,
    tune: impl FnOnce(&mut Settings),
) -> Node {
    let config = zoned_config(others(addrs, addrs[i]), zones[i], tune);
    Node::start(router, addrs[i], store_for(id(i, 1)), config)
}

/// Node `i` as [`zoned_node_with`], on a tap.
fn tapped_node(
    router: &SimRouter,
    addrs: &[SocketAddr],
    zones: &[&str],
    i: usize,
    tune: impl FnOnce(&mut Settings),
) -> (Node, Taps) {
    let taps = Taps::default();
    let tap = Tap {
        inner: router.bind(addrs[i]),
        sent: taps.clone(),
    };
    let config = zoned_config(others(addrs, addrs[i]), zones[i], tune);
    let node = Node::start_on(tap, addrs[i], store_for(id(i, 1)), config);
    (node, taps)
}

/// `n` addresses from ports `base..base + 64`, in descending rendezvous
/// weight at the node `origin`: each outranks the next there.
fn ranked(origin: NodeIdentity, base: u16, n: usize) -> Vec<SocketAddr> {
    let key = hash_fingerprint(origin.node_id.0);
    let mut ports: Vec<_> = (base..base + 64).map(sock).collect();
    ports.sort_by_key(|&addr| std::cmp::Reverse(rendezvous_weight(key, addr)));
    ports.truncate(n);
    ports
}

/// The peer `origin` sends its own stream to in `zone`.
async fn gateway_of(origin: &Node, name: &str) -> SocketAddr {
    let peers = origin.snapshot().await.peers;
    let gateway = peers
        .iter()
        .find(|p| p.zone == PeerZone::Other(zone(name)) && p.role == Role::Gateway);
    gateway.expect("a gateway").addr
}

/// Block both directions between `a` and `b`, or pass them.
fn cut(router: &SimRouter, a: SocketAddr, b: SocketAddr, blocked: bool) {
    let policy = if blocked {
        LinkPolicy::Block
    } else {
        LinkPolicy::Pass
    };
    router.set_link_policy(a, b, policy);
    router.set_link_policy(b, a, policy);
}

async fn stop_all(nodes: Vec<Node>) {
    for node in nodes {
        node.stop().await;
    }
}

/// Own-stream packets: destination, range lower end, and the relay flag.
fn own_packets(packets: &[(SocketAddr, Header)]) -> Vec<(SocketAddr, u64, bool)> {
    packets
        .iter()
        .filter_map(|(dst, header)| match header.payload {
            Payload::Cells(CellPayload::Own {
                range,
                relay_into_zone,
                ..
            }) => Some((*dst, range.lower(), relay_into_zone)),
            _ => None,
        })
        .collect()
}

/// Twelve nodes in three zones of four, all on one address prefix (as a
/// wildcard bind gives; zones come from the setting, never the address): an
/// origin sends its own stream directly to its three zone peers and, flagged,
/// to one gateway in each other zone, which relays it.
#[test]
fn three_zones_send_direct_in_zone_and_one_gateway_each() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_000, 12);
        let names = ["z0", "z1", "z2"];
        let zones: Vec<&str> = (0..12).map(|i| names[i % 3]).collect();
        let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, |_| {});
        let mut nodes = vec![origin];
        nodes.extend((1..12).map(|i| zoned_node(&router, &addrs, &zones, i)));
        run_for(1_000).await;
        let roles = nodes[0].snapshot().await.peers_by_role;
        let expected = RoleCounts {
            unheard: 0,
            zone: 3,
            gateway: 2,
            remote: 6,
            incompatible: 0,
        };
        assert_eq!(roles, expected);
        let gateways = [
            gateway_of(&nodes[0], "z1").await,
            gateway_of(&nodes[0], "z2").await,
        ];
        let mark = taps.borrow().len();
        nodes[0].hits(0..5).await;
        run_for(1_000).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 5);
        let own = own_packets(&tapped(&taps, mark));
        let zone_peers = [addrs[3], addrs[6], addrs[9]];
        for (dst, _, flagged) in &own {
            assert!(zone_peers.contains(dst) || gateways.contains(dst), "{dst}");
            assert_eq!(*flagged, gateways.contains(dst), "{dst}");
        }
        for dst in zone_peers.iter().chain(&gateways) {
            assert!(own.iter().any(|(to, ..)| to == dst), "nothing to {dst}");
        }
        stop_all(nodes).await;
    });
}

/// Zone a = {O, a1, a2}, zone b = {b1, b2, b3}. O's gateway into b dies; it
/// stays gateway for `suspect_after` from O's first unanswered packet, then
/// the next member takes over, receives O's whole live stream and relays it.
/// A busy origin, whose later sends never move the wait's start, fails over
/// as fast.
#[test]
fn dead_gateway_fails_over_after_suspect_after() {
    for busy in [false, true] {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(47_200, 6);
            let zones = ["a", "a", "a", "b", "b", "b"];
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let nodes: Vec<_> = (0..6)
                .map(|i| zoned_node_with(&router, &addrs, &zones, i, tune))
                .collect();
            run_for(999).await;
            let gateway = gateway_of(&nodes[0], "b").await;
            for &other in &addrs {
                if other != gateway {
                    cut(&router, gateway, other, true);
                }
            }
            // t = 999: the hit leaves on O's heartbeat at t0 = 1,000.
            hit(&nodes[0].client, 1).await;
            let t0 = 1_000;
            let mut now = 999;
            let mut replaced_at = None;
            while now < t0 + 400 + 3 * 5 + 5 {
                run_for(1).await;
                now += 1;
                if busy && now % 100 == 50 {
                    hit(&nodes[0].client, now as u128).await;
                }
                if replaced_at.is_none() && gateway_of(&nodes[0], "b").await != gateway {
                    replaced_at = Some(now);
                }
            }
            let replaced_at = replaced_at.expect("a failover");
            assert!(replaced_at >= t0 + 400, "replaced at {replaced_at}");
            assert!(replaced_at <= t0 + 400 + 2 * 5, "replaced at {replaced_at}");
            let hits = nodes[0].agg.total();
            for i in (3..6).filter(|&i| addrs[i] != gateway) {
                assert_eq!(nodes[i].agg.total(), hits, "{}", addrs[i]);
            }
            assert_eq!(nodes[0].counters().await.gateway_failovers, 1);
            stop_all(nodes).await;
        });
    }
}

/// Hits recorded at `node`, one per call, keyed by the time.
async fn hit_at(node: &Node, t: u64) {
    hit(
        &node.client,
        u128::from(t) | (u128::from(node.addr.port()) << 64),
    )
    .await;
}

/// A gateway that the origin reaches but that cannot reach most of its own
/// zone says so on every packet; the origin fails over once and stays
/// failed over for the whole cut, and the zone keeps the origin's counts.
#[test]
fn gateway_cut_from_its_zone_is_replaced() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_300, 5);
        let zones = ["a", "b", "b", "b", "b"];
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (nodes, taps): (Vec<_>, Vec<_>) = (0..5)
            .map(|i| tapped_node(&router, &addrs, &zones, i, tune))
            .unzip();
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let g = addrs.iter().position(|&a| a == gateway).unwrap();
        let members: Vec<usize> = (1..5).filter(|&i| i != g).collect();
        for &m in &members {
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Block);
        }
        let mark = taps[g].borrow().len();
        let mut switched = None;
        for t in 0..3_000 {
            if t % 100 == 0 {
                hit_at(&nodes[0], t).await;
                for &m in &members {
                    hit_at(&nodes[m], t).await;
                }
            }
            run_for(1).await;
            if switched.is_none() && gateway_of(&nodes[0], "b").await != gateway {
                switched = Some(t);
            }
            if t == 2_900 {
                // O's hits up to t = 2,700 and the members' own.
                let o_hits = 28;
                for &m in &members {
                    assert!(nodes[m].agg.total() >= o_hits, "{}", addrs[m]);
                }
            }
        }
        // The first relay G leaves unanswered goes out by t = 100; its zone
        // rows are suspect 400 ms later, the heartbeat after raises the flag,
        // and O hears it within two heartbeats.
        let switched = switched.expect("a failover");
        assert!(switched <= 100 + 400 + 3 * 100 + 2 * 5, "{switched}");
        assert_eq!(nodes[0].counters().await.gateway_failovers, 1);
        let to_origin: Vec<bool> = tapped(&taps[g], mark)
            .into_iter()
            .filter(|(dst, _)| *dst == addrs[0])
            .map(|(_, header)| header.acks.cannot_relay)
            .skip_while(|flag| !flag)
            .collect();
        assert!(!to_origin.is_empty() && to_origin.iter().all(|&flag| flag));
        stop_all(nodes).await;
    });
}

/// A gateway that cannot reach one member of three keeps the role; its relay
/// to that member is resent with `PROBE` every heartbeat until the member is
/// suspect, then probed with one header-only packet per heartbeat, logged once
/// as stalled, and the member catches up as soon as the path heals.
#[test]
fn minority_cut_heals_and_warns() {
    let (member, logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(47_400, 4);
            let zones = ["a", "b", "b", "b"];
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let (nodes, taps): (Vec<_>, Vec<_>) = (0..4)
                .map(|i| tapped_node(&router, &addrs, &zones, i, tune))
                .unzip();
            run_for(1_000).await;
            let gateway = gateway_of(&nodes[0], "b").await;
            let g = addrs.iter().position(|&a| a == gateway).unwrap();
            let m = (1..4).find(|&i| i != g).unwrap();
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Block);
            let mark = taps[g].borrow().len();
            nodes[0].hits(0..5).await;
            run_for(3_000).await;
            assert_eq!(nodes[0].counters().await.gateway_failovers, 0);
            let to_m: Vec<Header> = tapped(&taps[g], mark)
                .into_iter()
                .filter(|(dst, _)| *dst == addrs[m])
                .map(|(_, header)| header)
                .filter(|header| header.payload != Payload::Ack { probe: false })
                .collect();
            let probes = to_m
                .iter()
                .position(|header| header.payload == Payload::Ack { probe: true })
                .expect("probes once suspect");
            assert!(probes >= 3, "{to_m:?}");
            // The relay frame, then its resends, each with `PROBE`.
            for (i, header) in to_m[..probes].iter().enumerate() {
                let relay = matches!(
                    header.payload,
                    Payload::Cells(CellPayload::Relay { probe, .. }) if probe == (i > 0)
                );
                assert!(relay, "{to_m:?}");
            }
            assert!(
                to_m[probes..]
                    .iter()
                    .all(|header| header.payload == Payload::Ack { probe: true })
            );
            // Suspect about 500 ms into the cut: one probe per heartbeat after.
            assert!((24..=26).contains(&(to_m.len() - probes)), "{to_m:?}");
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Pass);
            run_for(100 + 2 * 5 + 5).await;
            assert_eq!(nodes[m].agg.total(), 5);
            stop_all(nodes).await;
            addrs[m]
        })
    });
    let peer = format!("peer={member}");
    assert_logged(
        &logs,
        "relay_stalled",
        1,
        &[&peer, "stall_total=1", "relay_unacked=5"],
    );
}

/// Both sides of a zone partition keep counting; on heal every node holds
/// every hit. Zone b's one member is every origin's suspect gateway with no
/// other member to fail over to, so only its probe answer's rewind brings
/// zone a's hits.
#[test]
fn zone_partition_heals_to_exact_counts() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_500, 6);
        let zones = ["a", "a", "a", "a", "a", "b"];
        let nodes: Vec<_> = (0..6)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        run_for(1_000).await;
        let cut_all = |blocked| {
            for a in 0..5 {
                cut(&router, addrs[a], addrs[5], blocked);
            }
        };
        cut_all(true);
        for (i, node) in nodes.iter().enumerate() {
            node.hits(0..i as u128 + 1).await;
            run_for(500).await;
        }
        run_for(1_000).await;
        cut_all(false);
        run_for(1_500).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 21);
        stop_all(nodes).await;
    });
}

/// A node whose own zone becomes known after its cells went out directly
/// rewinds its own stream to the gateway it now picks, which relays every
/// live cell to the members it can no longer reach directly.
#[test]
fn late_zone_rewinds_the_new_recipient() {
    run_paused(async {
        let router = SimRouter::new();
        let o = id(0, 1);
        let b = ranked(o, 47_610, 3);
        let mut addrs = vec![sock(47_600), sock(47_601)];
        addrs.extend(&b);
        let zones = ["", "a", "b", "b", "b"];
        let nodes: Vec<_> = (0..5)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        for &blocked in &b[1..] {
            router.set_link_policy(addrs[0], blocked, LinkPolicy::Block);
        }
        run_for(500).await;
        nodes[0].hits(0..10).await;
        run_for(500).await;
        assert_eq!(nodes[2].agg.total(), 10);
        nodes[0].tell(PeerEvent::OwnEntry { zone: zone("a") });
        run_for(50).await;
        assert_eq!(gateway_of(&nodes[0], "b").await, b[0]);
        run_for(2_000).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 10);
        stop_all(nodes).await;
    });
}

/// A joiner that outranks the gateway is sent the origin's whole live own
/// stream flagged once its zone is heard, on its first packet after it learns
/// it, although it already holds and has acked the cells, sent directly
/// while its zone was unknown, and relays them to members that never got
/// them.
#[test]
fn joiner_that_outranks_the_gateway_relays_lost_cells() {
    run_paused(async {
        let router = SimRouter::new();
        let o = id(0, 1);
        let ranked = ranked(o, 47_710, 3);
        let (joiner, addrs) = (ranked[0], vec![sock(47_700), ranked[1], ranked[2]]);
        let zones = ["a", "b", "b"];
        let mut nodes: Vec<_> = (0..3)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        run_for(1_000).await;
        assert_eq!(gateway_of(&nodes[0], "b").await, addrs[1]);
        router.set_link_policy(addrs[0], addrs[1], LinkPolicy::Block);
        nodes[0].hits(0..10).await;
        run_for(150).await;
        let mut all = addrs.clone();
        all.push(joiner);
        let config = zoned_config(others(&all, joiner), "", |_| {});
        nodes.push(Node::start(&router, joiner, store_for(id(3, 1)), config));
        for node in &nodes[..3] {
            node.tell(PeerEvent::Added(Peer::new(joiner)));
        }
        run_for(300).await;
        assert_eq!(nodes[3].agg.total(), 10);
        assert_eq!(nodes[0].peer(joiner).await.own_unacked, 0);
        nodes[3].tell(PeerEvent::OwnEntry { zone: zone("b") });
        run_for(1).await;
        nodes[3].hits(10..11).await;
        run_for(300).await;
        assert_eq!(gateway_of(&nodes[0], "b").await, joiner);
        run_for(700).await;
        for node in &nodes[1..] {
            assert_eq!(node.agg.total(), 11, "{}", node.addr);
        }
        stop_all(nodes).await;
    });
}

/// A gateway suspected only because it had not yet discovered the origin
/// answers a probe once it does and is the gateway again at once, sent the
/// origin's stream from 0 on that same round; when it dies, the other member
/// takes over again. When it heals, its first probe answer clears it
/// although its old own stream is still unacked (it gets no own stream as a
/// `Remote` row), so it is the gateway again within a heartbeat.
#[test]
fn transient_suspicion_clears() {
    run_paused(async {
        let router = SimRouter::new();
        let o = id(0, 1);
        let b = ranked(o, 47_810, 2);
        let addrs = vec![sock(47_800), b[0], b[1]];
        let zones = ["a", "b", "b"];
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, tune);
        // G1 does not know O: O's packets are merge-only there.
        let g1_config = zoned_config(vec![addrs[2]], "b", tune);
        let g1 = Node::start(&router, addrs[1], store_for(id(1, 1)), g1_config);
        let g2 = zoned_node_with(&router, &addrs, &zones, 2, tune);
        origin.hits(0..5).await;
        run_for(1_500).await;
        assert_eq!(gateway_of(&origin, "b").await, addrs[2]);
        assert_eq!(origin.peer(addrs[1]).await.answer, AnswerState::Suspect);
        let g1_restored_within = async |ms| {
            for _ in 0..ms {
                run_for(1).await;
                if gateway_of(&origin, "b").await == addrs[1] {
                    return true;
                }
            }
            false
        };

        let mark = taps.borrow().len();
        g1.tell(PeerEvent::Added(Peer::new(addrs[0])));
        assert!(g1_restored_within(2 * 100).await, "not within 2T");
        assert_eq!(origin.peer(addrs[1]).await.answer, AnswerState::Owed);
        run_for(2 * 5).await;
        let own = own_packets(&tapped(&taps, mark));
        assert!(own.contains(&(addrs[1], 0, true)), "{own:?}");

        cut(&router, addrs[0], addrs[1], true);
        origin.hits(5..8).await;
        run_for(400 + 100 + 2 * 5 + 100).await;
        assert_eq!(gateway_of(&origin, "b").await, addrs[2]);
        assert!(origin.peer(addrs[1]).await.own_unacked > 0);
        cut(&router, addrs[0], addrs[1], false);
        // The next probe, at most a heartbeat away, answered within `F_r`.
        assert!(g1_restored_within(100 + 2 * 5).await, "not within T");
        run_for(100).await;
        assert_eq!(g2.agg.total(), 8);
        assert_eq!(g1.agg.total(), 8);
        stop_all(vec![origin, g1, g2]).await;
    });
}

/// An idle origin sends its gateway nothing, not even a probe, so a gateway
/// that dies while it is idle goes unnoticed until the origin's next change.
/// That change fails it over within `suspect_after` plus a heartbeat and two
/// relay floors, and the next member gets every live cell. The gateway is cut
/// from its zone too, so only the failover brings that member the origin's
/// cells.
#[test]
fn idle_origin_replaces_a_dead_gateway() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_900, 3);
        let zones = ["a", "b", "b"];
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, tune);
        let mut nodes = vec![origin];
        nodes.extend((1..3).map(|i| zoned_node_with(&router, &addrs, &zones, i, tune)));
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let m = if gateway == addrs[1] { 2 } else { 1 };
        router.set_link_policy(gateway, addrs[m], LinkPolicy::Block);
        hit(&nodes[0].client, 1).await;
        run_for(300).await;
        assert_eq!(nodes[0].peer(gateway).await.own_unacked, 0);
        cut(&router, gateway, addrs[0], true);
        let mark = taps.borrow().len();
        run_for(2 * 400).await;
        // Every peer of the origin is in zone b.
        let idle = tapped(&taps, mark);
        assert!(idle.is_empty(), "{idle:?}");
        assert_eq!(gateway_of(&nodes[0], "b").await, gateway);
        hit(&nodes[0].client, 2).await;
        const BOUND: u64 = 100 + 400 + 2 * 5;
        let mut replaced = None;
        for t in 1..=BOUND {
            run_for(1).await;
            if replaced.is_none() && gateway_of(&nodes[0], "b").await != gateway {
                replaced = Some(t);
            }
        }
        let replaced = replaced.expect("a failover");
        assert!(replaced >= 400, "{replaced}");
        run_for(2 * 5).await;
        assert_eq!(nodes[m].agg.total(), 2);
        stop_all(nodes).await;
    });
}

/// A node with no zone sweeps from its peers; once its zone becomes known it
/// asks for a sweep again within a heartbeat.
#[test]
fn node_with_late_zone_gets_a_sweep() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(48_000, 4);
        let zones = ["a", "a", "a", ""];
        let mut nodes: Vec<_> = (0..3)
            .map(|i| zoned_node(&router, &addrs[..3], &zones, i))
            .collect();
        nodes[2].hits(0..4).await;
        run_for(500).await;
        let departed = nodes.pop().unwrap();
        departed.stop().await;
        for node in &nodes {
            node.tell(PeerEvent::Removed(Peer::new(addrs[2])));
            node.tell(PeerEvent::Added(Peer::new(addrs[3])));
        }
        let taps = Taps::default();
        let tap = Tap {
            inner: router.bind(addrs[3]),
            sent: taps.clone(),
        };
        let config = zoned_config(addrs[..2].to_vec(), "", |_| {});
        let x = Node::start_on(tap, addrs[3], store_for(id(3, 1)), config);
        run_for(500).await;
        assert_eq!(x.agg.total(), 4);
        let requests = |from| {
            tapped(&taps, from)
                .into_iter()
                .filter(|(_, header)| header.payload == Payload::SweepRequest)
                .map(|(_, header)| header.sender_zone)
                .collect::<Vec<_>>()
        };
        let first = requests(0);
        assert!(!first.is_empty() && first.iter().all(Option::is_none));
        let mark = taps.borrow().len();
        x.tell(PeerEvent::OwnEntry { zone: zone("a") });
        run_for(100).await;
        assert_eq!(requests(mark).first(), Some(&Some(zone("a"))));
        nodes.push(x);
        stop_all(nodes).await;
    });
}

/// The first member of a new zone sweeps from one peer of another zone, and
/// takes one sweep.
#[test]
fn first_member_of_a_new_zone_sweeps_from_any_zone() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(48_100, 6);
        let zones = ["a", "a", "b", "b", "a", "c"];
        let mut nodes: Vec<_> = (0..5)
            .map(|i| zoned_node(&router, &addrs[..5], &zones, i))
            .collect();
        nodes[4].hits(0..3).await;
        run_for(500).await;
        nodes.pop().unwrap().stop().await;
        for node in &nodes {
            node.tell(PeerEvent::Removed(Peer::new(addrs[4])));
            node.tell(PeerEvent::Added(Peer::new(addrs[5])));
        }
        let taps = Taps::default();
        let tap = Tap {
            inner: router.bind(addrs[5]),
            sent: taps.clone(),
        };
        let config = zoned_config(addrs[..4].to_vec(), "c", |_| {});
        let x = Node::start_on(tap, addrs[5], store_for(id(5, 1)), config);
        run_for(1_000).await;
        assert_eq!(x.agg.total(), 3);
        let requests = tapped(&taps, 0)
            .into_iter()
            .filter(|(_, header)| header.payload == Payload::SweepRequest)
            .count();
        assert_eq!(requests, 1);
        nodes.push(x);
        stop_all(nodes).await;
    });
}

/// A gateway that forgot its origin's stream (discovery removed and re-added
/// the origin, with no restart) reports a flagged gap from 0: the origin
/// rewinds below its `acked` and resends flagged, the gap closes instead of
/// being abandoned, and the zone ends exact.
#[test]
fn gateway_that_forgot_the_stream_is_resynced() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(48_900, 3);
            let zones = ["a", "b", "b"];
            let nodes: Vec<_> = (0..3)
                .map(|i| zoned_node(&router, &addrs, &zones, i))
                .collect();
            nodes[0].hits(1..4).await;
            run_for(300).await;
            let gateway = gateway_of(&nodes[0], "b").await;
            let g = nodes.iter().find(|node| node.addr == gateway).unwrap();
            let reports = g.counters().await.gap_reports_sent;
            g.tell(PeerEvent::Removed(Peer::new(addrs[0])));
            g.tell(PeerEvent::Added(Peer::new(addrs[0])));
            run_for(1).await;
            nodes[0].hits(4..6).await;
            run_for(300).await;
            let counters = g.counters().await;
            assert_eq!(counters.gaps_abandoned, 0);
            assert!(counters.gap_reports_sent > reports);
            assert_eq!(nodes[0].peer(gateway).await.own_unacked, 0);
            assert_totals(&[&nodes[0], &nodes[1], &nodes[2]], 5);
            stop_all(nodes).await;
        })
    });
    assert_logged(&logs, "gap_abandoned", 0, &[]);
}

/// O cannot reach its gateway G, which still reaches O: G's packets repeat
/// the stuck flagged ack of O's hit before the cut. O fails over once, on its
/// first change after the cut, within `suspect_after` plus a heartbeat and two
/// relay floors, and stays failed over while the cut lasts; the other member
/// holds O's hits. An idle O asks G nothing while the cut lasts (it still
/// acks G's cells), so it keeps G until that change.
#[test]
fn one_way_cut_to_gateway_fails_over_once() {
    for idle in [false, true] {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(48_200, 3);
            let zones = ["a", "b", "b"];
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, tune);
            let mut nodes = vec![origin];
            nodes.extend((1..3).map(|i| zoned_node_with(&router, &addrs, &zones, i, tune)));
            run_for(1_000).await;
            let gateway = gateway_of(&nodes[0], "b").await;
            let (g, m) = if gateway == addrs[1] { (1, 2) } else { (2, 1) };
            nodes[0].hits(3..4).await;
            run_for(200).await;
            assert_eq!(nodes[0].peer(gateway).await.own_unacked, 0);
            router.set_link_policy(addrs[0], gateway, LinkPolicy::Block);
            if idle {
                let mark = taps.borrow().len();
                for t in 0..2 * 400 {
                    if t % 100 == 0 {
                        hit_at(&nodes[g], t).await;
                    }
                    run_for(1).await;
                }
                let asked = tapped(&taps, mark)
                    .iter()
                    .any(|(dst, header)| *dst == gateway && header.payload.asks_answer());
                assert!(!asked, "the idle origin asked its gateway for an answer");
                assert_eq!(nodes[0].counters().await.gateway_failovers, 0);
            }
            nodes[0].hits(0..3).await;
            let mut switched = None;
            for t in 0..5 * 400 {
                if t % 100 == 0 {
                    hit_at(&nodes[g], t).await;
                }
                run_for(1).await;
                if switched.is_none() && gateway_of(&nodes[0], "b").await != gateway {
                    switched = Some(t);
                }
            }
            let switched = switched.expect("a failover");
            assert!(switched <= 100 + 400 + 2 * 5, "{switched}");
            assert_eq!(nodes[0].counters().await.gateway_failovers, 1);
            assert_eq!(gateway_of(&nodes[0], "b").await, addrs[m]);
            // O's three hits and G's, which reach both directly.
            assert_eq!(nodes[m].agg.total(), nodes[0].agg.total());
            stop_all(nodes).await;
        });
    }
}

/// Before suspicion a silent peer is resent the window with `PROBE` each
/// heartbeat; once suspect, only one 86-byte probe per heartbeat, while a
/// silent gateway is replaced. The first answer after the heal brings the
/// resend (or the gateway role) back at once.
#[test]
fn suspect_peer_gets_header_only_probes() {
    for gateway in [false, true] {
        run_paused(async {
            let router = SimRouter::new();
            let o = id(0, 1);
            let mut addrs = vec![sock(48_400)];
            let zones = if gateway {
                addrs.extend(ranked(o, 48_410, 2));
                ["a", "b", "b"]
            } else {
                addrs.extend([sock(48_401), sock(48_402)]);
                ["a", "a", "b"]
            };
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, tune);
            let others: Vec<_> = (1..3)
                .map(|i| zoned_node_with(&router, &addrs, &zones, i, tune))
                .collect();
            run_for(1_000).await;
            // A gateway that has acked a cell is resent, not probed, until
            // it is suspect.
            origin.hits(9..10).await;
            run_for(300).await;
            let silent = addrs[1];
            cut(&router, addrs[0], silent, true);
            let mark = taps.borrow().len();
            origin.hits(0..3).await;
            run_for(400 + 100).await;
            let before = tapped(&taps, mark);
            let retries = own_packets(&before)
                .into_iter()
                .filter(|&(dst, lower, _)| dst == silent && lower == 1)
                .count();
            assert!(retries >= 4, "{retries}");
            if gateway {
                assert_eq!(gateway_of(&origin, "b").await, addrs[2]);
            }
            let mark = taps.borrow().len();
            run_for(20 * 100).await;
            let after: Vec<_> = taps.borrow()[mark..]
                .iter()
                .filter(|(_, dst, _)| *dst == silent)
                .map(|(_, _, bytes)| bytes.clone())
                .collect();
            assert_eq!(after.len(), 20);
            for bytes in after {
                assert_eq!(bytes.len(), 86);
                assert_eq!(payload_of(&bytes), Payload::Ack { probe: true });
            }
            cut(&router, addrs[0], silent, false);
            run_for(100 + 2 * 5 + 5).await;
            if gateway {
                assert_eq!(gateway_of(&origin, "b").await, silent);
            } else {
                assert_eq!(others[0].agg.total(), 4);
            }
            let mut nodes = others;
            nodes.push(origin);
            stop_all(nodes).await;
        });
    }
}

/// Zone a = {O}, zone b = {G, G2} with G outranking at O; G's heartbeats
/// trail O's by 50 ms, so its standalone acks never land on O's judging
/// instants. An answer that arrives before `suspect_after` keeps G; after
/// it, G is replaced.
#[test]
fn answer_just_inside_suspect_after_keeps_the_gateway() {
    for (suspect_after, failovers) in [(910, 0), (890, 1)] {
        run_paused(async {
            let router = SimRouter::new();
            let o = id(0, 1);
            let mut addrs = vec![sock(48_500)];
            addrs.extend(ranked(o, 48_510, 2));
            let zones = ["a", "b", "b"];
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(suspect_after));
            let origin = zoned_node_with(&router, &addrs, &zones, 0, tune);
            let g2 = zoned_node_with(&router, &addrs, &zones, 2, tune);
            run_for(50).await;
            let g = zoned_node_with(&router, &addrs, &zones, 1, tune);
            run_for(949).await;
            assert_eq!(gateway_of(&origin, "b").await, addrs[1]);
            // t = 999; the hit leaves on O's heartbeat at t0 = 1,000.
            hit(&origin.client, 1).await;
            router.set_link_policy(addrs[1], addrs[0], LinkPolicy::Block);
            run_for(1 + 895).await;
            router.set_link_policy(addrs[1], addrs[0], LinkPolicy::Pass);
            run_for(5 + 15).await;
            let counters = origin.counters().await;
            assert_eq!(counters.gateway_failovers, failovers, "{suspect_after}");
            stop_all(vec![origin, g, g2]).await;
        });
    }
}

/// A gateway cut from its zone says so to an origin that sends to it until
/// the cut heals, then stops, and the origin makes it its gateway again.
#[test]
fn healed_gateway_clears_cannot_relay() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(48_600, 4);
        let zones = ["a", "b", "b", "b"];
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (nodes, taps): (Vec<_>, Vec<_>) = (0..4)
            .map(|i| tapped_node(&router, &addrs, &zones, i, tune))
            .unzip();
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let g = addrs.iter().position(|&a| a == gateway).unwrap();
        let members: Vec<usize> = (1..4).filter(|&i| i != g).collect();
        for &m in &members {
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Block);
        }
        for t in 0..2_500 {
            if t % 100 == 0 {
                hit_at(&nodes[0], t).await;
            }
            run_for(1).await;
        }
        assert_ne!(gateway_of(&nodes[0], "b").await, gateway);
        let flags: Vec<bool> = tapped(&taps[g], 0)
            .into_iter()
            .map(|(_, header)| header.acks.cannot_relay)
            .skip_while(|flag| !flag)
            .collect();
        assert!(!flags.is_empty() && flags.iter().all(|&flag| flag));
        for &m in &members {
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Pass);
        }
        run_for(400 + 4 * 100 + 5).await;
        assert_eq!(gateway_of(&nodes[0], "b").await, gateway);
        for &m in &members {
            assert_eq!(nodes[m].agg.total(), 25);
        }
        stop_all(nodes).await;
    });
}

/// A failover, on the origin's first change after its gateway dies, rewinds
/// the origin's whole live stream to the new gateway, which holds every cell
/// after at most 21 prompt rounds, a quantum each.
#[test]
fn failover_drains_a_full_store_on_prompt_rounds() {
    run_paused(async {
        const CELLS: u128 = 20_000;
        let router = SimRouter::with_channel_capacity(4096);
        let addrs = addrs(48_700, 3);
        let zones = ["a", "b", "b"];
        let tune = |s: &mut Settings| {
            s.max_payload_bytes = 1400;
            s.tick_interval = ms(500);
            s.suspect_after = Some(ms(1_000));
        };
        let big = |i| CellStore::new(storage(32_768, 32), id(i, 1), &rules(32));
        let nodes: Vec<_> = (0..3)
            .map(|i| {
                let config = zoned_config(others(&addrs, addrs[i]), zones[i], tune);
                Node::start(&router, addrs[i], big(i), config)
            })
            .collect();
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let next = if gateway == addrs[1] { 2 } else { 1 };
        // The gateway's relay never reaches the next member, so only the
        // failover's rewind brings O's cells there.
        router.set_link_policy(gateway, addrs[next], LinkPolicy::Block);
        nodes[0].hits(0..CELLS).await;
        run_for(1_000).await;
        cut(&router, addrs[0], gateway, true);
        hit(&nodes[0].client, CELLS).await;
        let mut before = None;
        for _ in 0..3_000 {
            run_for(1).await;
            if before.is_none() && gateway_of(&nodes[0], "b").await == addrs[next] {
                before = Some(nodes[0].counters().await.rounds_prompt);
            }
            if before.is_some() && nodes[next].agg.total() == CELLS as u64 + 1 {
                break;
            }
        }
        let before = before.expect("a failover");
        assert_eq!(nodes[next].agg.total(), CELLS as u64 + 1);
        let rounds = nodes[0].counters().await.rounds_prompt - before;
        assert!(rounds <= 21, "{rounds} prompt rounds");
        stop_all(nodes).await;
    });
}

/// A node whose zone becomes known under 20 % loss on every link still ends
/// with every hit everywhere, although its direct mail into zone b reached
/// only the member that becomes its gateway.
#[test]
fn late_own_zone_under_loss_ends_exact() {
    run_paused(async {
        let router = SimRouter::new();
        let o = id(0, 1);
        let b = ranked(o, 48_810, 3);
        let mut addrs = vec![sock(48_800), sock(48_801)];
        addrs.extend(&b);
        for &src in &addrs {
            for &dst in &addrs {
                router.set_link_policy(src, dst, LinkPolicy::DropProb { p: 0.2 });
            }
        }
        for &blocked in &b[1..] {
            router.set_link_policy(addrs[0], blocked, LinkPolicy::Block);
        }
        let zones = ["", "a", "b", "b", "b"];
        let nodes: Vec<_> = (0..5)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        run_for(500).await;
        nodes[0].hits(0..50).await;
        run_for(300).await;
        assert_eq!(nodes[2].agg.total(), 50);
        assert_eq!(nodes[0].peer(b[0]).await.own_unacked, 0);
        nodes[0].tell(PeerEvent::OwnEntry { zone: zone("a") });
        run_for(5_000).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 50);
        stop_all(nodes).await;
    });
}

/// A peer this node has not heard from is greeted with `PROBE` at once and
/// then each heartbeat, on the new cells that heartbeat sends it or alone, and
/// sent only the own cells raised since it was added: no relay stream and no
/// rewind to 0. Once its packets name its zone,
/// here this node's, it is a zone peer, sent both streams from 0.
#[test]
fn unheard_peer_gets_new_own_cells_and_greetings() {
    run_paused(async {
        let router = SimRouter::new();
        // G, in zone b, ranks O above Z, so O relays G's cells into zone a.
        let at_g = ranked(id(2, 1), 49_210, 2);
        let (n, addrs) = (sock(49_200), vec![at_g[0], at_g[1], sock(49_201)]);
        let zones = ["a", "a", "b"];
        let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, |_| {});
        let mut nodes = vec![origin];
        nodes.extend((1..3).map(|i| zoned_node(&router, &addrs, &zones, i)));
        nodes[0].hits(0..3).await;
        nodes[2].hits(10..13).await;
        run_for(1_000).await;
        let to = |dst: SocketAddr, from: usize| -> Vec<Payload> {
            let sent = tapped(&taps, from).into_iter();
            sent.filter(|(to, _)| *to == dst)
                .map(|(_, h)| h.payload)
                .collect()
        };
        let relay = |p: &Payload| matches!(p, Payload::Cells(CellPayload::Relay { .. }));
        assert!(to(addrs[1], 0).iter().any(relay), "O relays G's cells");
        let hello = Payload::Ack { probe: true };

        let mark = taps.borrow().len();
        let before = nodes[0].counters().await.rounds_heartbeat;
        nodes[0].tell(PeerEvent::Added(Peer::new(n)));
        run_for(300).await;
        let beats = nodes[0].counters().await.rounds_heartbeat - before;
        let sent = to(n, mark);
        // The first greeting goes on the first round of any kind.
        let greeted = sent.len() as u64;
        let only_greetings = sent.iter().all(|p| *p == hello);
        assert!(
            only_greetings && (beats..=beats + 1).contains(&greeted),
            "{sent:?}"
        );

        let mark = taps.borrow().len();
        let before = nodes[0].counters().await.rounds_heartbeat;
        nodes[0].hits(3..5).await;
        nodes[2].hits(13..15).await;
        run_for(150).await;
        let beats = nodes[0].counters().await.rounds_heartbeat - before;
        let sent = to(n, mark);
        let new_own = |p: &Payload| {
            matches!(p, Payload::Cells(CellPayload::Own { range, relay_into_zone: false, .. })
                if range.lower() == 3)
        };
        let greets = |p: &&Payload| {
            **p == hello || matches!(p, Payload::Cells(CellPayload::Own { probe: true, .. }))
        };
        assert_eq!(sent.iter().filter(greets).count() as u64, beats, "{sent:?}");
        let only_new = sent.iter().all(|p| *p == hello || new_own(p));
        assert!(sent.iter().any(new_own) && only_new, "{sent:?}");

        let mark = taps.borrow().len();
        let config = zoned_config(addrs.clone(), "a", |_| {});
        nodes.push(Node::start(&router, n, store_for(id(3, 1)), config));
        run_for(300).await;
        let sent = to(n, mark);
        let from_0 = |own: bool| {
            sent.iter().any(|p| match p {
                Payload::Cells(CellPayload::Own { range, .. }) => own && range.lower() == 0,
                Payload::Cells(CellPayload::Relay { range, .. }) => !own && range.lower() == 0,
                _ => false,
            })
        };
        assert!(from_0(true) && from_0(false), "{sent:?}");
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 10);
        stop_all(nodes).await;
    });
}

/// A peer of another zone that the origin discovers late is first sent the
/// origin's new cells unflagged, above the stream's start; it merges them but
/// opens no gap, since the origin stops once it hears that peer's zone, so a
/// healthy start abandons none.
#[test]
fn late_peer_of_another_zone_opens_no_gap() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let b = ranked(id(0, 1), 49_110, 2);
            let addrs = [sock(49_100), b[0], b[1]];
            let zones = ["a", "b", "b"];
            let o_config = zoned_config(vec![b[0]], "a", |_| {});
            let taps = Taps::default();
            let tap = Tap {
                inner: router.bind(addrs[0]),
                sent: taps.clone(),
            };
            let mut nodes = vec![Node::start_on(tap, addrs[0], store_for(id(0, 1)), o_config)];
            nodes.extend((1..3).map(|i| zoned_node(&router, &addrs, &zones, i)));
            nodes[0].hits(0..3).await;
            run_for(500).await;
            nodes[0].tell(PeerEvent::Added(Peer::new(b[1])));
            run_for(1).await;
            nodes[0].hits(3..5).await;
            run_for(1_000).await;
            let own = own_packets(&tapped(&taps, 0));
            assert!(
                own.contains(&(b[1], 3, false)),
                "b[1] got O's new cells unflagged"
            );
            for node in &nodes {
                assert_eq!(node.counters().await.gaps_abandoned, 0, "{}", node.addr);
            }
            assert_totals(&nodes.iter().collect::<Vec<_>>(), 5);
            stop_all(nodes).await;
        })
    });
    assert_logged(&logs, "gap_abandoned", 0, &[]);
}

// -- scripted peers -----------------------------------------------------------

/// Wake the scripted runtime to read what was just queued.
fn wake(wire: &Wire) {
    if let Some(waker) = wire.armed.take() {
        waker.wake();
    }
}

/// A scripted runtime in zone `name` with `peers` bootstrapped.
async fn scripted(
    wire: &Rc<Wire>,
    peers: Vec<SocketAddr>,
    name: &str,
    tune: impl FnOnce(&mut Settings),
) -> (GossipClient, mpsc::Sender<AdminCommand>) {
    let config = zoned_config(peers, name, tune);
    let (client, admin, _runtime) = mocked(wire, store_for(node(0xA)), config).await;
    (client, admin)
}

/// Make each `(addr, id, zone)` heard, in its zone, with an empty sweep page
/// that also ends this node's join sweep; the next heartbeat reads them.
fn introduce(wire: &Wire, peers: &[(SocketAddr, NodeIdentity, &str)]) {
    for &(addr, identity, name) in peers {
        let page = CellPayload::Sweep.into();
        inject_zoned(
            wire,
            addr,
            identity,
            Some(zone(name)),
            AckInfo::default(),
            page,
        );
    }
}

/// The packets of `sender`'s own cells `1..=upper` above `lower`, closing
/// `(lower, upper]`, as the sender encodes them, flagged or not.
fn cell_packets(sender: NodeIdentity, lower: u64, upper: u64, flagged: bool) -> Vec<Vec<u8>> {
    let mut store = store_for(sender);
    let mut obs = ObservationBatch::with_capacity(1);
    let mut sink = DeltaSink::with_capacity(1);
    for key in 1..=upper {
        obs.clear();
        obs.push(Observation {
            rule_fingerprint: 0xBEE,
            key_hash: KeyHash(u128::from(key)),
            bucket: 0,
            origin: sender.node_id,
            incarnation: sender.incarnation,
            count: 1,
            last_update_millis: 0,
        });
        store.ingest_local(&obs, &mut sink);
        sink.clear();
    }
    let mut handles = Vec::with_capacity(upper as usize);
    store.stream_after(
        crate::crdt::Stream::Own,
        lower,
        upper as usize,
        &mut handles,
    );
    let header = Header {
        cluster_id_hash: 0xC1.try_into().unwrap(),
        sender,
        sender_zone: Some(zone("a")),
        acks: AckInfo::default(),
        payload: CellPayload::Own {
            range: wire::StreamRange::new(lower, upper).unwrap(),
            relay_into_zone: flagged,
            probe: false,
        },
    };
    let mut scratch = wire::WireScratch::for_store(&store);
    let limits = wire::FrameLimits::new(wire::MAX_PAYLOAD_BYTES).unwrap();
    let mut packets = wire::Packets::new(header, &store, &handles, &mut scratch, limits);
    let mut buf = wire::PacketBuf::for_limits(limits);
    let mut out = Vec::new();
    while packets.next_into(&mut buf).is_some() {
        out.push(buf.as_bytes().to_vec());
    }
    out
}

/// Cells in the relay frames sent to `to` from attempt `from` on.
fn relayed_cells(wire: &Wire, to: SocketAddr, from: usize) -> usize {
    let limits = wire::FrameLimits::new(wire::MAX_PAYLOAD_BYTES).unwrap();
    wire.attempts.borrow()[from..]
        .iter()
        .filter(|a| a.dst == to)
        .map(|a| {
            let mut cells = 0;
            let header = wire::decode(&a.bytes, limits, 0xC1.try_into().unwrap(), |_| cells += 1);
            match header.unwrap().payload {
                Payload::Cells(CellPayload::Relay { .. }) => cells,
                _ => 0,
            }
        })
        .sum()
}

/// A zone peer Z and a gateway G that never answer: each heartbeat resends Z
/// the window with `PROBE` until it is suspect, while G, a gateway that has
/// acked nothing, gets only probes; once suspect, one 86-byte probe and no old
/// cell. A new cell still goes to a suspect at once, beside
/// that heartbeat's probe, and a probe answer from this incarnation brings the
/// window back on the next prompt round, without `PROBE`.
#[test]
fn suspect_retries_are_header_only_probes() {
    run_paused(async {
        let (z, g) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, admin) = scripted(&wire, vec![z, g], "a", tune).await;
        introduce(&wire, &[(z, node(0xB), "a"), (g, node(0xC), "b")]);
        heartbeats(1).await;
        for key in 1..=3 {
            hit(&client, key).await;
        }
        let mark = wire.attempts.borrow().len();
        // The frame at 200, then one packet a heartbeat through 1,000: Z is
        // resent the window with `PROBE` until it turns suspect at 600, then
        // probed alone; G, which has acked nothing since it became the
        // gateway, is probed alone from its first retry.
        heartbeats(9).await;
        for (dst, gateway) in [(z, false), (g, true)] {
            let sent: Vec<_> = wire.attempts.borrow()[mark..]
                .iter()
                .filter(|a| a.dst == dst)
                .map(|a| (a.bytes.len(), payload_of(&a.bytes)))
                .collect();
            assert_eq!(sent.len(), 9, "{dst}: {sent:?}");
            let probes = sent
                .iter()
                .position(|(_, p)| *p == Payload::Ack { probe: true })
                .expect("probes");
            assert_eq!(probes, if gateway { 1 } else { 4 }, "{dst}: {sent:?}");
            for (i, (_, payload)) in sent[..probes].iter().enumerate() {
                let resend = Payload::Cells(CellPayload::Own {
                    range: wire::StreamRange::new(0, 3).unwrap(),
                    relay_into_zone: gateway,
                    probe: i > 0,
                });
                assert_eq!(*payload, resend, "{dst}");
            }
            let probe = (86, Payload::Ack { probe: true });
            assert!(sent[probes..].iter().all(|sent| *sent == probe), "{dst}");
        }
        let mark = wire.attempts.borrow().len();
        hit(&client, 4).await;
        heartbeats(1).await;
        let new_cell = Payload::Cells(CellPayload::Own {
            range: wire::StreamRange::new(3, 4).unwrap(),
            relay_into_zone: false,
            probe: false,
        });
        let sent = sent_payloads(&wire, mark);
        assert!(sent.contains(&(z, new_cell)), "{sent:?}");
        // The cell asks for an answer, but only the probe for a prompt one.
        assert!(
            sent.contains(&(z, Payload::Ack { probe: true })),
            "{sent:?}"
        );

        let answer = |incarnation| AckInfo {
            incarnation,
            probe_reply: true,
            ..AckInfo::default()
        };
        let rewinds = admin_snapshot(&admin).await.counters.rto_rewinds_own;
        let mark = wire.attempts.borrow().len();
        inject_zoned(
            &wire,
            z,
            node(0xB),
            Some(zone("a")),
            answer(6),
            Payload::Ack { probe: false },
        );
        wake(&wire);
        sim_advance_ticks(Duration::from_millis(1), 6).await;
        assert!(sent_payloads(&wire, mark).is_empty());
        inject_zoned(
            &wire,
            z,
            node(0xB),
            Some(zone("a")),
            answer(1),
            Payload::Ack { probe: false },
        );
        wake(&wire);
        sim_advance_ticks(Duration::from_millis(1), 6).await;
        let resend = Payload::Cells(CellPayload::Own {
            range: wire::StreamRange::new(0, 4).unwrap(),
            relay_into_zone: false,
            probe: false,
        });
        assert_eq!(sent_payloads(&wire, mark), [(z, resend)]);
        let counters = admin_snapshot(&admin).await.counters;
        assert_eq!(counters.rto_rewinds_own, rewinds + 1);
    });
}

/// A gateway that has acked nothing since it took the role is retried with
/// header-only probes from its first retry, not resent the window, and its
/// first answer brings the window back on the next prompt round.
#[test]
fn new_gateway_is_probed_until_it_answers() {
    run_paused(async {
        let g = sock(3);
        let wire = Rc::new(Wire::default());
        let (client, _admin) = scripted(&wire, vec![g], "a", |_| {}).await;
        introduce(&wire, &[(g, node(0xC), "b")]);
        heartbeats(1).await;
        hit(&client, 1).await;
        let mark = wire.attempts.borrow().len();
        heartbeats(3).await;
        let window = Payload::Cells(CellPayload::Own {
            range: wire::StreamRange::new(0, 1).unwrap(),
            relay_into_zone: true,
            probe: false,
        });
        let probe = Payload::Ack { probe: true };
        assert_eq!(
            sent_payloads(&wire, mark),
            [(g, window), (g, probe), (g, probe)]
        );
        let mark = wire.attempts.borrow().len();
        let answer = AckInfo {
            incarnation: 1,
            own: wire::OwnAck::Flagged(0),
            probe_reply: true,
            ..AckInfo::default()
        };
        let ack = Payload::Ack { probe: false };
        inject_zoned(&wire, g, node(0xC), Some(zone("b")), answer, ack);
        wake(&wire);
        sim_advance_ticks(Duration::from_millis(1), 6).await;
        assert_eq!(sent_payloads(&wire, mark), [(g, window)]);
    });
}

/// A refused send to a suspect holds the row: the heartbeat that owes it a
/// new cell and a probe beside it tries only the cell's packet, unchanged on
/// every retry, and the probe waits for a later round after the release.
#[test]
fn refused_send_to_a_suspect_holds_one_packet() {
    run_paused(async {
        let z = sock(2);
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, _admin) = scripted(&wire, vec![z], "a", tune).await;
        introduce(&wire, &[(z, node(0xB), "a")]);
        heartbeats(1).await;
        hit(&client, 1).await;
        heartbeats(9).await;
        let probe = Payload::Ack { probe: true };
        assert_eq!(sent_payloads(&wire, 0).last(), Some(&(z, probe)));

        hit(&client, 2).await;
        wire.refuse.set(true);
        let mark = wire.attempts.borrow().len();
        heartbeats(1).await;
        let held = wire.attempts.borrow()[mark].clone();
        let new_cell = Payload::Cells(CellPayload::Own {
            range: wire::StreamRange::new(1, 2).unwrap(),
            relay_into_zone: false,
            probe: false,
        });
        assert_eq!((held.dst, payload_of(&held.bytes)), (z, new_cell));
        assert!(wire.attempts.borrow()[mark..].iter().all(|a| *a == held));

        wire.refuse.set(false);
        let mark = wire.attempts.borrow().len();
        heartbeats(1).await;
        assert_eq!(wire.attempts.borrow()[mark].bytes, held.bytes);
        let sent = sent_payloads(&wire, mark + 1);
        assert!(sent.contains(&(z, probe)), "{sent:?}");
    });
}

/// A greeting starts no wait, sent at once or held by a refusal and sent
/// later, so a discovered peer that never answers is never awaited.
#[test]
fn greeting_starts_no_wait() {
    run_paused(async {
        let u = sock(4);
        let wire = Rc::new(Wire::default());
        wire.refuse.set(true);
        let (_client, admin) = scripted(&wire, vec![u], "a", |_| {}).await;
        heartbeats(1).await;
        assert!(!wire.attempts.borrow().is_empty(), "the greeting was tried");
        wire.refuse.set(false);
        heartbeats(3).await;
        let peer = *admin_snapshot(&admin).await.peers.first().unwrap();
        assert_eq!(
            (peer.role, peer.answer),
            (Role::Unheard, AnswerState::Settled)
        );
    });
}

/// A sweep page held by a refusal and sent later starts no wait, empty or
/// not, just as one sent at once does: no requester answers a page.
#[test]
fn held_sweep_page_starts_no_wait() {
    run_paused(async {
        let (g1, g2) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let (client, admin) = scripted(&wire, vec![g1, g2], "a", |_| {}).await;
        introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
        heartbeats(1).await;
        let peers = admin_snapshot(&admin).await.peers;
        let remote = peers.iter().find(|p| p.role == Role::Remote).unwrap().addr;
        let (gateway, gateway_id, remote_id) = if remote == g1 {
            (g2, node(0xC), node(0xB))
        } else {
            (g1, node(0xB), node(0xC))
        };
        for cells in [0, 1] {
            if cells > 0 {
                hit(&client, 1).await;
                heartbeats(1).await;
                let acked = AckInfo {
                    incarnation: 1,
                    own: wire::OwnAck::Flagged(1),
                    ..AckInfo::default()
                };
                let ack = Payload::Ack { probe: false };
                inject_zoned(&wire, gateway, gateway_id, Some(zone("b")), acked, ack);
                heartbeats(1).await;
            }
            let request = Payload::SweepRequest;
            inject_zoned(
                &wire,
                remote,
                remote_id,
                Some(zone("b")),
                AckInfo::default(),
                request,
            );
            wire.refuse.set(true);
            let mark = wire.attempts.borrow().len();
            wake(&wire);
            sim_advance_ticks(Duration::from_millis(1), 50).await;
            let held = sent_payloads(&wire, mark);
            let page = Payload::Cells(CellPayload::Sweep);
            assert_eq!(held.first(), Some(&(remote, page)), "{cells}: {held:?}");
            wire.refuse.set(false);
            heartbeats(3).await;
            let peers = admin_snapshot(&admin).await.peers;
            let peer = peers.iter().find(|p| p.addr == remote).unwrap();
            assert_eq!(peer.answer, AnswerState::Settled, "{cells}");
        }
    });
}

/// While the kernel refuses sends no peer can answer, so the gateway is not
/// judged during the hold, and the wait it shows after the release counts
/// only the time before the hold.
#[test]
fn held_time_does_not_count_toward_suspicion() {
    run_paused(async {
        let g = sock(3);
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, admin) = scripted(&wire, vec![g], "a", tune).await;
        introduce(&wire, &[(g, node(0xC), "b")]);
        heartbeats(1).await;
        hit(&client, 1).await;
        // Owed since the flagged frame at 200; the retry at 400 is held.
        heartbeats(1).await;
        sim_advance_ticks(Duration::from_millis(1), 150).await;
        wire.refuse.set(true);
        let gateway = async |admin| *admin_snapshot(admin).await.peers.first().unwrap();
        // The hold starts at the next retry, at most a heartbeat away.
        let before_hold = gateway(&admin).await.unanswered_ms.unwrap() + 100;
        for _ in 0..10 {
            heartbeats(1).await;
            let peer = gateway(&admin).await;
            assert_eq!(peer.answer, AnswerState::Owed);
        }
        assert!(gateway(&admin).await.unanswered_ms > Some(400));
        wire.refuse.set(false);
        sim_advance_ticks(Duration::from_millis(1), 6).await;
        let after = gateway(&admin).await.unanswered_ms.unwrap();
        assert!((1..=before_hold + 10).contains(&after), "{after}");
        sim_advance_ticks(Duration::from_millis(1), 400 - after - 2).await;
        assert_eq!(gateway(&admin).await.answer, AnswerState::Owed);
        heartbeats(2).await;
        assert_eq!(gateway(&admin).await.answer, AnswerState::Suspect);
        assert_eq!(admin_snapshot(&admin).await.counters.gateway_failovers, 0);
    });
}

/// A probe the kernel refused again and again still asks for an answer once
/// it leaves: a gateway that acked everything during the hold is awaited
/// again from the release.
#[test]
fn held_probe_starts_its_wait_when_it_leaves() {
    run_paused(async {
        let g = sock(3);
        let wire = Rc::new(Wire::default());
        let (client, admin) = scripted(&wire, vec![g], "a", |_| {}).await;
        introduce(&wire, &[(g, node(0xC), "b")]);
        heartbeats(1).await;
        hit(&client, 1).await;
        heartbeats(1).await;
        wire.refuse.set(true);
        let mark = wire.attempts.borrow().len();
        heartbeats(4).await;
        let probe = (g, Payload::Ack { probe: true });
        let tries = sent_payloads(&wire, mark);
        assert!(
            tries.len() > 1 && tries.iter().all(|t| *t == probe),
            "{tries:?}"
        );
        let acks = AckInfo {
            incarnation: 1,
            own: wire::OwnAck::Flagged(1),
            ..AckInfo::default()
        };
        let ack = Payload::Ack { probe: false };
        inject_zoned(&wire, g, node(0xC), Some(zone("b")), acks, ack);
        wake(&wire);
        sim_advance_ticks(Duration::from_millis(1), 6).await;
        let gateway = async |admin| *admin_snapshot(admin).await.peers.first().unwrap();
        assert_eq!(gateway(&admin).await.answer, AnswerState::Settled);
        wire.refuse.set(false);
        heartbeats(2).await;
        assert_eq!(gateway(&admin).await.answer, AnswerState::Owed);
    });
}

/// A suspect that receives no stream, the gateway it replaced after a
/// failover, is settled by a probe answer only for this node's incarnation,
/// and is then the gateway again.
#[test]
fn probe_answer_counts_only_for_this_incarnation() {
    run_paused(async {
        let (g1, g2) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
        hit(&client, 1).await;
        introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
        heartbeats(6).await;
        let peers = admin_snapshot(&admin).await.peers;
        let old = *peers.iter().find(|p| p.role == Role::Remote).unwrap();
        assert_eq!(old.answer, AnswerState::Suspect);
        let id = if old.addr == g1 { node(0xB) } else { node(0xC) };
        for (incarnation, role) in [(6, Role::Remote), (1, Role::Gateway)] {
            let acks = AckInfo {
                incarnation,
                probe_reply: true,
                ..AckInfo::default()
            };
            let ack = Payload::Ack { probe: false };
            inject_zoned(&wire, old.addr, id, Some(zone("b")), acks, ack);
            wake(&wire);
            sim_advance_ticks(Duration::from_millis(1), 6).await;
            let peers = admin_snapshot(&admin).await.peers;
            let now = peers.iter().find(|p| p.addr == old.addr).unwrap();
            assert_eq!(now.role, role, "{incarnation}");
        }
    });
}

/// A packet from an address outside the peer table is merged and nothing
/// more: flagged and fresh, it is still neither relayed nor acked.
#[test]
fn unknown_sender_is_merge_only() {
    run_paused(async {
        let (m, stranger) = (sock(2), sock(9));
        let wire = Rc::new(Wire::default());
        let (_client, admin) = scripted(&wire, vec![m], "b", |_| {}).await;
        introduce(&wire, &[(m, node(0xB), "b")]);
        heartbeats(1).await;
        let mark = wire.attempts.borrow().len();
        for packet in cell_packets(node(0xD), 0, 3, true) {
            wire.inbound.borrow_mut().push_back((packet, stranger));
        }
        heartbeats(3).await;
        assert_eq!(admin_snapshot(&admin).await.counters.cells_merged, 3);
        assert_eq!(relayed_cells(&wire, m, mark), 0);
        assert!(
            wire.attempts.borrow()[mark..]
                .iter()
                .all(|a| a.dst != stranger)
        );
    });
}

/// `CANNOT_RELAY` from a peer of this node's zone (or from any peer of a node
/// with no zone) changes nothing: the peer is never suspect for it, and an
/// unacked burst to it is retried with data, not a header-only probe.
#[test]
fn cannot_relay_from_a_zone_row_is_ignored() {
    for local in ["", "a"] {
        run_paused(async {
            let s = sock(2);
            let wire = Rc::new(Wire::default());
            let (client, admin) = scripted(&wire, vec![s], local, |_| {}).await;
            let flagged = AckInfo {
                cannot_relay: true,
                ..AckInfo::default()
            };
            let page = CellPayload::Sweep.into();
            inject_zoned(&wire, s, node(0xB), Some(zone("a")), flagged, page);
            heartbeats(1).await;
            hit(&client, 1).await;
            let mark = wire.attempts.borrow().len();
            for _ in 0..3 {
                inject_zoned(
                    &wire,
                    s,
                    node(0xB),
                    Some(zone("a")),
                    flagged,
                    Payload::Ack { probe: false },
                );
                heartbeats(1).await;
            }
            let peer = admin_snapshot(&admin).await.peers[0];
            assert_eq!((peer.role, peer.answer), (Role::Zone, AnswerState::Owed));
            let retried = sent_payloads(&wire, mark).into_iter().any(|(_, payload)| {
                matches!(
                    payload,
                    Payload::Cells(CellPayload::Own { probe: true, .. })
                )
            });
            assert!(retried, "{local}");
            let probes = sent_payloads(&wire, mark).contains(&(s, Payload::Ack { probe: true }));
            assert!(!probes, "{local}");
        });
    }
}

/// A gateway owed one cell, `suspect_after` four heartbeats: it answers each
/// probe within a millisecond and data at its own heartbeat, 50 ms after this
/// node's, acking that cell, and the first `withheld` packets to it that ask
/// for an answer, the frame and its retries, are lost.
/// Each heartbeat of this node, 10 ms past each 100 (`mocked` starts it 10 ms
/// in), runs `late(at)` ms after the instant `at` it was scheduled for.
/// Returns the failovers, and how long after the first lost packet the first
/// one came.
async fn lost_attempts(withheld: usize, late: impl Fn(u64) -> u64) -> (u64, Option<u64>) {
    let (g1, g2) = (sock(2), sock(3));
    let wire = Rc::new(Wire::default());
    let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
    let (client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
    hit(&client, 1).await;
    introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
    // Which member ranks first is known once they are heard; the frame goes
    // at the first heartbeat after.
    let (mut seen, mut lost) = (0, 0);
    let mut gateway = None;
    let mut due: Vec<(u64, SocketAddr, NodeIdentity)> = Vec::new();
    let (mut first_lost, mut failed_over_at) = (None, None);
    let mut resume_at = None;
    for t in 10..1_500_u64 {
        if resume_at == Some(t) {
            resume_at = None;
            wire.stall(false);
            tokio::task::yield_now().await;
        }
        // A stalled runtime answers no snapshot.
        let running = resume_at.is_none();
        if gateway.is_none() && running {
            let peers = admin_snapshot(&admin).await.peers;
            gateway = peers
                .iter()
                .find(|p| p.role == Role::Gateway)
                .map(|p| p.addr);
        }
        let attempts = wire.attempts.borrow()[seen..].to_vec();
        seen += attempts.len();
        for a in attempts {
            let header = header_of(&a.bytes);
            let probe = header.payload == Payload::Ack { probe: true };
            let peer = if a.dst == g1 { node(0xB) } else { node(0xC) };
            if Some(a.dst) == gateway && header.payload.asks_answer() && lost < withheld {
                first_lost.get_or_insert(t);
                lost += 1;
                continue;
            }
            let at = if probe {
                t + 1
            } else {
                t + (160 - t % 100) % 100
            };
            due.push((at, a.dst, peer));
        }
        for &(_, dst, peer) in due.iter().filter(|(at, ..)| *at == t) {
            let acks = AckInfo {
                incarnation: 1,
                own: wire::OwnAck::Flagged(1),
                probe_reply: true,
                ..AckInfo::default()
            };
            inject_zoned(
                &wire,
                dst,
                peer,
                Some(zone("b")),
                acks,
                Payload::Ack { probe: false },
            );
        }
        due.retain(|(at, ..)| *at > t);
        wake(&wire);
        let heartbeat = t + 1;
        if heartbeat % 100 == 10 && late(heartbeat) > 0 {
            tokio::task::yield_now().await;
            wire.stall(true);
            resume_at = Some(heartbeat + late(heartbeat));
        }
        sim_advance(Duration::from_millis(1)).await;
        if failed_over_at.is_none() && resume_at.is_none() {
            let counters = admin_snapshot(&admin).await.counters;
            if counters.gateway_failovers > 0 {
                failed_over_at = Some(t + 1);
            }
        }
    }
    wire.stall(false);
    let counters = admin_snapshot(&admin).await.counters;
    let after = failed_over_at.zip(first_lost).map(|(at, since)| at - since);
    (counters.gateway_failovers, after)
}

/// Three lost attempts in a row never fail the gateway over, and a fourth
/// fails it over exactly once, at the heartbeat `suspect_after` after the
/// first: seen within the millisecond it left, judged exactly then.
#[test]
fn three_lost_attempts_never_suspect() {
    for (withheld, failovers) in [(3, 0), (4, 1)] {
        let (failed_over, after) = run_paused(lost_attempts(withheld, |_| 0));
        assert_eq!(failed_over, failovers, "{withheld}");
        if failovers == 1 {
            assert!(after.is_some_and(|after| (399..=400).contains(&after)));
        }
    }
}

/// As [`three_lost_attempts_never_suspect`], with every heartbeat of this
/// node 0 to 4 ms late, within `F_r`: each is still stamped with its
/// scheduled instant, so no retry slips to the next heartbeat and the counts
/// hold.
#[test]
fn late_heartbeats_keep_one_retry_per_heartbeat() {
    let late = |at: u64| [3, 0, 4, 1, 2][(at / 100 % 5) as usize];
    for (withheld, failovers) in [(3, 0), (4, 1)] {
        let (failed_over, _) = run_paused(lost_attempts(withheld, late));
        assert_eq!(failed_over, failovers, "{withheld}");
    }
}

/// A heartbeat that runs more than `F_r` late is stamped `F_r` before the
/// clock, not at its schedule, so the wait for the frame it then sends its
/// gateway starts then, not 300 ms in the past.
#[test]
fn late_heartbeat_is_stamped_near_the_clock() {
    run_paused(async {
        let g = sock(3);
        let wire = Rc::new(Wire::default());
        let (client, admin) = scripted(&wire, vec![g], "a", |_| {}).await;
        hit(&client, 1).await;
        introduce(&wire, &[(g, node(0xC), "b")]);
        wire.stall(true);
        sim_advance_ticks(Duration::from_millis(1), 400).await;
        wire.stall(false);
        sim_advance_ticks(Duration::from_millis(1), 1).await;
        let peer = admin_snapshot(&admin).await.peers[0];
        assert_eq!((peer.role, peer.answer), (Role::Gateway, AnswerState::Owed));
        assert!(peer.unanswered_ms <= Some(5 + 1), "{peer:?}");
    });
}

/// A node whose runtime is not polled for three `suspect_after` while its
/// peers answer reads those answers before it judges anyone: on resume no
/// peer is suspect, no gateway failed over, and the relay to its zone peer is
/// not reported stalled.
#[test]
fn local_stall_suspects_nobody() {
    run_paused(async {
        let (z, o, p) = (sock(2), sock(3), sock(4));
        let origin = NodeIdentity::new(NodeId(0xD), 1);
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, admin) = scripted(&wire, vec![z, o, p], "a", tune).await;
        hit(&client, 1).await;
        introduce(
            &wire,
            &[(z, node(0xB), "a"), (o, origin, "b"), (p, node(0xC), "b")],
        );
        // At 100 this node's one cell goes to Z and, flagged, to its zone-b
        // gateway; O's flagged cells then go on to Z: all owed an answer when
        // the runtime stalls.
        heartbeats(1).await;
        for packet in cell_packets(origin, 0, 3, true) {
            wire.inbound.borrow_mut().push_back((packet, o));
        }
        wake(&wire);
        sim_advance_ticks(Duration::from_millis(1), 10).await;
        assert_eq!(relayed_cells(&wire, z, 0), 3);
        wire.stall(true);
        let answer = |own, relay| AckInfo {
            incarnation: 1,
            own,
            relay,
            probe_reply: true,
            ..AckInfo::default()
        };
        let ack = Payload::Ack { probe: false };
        let (plain, flagged) = (wire::OwnAck::Plain(1), wire::OwnAck::Flagged(1));
        inject_zoned(&wire, z, node(0xB), Some(zone("a")), answer(plain, 3), ack);
        inject_zoned(&wire, o, origin, Some(zone("b")), answer(flagged, 0), ack);
        inject_zoned(
            &wire,
            p,
            node(0xC),
            Some(zone("b")),
            answer(flagged, 0),
            ack,
        );
        heartbeats(12).await;
        wire.stall(false);
        sim_advance_ticks(Duration::from_millis(1), 10).await;
        let snapshot = admin_snapshot(&admin).await;
        let peers = &snapshot.peers;
        assert!(
            peers.iter().all(|peer| peer.answer != AnswerState::Suspect),
            "{peers:?}"
        );
        let counters = snapshot.counters;
        assert_eq!((counters.gateway_failovers, counters.relay_stalls), (0, 0));
    });
}

/// A dead gateway behind a socket that refuses every send for twice
/// `suspect_after` is not judged during the hold, and once sends leave again
/// it fails over no sooner than `suspect_after − T`, since the hold does not
/// count, and within `suspect_after + T + 2 · F_r`.
#[test]
fn dead_gateway_fails_over_soon_after_backpressure_ends() {
    run_paused(async {
        let (g1, g2) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let (client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
        hit(&client, 1).await;
        introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
        // The gateway's frame at 100 is never answered.
        heartbeats(1).await;
        wire.refuse.set(true);
        heartbeats(8).await;
        let snapshot = admin_snapshot(&admin).await;
        assert_eq!(snapshot.counters.gateway_failovers, 0);
        let peers = &snapshot.peers;
        assert!(
            peers.iter().all(|peer| peer.answer != AnswerState::Suspect),
            "{peers:?}"
        );
        wire.refuse.set(false);
        let mut after = 0;
        while admin_snapshot(&admin).await.counters.gateway_failovers == 0 {
            assert!(
                after < 400 + 100 + 2 * 5,
                "no failover {after} ms after the release"
            );
            sim_advance(Duration::from_millis(1)).await;
            after += 1;
        }
        assert!(after >= 400 - 100, "failover {after} ms after the release");
    });
}

/// A join sweep's request and its pages to a peer of another zone count as
/// cross-zone bytes, like every other packet to it.
#[test]
fn sweeps_to_another_zone_count_as_cross_zone_bytes() {
    run_paused(async {
        let s = sock(2);
        let wire = Rc::new(Wire::default());
        let (client, admin) = scripted(&wire, vec![s], "a", |_| {}).await;
        hit(&client, 1).await;
        // S is heard, in zone b, and asks for a sweep; alone in its zone,
        // this node asks S for one too.
        for payload in [Payload::Ack { probe: false }, Payload::SweepRequest] {
            inject_zoned(
                &wire,
                s,
                node(0xB),
                Some(zone("b")),
                AckInfo::default(),
                payload,
            );
        }
        heartbeats(1).await;
        sim_advance_ticks(Duration::from_millis(1), 10).await;
        let sent = sent_payloads(&wire, 0);
        assert!(sent.contains(&(s, Payload::SweepRequest)), "{sent:?}");
        assert!(sent.contains(&(s, CellPayload::Sweep.into())), "{sent:?}");
        let bytes: u64 = wire.accepted().iter().map(|a| a.bytes.len() as u64).sum();
        let counters = admin_snapshot(&admin).await.counters;
        assert_eq!(counters.bytes_sent_cross_zone, bytes);
    });
}

/// A gateway whose `suspect_after` ends between heartbeats, when no retry is
/// due, is judged suspect by the prompt round at that instant and replaced
/// then: the row's deadline holds the instant, so the crossing is not lost.
#[test]
fn suspicion_between_heartbeats_is_judged_at_its_instant() {
    run_paused(async {
        let (g1, g2) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(446));
        let (client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
        hit(&client, 1).await;
        introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
        // The gateway's frame at 100 goes unanswered: suspect at 546, with
        // its next retry due at 600.
        heartbeats(1).await;
        let peers = admin_snapshot(&admin).await.peers;
        let other = peers.iter().find(|p| p.role == Role::Remote).unwrap().addr;
        let id = if other == g1 { node(0xB) } else { node(0xC) };
        let mut failed_over_at = None;
        for t in 101..=600 {
            // The other member's probes from 500 on run a prompt round every
            // `F_r` from 501, one of them at 546.
            if t > 500 {
                let probe = Payload::Ack { probe: true };
                inject_zoned(&wire, other, id, Some(zone("b")), AckInfo::default(), probe);
                wake(&wire);
            }
            sim_advance(Duration::from_millis(1)).await;
            let counters = admin_snapshot(&admin).await.counters;
            if failed_over_at.is_none() && counters.gateway_failovers > 0 {
                failed_over_at = Some(t);
            }
        }
        assert_eq!(failed_over_at, Some(546));
    });
}

/// A gateway every send to which fails is replaced as if its packets were
/// lost, and the next member is sent the stream from 0, flagged.
#[test]
fn gateway_behind_send_errors_fails_over() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let (g1, g2) = (sock(2), sock(3));
            let wire = Rc::new(Wire::default());
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let (client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
            introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
            heartbeats(1).await;
            let peers = admin_snapshot(&admin).await.peers;
            let gateway = peers.iter().find(|p| p.role == Role::Gateway).unwrap();
            // Nothing awaits it yet: only the failed sends below are owed.
            assert_eq!(gateway.answer, AnswerState::Settled);
            let gateway = gateway.addr;
            let next = if gateway == g1 { g2 } else { g1 };
            wire.forbidden.set(Some(gateway));
            hit(&client, 1).await;
            let mark = wire.attempts.borrow().len();
            // The first failed send at 200; replaced by 200 + 400 + 100 + 10.
            heartbeats(6).await;
            sim_advance_ticks(Duration::from_millis(1), 10).await;
            let counters = admin_snapshot(&admin).await.counters;
            assert_eq!(counters.gateway_failovers, 1);
            let first = sent_payloads(&wire, mark)
                .into_iter()
                .find(|&(dst, ref payload)| dst == next && matches!(payload, Payload::Cells(_)));
            let flagged = Payload::Cells(CellPayload::Own {
                range: wire::StreamRange::new(0, 1).unwrap(),
                relay_into_zone: true,
                probe: false,
            });
            assert_eq!(first, Some((next, flagged)));
        })
    });
    assert_logged(&logs, "send_error", 3, &["send_errors_total="]);
    assert_logged(
        &logs,
        "gateway_failover",
        1,
        &[
            "reason=\"unanswered\"",
            "failover_total=1",
            "suspect_after_ms=400",
        ],
    );
}

/// A new gateway that held an origin's cells directly relays every one when
/// the origin's rewound flagged packets arrive: a lost first packet opens a
/// flagged gap, reported with `ACK_FLAGGED`, and a reordered one is still
/// fresh, so each cell is relayed exactly once in total.
#[test]
fn handoff_with_first_rewound_packet_lost_ends_exact() {
    for lost in [true, false] {
        run_paused(async {
            let (o, m) = (sock(2), sock(3));
            let origin = NodeIdentity::new(NodeId(0xD), 1);
            let wire = Rc::new(Wire::default());
            let (_client, admin) = scripted(&wire, vec![o, m], "b", |s| {
                s.max_payload_bytes = 1400;
            })
            .await;
            introduce(&wire, &[(o, origin, "a"), (m, node(0xB), "b")]);
            heartbeats(1).await;
            let inject_all = |packets: &[Vec<u8>]| {
                for packet in packets {
                    wire.inbound.borrow_mut().push_back((packet.clone(), o));
                }
            };
            let plain: Vec<_> = (0..4)
                .flat_map(|i| cell_packets(origin, 25 * i, 25 * (i + 1), false))
                .collect();
            inject_all(&plain);
            heartbeats(1).await;
            let mark = wire.attempts.borrow().len();
            let flagged: Vec<_> = (0..4)
                .flat_map(|i| cell_packets(origin, 25 * i, 25 * (i + 1), true))
                .collect();
            if lost {
                inject_all(&flagged[1..]);
                wake(&wire);
                sim_advance_ticks(Duration::from_millis(1), 7).await;
                let report = wire.attempts.borrow()[mark..]
                    .iter()
                    .map(|a| header_of(&a.bytes))
                    .find(|h| h.acks.gap_own)
                    .expect("a flagged gap report");
                assert_eq!(report.acks.own, wire::OwnAck::Flagged(0));
                inject_all(&flagged[..1]);
            } else {
                inject_all(&[flagged[1].clone(), flagged[0].clone()]);
                inject_all(&flagged[2..]);
            }
            // Within a heartbeat of the first relay, before any retry.
            wake(&wire);
            sim_advance_ticks(Duration::from_millis(1), 20).await;
            assert_eq!(relayed_cells(&wire, m, mark), 100);
            assert_eq!(admin_snapshot(&admin).await.counters.gaps_abandoned, 0);
        });
    }
}

/// A node alone in its zone asks one peer of any zone for a join sweep, the
/// highest-weight one, and runs only heartbeats, both while it waits for that
/// page and once it arrives. The request starts no wait, even held by a
/// refusal and sent later, so a peer slow to page is never awaited.
#[test]
fn node_alone_in_its_zone_asks_one_peer_of_any_zone() {
    run_paused(async {
        let (a, b) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let (_client, admin) = scripted(&wire, vec![a, b], "c", |_| {}).await;
        let ack = Payload::Ack { probe: false };
        for (addr, id) in [(a, node(0xB)), (b, node(0xC))] {
            inject_zoned(&wire, addr, id, Some(zone("a")), AckInfo::default(), ack);
        }
        wire.refuse.set(true);
        heartbeats(1).await;
        wire.refuse.set(false);
        let requests = |from| {
            sent_payloads(&wire, from)
                .into_iter()
                .filter(|(_, payload)| *payload == Payload::SweepRequest)
                .map(|(dst, _)| dst)
                .collect::<Vec<_>>()
        };
        let key = hash_fingerprint(node(0xA).node_id.0);
        let top = [a, b]
            .into_iter()
            .max_by_key(|&addr| rendezvous_weight(key, addr))
            .unwrap();
        // The one request, held by the refusal and tried again each round.
        let held = requests(0);
        assert!(
            held.len() > 1 && held.iter().all(|&dst| dst == top),
            "{held:?}"
        );
        let before = admin_snapshot(&admin).await.counters;
        heartbeats(3).await;
        let peers = admin_snapshot(&admin).await.peers;
        assert!(
            peers.iter().all(|p| p.answer == AnswerState::Settled),
            "{peers:?}"
        );
        let id = if top == a { node(0xB) } else { node(0xC) };
        let page = CellPayload::Sweep.into();
        inject_zoned(&wire, top, id, Some(zone("a")), AckInfo::default(), page);
        heartbeats(1).await;
        let mark = wire.attempts.borrow().len();
        heartbeats(15).await;
        let after = admin_snapshot(&admin).await.counters;
        assert_eq!(after.rounds_heartbeat - before.rounds_heartbeat, 19);
        assert_eq!(after.rounds_prompt, before.rounds_prompt);
        assert!(requests(mark).is_empty());
    });
}

/// A node that has heard only a peer of another zone does not ask it for a
/// join sweep while a discovered peer is unheard, since that one may be of its
/// own zone; once that peer is heard in its zone, it asks it.
#[test]
fn join_sweep_waits_for_every_peer_before_asking_across_zones() {
    run_paused(async {
        let (r, u) = (sock(2), sock(3));
        let wire = Rc::new(Wire::default());
        let (_client, _admin) = scripted(&wire, vec![r, u], "c", |_| {}).await;
        let requests = |from| {
            let sent = sent_payloads(&wire, from).into_iter();
            sent.filter(|(_, payload)| *payload == Payload::SweepRequest)
                .map(|(dst, _)| dst)
                .collect::<Vec<_>>()
        };
        let ack = Payload::Ack { probe: false };
        inject_zoned(
            &wire,
            r,
            node(0xB),
            Some(zone("a")),
            AckInfo::default(),
            ack,
        );
        heartbeats(3).await;
        assert!(requests(0).is_empty(), "{:?}", requests(0));
        let mark = wire.attempts.borrow().len();
        inject_zoned(
            &wire,
            u,
            node(0xC),
            Some(zone("c")),
            AckInfo::default(),
            ack,
        );
        heartbeats(1).await;
        assert_eq!(requests(mark), [u]);
    });
}
