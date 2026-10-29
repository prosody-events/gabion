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
/// to one gateway in each other zone, which relays it. This is also SIM11.
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

/// A peer whose zone this node has not heard is sent everything directly.
#[test]
fn peer_with_unknown_zone_gets_direct_mail() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_100, 2);
        router.set_link_policy(addrs[1], addrs[0], LinkPolicy::Block);
        let zones = ["a", "b"];
        let (origin, taps) = tapped_node(&router, &addrs, &zones, 0, |_| {});
        let peer = zoned_node(&router, &addrs, &zones, 1);
        run_for(300).await;
        let mark = taps.borrow().len();
        hit(&origin.client, 1).await;
        run_for(200).await;
        assert_eq!(origin.peer(addrs[1]).await.zone, PeerZone::Unknown);
        let own = own_packets(&tapped(&taps, mark));
        assert!(!own.is_empty());
        assert!(
            own.iter().all(|&packet| packet == (addrs[1], 0, false)),
            "{own:?}"
        );
        assert_eq!(peer.agg.total(), 1);
        stop_all(vec![origin, peer]).await;
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
/// to that member is resent every heartbeat until the member is suspect,
/// then probed with one header-only packet per heartbeat, logged once as
/// stalled, and the member catches up as soon as the path heals.
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
            assert!(
                to_m[..probes].iter().all(|header| matches!(
                    header.payload,
                    Payload::Cells(CellPayload::Relay { .. })
                ))
            );
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
/// every hit.
#[test]
fn zone_partition_heals_to_exact_counts() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(47_500, 6);
        let zones = ["a", "a", "a", "b", "b", "b"];
        let nodes: Vec<_> = (0..6)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        run_for(1_000).await;
        for a in 0..3 {
            for b in 3..6 {
                cut(&router, addrs[a], addrs[b], true);
            }
        }
        for (i, node) in nodes.iter().enumerate() {
            node.hits(0..i as u128 + 1).await;
            run_for(500).await;
        }
        run_for(1_000).await;
        for a in 0..3 {
            for b in 3..6 {
                cut(&router, addrs[a], addrs[b], false);
            }
        }
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
/// stream flagged once its zone is heard, although it already holds the
/// cells directly, and relays them to members that never got them.
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
        let config = zoned_config(others(&all, joiner), "b", |_| {});
        nodes.push(Node::start(&router, joiner, store_for(id(3, 1)), config));
        for node in &nodes[..3] {
            node.tell(PeerEvent::Added(Peer::new(joiner)));
        }
        run_for(300).await;
        assert_eq!(gateway_of(&nodes[0], "b").await, joiner);
        run_for(700).await;
        for node in &nodes[1..] {
            assert_eq!(node.agg.total(), 10, "{}", node.addr);
        }
        stop_all(nodes).await;
    });
}

/// A gateway suspected only because it had not yet discovered the origin
/// answers a probe once it does and is the gateway again at once, sent the
/// origin's stream from 0 on that same round; when it dies, the other member
/// takes over again.
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

        let mark = taps.borrow().len();
        g1.tell(PeerEvent::Added(Peer::new(addrs[0])));
        let mut restored = None;
        for t in 0..2 * 100 {
            run_for(1).await;
            if gateway_of(&origin, "b").await == addrs[1] {
                restored = Some(t);
                break;
            }
        }
        assert!(restored.is_some(), "G1 not restored within 2T");
        assert_eq!(origin.peer(addrs[1]).await.answer, AnswerState::Owed);
        run_for(2 * 5).await;
        let own = own_packets(&tapped(&taps, mark));
        assert!(own.contains(&(addrs[1], 0, true)), "{own:?}");

        cut(&router, addrs[0], addrs[1], true);
        run_for(400 + 100 + 2 * 5 + 100).await;
        assert_eq!(gateway_of(&origin, "b").await, addrs[2]);
        assert_eq!(g2.agg.total(), 5);
        assert_eq!(g1.agg.total(), 5);
        stop_all(vec![origin, g1, g2]).await;
    });
}

/// An origin that went quiet probes its gateway every heartbeat, so the
/// gateway's death still fails it over, and the next member gets its live
/// cells; a standalone ack the origin owes the gateway does not replace the
/// probe.
#[test]
fn idle_origin_replaces_a_dead_gateway() {
    for gateway_busy in [false, true] {
        run_paused(async {
            let router = SimRouter::new();
            let addrs = addrs(47_900, 3);
            let zones = ["a", "b", "b"];
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let nodes: Vec<_> = (0..3)
                .map(|i| zoned_node_with(&router, &addrs, &zones, i, tune))
                .collect();
            run_for(1_000).await;
            let gateway = gateway_of(&nodes[0], "b").await;
            let (g, m) = if gateway == addrs[1] { (1, 2) } else { (2, 1) };
            router.set_link_policy(gateway, addrs[m], LinkPolicy::Block);
            hit(&nodes[0].client, 1).await;
            for t in 0..300 {
                if gateway_busy && t % 100 == 0 {
                    hit_at(&nodes[g], t).await;
                }
                run_for(1).await;
            }
            assert_eq!(nodes[0].peer(gateway).await.own_unacked, 0);
            cut(&router, gateway, addrs[0], true);
            let mut replaced = None;
            for t in 0..400 + 3 * 100 + 2 * 5 + 1 {
                if gateway_busy && t % 100 == 0 {
                    hit_at(&nodes[g], t).await;
                }
                run_for(1).await;
                if replaced.is_none() && gateway_of(&nodes[0], "b").await != gateway {
                    replaced = Some(t);
                }
            }
            let replaced = replaced.expect("a failover");
            assert!(replaced >= 400, "{replaced}");
            assert_eq!(nodes[m].agg.total(), 1);
            stop_all(nodes).await;
        });
    }
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

/// O cannot reach its gateway G, which still reaches O: G's packets repeat
/// the stuck acks, so O fails over once and stays failed over while the cut
/// lasts, and the other member holds O's hits.
#[test]
fn one_way_cut_to_gateway_fails_over_once() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(48_200, 3);
        let zones = ["a", "b", "b"];
        let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
        let nodes: Vec<_> = (0..3)
            .map(|i| zoned_node_with(&router, &addrs, &zones, i, tune))
            .collect();
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let (g, m) = if gateway == addrs[1] { (1, 2) } else { (2, 1) };
        router.set_link_policy(addrs[0], gateway, LinkPolicy::Block);
        nodes[0].hits(0..3).await;
        for t in 0..5 * 400 {
            if t % 100 == 0 {
                hit_at(&nodes[g], t).await;
            }
            run_for(1).await;
        }
        assert_eq!(nodes[0].counters().await.gateway_failovers, 1);
        assert_eq!(gateway_of(&nodes[0], "b").await, addrs[m]);
        // O's three hits and G's, which reach both directly.
        assert_eq!(nodes[m].agg.total(), nodes[0].agg.total());
        stop_all(nodes).await;
    });
}

/// A peer answers each `PROBE` within a relay floor, not at its next
/// heartbeat a whole phase later; one lost answer fails nothing over.
#[test]
fn probe_answers_are_prompt() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = addrs(48_300, 2);
        let zones = ["a", "b"];
        let (origin, o_taps) = tapped_node(&router, &addrs, &zones, 0, |_| {});
        run_for(99).await;
        let (gateway, g_taps) = tapped_node(&router, &addrs, &zones, 1, |_| {});
        run_for(3_000).await;
        router.set_link_policy(addrs[1], addrs[0], LinkPolicy::DropFirst { count: 1 });
        run_for(3_000).await;
        let times = |taps: &Taps, to: SocketAddr, pick: fn(&Header) -> bool| {
            taps.borrow()
                .iter()
                .filter(|(_, dst, bytes)| *dst == to && pick(&header_of(bytes)))
                .map(|(at, ..)| *at)
                .collect::<Vec<_>>()
        };
        let probes = times(&o_taps, addrs[1], |h| {
            h.payload == Payload::Ack { probe: true }
        });
        let answers = times(&g_taps, addrs[0], |h| h.acks.probe_reply);
        assert!(probes.len() >= 50, "{}", probes.len());
        assert_eq!(probes.len(), answers.len());
        for (probe, answer) in probes.iter().zip(&answers) {
            assert!(*answer - *probe <= Duration::from_millis(5 + 1));
        }
        assert_eq!(origin.counters().await.gateway_failovers, 0);
        stop_all(vec![origin, gateway]).await;
    });
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
            let silent = addrs[1];
            cut(&router, addrs[0], silent, true);
            let mark = taps.borrow().len();
            origin.hits(0..3).await;
            run_for(400 + 100).await;
            let before = tapped(&taps, mark);
            let retries = own_packets(&before)
                .into_iter()
                .filter(|&(dst, lower, _)| dst == silent && lower == 0)
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
                assert_eq!(others[0].agg.total(), 3);
            }
            let mut nodes = others;
            nodes.push(origin);
            stop_all(nodes).await;
        });
    }
}

/// Zone a = {O}, zone b = {G, G2} with G outranking at O; G's heartbeats
/// trail O's by 50 ms, so its standalone acks never land on O's judging
/// instants. An answer that arrives before `suspect_after` keeps G; at or
/// after it, G is replaced, and the judgment is `≥` at equality.
#[test]
fn answer_just_inside_suspect_after_keeps_the_gateway() {
    for (suspect_after, failovers) in [(910, 0), (890, 1), (900, 1)] {
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

/// A gateway cut from its zone says so until the cut heals, then stops, and
/// the origin makes it its gateway again.
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
        nodes[0].hits(0..3).await;
        run_for(2_500).await;
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
            assert_eq!(nodes[m].agg.total(), 3);
        }
        stop_all(nodes).await;
    });
}

/// A failover rewinds the origin's whole live stream to the new gateway,
/// which drains on prompt rounds, a quantum each.
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
        nodes[0].hits(0..CELLS).await;
        run_for(1_000).await;
        let gateway = gateway_of(&nodes[0], "b").await;
        let next = if gateway == addrs[1] {
            addrs[2]
        } else {
            addrs[1]
        };
        cut(&router, addrs[0], gateway, true);
        let mut before = None;
        for _ in 0..3_000 {
            run_for(1).await;
            if before.is_none() && gateway_of(&nodes[0], "b").await == next {
                before = Some(nodes[0].counters().await.rounds_prompt);
            }
            if before.is_some() && nodes[0].peer(next).await.own_unacked == 0 {
                break;
            }
        }
        let before = before.expect("a failover");
        let rounds = nodes[0].counters().await.rounds_prompt - before;
        assert!(rounds <= 21, "{rounds} prompt rounds");
        stop_all(nodes).await;
    });
}

/// A node whose zone becomes known under 20 % loss on every link still ends
/// with every hit everywhere.
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
        let zones = ["", "a", "b", "b", "b"];
        let nodes: Vec<_> = (0..5)
            .map(|i| zoned_node(&router, &addrs, &zones, i))
            .collect();
        run_for(500).await;
        nodes[0].hits(0..50).await;
        run_for(300).await;
        nodes[0].tell(PeerEvent::OwnEntry { zone: zone("a") });
        run_for(5_000).await;
        assert_totals(&nodes.iter().collect::<Vec<_>>(), 50);
        stop_all(nodes).await;
    });
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

/// A zone peer Z and a gateway G that never answer: each heartbeat resends
/// the window with `PROBE` until they are suspect, then sends each one 86-byte
/// probe and no old cell. A new cell still goes to a suspect at once, and a
/// probe answer from this incarnation brings the window back on the next
/// prompt round, without `PROBE`.
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
        // The frame at 200, then one packet a heartbeat through 1,000: the
        // window resent with `PROBE` until suspect (G, probed idle at 100,
        // turns suspect a heartbeat before Z), then probes alone.
        heartbeats(9).await;
        for (dst, flagged) in [(z, false), (g, true)] {
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
            assert!(probes >= 3, "{dst}: {sent:?}");
            for (i, (_, payload)) in sent[..probes].iter().enumerate() {
                let resend = Payload::Cells(CellPayload::Own {
                    range: wire::StreamRange::new(0, 3).unwrap(),
                    relay_into_zone: flagged,
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
        assert!(sent_payloads(&wire, mark).contains(&(z, new_cell)));

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
        // Owed since a probe or the flagged frame; the retry at 400 is held.
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

/// An idle gateway made `Owed` by a liveness probe is settled by a probe
/// answer only for this node's incarnation.
#[test]
fn probe_answer_counts_only_for_this_incarnation() {
    run_paused(async {
        let g = sock(3);
        let wire = Rc::new(Wire::default());
        let (_client, admin) = scripted(&wire, vec![g], "a", |_| {}).await;
        introduce(&wire, &[(g, node(0xC), "b")]);
        heartbeats(2).await;
        let peer = admin_snapshot(&admin).await.peers[0];
        assert_eq!((peer.role, peer.answer), (Role::Gateway, AnswerState::Owed));
        for (incarnation, answer) in [(6, AnswerState::Owed), (1, AnswerState::Settled)] {
            let acks = AckInfo {
                incarnation,
                probe_reply: true,
                ..AckInfo::default()
            };
            inject_zoned(
                &wire,
                g,
                node(0xC),
                Some(zone("b")),
                acks,
                Payload::Ack { probe: false },
            );
            wake(&wire);
            sim_advance_ticks(Duration::from_millis(1), 2).await;
            assert_eq!(admin_snapshot(&admin).await.peers[0].answer, answer);
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

/// A gateway that answers each probed retry within a millisecond and anything
/// else at its own heartbeat, 50 ms after this node's: three lost attempts in
/// a row never fail it over, and a fourth fails it over exactly once, at the
/// heartbeat `suspect_after` after the first.
#[test]
fn three_lost_attempts_never_suspect() {
    for (withheld, failovers) in [(3, 0), (4, 1)] {
        run_paused(async {
            let (g1, g2) = (sock(2), sock(3));
            let wire = Rc::new(Wire::default());
            let tune = |s: &mut Settings| s.suspect_after = Some(ms(400));
            let (_client, admin) = scripted(&wire, vec![g1, g2], "a", tune).await;
            introduce(&wire, &[(g1, node(0xB), "b"), (g2, node(0xC), "b")]);
            // Which member ranks first is known once they are heard; until
            // then nothing is probed.
            let (mut seen, mut lost) = (0, 0);
            let mut gateway = None;
            let mut due: Vec<(u64, SocketAddr, NodeIdentity)> = Vec::new();
            let (mut first_lost, mut failed_over_at) = (None, None);
            for t in 10..1_500_u64 {
                if gateway.is_none() {
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
                    if Some(a.dst) == gateway && probe && lost < withheld {
                        first_lost.get_or_insert(t);
                        lost += 1;
                        continue;
                    }
                    let at = if probe {
                        t + 1
                    } else {
                        t + (150 - t % 100) % 100
                    };
                    due.push((at, a.dst, peer));
                }
                for &(_, dst, peer) in due.iter().filter(|(at, ..)| *at == t) {
                    let acks = AckInfo {
                        incarnation: 1,
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
                sim_advance(Duration::from_millis(1)).await;
                let counters = admin_snapshot(&admin).await.counters;
                if failed_over_at.is_none() && counters.gateway_failovers > 0 {
                    failed_over_at = Some(t + 1);
                }
            }
            let counters = admin_snapshot(&admin).await.counters;
            assert_eq!(counters.gateway_failovers, failovers, "{withheld}");
            if failovers == 1 {
                // Seen within the millisecond it left, judged exactly then.
                let since = first_lost.unwrap();
                assert!((since - 1..=since).contains(&(failed_over_at.unwrap() - 400)));
            }
        });
    }
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
            let gateway = peers.iter().find(|p| p.role == Role::Gateway).unwrap().addr;
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
/// highest-weight one, and once that page arrives runs only heartbeats.
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
        heartbeats(1).await;
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
        assert_eq!(requests(0), [top]);
        let id = if top == a { node(0xB) } else { node(0xC) };
        let page = CellPayload::Sweep.into();
        inject_zoned(&wire, top, id, Some(zone("a")), AckInfo::default(), page);
        heartbeats(1).await;
        let (before, mark) = (
            admin_snapshot(&admin).await.counters,
            wire.attempts.borrow().len(),
        );
        heartbeats(15).await;
        let after = admin_snapshot(&admin).await.counters;
        assert_eq!(after.rounds_heartbeat - before.rounds_heartbeat, 15);
        assert_eq!(after.rounds_prompt, before.rounds_prompt);
        assert!(requests(mark).is_empty());
    });
}
