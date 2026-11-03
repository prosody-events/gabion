use super::*;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::{BTreeMap, BTreeSet};

/// The one Service most tests watch.
const T: TargetId = TargetId(0);

impl EndpointSlicePeerSet {
    /// Sorted, deduplicated union of every cached slice's peer set.
    fn snapshot_peers(&self) -> Vec<Peer> {
        let mut peers: Vec<Peer> = (self.targets.values())
            .flat_map(|slices| slices.by_slice.values())
            .flat_map(|peers| peers.values())
            .copied()
            .collect();
        peers.sort();
        peers.dedup();
        peers
    }
}

#[derive(Clone, Debug)]
struct EndpointSliceEventsCase {
    events: Vec<EndpointSliceEvent>,
}

#[derive(Clone, Debug)]
struct EndpointSliceEvent {
    /// Which of two Services the slice belongs to.
    service: u8,
    slice: u8,
    first: u8,
    second: u8,
    ready: bool,
    delete: bool,
    /// A `delete` the watch missed while disconnected, so a relist follows.
    lost: bool,
}

impl Arbitrary for EndpointSliceEventsCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut events = Vec::<EndpointSliceEvent>::arbitrary(g);
        events.truncate(96);
        Self { events }
    }
}

impl Arbitrary for EndpointSliceEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            service: u8::arbitrary(g) % 2,
            slice: u8::arbitrary(g) % 8,
            first: (u8::arbitrary(g) % 8) + 2,
            second: (u8::arbitrary(g) % 8) + 2,
            ready: bool::arbitrary(g),
            delete: bool::arbitrary(g),
            lost: bool::arbitrary(g),
        }
    }
}

/// Build a synthetic EndpointSlice with one endpoint per address, as
/// Kubernetes lists one per pod, and a single `gabion` UDP port. Mirrors what
/// `kube-rs`'s watcher hands us over the wire — every field the production
/// parser inspects is set, every field it ignores is `Default::default()`.
fn slice(name: &str, addresses: &[&str], ready: Option<bool>) -> EndpointSlice {
    let endpoints = (addresses.iter())
        .map(|address| endpoint(address, ready, None, None, None))
        .collect();
    slice_of(name, endpoints)
}

/// An EndpointSlice of `endpoints` with a single `gabion` UDP port.
fn slice_of(name: &str, endpoints: Vec<Endpoint>) -> EndpointSlice {
    EndpointSlice {
        address_type: "IPv4".to_string(),
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(BTreeMap::from([(
                "kubernetes.io/service-name".to_string(),
                "gabion".to_string(),
            )])),
            ..Default::default()
        },
        endpoints,
        ports: Some(vec![EndpointPort {
            name: Some("gabion".to_string()),
            port: Some(18080),
            protocol: Some("UDP".to_string()),
            ..Default::default()
        }]),
    }
}

/// One endpoint at `address`, with its readiness, zone, Pod and node.
fn endpoint(
    address: &str,
    ready: Option<bool>,
    zone: Option<&str>,
    pod: Option<&str>,
    node: Option<&str>,
) -> Endpoint {
    Endpoint {
        addresses: vec![address.to_string()],
        conditions: ready.map(|ready| EndpointConditions {
            ready: Some(ready),
            ..Default::default()
        }),
        zone: zone.map(str::to_owned),
        node_name: node.map(str::to_owned),
        target_ref: pod.map(|pod| ObjectReference {
            kind: Some("Pod".to_string()),
            name: Some(pod.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// This node, matched by `discovery.self_addr`.
fn by_addr() -> Option<SelfMatch> {
    Some(SelfMatch::Addr(
        "10.0.0.1:18080".parse().expect("self addr"),
    ))
}

fn peer(addr: &str) -> Peer {
    Peer::new(format!("{addr}:18080").parse().expect("addr"))
}

#[test]
fn endpoint_slice_parser_deduplicates_and_ignores_self() {
    let peers = peers_from_endpoint_slice(
        &slice("slice-a", &["10.0.0.1", "10.0.0.2", "10.0.0.2"], Some(true)),
        by_addr().as_ref(),
    )
    .0;

    let peers: Vec<Peer> = peers.into_values().collect();
    assert_eq!(
        peers,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_parser_ignores_not_ready_endpoints() {
    let peers = peers_from_endpoint_slice(
        &slice("slice-a", &["10.0.0.2"], Some(false)),
        by_addr().as_ref(),
    )
    .0;

    assert!(peers.is_empty());
}

#[test]
fn endpoint_slice_peer_set_updates_and_deletes_snapshots() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
    let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));

    peer_set.apply(T, &slice_a, by_addr().as_ref());
    peer_set.apply(T, &slice_b, by_addr().as_ref());
    assert_eq!(peer_set.snapshot_peers().len(), 2);

    let removed = peer_set.delete(T, &slice_a);
    assert_eq!(removed, [peer("10.0.0.2")]);
    assert_eq!(peer_set.snapshot_peers(), [peer("10.0.0.3")]);
}

/// A relist forgets each slice it did not list, since the watcher reports no
/// `Delete` for one deleted while it was disconnected: a peer only such a
/// slice listed goes when the relist ends, one a listed slice took over goes
/// once that slice drops it, and a slice the relist listed again is kept.
#[test]
fn endpoint_slice_peer_set_relist_forgets_unlisted_slices() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2", "10.0.0.3"], Some(true));
    peer_set.apply(T, &slice_a, by_addr().as_ref());
    let slice_b = slice("slice-b", &["10.0.0.4", "10.0.0.5"], Some(true));
    peer_set.apply(T, &slice_b, by_addr().as_ref());

    peer_set.relist(T);
    assert_eq!(
        peer_set.apply(T, &slice_a, by_addr().as_ref()),
        PeerSetDiff::default()
    );
    let moved = slice("slice-c", &["10.0.0.4"], Some(true));
    assert_eq!(peer_set.apply(T, &moved, by_addr().as_ref()).removed, []);
    assert_eq!(peer_set.relisted(T), [peer("10.0.0.5")]);

    let emptied = slice("slice-c", &[], Some(true));
    assert_eq!(
        peer_set.apply(T, &emptied, by_addr().as_ref()).removed,
        [peer("10.0.0.4")]
    );
    assert_eq!(
        peer_set.snapshot_peers(),
        [peer("10.0.0.2"), peer("10.0.0.3")]
    );
}

#[test]
fn endpoint_slice_peer_set_apply_emits_added_and_removed_diff() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let initial = slice("slice-a", &["10.0.0.2"], Some(true));
    let updated = slice("slice-a", &["10.0.0.3"], Some(true));

    let first = peer_set.apply(T, &initial, by_addr().as_ref());
    assert_eq!(first.added, [peer("10.0.0.2")]);
    assert!(first.removed.is_empty());

    let second = peer_set.apply(T, &updated, by_addr().as_ref());
    assert_eq!(second.removed, [peer("10.0.0.2")]);
    assert_eq!(second.added, [peer("10.0.0.3")]);
}

/// A diff yields the own entry first, so the peers after it take their roles
/// against its zone, then `Removed` before `Added`.
#[test]
fn diff_events_yield_own_entry_then_removed_then_added() {
    let mut peer_set = EndpointSlicePeerSet::default();
    peer_set.apply(
        T,
        &slice("slice-a", &["10.0.0.2"], Some(true)),
        by_addr().as_ref(),
    );
    let endpoints = vec![
        endpoint("10.0.0.1", Some(true), Some("us-east-1a"), None, None),
        endpoint("10.0.0.3", Some(true), None, None, None),
    ];
    let diff = peer_set.apply(T, &slice_of("slice-a", endpoints), by_addr().as_ref());
    let own = OwnEntry {
        zone: ZoneName::parse("us-east-1a").ok(),
        node_name: None,
    };
    assert_eq!(
        diff.into_events().collect::<Vec<_>>(),
        [
            PeerEvent::OwnEntry(own),
            PeerEvent::Removed(peer("10.0.0.2")),
            PeerEvent::Added(peer("10.0.0.3")),
        ]
    );
}

/// Both the cached peer set and the peers its emitted events add and remove,
/// folded in the order the discovery stream yields them, match the union of
/// the live slices of two Services, so an address two slices or two Services
/// list survives either one going, and a relist of one Service after a missed
/// `Delete` removes what only that slice listed.
#[quickcheck]
fn quickcheck_endpoint_slice_events_match_live_slice_model(
    case: EndpointSliceEventsCase,
) -> TestResult {
    let mut peer_set = EndpointSlicePeerSet::default();
    let mut live: BTreeMap<(u8, String), (EndpointSlice, Vec<Peer>)> = BTreeMap::new();
    let mut emitted = BTreeSet::new();
    let fold = |emitted: &mut BTreeSet<SocketAddr>, diff: PeerSetDiff| {
        for event in diff.into_events() {
            match event {
                PeerEvent::Added(peer) => emitted.insert(peer.addr),
                PeerEvent::Removed(peer) => emitted.remove(&peer.addr),
                PeerEvent::OwnEntry(_) => false,
            };
        }
    };

    for event in case.events {
        let id = TargetId(u64::from(event.service));
        let name = format!("slice-{}", event.slice);
        if event.delete {
            live.remove(&(event.service, name.clone()));
            let removed = if event.lost {
                peer_set.relist(id);
                for ((service, _), (current, _)) in &live {
                    if *service == event.service {
                        fold(
                            &mut emitted,
                            peer_set.apply(id, current, by_addr().as_ref()),
                        );
                    }
                }
                peer_set.relisted(id)
            } else {
                peer_set.delete(id, &slice(&name, &["10.0.0.9"], Some(true)))
            };
            for peer in removed {
                emitted.remove(&peer.addr);
            }
        } else {
            let first = format!("10.0.0.{}", event.first);
            let second = format!("10.0.0.{}", event.second);
            let current = slice(&name, &[&first, &second], Some(event.ready));
            fold(
                &mut emitted,
                peer_set.apply(id, &current, by_addr().as_ref()),
            );

            let mut peers = if event.ready {
                vec![
                    Peer::new(format!("{first}:18080").parse().expect("first peer addr")),
                    Peer::new(format!("{second}:18080").parse().expect("second peer addr")),
                ]
            } else {
                Vec::new()
            };
            peers.sort();
            peers.dedup();
            live.insert((event.service, name), (current, peers));
        }

        let mut expected = (live.values())
            .flat_map(|(_, peers)| peers)
            .copied()
            .collect::<Vec<_>>();
        expected.sort();
        expected.dedup();
        if peer_set.snapshot_peers() != expected {
            return TestResult::error("EndpointSlice peer set diverged from live slice model");
        }
        if !emitted
            .iter()
            .copied()
            .eq(expected.iter().map(|peer| peer.addr))
        {
            return TestResult::error("emitted peer events diverged from the live slice model");
        }
    }
    TestResult::passed()
}

/// U21: the own entry is found by `discovery.self_addr` or by its Pod name,
/// its zone read before the readiness filter, and it is never a peer. It is
/// reported the first time, zoneless too, and on each zone change only; a
/// slice without it retracts nothing.
#[test]
fn own_entry_found_by_addr_or_pod_name() {
    let own = |ready, zone| {
        endpoint(
            "10.0.0.1",
            Some(ready),
            zone,
            Some("gabion-0"),
            Some("node-a"),
        )
    };
    let other = || {
        endpoint(
            "10.0.0.2",
            Some(true),
            Some("us-east-1b"),
            Some("gabion-1"),
            None,
        )
    };
    let zoned = |ready, zone| slice_of("slice-a", vec![own(ready, zone), other()]);
    let peer_b = Peer {
        zone: ZoneHash::parse("us-east-1b").ok(),
        ..peer("10.0.0.2")
    };
    let pod = Some(SelfMatch::PodName("gabion-0".into()));
    for self_match in [by_addr(), pod] {
        let self_match = self_match.as_ref();
        for ready in [false, true] {
            let parsed = zoned(ready, Some("us-east-1a"));
            let (peers, found) = peers_from_endpoint_slice(&parsed, self_match);
            assert_eq!(
                peers.into_values().collect::<Vec<_>>(),
                [peer_b],
                "{self_match:?}"
            );
            assert_eq!(
                found,
                Some(&parsed.endpoints[0]),
                "{self_match:?} ready={ready}"
            );
        }

        let mut peer_set = EndpointSlicePeerSet::default();
        let east = |zone: &str| {
            Some(OwnEntry {
                zone: ZoneName::parse(zone).ok(),
                node_name: Some("node-a".into()),
            })
        };
        let first = peer_set.apply(T, &zoned(false, Some("us-east-1a")), self_match);
        assert_eq!(first.own, east("us-east-1a"), "{self_match:?}");
        assert_eq!(first.added, [peer_b]);
        let again = peer_set.apply(T, &zoned(false, Some("us-east-1a")), self_match);
        assert_eq!(again.own, None, "{self_match:?}");
        let gone = slice_of("slice-a", vec![other()]);
        assert_eq!(peer_set.apply(T, &gone, self_match).own, None);
        let back = peer_set.apply(T, &zoned(true, Some("us-east-1a")), self_match);
        assert_eq!(back.own, None, "{self_match:?}");
        let moved = peer_set.apply(T, &zoned(true, Some("us-east-1c")), self_match);
        assert_eq!(moved.own, east("us-east-1c"), "{self_match:?}");

        let mut peer_set = EndpointSlicePeerSet::default();
        let zoneless = Some(OwnEntry {
            zone: None,
            node_name: Some("node-a".into()),
        });
        let first = peer_set.apply(T, &zoned(true, None), self_match);
        assert_eq!(first.own, zoneless, "{self_match:?}");
        let again = peer_set.apply(T, &zoned(true, None), self_match);
        assert_eq!(again.own, None, "{self_match:?}");
        let labelled = peer_set.apply(T, &zoned(true, Some("us-east-1a")), self_match);
        assert_eq!(labelled.own, east("us-east-1a"), "{self_match:?}");
    }

    let stranger = Some(SelfMatch::PodName("other".into()));
    let ready = zoned(true, None);
    let (peers, found) = peers_from_endpoint_slice(&ready, stranger.as_ref());
    assert_eq!(found, None);
    assert!(peers.contains_key(&peer("10.0.0.1").addr));
}

/// U22: an address moving between slices, or listed by two Services, is
/// removed only when the last lister lets go, whichever lets go first.
#[test]
fn address_moving_between_slices_is_never_removed() {
    let (one, two) = (TargetId(1), TargetId(2));
    let listed = slice("slice-a", &["10.0.0.2"], Some(true));
    let dropped = slice("slice-a", &[], Some(true));
    let mut peer_set = EndpointSlicePeerSet::default();
    peer_set.apply(one, &listed, by_addr().as_ref());
    let moved = slice("slice-b", &["10.0.0.2"], Some(true));
    peer_set.apply(one, &moved, by_addr().as_ref());
    assert_eq!(
        peer_set.apply(one, &dropped, by_addr().as_ref()).removed,
        []
    );
    for (first, other) in [(one, two), (two, one)] {
        let mut peer_set = EndpointSlicePeerSet::default();
        peer_set.apply(one, &listed, by_addr().as_ref());
        peer_set.apply(two, &listed, by_addr().as_ref());
        assert_eq!(
            peer_set.apply(first, &dropped, by_addr().as_ref()).removed,
            []
        );
        assert_eq!(peer_set.forget(other), [peer("10.0.0.2")]);
    }
    let mut peer_set = EndpointSlicePeerSet::default();
    peer_set.apply(one, &listed, by_addr().as_ref());
    peer_set.apply(two, &listed, by_addr().as_ref());
    assert_eq!(peer_set.forget(one), []);
}

/// A static list or a node alone never starts the Kubernetes watcher, so it
/// never logs that it cannot reach the Kubernetes API.
#[test]
fn static_membership_starts_no_watcher() {
    use crate::discovery::Membership;
    use futures::StreamExt;

    let ((), logs) = crate::mock::with_logs(|| {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let addr = "192.0.2.11:9000".parse().expect("addr");
        for membership in [Membership::Static([addr].into()), Membership::Alone] {
            let mut events = std::pin::pin!(membership.peer_events());
            let polled = events.poll_next_unpin(&mut cx);
            assert_eq!(polled, std::task::Poll::Ready(None), "{membership:?}");
        }
    });
    assert!(
        !logs.contains("Could not connect to the Kubernetes API"),
        "{logs}"
    );
}

/// An endpoint's `zone` rides on each of its peers as that zone name's hash;
/// one without a zone, or with an empty one, gives none; and a peer
/// re-reported with a zone it lacked is one `Added` and no `Removed`.
#[test]
fn endpoint_zone_rides_on_its_peers() {
    let zoned = |zone: Option<&str>| {
        let mut slice = slice("slice-a", &["10.0.0.2"], Some(true));
        slice.endpoints[0].zone = zone.map(str::to_owned);
        slice
    };
    let addr = "10.0.0.2:18080".parse().expect("addr");
    let east = Peer {
        addr,
        zone: crate::zone::ZoneHash::parse("us-east-1a").ok(),
    };
    for (zone, peer) in [
        (Some("us-east-1a"), east),
        (None, Peer::new(addr)),
        (Some(""), Peer::new(addr)),
    ] {
        let peers = peers_from_endpoint_slice(&zoned(zone), by_addr().as_ref()).0;
        assert_eq!(peers.into_values().collect::<Vec<_>>(), [peer], "{zone:?}");
    }
    let mut peer_set = EndpointSlicePeerSet::default();
    peer_set.apply(T, &zoned(None), by_addr().as_ref());
    let diff = peer_set.apply(T, &zoned(Some("us-east-1a")), by_addr().as_ref());
    let expected = PeerSetDiff {
        own: None,
        added: vec![east],
        removed: vec![],
    };
    assert_eq!(diff, expected);
}

// -- service_change ---------------------------------------------------------

/// Build a synthetic Service exposing one named UDP port and one TCP
/// port. The `gabion_named` and `udp_protocol` knobs let one helper
/// drive both the happy path and every flavour of misconfiguration the
/// discovery filter rejects.
fn service(namespace: &str, name: &str, gabion_named: bool, udp_protocol: bool) -> Service {
    use k8s_openapi::api::core::v1::{ServicePort, ServiceSpec};

    let port_name = if gabion_named { "gabion" } else { "gossip" };
    let protocol = if udp_protocol { "UDP" } else { "TCP" };
    Service {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some(port_name.to_string()),
                port: 9000,
                protocol: Some(protocol.to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn service_change_returns_track_for_gabion_udp_apply() {
    let svc = service("default", "gabion", true, true);
    let change = service_change(Event::Apply(svc), &[]);
    match change {
        ServiceChange::Track(target) => {
            assert_eq!(target.namespace, "default");
            assert_eq!(target.service_name, "gabion");
        }
        other => panic!("expected Track, got {other:?}"),
    }
}

#[test]
fn service_change_returns_untrack_when_gabion_port_disappears() {
    // Apply event with no gabion-named UDP port — service was renamed
    // or had its protocol switched to TCP. Discovery untracks it so
    // the EndpointSlice watcher can be torn down.
    let svc = service("default", "gabion", false, true);
    let change = service_change(Event::Apply(svc), &[]);
    assert!(
        matches!(change, ServiceChange::Untrack(_)),
        "Apply without gabion-named UDP port should Untrack",
    );

    let svc = service("default", "gabion", true, false);
    let change = service_change(Event::Apply(svc), &[]);
    assert!(
        matches!(change, ServiceChange::Untrack(_)),
        "Apply with gabion name but TCP protocol should Untrack",
    );
}

#[test]
fn service_change_returns_init_done_at_end_of_initial_sync() {
    // The variant the zero-match warning hooks: at end-of-init-sync
    // the caller can detect "no tracks emitted" and warn once.
    let change = service_change(Event::<Service>::InitDone, &[]);
    assert!(
        matches!(change, ServiceChange::InitDone),
        "Event::InitDone must map to ServiceChange::InitDone",
    );
}

#[test]
fn relist_of_one_namespace_does_not_count_for_another() {
    let mut listed = [false; 2];
    assert!(!mark_listed(&mut listed, 0));
    assert!(
        !mark_listed(&mut listed, 0),
        "a relist of namespace 0 listed 1"
    );
    assert!(mark_listed(&mut listed, 1));
}

#[test]
fn service_change_ignores_init_marker() {
    let change = service_change(Event::<Service>::Init, &[]);
    assert!(
        matches!(change, ServiceChange::Ignore),
        "Event::Init is a no-op",
    );
}

#[test]
fn service_change_filters_by_allow_list() {
    let allow = ["gabion".into()];
    let change = |name, allow: &[Box<str>]| {
        service_change(Event::Apply(service("default", name, true, true)), allow)
    };
    assert!(
        matches!(change("other", &allow), ServiceChange::Ignore),
        "Service not in allow list should Ignore",
    );
    assert!(
        matches!(change("gabion", &allow), ServiceChange::Track(_)),
        "Service in the allow list should Track",
    );
    assert!(
        matches!(change("other", &[]), ServiceChange::Track(_)),
        "an empty allow list should Track every Service",
    );
}

// Manual Debug impl so the test panic messages above are useful — the
// production code never displays it, so an inline impl is cheaper than
// deriving Debug across the enum's full surface.
impl core::fmt::Debug for ServiceChange {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ServiceChange::Track(t) => write!(f, "Track({t:?})"),
            ServiceChange::Untrack(t) => write!(f, "Untrack({t:?})"),
            ServiceChange::InitDone => write!(f, "InitDone"),
            ServiceChange::Ignore => write!(f, "Ignore"),
        }
    }
}

// -- live cluster integration ---------------------------------------------

/// End-to-end: author a Service + EndpointSlice pair in a real kind-based
/// kube cluster and assert `EndpointSliceDiscovery`'s watcher emits a
/// `PeerEvent::Added` for one of its endpoints. Gossip over real UDP is
/// covered by `gossip::tests::udp_round_trip_smoke`.
///
/// `#[ignore]` because it needs `kubectl` credentials wired up. Driven
/// by `deploy/kubernetes/local-smoke.sh` after `kind` is provisioned.
/// Cleans up the unique-randomized namespace it creates on success
/// and on panic, via the cluster's namespace cascade delete.
///
/// The kubeconfig is consumed from the env via `Config::infer()`, not
/// from an injected client — `Config::infer()` falls through to the
/// kubeconfig branch when the in-cluster env vars are missing, which is
/// exactly the path `kubectl`-on-a-laptop takes.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a live kubernetes cluster (kind)"]
async fn local_kubernetes_endpoint_slice_watcher_emits_added_peer() {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use futures::TryStreamExt;
    use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
    use k8s_openapi::api::discovery::v1::EndpointSlice as ApiEndpointSlice;
    use kube::api::{DeleteParams, PostParams};
    use kube::core::ObjectMeta;

    use crate::discovery::PeerDiscovery;

    const PORT: u16 = 9000;
    const ADDRESSES: [Ipv4Addr; 2] = [Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)];

    // Use a fresh per-process namespace so concurrent runs (and aborted
    // past runs) don't collide. `local-clean.sh` sweeps up any that an aborted
    // run leaves behind.
    let suffix = std::process::id();
    let namespace = format!("gabion-disc-{suffix}");

    // Build a kubeconfig-aware client via Config::infer — picks up
    // KUBECONFIG / ~/.kube/config / in-cluster env.
    let config = kube::Config::infer()
        .await
        .expect("kubeconfig: set KUBECONFIG or run inside a pod");
    let client = kube::Client::try_from(config).expect("kube client");

    // ---- namespace + service scaffolding --------------------------------
    let namespaces: kube::Api<k8s_openapi::api::core::v1::Namespace> =
        kube::Api::all(client.clone());
    let ns = k8s_openapi::api::core::v1::Namespace {
        metadata: ObjectMeta {
            name: Some(namespace.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    namespaces
        .create(&PostParams::default(), &ns)
        .await
        .expect("create namespace");

    // Service. The discovery code keys off a UDP port named "gabion";
    // EndpointSlice's port spec must match.
    let svc: kube::Api<Service> = kube::Api::namespaced(client.clone(), &namespace);
    let service = Service {
        metadata: ObjectMeta {
            name: Some("gabion".to_string()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()), // headless: no IPAM needed
            ports: Some(vec![ServicePort {
                name: Some("gabion".to_string()),
                protocol: Some("UDP".to_string()),
                port: PORT.into(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };
    svc.create(&PostParams::default(), &service)
        .await
        .expect("create service");

    // EndpointSlice. Manually authored (no Pod selector) because we want
    // the watcher's input to be deterministic. The apiserver REJECTS
    // loopback addresses (`127.0.0.0/8`, `::1/128`) in
    // `endpoints[].addresses` — that's a built-in EndpointSlice
    // validation rule, not RBAC — so advertise non-loopback IPs. They are
    // never dialed: this test only asserts what the discovery stream emits.
    let slices: kube::Api<ApiEndpointSlice> = kube::Api::namespaced(client.clone(), &namespace);
    let slice = ApiEndpointSlice {
        metadata: ObjectMeta {
            name: Some("gabion-host".to_string()),
            labels: Some(BTreeMap::from([(
                "kubernetes.io/service-name".to_string(),
                "gabion".to_string(),
            )])),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        endpoints: ADDRESSES
            .map(|address| Endpoint {
                addresses: vec![address.to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .into(),
        ports: Some(vec![EndpointPort {
            name: Some("gabion".to_string()),
            protocol: Some("UDP".to_string()),
            port: Some(PORT.into()),
            ..Default::default()
        }]),
    };
    slices
        .create(&PostParams::default(), &slice)
        .await
        .expect("create endpointslice");

    // ---- discovery --------------------------------------------------------
    // The watcher's stream is the signal: the test proceeds the moment the
    // first `Added` arrives. The timeout only bounds a broken watcher.
    let discovery = EndpointSliceDiscovery {
        config: DiscoveryConfig {
            self_match: None,
            namespace_allow: Box::new([namespace.as_str().into()]),
            service_allow: Box::new(["gabion".into()]),
        },
        preconnected: Some(client),
    };
    let first_added = async {
        let stream = discovery.peer_events();
        futures::pin_mut!(stream);
        while let Some(event) = stream.try_next().await.expect("discovery stream failed") {
            if let PeerEvent::Added(peer) = event {
                return peer;
            }
        }
        panic!("discovery stream ended without emitting PeerEvent::Added");
    };
    let peer = tokio::time::timeout(Duration::from_secs(60), first_added).await;

    // Delete before asserting. A `Drop` guard can't await, and a task it
    // spawns never runs once the test's runtime shuts down.
    namespaces
        .delete(&namespace, &DeleteParams::default())
        .await
        .expect("delete namespace");

    let peer = peer.expect("EndpointSliceDiscovery did not emit a PeerEvent::Added within 60s");
    assert!(
        ADDRESSES
            .map(|ip| SocketAddr::from((ip, PORT)))
            .contains(&peer.addr),
        "unexpected peer {peer:?}",
    );
}

#[test]
fn fatal_status_stops_only_on_auth_and_missing_resource() {
    let status = |code| kube::core::Status::failure("", "").with_code(code).boxed();
    for code in [401, 403, 404] {
        assert_eq!(
            fatal_status(&WatcherError::WatchError(status(code))),
            Some(code)
        );
        let failed = WatcherError::InitialListFailed(kube::Error::Api(status(code)));
        assert_eq!(fatal_status(&failed), Some(code));
    }
    for code in [409, 500, 503] {
        assert_eq!(fatal_status(&WatcherError::WatchError(status(code))), None);
        let failed = WatcherError::WatchFailed(kube::Error::Api(status(code)));
        assert_eq!(fatal_status(&failed), None);
    }
    assert_eq!(fatal_status(&WatcherError::NoResourceVersion), None);
}
