use super::*;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
struct EndpointSliceEventsCase {
    events: Vec<EndpointSliceEvent>,
}

#[derive(Clone, Debug)]
struct EndpointSliceEvent {
    slice: u8,
    first: u8,
    second: u8,
    ready: bool,
    delete: bool,
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
            slice: u8::arbitrary(g) % 8,
            first: (u8::arbitrary(g) % 8) + 2,
            second: (u8::arbitrary(g) % 8) + 2,
            ready: bool::arbitrary(g),
            delete: bool::arbitrary(g),
        }
    }
}

/// Build a synthetic EndpointSlice with a single endpoint group and a
/// single `gabion` UDP port. Mirrors what `kube-rs`'s watcher hands us
/// over the wire — every field the production parser inspects is set,
/// every field it ignores is `Default::default()`.
fn slice(name: &str, addresses: &[&str], ready: Option<bool>) -> EndpointSlice {
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
        endpoints: vec![Endpoint {
            addresses: addresses
                .iter()
                .map(|address| (*address).to_string())
                .collect(),
            conditions: ready.map(|ready| EndpointConditions {
                ready: Some(ready),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ports: Some(vec![EndpointPort {
            name: Some("gabion".to_string()),
            port: Some(18080),
            protocol: Some("UDP".to_string()),
            ..Default::default()
        }]),
    }
}

fn self_addr() -> Option<SocketAddr> {
    Some("10.0.0.1:18080".parse().expect("self addr"))
}

#[test]
fn endpoint_slice_parser_deduplicates_and_ignores_self() {
    let peers = peers_from_endpoint_slice(
        &slice("slice-a", &["10.0.0.1", "10.0.0.2", "10.0.0.2"], Some(true)),
        self_addr(),
    );

    let peers: Vec<Peer> = peers.into_iter().collect();
    assert_eq!(
        peers,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_parser_ignores_not_ready_endpoints() {
    let peers =
        peers_from_endpoint_slice(&slice("slice-a", &["10.0.0.2"], Some(false)), self_addr());

    assert!(peers.is_empty());
}

#[test]
fn endpoint_slice_peer_set_updates_and_deletes_snapshots() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
    let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));

    peer_set.apply(&slice_a, self_addr());
    peer_set.apply(&slice_b, self_addr());
    assert_eq!(peer_set.snapshot_peers().len(), 2);

    let removed = peer_set.delete(&slice_a);
    assert_eq!(
        removed,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
    assert_eq!(
        peer_set.snapshot_peers(),
        vec![Peer::new("10.0.0.3:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_peer_set_relist_replaces_missing_slices() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
    let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));
    let slice_c = slice("slice-c", &["10.0.0.4"], Some(true));

    peer_set.apply(&slice_a, self_addr());
    peer_set.apply(&slice_b, self_addr());

    let mut relist = EndpointSlicePeerSet::default();
    relist.apply(&slice_c, self_addr());
    peer_set.replace(relist);

    assert_eq!(
        peer_set.snapshot_peers(),
        vec![Peer::new("10.0.0.4:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_peer_set_apply_emits_added_and_removed_diff() {
    let mut peer_set = EndpointSlicePeerSet::default();
    let initial = slice("slice-a", &["10.0.0.2"], Some(true));
    let updated = slice("slice-a", &["10.0.0.3"], Some(true));

    let first = peer_set.apply(&initial, self_addr());
    assert_eq!(
        first.added,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
    assert!(first.removed.is_empty());

    let second = peer_set.apply(&updated, self_addr());
    assert_eq!(
        second.removed,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
    assert_eq!(
        second.added,
        vec![Peer::new("10.0.0.3:18080".parse().expect("addr"))]
    );
}

#[quickcheck]
fn quickcheck_endpoint_slice_events_match_live_slice_model(
    case: EndpointSliceEventsCase,
) -> TestResult {
    let mut peer_set = EndpointSlicePeerSet::default();
    let mut live = BTreeMap::new();

    for event in case.events {
        let name = format!("slice-{}", event.slice);
        if event.delete {
            peer_set.delete(&slice(&name, &["10.0.0.9"], Some(true)));
            live.remove(&name);
        } else {
            let first = format!("10.0.0.{}", event.first);
            let second = format!("10.0.0.{}", event.second);
            let current = slice(&name, &[&first, &second], Some(event.ready));
            peer_set.apply(&current, self_addr());

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
            live.insert(name, peers);
        }

        let mut expected = live.values().flatten().copied().collect::<Vec<_>>();
        expected.sort();
        expected.dedup();
        if peer_set.snapshot_peers() != expected {
            return TestResult::error("EndpointSlice peer set diverged from live slice model");
        }
    }
    TestResult::passed()
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
fn service_change_ignores_init_marker() {
    let change = service_change(Event::<Service>::Init, &[]);
    assert!(
        matches!(change, ServiceChange::Ignore),
        "Event::Init is a no-op",
    );
}

#[test]
fn service_change_filters_by_allow_list() {
    let svc = service("default", "other", true, true);
    let change = service_change(Event::Apply(svc), &["gabion".to_string()]);
    assert!(
        matches!(change, ServiceChange::Ignore),
        "Service not in allow list should Ignore",
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
    let discovery = EndpointSliceDiscovery::with_client(
        client,
        None,
        vec![namespace.clone()],
        vec!["gabion".to_string()],
    );
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
