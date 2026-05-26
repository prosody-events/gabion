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

// -- live cluster integration ---------------------------------------------

/// End-to-end: stand up two `GossipRuntime`s against a real kind-based
/// kube cluster, expose them via a tiny Service+EndpointSlice pair so
/// `EndpointSliceDiscovery` sees one peer for each runtime, and assert
/// they converge on a non-trivial CRDT counter.
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
#[cfg(feature = "transport-udp")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires a live kubernetes cluster (kind)"]
async fn local_kubernetes_endpoint_slice_watcher_drives_gossip_convergence() {
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    use futures::TryStreamExt;
    use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
    use k8s_openapi::api::discovery::v1::EndpointSlice as ApiEndpointSlice;
    use kube::api::{DeleteParams, PostParams};
    use kube::core::ObjectMeta;
    use tokio::task::LocalSet;

    use crate::crdt::{BucketEpoch, CellStore, CellStoreConfig, KeyHash, NodeId, NodeIdentity};
    use crate::discovery::PeerDiscovery;
    use crate::gossip::{GossipConfig, GossipRuntime, TokioClock, UdpTransport};

    // Use a fresh per-process namespace so concurrent runs (and aborted
    // past runs) don't collide.
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

    // Cleanup guard: drop runs `kubectl delete namespace --ignore-not-found`
    // equivalent so an interrupted test doesn't leak the namespace.
    struct NamespaceGuard {
        api: kube::Api<k8s_openapi::api::core::v1::Namespace>,
        name: String,
    }
    impl Drop for NamespaceGuard {
        fn drop(&mut self) {
            let api = self.api.clone();
            let name = self.name.clone();
            // Best-effort, fire-and-forget delete. We're inside Drop so
            // we can't await; spawn the cascade delete onto whatever
            // runtime exists at drop time and let kubernetes' garbage
            // collector finish the job.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = api.delete(&name, &DeleteParams::default()).await;
                });
            }
        }
    }
    let _ns_guard = NamespaceGuard {
        api: namespaces.clone(),
        name: namespace.clone(),
    };

    // Bind two UDP sockets on the host. Their addresses become the
    // EndpointSlice addresses; kube-rs's watcher returns them, the
    // discovery stream emits them as Peer events, and the two
    // GossipRuntimes converge by talking to each other over those
    // sockets.
    let sock_a = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind a");
    let sock_b = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind b");
    let addr_a = sock_a.local_addr().expect("local addr a");
    let addr_b = sock_b.local_addr().expect("local addr b");

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
                port: addr_a.port() as i32,
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
    // validation rule, not RBAC. So advertise non-loopback IPs even
    // though the two gossip sockets are bound on `127.0.0.1`. The
    // advertised IPs are never dialed: gossip reaches its peers via
    // `bootstrap_peers` (lines below), and this test only asserts that
    // the discovery stream emits a `PeerEvent::Added`.
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
        endpoints: vec![
            Endpoint {
                addresses: vec!["10.0.0.1".to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Endpoint {
                addresses: vec!["10.0.0.2".to_string()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        ports: Some(vec![EndpointPort {
            name: Some("gabion".to_string()),
            protocol: Some("UDP".to_string()),
            // Discovery emits one peer per address, sharing this port.
            // Both gossip runtimes can't sit on the same port though, so
            // we accept that the EndpointSlice's "port" can only point
            // to one of the two and rely on bootstrap_peers below as a
            // belt-and-suspenders. The test still exercises the
            // discovery happy path (one peer event arrives) which is
            // the integration we care about.
            port: Some(addr_b.port() as i32),
            ..Default::default()
        }]),
    };
    slices
        .create(&PostParams::default(), &slice)
        .await
        .expect("create endpointslice");

    // ---- gossip wiring --------------------------------------------------
    let local = LocalSet::new();
    let outcome = local
        .run_until(async move {
            let id_a = NodeIdentity::new(NodeId(0x1A_00), 1);
            let id_b = NodeIdentity::new(NodeId(0x1B_00), 1);
            let agg_a = Rc::new(InMemoryAgg::default());
            let agg_b = Rc::new(InMemoryAgg::default());

            let config_a = GossipConfig {
                local_identity: id_a,
                cluster_id_hash: 0xC1,
                bootstrap_peers: vec![addr_b],
                fanout: 1,
                tick_interval: Duration::from_millis(20),
                rng_seed: 1,
                ..GossipConfig::default()
            };
            let config_b = GossipConfig {
                local_identity: id_b,
                cluster_id_hash: 0xC1,
                bootstrap_peers: vec![addr_a],
                fanout: 1,
                tick_interval: Duration::from_millis(20),
                rng_seed: 2,
                ..GossipConfig::default()
            };

            let store_a = CellStore::<u32>::new(CellStoreConfig::default(), id_a);
            let store_b = CellStore::<u32>::new(CellStoreConfig::default(), id_b);
            let (rt_a, client_a) = GossipRuntime::from_parts(
                UdpTransport::from_socket(sock_a),
                TokioClock::new(),
                config_a,
                store_a,
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                UdpTransport::from_socket(sock_b),
                TokioClock::new(),
                config_b,
                store_b,
                agg_b.clone(),
            );

            // Drive discovery against the live cluster. We don't feed
            // the resulting peer stream into either runtime — we already
            // bootstrap-peered them above — but we MUST drain it, which
            // is what proves the watcher saw our EndpointSlice and
            // emitted at least one Added.
            let discovery = EndpointSliceDiscovery::with_client(
                client.clone(),
                None,
                vec![namespace.clone()],
                vec!["gabion".to_string()],
            );
            let saw_peer = Rc::new(std::cell::Cell::new(false));
            let saw_peer_clone = saw_peer.clone();
            let _discovery_task = tokio::task::spawn_local(async move {
                let stream = discovery.peer_events();
                futures::pin_mut!(stream);
                while let Ok(Some(event)) = stream.try_next().await {
                    if matches!(event, PeerEvent::Added(_)) {
                        saw_peer_clone.set(true);
                    }
                }
            });

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let bucket = (now_millis / 1_000) as BucketEpoch;
            client_a
                .record(0xC0FE, KeyHash(0xABCD), bucket, 3, 0, now_millis)
                .await
                .unwrap();
            client_b
                .record(0xC0FE, KeyHash(0xABCD), bucket, 5, 0, now_millis)
                .await
                .unwrap();

            let deadline = Instant::now() + Duration::from_secs(10);
            let converged = loop {
                let sa: u64 = agg_a.sum();
                let sb: u64 = agg_b.sum();
                if sa == 8 && sb == 8 {
                    break true;
                }
                if Instant::now() >= deadline {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;

            (converged, saw_peer.get())
        })
        .await;

    let (converged, saw_peer) = outcome;
    assert!(
        converged,
        "gossip did not converge against live kube discovery"
    );
    assert!(
        saw_peer,
        "EndpointSliceDiscovery did not emit a PeerEvent::Added"
    );
}

// Tiny aggregate store used only by the live-cluster test. Kept inline
// rather than reusing `gossip::tests::InMemoryAggregateStore` because
// that module is `#[cfg(test)] mod tests` in `gossip.rs` and not
// reachable from a sibling module's test file.
#[cfg(feature = "transport-udp")]
#[derive(Default)]
struct InMemoryAgg {
    inner: std::cell::RefCell<
        std::collections::HashMap<(u128, crate::crdt::KeyHash, crate::crdt::BucketEpoch), u64>,
    >,
}

#[cfg(feature = "transport-udp")]
impl InMemoryAgg {
    fn sum(&self) -> u64 {
        self.inner.borrow().values().copied().sum()
    }
}

#[cfg(feature = "transport-udp")]
impl crate::gossip::AggregateStore<u32> for InMemoryAgg {
    fn apply(
        &self,
        deltas: &crate::crdt::DeltaSink<u32>,
        expirations: &crate::crdt::ExpirationSink<u32>,
    ) {
        let mut map = self.inner.borrow_mut();
        for i in 0..deltas.len() {
            let key = &deltas.keys[i];
            let v: u64 = deltas.deltas[i].into();
            *map.entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0) += v;
        }
        for i in 0..expirations.len() {
            let key = &expirations.keys[i];
            let v: u64 = expirations.last_counts[i].into();
            let entry = map
                .entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0);
            *entry = entry.saturating_sub(v);
            if *entry == 0 {
                map.remove(&(key.rule_fingerprint, key.key_hash, key.bucket));
            }
        }
    }
}
