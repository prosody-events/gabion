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
            name: Some("gossip".to_string()),
            port: Some(18080),
            ..Default::default()
        }]),
    }
}

fn config() -> EndpointSliceDiscoveryConfig {
    EndpointSliceDiscoveryConfig {
        namespace: "default".to_string(),
        service_name: "gabion".to_string(),
        port_name: Some("gossip".to_string()),
        self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
    }
}

#[test]
fn endpoint_slice_parser_deduplicates_and_ignores_self() {
    let peers = peers_from_endpoint_slice(
        &slice("slice-a", &["10.0.0.1", "10.0.0.2", "10.0.0.2"], Some(true)),
        &config(),
    );

    assert_eq!(
        peers,
        vec![Peer::new("10.0.0.2:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_parser_ignores_not_ready_endpoints() {
    let peers = peers_from_endpoint_slice(&slice("slice-a", &["10.0.0.2"], Some(false)), &config());

    assert!(peers.is_empty());
}

#[test]
fn endpoint_slice_peer_set_updates_and_deletes_snapshots() {
    let config = config();
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
    let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));

    peer_set.apply(&slice_a, &config);
    peer_set.apply(&slice_b, &config);
    assert_eq!(peer_set.snapshot_peers().len(), 2);

    peer_set.delete(&slice_a);
    assert_eq!(
        peer_set.snapshot_peers(),
        vec![Peer::new("10.0.0.3:18080".parse().expect("addr"))]
    );
}

#[test]
fn endpoint_slice_peer_set_relist_replaces_missing_slices() {
    let config = config();
    let mut peer_set = EndpointSlicePeerSet::default();
    let slice_a = slice("slice-a", &["10.0.0.2"], Some(true));
    let slice_b = slice("slice-b", &["10.0.0.3"], Some(true));
    let slice_c = slice("slice-c", &["10.0.0.4"], Some(true));

    peer_set.apply(&slice_a, &config);
    peer_set.apply(&slice_b, &config);

    let mut relist = EndpointSlicePeerSet::default();
    relist.apply(&slice_c, &config);
    peer_set.replace(relist);

    assert_eq!(
        peer_set.snapshot_peers(),
        vec![Peer::new("10.0.0.4:18080".parse().expect("addr"))]
    );
}

#[test]
fn merged_endpoint_slice_state_deduplicates_across_selectors() {
    let mut state = MergedEndpointSliceState::new(2);
    let peer_a = Peer::new("10.0.0.2:18080".parse().expect("addr"));
    let peer_b = Peer::new("10.0.0.3:18080".parse().expect("addr"));

    let first = state.update(0, vec![peer_a, peer_b]);
    let second = state.update(1, vec![peer_b]);

    assert_eq!(first, vec![peer_a, peer_b]);
    assert_eq!(second, vec![peer_a, peer_b]);
}

#[quickcheck]
fn quickcheck_endpoint_slice_events_match_live_slice_model(
    case: EndpointSliceEventsCase,
) -> TestResult {
    let config = config();
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
            peer_set.apply(&current, &config);

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
