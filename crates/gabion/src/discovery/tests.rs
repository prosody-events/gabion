use super::*;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use quickcheck_macros::quickcheck;

#[derive(Clone, Debug)]
struct PeerSnapshotCase {
    octets: Vec<u8>,
}

impl Arbitrary for PeerSnapshotCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut octets = Vec::<u8>::arbitrary(g);
        octets.truncate(64);
        Self { octets }
    }
}

#[derive(Clone, Debug)]
struct PeerEventCase {
    events: Vec<PeerEvent>,
}

#[derive(Clone, Debug)]
struct PeerEvent {
    octet: u8,
    add: bool,
}

#[derive(Clone, Debug)]
struct FileRetryCase {
    first_octet: u8,
    second_octet: u8,
}

impl Arbitrary for PeerEventCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut events = Vec::<PeerEvent>::arbitrary(g);
        events.truncate(128);
        Self { events }
    }
}

impl Arbitrary for PeerEvent {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            octet: (u8::arbitrary(g) % 16) + 1,
            add: bool::arbitrary(g),
        }
    }
}

impl Arbitrary for FileRetryCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            first_octet: (u8::arbitrary(g) % 16) + 1,
            second_octet: (u8::arbitrary(g) % 16) + 17,
        }
    }
}

macro_rules! async_quickcheck {
        (fn $name:ident($case:ident: $case_ty:ty) -> TestResult $body:block) => {
            #[test]
            fn $name() {
                fn property($case: $case_ty) -> TestResult {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .start_paused(true)
                        .build()
                        .expect("paused tokio runtime")
                        .block_on(async move $body)
                }

                QuickCheck::new().quickcheck(property as fn($case_ty) -> TestResult);
            }
        };
    }

#[test]
fn static_provider_deduplicates_and_ignores_self() {
    let self_addr = "127.0.0.1:18080".parse().expect("addr");
    let peer = "127.0.0.2:18080".parse().expect("addr");
    let provider = StaticPeerHandler::new(vec![self_addr, peer, peer], Some(self_addr));

    let snapshot = provider.snapshot();

    assert_eq!(snapshot.peers(), &[Peer::new(peer)]);
    assert!(!snapshot.stale());
    assert!(!snapshot.local_only());
}

#[test]
fn snapshot_peer_handler_adds_and_removes_individual_peers() {
    let peer = "127.0.0.2:18080".parse().expect("addr");
    let provider = SnapshotPeerHandler::new(PeerSnapshot::default());

    provider.peer_added(Peer::new(peer));
    provider.peer_removed(Peer::new(peer));
    let snapshot = provider.snapshot();

    assert!(snapshot.peers().is_empty());
    assert!(!snapshot.stale());
    assert!(snapshot.local_only());
}

#[test]
fn snapshot_peer_handler_retains_last_good_set_when_capacity_is_full() {
    let first = Peer::new("127.0.0.2:18080".parse().expect("addr"));
    let second = Peer::new("127.0.0.3:18080".parse().expect("addr"));
    let provider = SnapshotPeerHandler::with_capacity(1);

    provider.peer_added(first);
    provider.peer_added(second);
    let snapshot = provider.snapshot();

    assert_eq!(snapshot.peers(), &[first]);
    assert!(!snapshot.stale());
    assert!(!snapshot.local_only());
}

#[test]
fn peer_file_parser_accepts_line_based_addresses() {
    let self_addr = "127.0.0.1:18080".parse().expect("addr");
    let peers = parse_peer_lines(
        "
            # ignored
            127.0.0.1:18080
            127.0.0.2:18080
            127.0.0.2:18080
            ",
        Some(self_addr),
    );

    assert_eq!(
        peers,
        vec![Peer::new("127.0.0.2:18080".parse().expect("addr"))]
    );
}

#[quickcheck]
fn quickcheck_peer_snapshots_are_sorted_deduped_bounded_and_selfless(
    case: PeerSnapshotCase,
) -> TestResult {
    let self_addr = "127.0.0.1:18080".parse().expect("addr");
    let mut raw = Vec::new();
    let mut expected = Vec::new();
    for octet in case.octets {
        let addr = SocketAddr::from(([127, 0, 0, (octet % 16) + 1], 18080));
        raw.push(addr);
        if addr != self_addr {
            expected.push(Peer::new(addr));
        }
    }
    expected.sort();
    expected.dedup();

    let static_handler = StaticPeerHandler::new(raw, Some(self_addr));
    let snapshot = static_handler.snapshot();

    if snapshot.peers() != expected.as_slice() {
        return TestResult::error("peer snapshot is not sorted, deduped, or selfless");
    }
    if snapshot.stale() {
        return TestResult::error("static peer snapshot was unexpectedly stale");
    }
    if snapshot.local_only() != expected.is_empty() {
        return TestResult::error("peer snapshot local_only flag does not match peer set");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_snapshot_handler_add_remove_events_match_bounded_set_model(
    case: PeerEventCase,
) -> TestResult {
    let handler = SnapshotPeerHandler::with_capacity(6);
    let mut expected = Vec::with_capacity(6);

    for event in case.events {
        let peer = Peer::new(SocketAddr::from(([10, 0, 0, event.octet], 18080)));
        if event.add {
            handler.peer_added(peer);
            if expected.binary_search(&peer).is_err() && expected.len() < 6 {
                let index = expected.partition_point(|stored| stored < &peer);
                expected.insert(index, peer);
            }
        } else {
            handler.peer_removed(peer);
            if let Ok(index) = expected.binary_search(&peer) {
                expected.remove(index);
            }
        }

        let snapshot = handler.snapshot();
        if snapshot.peers() != expected.as_slice()
            || snapshot.stale()
            || snapshot.local_only() != expected.is_empty()
            || !snapshot.peers().windows(2).all(|peers| peers[0] < peers[1])
            || snapshot.peers().len() > 6
        {
            return TestResult::error(
                "snapshot handler diverged from bounded add/remove set model",
            );
        }
    }
    TestResult::passed()
}

async_quickcheck! {
    fn quickcheck_file_peer_events_keep_retrying_under_paused_time(case: FileRetryCase) -> TestResult {
        let path = std::env::temp_dir().join(format!(
            "gabion-peer-events-{}-{}-{}",
            std::process::id(),
            case.first_octet,
            case.second_octet,
        ));
        let first = Peer::new(SocketAddr::from(([127, 0, 0, case.first_octet], 18080)));
        let second = Peer::new(SocketAddr::from(([127, 0, 0, case.second_octet], 18080)));
        let handler = FilePeerHandler::new(&path, None, Vec::new());
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        publish_file_peer_event(&handler, &mut interval).await;
        if !handler.snapshot().peers().is_empty() {
            let _ = fs::remove_file(&path);
            return TestResult::error("missing peer file did not keep empty last-good snapshot");
        }

        if fs::write(&path, first.addr.to_string()).is_err() {
            return TestResult::error("failed to write first generated peer file");
        }
        tokio::time::advance(std::time::Duration::from_millis(1_001)).await;
        publish_file_peer_event(&handler, &mut interval).await;
        if handler.snapshot().peers() != [first].as_slice() {
            let _ = fs::remove_file(&path);
            return TestResult::error("file peer retry did not publish first peer set");
        }

        if fs::write(&path, second.addr.to_string()).is_err() {
            let _ = fs::remove_file(&path);
            return TestResult::error("failed to write second generated peer file");
        }
        tokio::time::advance(std::time::Duration::from_millis(1_001)).await;
        publish_file_peer_event(&handler, &mut interval).await;
        let passed = handler.snapshot().peers() == [second].as_slice();

        let _ = fs::remove_file(path);
        if passed {
            TestResult::passed()
        } else {
            TestResult::error("file peer retry did not replace old peers")
        }
    }
}
