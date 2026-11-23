use std::cell::RefCell;
use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroU64;
use std::rc::Rc;

use gabion::crdt::KeyHash;
use gabion::wire::{CellPayload, HEADER_LEN, Header, MAGIC, Payload, VERSION, read_header};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

use super::{
    CAPTURE_BODY_BYTES, CAPTURE_PACKETS, Capture, Fate, Filter, HEAD_SLOTS, LostBy, Pair, PairWay,
    ROW_SLOTS, Relation, RepeatCause, Scope, SharedCapture, TierFilter,
};
use crate::config::SimConfig;
use crate::engine::{CLUSTER_ID_HASH, Command, LinkPolicyKind, StepUntil, run_engine};
use crate::event::{
    EventBatch, EventKind, ListedRepeat, MessageRole, MessageType, PacketKind, Riders, Seq, Tier,
};

const GAP_OWN: u8 = 0x02;
const GAP_RELAY: u8 = 0x04;

/// Raw header bytes, laid out as gabion's `write_header` lays them out.
fn raw(
    kind: u8,
    flags: u8,
    cells: u16,
    incarnation: u32,
    marks: [u64; 2],
    range: [u64; 2],
) -> [u8; HEADER_LEN] {
    let mut b = [0; HEADER_LEN];
    b[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    b[4..6].copy_from_slice(&VERSION.to_le_bytes());
    b[6] = kind;
    b[7] = flags;
    b[8..24].copy_from_slice(&CLUSTER_ID_HASH.get().to_le_bytes());
    b[24..40].copy_from_slice(&0x101u128.to_le_bytes());
    b[40..44].copy_from_slice(&1u32.to_le_bytes());
    b[48..50].copy_from_slice(&cells.to_le_bytes());
    b[50..54].copy_from_slice(&incarnation.to_le_bytes());
    b[54..62].copy_from_slice(&marks[0].to_le_bytes());
    b[62..70].copy_from_slice(&marks[1].to_le_bytes());
    b[70..78].copy_from_slice(&range[0].to_le_bytes());
    b[78..86].copy_from_slice(&range[1].to_le_bytes());
    b
}

/// Every header the decoder admits, over each kind and flag byte, with 0, 1
/// and 33 cells and an ack incarnation of 0 and 1.
fn legal_headers() -> Vec<(Header, u16)> {
    let mut out = Vec::new();
    for kind in 0..5u8 {
        for flags in 0..=0x7fu8 {
            for cells in [0u16, 1, 33] {
                for incarnation in [0, 1] {
                    let range = if matches!(kind, 1 | 2) {
                        [3, 5]
                    } else {
                        [0, 0]
                    };
                    let bytes = raw(kind, flags, cells, incarnation, [4, 2], range);
                    if let Ok(read) = read_header(&bytes, CLUSTER_ID_HASH) {
                        out.push(read);
                    }
                }
            }
        }
    }
    out
}

/// A72: the role is total and follows its precedence, the wire kind is the
/// payload's, the role's own signal never rides, cannot relay rides exactly
/// across zones, and every message matches the By type rows its signals name.
#[test]
fn every_legal_header_has_one_role_and_its_riders() {
    let headers = legal_headers();
    assert!(
        headers.len() > 100,
        "the sweep reaches every kind: {}",
        headers.len()
    );
    for (header, cells) in headers {
        for tier in [Tier::InZone, Tier::CrossZone] {
            let (role, riders, repeat) = MessageRole::of(&header, cells, tier);
            let acks = header.acks;
            assert_eq!(role.kind(), PacketKind::from(header.payload));
            let gap = acks.gap_own || acks.gap_relay;
            let expected = match header.payload {
                Payload::Cells(CellPayload::Own { .. }) if cells > 0 => MessageRole::OwnData,
                Payload::Cells(CellPayload::Own { .. }) => MessageRole::OwnSkip,
                Payload::Cells(CellPayload::Relay { .. }) if cells > 0 => MessageRole::RelayData,
                Payload::Cells(CellPayload::Relay { .. }) => MessageRole::RelaySkip,
                Payload::Cells(CellPayload::Sweep) => MessageRole::SweepPage,
                Payload::SweepRequest => MessageRole::SweepRequest,
                Payload::Ack { .. } if gap => MessageRole::GapReport,
                Payload::Ack { probe: true } => MessageRole::Probe,
                Payload::Ack { .. } if acks.probe_reply => MessageRole::ProbeAnswer,
                Payload::Ack { .. } => MessageRole::Ack,
            };
            assert_eq!(role, expected, "{header:?}");
            let own_signal = match role {
                MessageRole::GapReport => Riders::GAP_REPORT,
                MessageRole::Probe => Riders::PROBE,
                MessageRole::ProbeAnswer => Riders::PROBE_ANSWER,
                _ => Riders::NONE,
            };
            assert!(!riders.contains(own_signal), "{role:?} rides itself");
            assert_eq!(
                riders.contains(Riders::CANNOT_RELAY),
                acks.cannot_relay && tier == Tier::CrossZone
            );
            let data = matches!(role.kind(), PacketKind::Own | PacketKind::Relay);
            assert_eq!(
                repeat.is_some(),
                data && header.payload.asks_answer()
                    && probe_of(header.payload)
                    && acks.incarnation != 0
            );
            let types: BTreeSet<usize> = MessageType::ALL
                .iter()
                .enumerate()
                .filter(|(_, t)| t.matches(role, riders, repeat))
                .map(|(i, _)| i)
                .collect();
            assert!(!types.is_empty(), "{role:?} matches some row");
            let named = |t: MessageType| {
                types.contains(&(MessageType::ALL.iter().position(|&x| x == t).unwrap()))
            };
            assert_eq!(named(MessageType::GapReport), gap);
            assert_eq!(
                named(MessageType::CannotRelay),
                riders.contains(Riders::CANNOT_RELAY)
            );
            assert_eq!(named(MessageType::Repeat), repeat.is_some());
            assert_eq!(
                named(MessageType::Probe),
                role == MessageRole::Probe || riders.contains(Riders::PROBE)
            );
        }
    }
}

fn probe_of(payload: Payload) -> bool {
    match payload {
        Payload::Ack { probe }
        | Payload::Cells(CellPayload::Own { probe, .. } | CellPayload::Relay { probe, .. }) => {
            probe
        }
        Payload::Cells(CellPayload::Sweep) | Payload::SweepRequest => false,
    }
}

/// Record `bytes` from `src` to `dst` and settle it at `at_ms`.
fn send(
    capture: &mut Capture,
    bytes: &[u8],
    route: (u32, u32),
    at_ms: u64,
    lost: Option<LostBy>,
) -> Seq {
    let read = read_header(bytes, CLUSTER_ID_HASH).expect("a legal header");
    let (seq, ..) = capture.record(
        bytes,
        (&read.0, read.1),
        route,
        Tier::InZone,
        lost.is_none(),
    );
    capture.settle(seq, at_ms, lost);
    seq
}

fn ack() -> [u8; HEADER_LEN] {
    raw(0, 0, 0, 1, [0, 0], [0, 0])
}

/// A69: the cap is honest. The newest 64 of 200 are kept, each still holding
/// its own header; a body the arena reused reads as not kept; and the
/// production capture's heap is what its columns state.
#[test]
fn the_capture_keeps_the_newest_messages_and_says_how_many() {
    let mut capture = Capture::new(64, 512);
    capture.mark_run_start();
    for i in 0..200u64 {
        // Each header states its own number in its relay mark.
        let bytes = raw(0, 0, 0, 1, [0, i + 1], [0, 0]);
        send(&mut capture, &bytes, (0, 1), i, None);
    }
    capture.write_rows(0, 0);
    let head = &capture.rows()[..HEAD_SLOTS];
    assert_eq!((head[0], head[1]), (200.0, 64.0));
    assert!(!capture.kept(136));
    assert!(capture.kept(137));
    for seq in 137..=200 {
        let header = &capture.header[capture.slot(seq)];
        assert_eq!(u64::from_le_bytes(header[62..70].try_into().unwrap()), seq);
    }

    // Bodies: two of 300 bytes cannot both fit 512, so the first is lost.
    let mut capture = Capture::new(8, 512);
    let mut body = raw(1, 0, 0, 1, [0, 0], [0, 1]).to_vec();
    body.extend([7; 300]);
    let read = read_header(&body, CLUSTER_ID_HASH).unwrap();
    let (first, ..) = capture.record(&body, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert!(capture.body_kept(first.0));
    let (second, ..) = capture.record(&body, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert!(!capture.body_kept(first.0), "the arena reused its bytes");
    assert!(capture.body_kept(second.0));
    assert!(capture.kept(first.0), "its header outlives its body");

    let production = Capture::new(CAPTURE_PACKETS, CAPTURE_BODY_BYTES).heap_bytes() as f64;
    let mib = production / (1024.0 * 1024.0);
    assert!((6.1..=6.4).contains(&mib), "the capture holds {mib:.2} MiB");
}

/// A delivered gap report flags the next data on its link and stream as its
/// repeat when its range starts at or below the report's mark; a lost report
/// flags nothing.
#[test]
fn a_delivered_gap_report_names_its_repeat() {
    let mut capture = Capture::new(64, 4096);
    // Node 1 reports to node 0 a gap in node 0's own stream after 6.
    send(
        &mut capture,
        &raw(0, GAP_OWN, 0, 1, [6, 0], [0, 0]),
        (1, 0),
        0,
        None,
    );
    let repeat = raw(1, 0, 0, 1, [0, 0], [6, 8]);
    let read = read_header(&repeat, CLUSTER_ID_HASH).unwrap();
    let (.., listed) = capture.record(&repeat, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert_eq!(listed, Some(ListedRepeat::AfterGapReport));
    // The entry is used up: the next one is fresh data.
    let (.., listed) = capture.record(&repeat, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert_eq!(listed, None);

    let mut capture = Capture::new(64, 4096);
    send(
        &mut capture,
        &raw(0, GAP_OWN, 0, 1, [6, 0], [0, 0]),
        (1, 0),
        0,
        Some(LostBy::Loss),
    );
    let (.., listed) = capture.record(&repeat, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert_eq!(listed, None, "a lost report asks for nothing");

    // A range above the mark is fresh data, and still clears the entry.
    let mut capture = Capture::new(64, 4096);
    send(
        &mut capture,
        &raw(0, GAP_RELAY, 0, 1, [0, 6], [0, 0]),
        (1, 0),
        0,
        None,
    );
    let fresh = raw(2, 0, 0, 1, [0, 0], [9, 10]);
    let read = read_header(&fresh, CLUSTER_ID_HASH).unwrap();
    let (.., listed) = capture.record(&fresh, (&read.0, read.1), (0, 1), Tier::InZone, true);
    assert_eq!(listed, None);
}

/// The view lists one interval entry for each silence over 100 ms, of its
/// length.
#[test]
fn a_silence_over_100_ms_is_one_interval_row() {
    let mut capture = Capture::new(64, 4096);
    for at in [0, 5, 105, 206] {
        send(&mut capture, &ack(), (0, 1), at, None);
    }
    let written = capture.write_rows(0, 64);
    let rows = &capture.rows()[HEAD_SLOTS..];
    let kinds: Vec<(u32, f64)> = (0..written)
        .map(|r| {
            (
                (rows[r * ROW_SLOTS + 6] as u32) >> 24,
                rows[r * ROW_SLOTS + 1],
            )
        })
        .collect();
    assert_eq!(
        kinds,
        [(0, 0.0), (0, 5.0), (0, 105.0), (2, 101.0), (0, 206.0)]
    );
}

/// The filter scopes, counts per type in scope, and rebuilds over what is
/// kept; a pair of one node is unrepresentable.
#[test]
fn the_filter_scopes_and_counts() {
    let mut capture = Capture::new(64, 4096);
    send(&mut capture, &ack(), (0, 1), 0, None);
    send(&mut capture, &ack(), (1, 0), 0, Some(LostBy::Loss));
    send(
        &mut capture,
        &raw(0, GAP_OWN, 0, 1, [1, 0], [0, 0]),
        (2, 1),
        0,
        None,
    );
    capture.set_filter(Filter {
        scope: Scope::Pair {
            pair: Pair::new(0, 1).unwrap(),
            way: PairWay::Both,
        },
        types: 0,
        lost_only: false,
    });
    capture.write_rows(0, 64);
    let head = &capture.rows()[..HEAD_SLOTS];
    // view length, lost listed, listed, in scope, lost in scope
    assert_eq!(
        [head[2], head[6], head[8], head[9], head[10]],
        [2.0, 1.0, 2.0, 2.0, 1.0]
    );
    let ack_row = 11
        + MessageType::ALL
            .iter()
            .position(|&t| t == MessageType::Ack)
            .unwrap();
    assert_eq!(head[ack_row], 2.0);
    capture.set_filter(Filter {
        scope: Scope::Node {
            node: 1,
            way: super::Way::Received,
            tier: TierFilter::Any,
        },
        types: 1
            << MessageType::ALL
                .iter()
                .position(|&t| t == MessageType::GapReport)
                .unwrap(),
        lost_only: false,
    });
    capture.write_rows(0, 64);
    let head = &capture.rows()[..HEAD_SLOTS];
    assert_eq!([head[2], head[8], head[9]], [1.0, 1.0, 2.0]);
    assert!(Pair::new(3, 3).is_none());
}

/// Stand up an engine on a paused runtime with its capture in hand.
fn with_capture<F, Fut>(config: SimConfig, driver: F)
where
    F: FnOnce(mpsc::Sender<Command>, SharedCapture) -> Fut,
    Fut: Future<Output = ()>,
{
    let rt = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");
    LocalSet::new().block_on(&rt, async move {
        let (tx, rx) = mpsc::channel(16);
        let capture = Capture::shared();
        let valid = config.validate().expect("test config validates");
        let engine = tokio::task::spawn_local(run_engine(valid, capture.clone(), rx));
        driver(tx.clone(), capture).await;
        drop(tx);
        engine
            .await
            .expect("engine joined")
            .expect("engine ended cleanly");
    });
}

async fn step(tx: &mpsc::Sender<Command>, delta_ms: u64, until: StepUntil) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::Step {
        delta_ms,
        until,
        reply,
    })
    .await
    .unwrap();
    rx.await.unwrap()
}

async fn submit(tx: &mpsc::Sender<Command>, node: u32, n: u64, until: StepUntil) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::SubmitRequest {
        node,
        key: KeyHash(1),
        requests: NonZeroU64::new(n).unwrap(),
        until,
        reply,
    })
    .await
    .unwrap();
    rx.await.unwrap().expect("the burst is decided")
}

async fn link(tx: &mpsc::Sender<Command>, src: u32, dst: u32, policy: LinkPolicyKind) {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::SetLinkPolicy {
        src,
        dst,
        policy,
        reply,
    })
    .await
    .unwrap();
    rx.await.unwrap().unwrap();
}

fn config(nodes: usize, zones: u8, limit: u64) -> SimConfig {
    SimConfig {
        nodes,
        zones: zones.into(),
        tick_interval_ms: 100,
        rule_limit: limit,
        ..SimConfig::default()
    }
}

/// Every message of `events`, as (seq, src, dst, role, lost).
fn messages(events: &[crate::event::Event]) -> Vec<(u64, u32, u32, MessageRole, bool)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::PacketSent {
                seq,
                src,
                dst,
                role,
                ..
            } => Some((seq.0, src, dst, role, false)),
            EventKind::PacketDropped {
                seq,
                src,
                dst,
                role,
                ..
            } => Some((seq.0, src, dst, role, true)),
            _ => None,
        })
        .collect()
}

/// A lost data packet, revealed by the next one, is reported missing and
/// repeated, and each message's links say so both ways; a cut link's losses
/// say it was cut.
#[test]
fn a_lost_packet_links_to_its_gap_report_and_its_repeat() {
    with_capture(config(2, 1, 100), |tx, capture| async move {
        step(&tx, 1_000, StepUntil::Time).await;
        capture.borrow_mut().mark_run_start();
        link(&tx, 0, 1, LinkPolicyKind::DropFirst { count: 1 }).await;
        let mut events = Vec::new();
        for _ in 0..3 {
            events.extend(submit(&tx, 0, 1, StepUntil::Time).await.events);
            events.extend(step(&tx, 10, StepUntil::Time).await.events);
        }
        events.extend(step(&tx, 300, StepUntil::Time).await.events);
        let all = messages(&events);
        let seqs: Vec<u64> = all.iter().map(|m| m.0).collect();
        assert_eq!(
            seqs,
            (1..=all.len() as u64).collect::<Vec<_>>(),
            "seqs run from 1"
        );
        let lost = all
            .iter()
            .find(|m| m.4 && m.3 == MessageRole::OwnData)
            .expect("the first own data is lost");
        let select = |seq: u64| capture.borrow_mut().select(Some(seq));
        let detail = select(lost.0).expect("kept");
        assert_eq!(detail.fate, Fate::Lost { by: LostBy::Loss });
        let find = |r: Relation| {
            detail
                .links
                .iter()
                .find(|l| l.relation == r)
                .map(|l| l.seq.0)
        };
        let report = find(Relation::ReportedMissingBy).expect("reported missing");
        let repeat = find(Relation::RepeatedAs).expect("repeated");
        assert!(report < repeat);
        let gap = select(report).unwrap();
        assert_eq!(gap.role, MessageRole::GapReport);
        let find = |r: Relation| gap.links.iter().find(|l| l.relation == r).map(|l| l.seq.0);
        assert_eq!(find(Relation::ReportsMissing), Some(lost.0));
        assert!(find(Relation::RevealedBy).is_some_and(|s| s > lost.0 && s < report));
        assert_eq!(find(Relation::RepeatedAs), Some(repeat));
        let again = select(repeat).unwrap();
        assert_eq!(again.repeat, Some(RepeatCause::AfterGapReport));
        assert!(
            again
                .links
                .iter()
                .any(|l| l.relation == Relation::Repeats && l.seq.0 == lost.0)
        );
        // The detail's bytes are the header the shim saw, decoded alike.
        assert_eq!(again.bytes.len(), usize::from(again.wire_len));
        let (header, cells) = read_header(&again.bytes, CLUSTER_ID_HASH).unwrap();
        assert_eq!(cells, again.header.cells);
        assert_eq!(again.header.kind, PacketKind::from(header.payload));
        assert_eq!(again.cells.len(), usize::from(cells));

        // Cut the link: what it drops is lost by the cut.
        link(&tx, 0, 1, LinkPolicyKind::Block).await;
        let mut events = submit(&tx, 0, 1, StepUntil::Time).await.events;
        events.extend(step(&tx, 300, StepUntil::Time).await.events);
        let cut = messages(&events)
            .into_iter()
            .find(|m| m.4)
            .expect("a cut loss");
        let fate = capture.borrow_mut().select(Some(cut.0)).unwrap().fate;
        assert_eq!(
            fate,
            Fate::Lost {
                by: LostBy::Blocked
            }
        );
    });
}

/// A wave held between a step's halves is in flight until the next command
/// delivers it, and reading the capture delivers nothing.
#[test]
fn a_held_wave_is_in_flight_until_it_lands() {
    with_capture(config(6, 3, 1_000_000), |tx, capture| async move {
        step(&tx, 1_000, StepUntil::Time).await;
        capture.borrow_mut().mark_run_start();
        submit(&tx, 0, 25, StepUntil::Time).await;
        let held = step(&tx, 1_000, StepUntil::Event { key: KeyHash(1) }).await;
        let own = messages(&held.events);
        assert!(
            own.iter().any(|m| m.3 == MessageRole::OwnData),
            "a wave is held"
        );
        assert!(
            !held
                .events
                .iter()
                .any(|e| matches!(e.kind, EventKind::PacketDelivered { .. }))
        );
        let in_flight = |capture: &SharedCapture| {
            let mut c = capture.borrow_mut();
            c.set_filter(Filter::default());
            c.write_rows(0, 0);
            let _ = c.select(Some(own[0].0));
            c.rows()[5]
        };
        assert_eq!(in_flight(&capture), own.len() as f64);
        let landed = step(&tx, 0, StepUntil::Time).await;
        assert!(
            landed
                .events
                .iter()
                .any(|e| matches!(e.kind, EventKind::PacketDelivered { .. }))
        );
        assert_eq!(in_flight(&capture), 0.0);
        let fate = capture.borrow_mut().select(Some(own[0].0)).unwrap().fate;
        assert_eq!(fate, Fate::Delivered);
    });
}

/// A74: the same scripted run twice lists the same rows and the same bytes.
#[test]
fn the_capture_is_deterministic() {
    let run = || {
        let out = Rc::new(RefCell::new((Vec::new(), 0u64)));
        let sink = out.clone();
        with_capture(
            SimConfig {
                uniform_loss: 0.3,
                ..config(6, 3, 100)
            },
            |tx, capture| async move {
                step(&tx, 1_000, StepUntil::Time).await;
                capture.borrow_mut().mark_run_start();
                for node in [0, 3, 0] {
                    submit(&tx, node, 5, StepUntil::Time).await;
                    step(&tx, 300, StepUntil::Time).await;
                }
                let mut c = capture.borrow_mut();
                let total = c.total();
                let mut hasher = DefaultHasher::new();
                let mut rows = Vec::new();
                for seq in 1..=total {
                    let d = c.select(Some(seq)).unwrap();
                    d.bytes.hash(&mut hasher);
                    rows.push(format!(
                        "{seq} {} {} {:?} {:?} {:?} {:?} {}",
                        d.src, d.dst, d.role, d.riders, d.repeat, d.fate, d.wire_len
                    ));
                }
                *sink.borrow_mut() = (rows, hasher.finish());
            },
        );
        Rc::try_unwrap(out).unwrap().into_inner()
    };
    let (a, b) = (run(), run());
    assert!(a.0.len() > 20, "the run sends messages: {}", a.0.len());
    assert_eq!(a, b);
}
