//! End-to-end wire codec tests.

use std::collections::HashSet;
use std::num::NonZeroU128;

use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use crate::crdt::{
    CellHandle, CellStore, DeltaSink, KeyHash, NodeId, NodeIdentity, Observation, ObservationBatch,
};
use crate::settings::mock::{rules, storage};

use super::{
    AckInfo, CellPayload, DecodeError, FrameLimits, HEADER_LEN, Header, MAGIC, MIN_PAYLOAD_BYTES,
    PacketBuf, Packets, Payload, StreamRange, WireScratch, decode, write_header,
};

const MAX_ROWS: usize = 32;
const CLUSTER: NonZeroU128 = NonZeroU128::new(0x1234_5678_9ABC_DEF0_1122_3344_5566_7788).unwrap();
const FOREIGN: NonZeroU128 = NonZeroU128::new(0xC2).unwrap();

#[derive(Clone, Debug)]
struct RawRow {
    rule_fingerprint: u128,
    key_hash: u128,
    bucket: u32,
    origin_node_id: u128,
    origin_incarnation: u32,
    count: u32,
    last_update_millis: u64,
}

impl Arbitrary for RawRow {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rule_fingerprint: u128::arbitrary(g),
            key_hash: u128::arbitrary(g),
            bucket: u32::arbitrary(g),
            origin_node_id: u128::arbitrary(g),
            origin_incarnation: u32::arbitrary(g),
            count: u32::arbitrary(g),
            last_update_millis: u64::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct Rows(Vec<RawRow>);

impl Arbitrary for Rows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % MAX_ROWS;
        Self((0..len).map(|_| RawRow::arbitrary(g)).collect())
    }
}

/// `n` rows of one rule and one origin, so a packet holds the most cells.
fn dense_rows(n: u128) -> Vec<RawRow> {
    (0..n)
        .map(|i| RawRow {
            rule_fingerprint: 0xAA,
            key_hash: 0xBEEF + i,
            bucket: 0,
            origin_node_id: 0x1_0000,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 1_000,
        })
        .collect()
}

fn local_identity() -> NodeIdentity {
    NodeIdentity::new(NodeId(0x4242), 1)
}

/// The header every test packet carries, as sent or as decoded.
fn sample_header<P: From<CellPayload>>() -> Header<P> {
    Header {
        cluster_id_hash: CLUSTER,
        sender: NodeIdentity::new(NodeId(0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0011_2233), 17),
        sender_zone: 0,
        acks: AckInfo::default(),
        payload: CellPayload::Sweep.into(),
    }
}

fn store_with_cap(cells: u32, dicts: u16) -> CellStore {
    CellStore::new(storage(cells, dicts), local_identity(), &rules(dicts))
}

/// Build a store, ingest the rows as merge_remote observations (so each
/// row's origin is its own peer), then return the store + ordered handle
/// list.
fn seed_store(rows: &[RawRow]) -> (CellStore, Vec<CellHandle>) {
    // Size dictionaries to comfortably hold every distinct identity in the
    // fixture.
    let mut store = store_with_cap((rows.len() as u32).max(64), 256);

    let mut obs = ObservationBatch::with_capacity(rows.len());
    let mut sink = DeltaSink::with_capacity(rows.len());
    for r in rows {
        obs.push(Observation {
            rule_fingerprint: r.rule_fingerprint,
            key_hash: KeyHash(r.key_hash),
            bucket: r.bucket,
            origin: NodeId(r.origin_node_id),
            incarnation: r.origin_incarnation,
            count: r.count,
            last_update_millis: r.last_update_millis,
        });
    }
    store.merge_remote(&obs, &mut sink);
    let handles = store.active_handles(0).collect();
    (store, handles)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    rule_fingerprint: u128,
    key_hash: u128,
    bucket: u32,
    origin_node_id: u128,
    origin_incarnation: u32,
    count: u32,
    last_update_millis: u64,
}

impl CellKey {
    fn from_wire(cell: &Observation) -> Self {
        Self {
            rule_fingerprint: cell.rule_fingerprint,
            key_hash: cell.key_hash.0,
            bucket: cell.bucket,
            origin_node_id: cell.origin.0,
            origin_incarnation: cell.incarnation,
            count: cell.count,
            last_update_millis: cell.last_update_millis,
        }
    }

    fn from_store(store: &CellStore, handle: CellHandle) -> Self {
        let row = store.get(handle).expect("handle valid");
        let rule = store
            .rule_dictionary()
            .descriptor(row.key.rule)
            .expect("rule");
        let node = store
            .node_dictionary()
            .descriptor(row.key.origin)
            .expect("node");
        Self {
            rule_fingerprint: rule.fingerprint,
            key_hash: row.key.key_hash.0,
            bucket: row.key.bucket,
            origin_node_id: node.node_id.0,
            origin_incarnation: row.key.incarnation,
            count: row.count,
            last_update_millis: row.last_update_millis,
        }
    }
}

/// Encode `handles` within `limits`, one entry per packet. Test-only
/// helper — production callers send directly from the `PacketBuf`.
fn encode(store: &CellStore, handles: &[CellHandle], limits: FrameLimits) -> Vec<Vec<u8>> {
    let mut scratch = WireScratch::for_store(store);
    let mut buf = PacketBuf::for_limits(limits);
    let mut packets = Packets::new(sample_header(), store, handles, &mut scratch, limits);
    let mut out = Vec::new();
    while packets.next_into(&mut buf).is_some() {
        out.push(buf.as_bytes().to_vec());
    }
    out
}

fn decode_cells(frame: &[u8], limits: FrameLimits) -> Result<Vec<Observation>, DecodeError> {
    let mut cells = Vec::new();
    let header = decode(frame, limits, CLUSTER, |cell| cells.push(cell))?;
    assert_eq!(header, sample_header::<Payload>());
    Ok(cells)
}

/// The byte offset of the first cell's `rule_slot`.
fn rule_slot_offset(frame: &[u8]) -> usize {
    let rule_dict_len = usize::from(frame[HEADER_LEN]);
    let node_dict_len_off = HEADER_LEN + 1 + 16 * rule_dict_len;
    node_dict_len_off + 1 + 16 * usize::from(frame[node_dict_len_off])
}

// -- Roundtrip --------------------------------------------------------------

/// Every cell of a frame arrives in exactly one packet, unchanged, with the
/// header it was sent with.
#[quickcheck]
fn packets_round_trip(rows: Rows) -> Result<(), String> {
    let (store, handles) = seed_store(&rows.0);
    let limits = FrameLimits::default();
    let want: HashSet<CellKey> = handles
        .iter()
        .map(|&h| CellKey::from_store(&store, h))
        .collect();
    let mut got = HashSet::new();
    for frame in encode(&store, &handles, limits) {
        let cells = decode_cells(&frame, limits).map_err(|e| format!("decode: {e:?}"))?;
        for cell in &cells {
            if !got.insert(CellKey::from_wire(cell)) {
                return Err("cell appeared in two packets".into());
            }
        }
    }
    if got != want {
        let missing: Vec<_> = want.difference(&got).collect();
        let extra: Vec<_> = got.difference(&want).collect();
        return Err(format!("missing={missing:?} extra={extra:?}"));
    }
    Ok(())
}

/// Each body column sits where the wire format puts it.
#[test]
fn body_layout_is_pinned() {
    let row = RawRow {
        rule_fingerprint: 0xA1,
        key_hash: 0xB2,
        bucket: 0xC3,
        origin_node_id: 0xD4,
        origin_incarnation: 0xE5,
        count: 0xF6,
        last_update_millis: 0x17,
    };
    let (store, handles) = seed_store(&[row]);
    let packet = encode(&store, &handles, FrameLimits::default()).remove(0);
    let mut body = vec![1];
    body.extend(0xA1_u128.to_le_bytes());
    body.push(1);
    body.extend(0xD4_u128.to_le_bytes());
    body.push(0);
    body.extend(0xB2_u128.to_le_bytes());
    body.extend(0xC3_u32.to_le_bytes());
    body.push(0);
    body.extend(0xE5_u32.to_le_bytes());
    body.extend(0x17_u64.to_le_bytes());
    body.extend(0xF6_u32.to_le_bytes());
    assert_eq!(packet[HEADER_LEN..], body);
}

// -- Multi-packet boundaries -------------------------------------------------

/// Build a store with `n` distinct origin nodes (one cell each) so the
/// 255-slot node-dict cap forces a packet split.
fn build_many_origins(n: u32) -> (CellStore, Vec<CellHandle>) {
    let mut store = CellStore::new(storage(n * 2, n as u16 + 8), local_identity(), &rules(4));
    let mut obs = ObservationBatch::with_capacity(n as usize);
    let mut sink = DeltaSink::with_capacity(n as usize);
    for i in 0..n {
        obs.push(Observation {
            rule_fingerprint: 0xAA_u128, // single shared rule fingerprint
            key_hash: KeyHash(0xBEEF + i as u128),
            bucket: 0,
            origin: NodeId(0x1_0000 + i as u128), // distinct origin per row
            incarnation: 1,
            count: 1 + i,
            last_update_millis: 1_000 + i as u64,
        });
    }
    store.merge_remote(&obs, &mut sink);
    let handles: Vec<CellHandle> = store.active_handles(0).collect();
    (store, handles)
}

#[test]
fn packets_split_on_intern_cap() {
    let n: u32 = 300;
    let (store, handles) = build_many_origins(n);
    assert_eq!(handles.len() as u32, n);

    let limits = FrameLimits::default();
    let frames = encode(&store, &handles, limits);
    assert!(
        frames.len() >= 2,
        "expected ≥2 packets, got {}",
        frames.len()
    );

    let mut total_cells = 0;
    for frame in &frames {
        total_cells += decode_cells(frame, limits).expect("decode").len();
    }
    assert_eq!(total_cells, n as usize);
    assert_eq!(
        frames[0][HEADER_LEN + 17],
        255,
        "the origin dictionary's cap split the frame"
    );
}

#[test]
fn packets_split_on_udp_budget() {
    let n: u32 = 1024;
    let (store, handles) = build_many_origins(n);
    assert_eq!(handles.len() as u32, n);

    // Each cell carries a unique node id here, so it costs its 38 bytes plus
    // 16 of dictionary; a 2 KiB budget splits the frame into many packets.
    let limits = FrameLimits::new(2048).unwrap();
    let frames = encode(&store, &handles, limits);
    assert!(
        frames.len() >= 2,
        "expected ≥2 packets, got {}",
        frames.len()
    );

    let mut total_cells = 0;
    for frame in &frames {
        assert!(frame.len() <= limits.max_payload_bytes);
        total_cells += decode_cells(frame, limits).expect("decode").len();
    }
    assert_eq!(total_cells, n as usize);
}

/// A cell costs 38 bytes, so a 1,400-byte packet holds 33 cells of one rule
/// and one origin, and `cell_bound` says so. A packet claiming one cell
/// more than its bytes hold fails the exact-length check.
#[test]
fn cell_costs_38_bytes_and_packet_holds_33() {
    let (store, handles) = seed_store(&dense_rows(64));
    let limits = FrameLimits::new(1400).unwrap();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    let mut packets = Packets::new(sample_header(), &store, &handles, &mut scratch, limits);
    assert_eq!(packets.next_into(&mut buf), Some(33));
    assert_eq!(buf.as_bytes().len(), 86 + 34 + 33 * 38);
    // The bound steps at 120 + 38k bytes: 1,411 holds 33 cells, 1,412 holds 34.
    for (bytes, cells) in [(1400, 33), (1411, 33), (1412, 34)] {
        assert_eq!(FrameLimits::new(bytes).unwrap().cell_bound(), cells);
    }

    let one = encode(&store, &handles[..1], limits);
    let two = encode(&store, &handles[..2], limits);
    assert_eq!(two[0].len() - one[0].len(), 38);

    let mut padded = buf.as_bytes().to_vec();
    padded[48..50].copy_from_slice(&34_u16.to_le_bytes());
    padded.resize(1400, 0);
    assert_eq!(
        decode(&padded, limits, CLUSTER, |_| {}),
        Err(DecodeError::BodyLength)
    );
}

/// An out-of-range slot drops the packet before any of its cells is handed
/// over, wherever the slot sits.
#[test]
fn decoder_rejects_bad_slot() {
    let (store, handles) = seed_store(&dense_rows(3));
    let limits = FrameLimits::default();
    let frame = encode(&store, &handles, limits).remove(0);
    let offset = rule_slot_offset(&frame);
    let node_dict_len = frame[HEADER_LEN + 1 + 16 * usize::from(frame[HEADER_LEN])];
    // The first cell's rule_slot set to R; the last cell's node_slot set to D.
    for (at, slot) in [
        (offset, frame[HEADER_LEN]),
        (offset + 21 * 3 + 2, node_dict_len),
    ] {
        let mut bad = frame.clone();
        bad[at] = slot;
        let mut seen = 0;
        assert_eq!(
            decode(&bad, limits, CLUSTER, |_| seen += 1),
            Err(DecodeError::BadSlot)
        );
        assert_eq!(seen, 0, "no cell is handed over before the error");
    }
}

#[test]
fn decoder_rejects_truncated_dict() {
    let (store, handles) = seed_store(&dense_rows(2));
    let limits = FrameLimits::default();
    let mut frame = encode(&store, &handles, limits).remove(0);
    // Drop one byte from the middle of the rule dictionary.
    frame.remove(HEADER_LEN + 2);
    assert_eq!(
        decode(&frame, limits, CLUSTER, |_| {}),
        Err(DecodeError::BodyLength)
    );
}

// -- Decode order -----------------------------------------------------------

/// Each row breaks two checks; the decoder reports the earlier one.
#[test]
fn decode_checks_run_in_order() {
    let foreign = |cells| {
        write_header(
            &Header {
                cluster_id_hash: FOREIGN,
                ..sample_header::<CellPayload>()
            },
            cells,
        )
        .to_vec()
    };
    let set = |mut bytes: Vec<u8>, at: usize, value: &[u8]| {
        bytes[at..at + value.len()].copy_from_slice(value);
        bytes
    };
    let version_1 = 1_u16.to_le_bytes();
    let (store, handles) = seed_store(&dense_rows(1));
    let mut bad_slot_and_trailing = encode(&store, &handles, FrameLimits::default()).remove(0);
    let offset = rule_slot_offset(&bad_slot_and_trailing);
    bad_slot_and_trailing[offset] = 1;
    bad_slot_and_trailing.push(0);

    let limits = FrameLimits::default();
    let rows: [(&str, FrameLimits, Vec<u8>, DecodeError); 9] = [
        (
            "too large and bad magic",
            FrameLimits::new(MIN_PAYLOAD_BYTES).unwrap(),
            vec![0; MIN_PAYLOAD_BYTES + 1],
            DecodeError::PayloadTooLarge,
        ),
        (
            "too short and bad magic",
            limits,
            vec![0; 10],
            DecodeError::TooShort,
        ),
        (
            "bad magic and version 1",
            limits,
            set(set(foreign(0), 0, &[0; 4]), 4, &version_1),
            DecodeError::BadMagic,
        ),
        (
            "version 1 and kind 9",
            limits,
            set(set(foreign(0), 6, &[9]), 4, &version_1),
            DecodeError::BadVersion { found: 1 },
        ),
        (
            "kind 7 and foreign cluster",
            limits,
            set(foreign(0), 6, &[7]),
            DecodeError::BadKind,
        ),
        (
            "reserved flag and foreign cluster",
            limits,
            set(foreign(0), 7, &[0x80]),
            DecodeError::BadFlags,
        ),
        (
            "ranged sweep and foreign cluster",
            limits,
            set(foreign(0), 70, &1_u64.to_le_bytes()),
            DecodeError::BadRange,
        ),
        (
            "foreign cluster and a missing body",
            limits,
            foreign(1),
            DecodeError::ForeignCluster {
                found: FOREIGN.get(),
            },
        ),
        (
            "trailing byte and bad slot",
            limits,
            bad_slot_and_trailing,
            DecodeError::BodyLength,
        ),
    ];
    for (name, limits, bytes, expected) in rows {
        assert_eq!(
            decode(&bytes, limits, CLUSTER, |_| {}),
            Err(expected),
            "{name}"
        );
    }
}

/// A version 1 packet is reported as another version, not as garbage.
#[test]
fn v1_packet_is_bad_version_not_garbage() {
    let mut packet = Vec::with_capacity(152);
    packet.extend_from_slice(&MAGIC.to_le_bytes());
    packet.extend_from_slice(&1_u16.to_le_bytes()); // version
    packet.extend_from_slice(&0_u16.to_le_bytes()); // flags
    packet.extend_from_slice(&CLUSTER.get().to_le_bytes());
    packet.extend_from_slice(&0xAAAA_u128.to_le_bytes()); // sender node id
    packet.extend_from_slice(&7_u32.to_le_bytes()); // sender incarnation
    packet.extend_from_slice(&4_u32.to_le_bytes()); // count width
    packet.extend_from_slice(&1_u32.to_le_bytes()); // cell count
    packet.extend_from_slice(&80_u32.to_le_bytes()); // body length
    packet.extend_from_slice(&[0; 16]); // min and max origin sequence
    packet.push(1); // one rule
    packet.extend_from_slice(&0xAA_u128.to_le_bytes());
    packet.push(1); // one origin
    packet.extend_from_slice(&0xAAAA_u128.to_le_bytes());
    packet.resize(152, 0); // one 46-byte cell
    assert_eq!(
        decode(&packet, FrameLimits::default(), CLUSTER, |_| {}),
        Err(DecodeError::BadVersion { found: 1 })
    );
}

// -- Outer guardrails -------------------------------------------------------

#[test]
fn empty_handles_emits_zero_packets() {
    let store = store_with_cap(8, 8);
    assert!(encode(&store, &[], FrameLimits::default()).is_empty());
}

/// An own-stream frame's packets tile its range: the first starts at the
/// frame's lower end, each starts where the one before ended and ends at its
/// last cell's sequence, and the last ends at the frame's upper end, so a
/// lost packet leaves a gap the receiver sees.
#[test]
fn own_frame_packets_tile_its_range() {
    let mut store = store_with_cap(128, 8);
    let mut obs = ObservationBatch::with_capacity(70);
    for key in 0..70 {
        obs.push(Observation {
            rule_fingerprint: 0xAA,
            key_hash: KeyHash(key),
            bucket: 0,
            origin: NodeId(0),
            incarnation: 0,
            count: 1,
            last_update_millis: 1_000,
        });
    }
    store.ingest_local(&obs, &mut DeltaSink::with_capacity(70));
    let mut handles = Vec::with_capacity(70);
    store.stream_after(0, 70, &mut handles);
    // Four cells a packet: the header, both dictionaries and four cells.
    let limits = FrameLimits::new(HEADER_LEN + 2 + 32 + 4 * 38).unwrap();
    let header = Header {
        payload: CellPayload::Own {
            range: StreamRange::new(0, 80).unwrap(),
            relay_into_zone: false,
            probe: false,
        },
        ..sample_header::<CellPayload>()
    };
    let mut scratch = WireScratch::for_store(&store);
    let mut packets = Packets::new(header, &store, &handles, &mut scratch, limits);
    let mut buf = PacketBuf::for_limits(limits);
    let mut lower = 0;
    let mut sent = 0;
    while packets.next_into(&mut buf).is_some() {
        let mut last_key = 0;
        let decoded = decode(buf.as_bytes(), limits, CLUSTER, |cell| {
            last_key = cell.key_hash.0 as u64;
            sent += 1;
        })
        .unwrap();
        let Payload::Cells(CellPayload::Own { range, .. }) = decoded.payload else {
            panic!("an own frame sends own packets");
        };
        assert_eq!(range.lower(), lower);
        // Key `k` was the (k + 1)th cell issued.
        let expected = if sent == 70 { 80 } else { last_key + 1 };
        assert_eq!(range.upper(), expected);
        lower = range.upper();
    }
    assert_eq!((sent, lower, packets.closed_through()), (70, 80, 80));
}
