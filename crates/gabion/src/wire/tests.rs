//! End-to-end wire codec tests.

use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use crate::crdt::{
    BucketEpoch, CellHandle, CellStore, CellStoreConfig, Count, DeltaSink, Incarnation, KeyHash,
    NodeId, NodeIdentity, Observation, ObservationBatch,
};

use super::{
    DecodeError, DecodedSummary, EncodeError, FLAG_AUTHENTICATED, FLAG_MORE, FrameLimits,
    HEADER_LEN, Header, HmacKey, PacketBuf, Packets, WireCell, WireScratch, decode_auth,
    decode_auth_visit, decode_unauth, decode_unauth_visit,
};

const MAX_ROWS: usize = 32;

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

fn local_identity() -> NodeIdentity {
    NodeIdentity::new(NodeId(0x4242), 1)
}

fn sample_header() -> Header {
    Header {
        cluster_id_hash: 0x1234_5678_9ABC_DEF0_1122_3344_5566_7788,
        sender_node_id: 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0011_2233,
        sender_incarnation: 17,
        count_width: 0,
        cell_count: 0,
        body_len: 0,
        min_origin_sequence: 0,
        max_origin_sequence: 0,
        flags: 0,
    }
}

fn store_with_cap<C: Count>(cells: u32, dicts: u16) -> CellStore<C> {
    CellStore::<C>::new(
        CellStoreConfig {
            cell_capacity: cells.max(1),
            rule_dictionary_capacity: dicts.max(1),
            node_dictionary_capacity: dicts.max(1),
            local_dirty_capacity: 128,
            forwarded_dirty_capacity: 128,
            peer_capacity: 64,
        },
        local_identity(),
    )
}

/// Build a store, ingest the rows as merge_remote observations (so each
/// row's origin is its own peer), then return the store + ordered handle
/// list.
fn seed_store<C: Count>(
    rows: &[RawRow],
    to_count: impl Fn(u32) -> C,
) -> (CellStore<C>, Vec<CellHandle>) {
    // Size dictionaries to comfortably hold every distinct identity in the
    // fixture.
    let mut store = store_with_cap::<C>((rows.len() as u32).max(64), 256);

    let mut obs = ObservationBatch::<C>::with_capacity(rows.len());
    let mut sink = DeltaSink::<C>::with_capacity(rows.len());
    for r in rows {
        obs.push(Observation {
            rule_fingerprint: r.rule_fingerprint,
            key_hash: KeyHash(r.key_hash),
            bucket: r.bucket as BucketEpoch,
            origin: NodeId(r.origin_node_id),
            incarnation: r.origin_incarnation as Incarnation,
            count: to_count(r.count),
            last_update_millis: r.last_update_millis,
        });
    }
    store.merge_remote(&obs, &mut sink);

    let mut handles = Vec::with_capacity(store.active_len() as usize);
    for h in store.active_handles() {
        handles.push(h);
    }
    (store, handles)
}

fn cell_set<C: Count>(cells: &[WireCell<C>]) -> std::collections::HashSet<CellKey> {
    cells.iter().map(CellKey::from_wire).collect()
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    rule_fingerprint: u128,
    key_hash: u128,
    bucket: u32,
    origin_node_id: u128,
    origin_incarnation: u32,
    count: u64,
    last_update_millis: u64,
    origin_sequence: u64,
}

impl CellKey {
    fn from_wire<C: Count>(cell: &WireCell<C>) -> Self {
        Self {
            rule_fingerprint: cell.rule_fingerprint,
            key_hash: cell.key_hash.0,
            bucket: cell.bucket,
            origin_node_id: cell.origin_node_id.0,
            origin_incarnation: cell.origin_incarnation,
            count: cell.count.into(),
            last_update_millis: cell.last_update_millis,
            origin_sequence: cell.origin_sequence,
        }
    }

    fn from_store<C: Count>(store: &CellStore<C>, handle: CellHandle) -> Self {
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
            count: row.count.into(),
            last_update_millis: row.last_update_millis,
            origin_sequence: row.origin_sequence,
        }
    }
}

/// Drain a `Packets` iterator into a `Vec<Vec<u8>>`, one entry per emitted
/// packet. Test-only helper — production callers send directly from the
/// `PacketBuf`.
fn drain_packets<C: Count>(mut packets: Packets<'_, C>, buf: &mut PacketBuf) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while packets.next_into(buf).expect("encode").is_some() {
        out.push(buf.as_bytes().to_vec());
    }
    out
}

// -- Roundtrip --------------------------------------------------------------

fn run_unauth_roundtrip<C: Count>(
    rows: &[RawRow],
    to_count: impl Fn(u32) -> C,
) -> Result<(), String> {
    let (store, handles) = seed_store::<C>(rows, &to_count);
    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<C>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
        .map_err(|e| format!("ctor: {e:?}"))?;
    let frames = drain_packets(packets, &mut buf);

    let want: std::collections::HashSet<CellKey> = handles
        .iter()
        .map(|&h| CellKey::from_store(&store, h))
        .collect();

    let mut got: std::collections::HashSet<CellKey> = std::collections::HashSet::new();
    for (i, frame) in frames.iter().enumerate() {
        let mut visited: Vec<WireCell<C>> = Vec::new();
        let summary = decode_unauth_visit::<C>(frame, limits, |_| true, |c| visited.push(c))
            .map_err(|e| format!("decode visit: {e:?}"))?;
        let last = i + 1 == frames.len();
        let more = summary.flags & FLAG_MORE != 0;
        if last && more {
            return Err("final packet has FLAG_MORE set".into());
        }
        if !last && !more {
            return Err("non-final packet missing FLAG_MORE".into());
        }
        if summary.cell_count as usize != visited.len() {
            return Err("summary cell_count vs visited mismatch".into());
        }
        for key in cell_set::<C>(&visited) {
            if !got.insert(key) {
                return Err("cell appeared in two packets".into());
            }
        }
    }

    if got != want {
        let diff: Vec<_> = want.difference(&got).collect();
        let extra: Vec<_> = got.difference(&want).collect();
        return Err(format!("missing={diff:?} extra={extra:?}"));
    }

    // Allocating path: decode each frame into a shared ObservationBatch.
    let mut obs = ObservationBatch::<C>::with_capacity(handles.len());
    for frame in &frames {
        decode_unauth::<C>(frame, limits, &mut obs).map_err(|e| format!("decode into: {e:?}"))?;
    }
    if obs.len() != handles.len() {
        return Err(format!(
            "allocating len mismatch {} vs {}",
            obs.len(),
            handles.len()
        ));
    }
    Ok(())
}

#[quickcheck]
fn roundtrip_unauth_u32(rows: Rows) -> bool {
    run_unauth_roundtrip::<u32>(&rows.0, |c| c).is_ok()
}

#[quickcheck]
fn roundtrip_unauth_u16(rows: Rows) -> bool {
    run_unauth_roundtrip::<u16>(&rows.0, |c| (c & 0xFFFF) as u16).is_ok()
}

#[quickcheck]
fn roundtrip_unauth_u64(rows: Rows) -> bool {
    run_unauth_roundtrip::<u64>(&rows.0, |c| c as u64).is_ok()
}

#[quickcheck]
fn roundtrip_auth_u32(rows: Rows, key_bytes: Vec<u8>) -> bool {
    let mut k = [0u8; 32];
    for (slot, src) in k.iter_mut().zip(key_bytes.iter()) {
        *slot = *src;
    }
    let key = HmacKey(k);
    let (store, handles) = seed_store::<u32>(&rows.0, |c| c);
    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<u32>::auth(
        sample_header(),
        &store,
        &handles,
        &key,
        &mut scratch,
        limits,
    )
    .expect("ctor");
    let frames = drain_packets(packets, &mut buf);

    let mut obs = ObservationBatch::<u32>::with_capacity(handles.len());
    for frame in &frames {
        if decode_auth::<u32>(frame, &key, limits, &mut obs).is_err() {
            return false;
        }
    }
    obs.len() == handles.len()
}

#[quickcheck]
fn tampering_rejects(rows: Rows, byte_idx: usize) -> bool {
    if rows.0.is_empty() {
        return true;
    }
    let key = HmacKey([5u8; 32]);
    let (store, handles) = seed_store::<u32>(&rows.0, |c| c);
    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<u32>::auth(
        sample_header(),
        &store,
        &handles,
        &key,
        &mut scratch,
        limits,
    )
    .expect("ctor");
    let mut frames = drain_packets(packets, &mut buf);
    if frames.is_empty() {
        return true;
    }
    // Tamper with one byte in the first emitted packet.
    let frame = &mut frames[0];
    if frame.is_empty() {
        return true;
    }
    let i = byte_idx % frame.len();
    frame[i] ^= 0xA5;
    let mut obs = ObservationBatch::<u32>::with_capacity(rows.0.len());
    matches!(
        decode_auth::<u32>(frame, &key, limits, &mut obs),
        Err(DecodeError::AuthenticationFailed),
    )
}

// -- Multi-packet boundaries -------------------------------------------------

/// Build a store with `n` distinct origin nodes (one cell each) so the
/// 255-slot node-dict cap forces a packet split.
fn build_many_origins(n: u32) -> (CellStore<u32>, Vec<CellHandle>) {
    let mut store = CellStore::<u32>::new(
        CellStoreConfig {
            cell_capacity: n.max(1) * 2,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: ((n as u16) + 8).max(2),
            local_dirty_capacity: 256,
            forwarded_dirty_capacity: 256,
            peer_capacity: 64,
        },
        local_identity(),
    );
    let mut obs = ObservationBatch::<u32>::with_capacity(n as usize);
    let mut sink = DeltaSink::<u32>::with_capacity(n as usize);
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
    let handles: Vec<CellHandle> = store.active_handles().collect();
    (store, handles)
}

#[test]
fn packets_split_on_intern_cap() {
    let n: u32 = 300;
    let (store, handles) = build_many_origins(n);
    assert_eq!(handles.len() as u32, n);

    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits {
        max_payload_bytes: 256 * 1024,
        max_cells: 4096,
    };
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
        .expect("ctor");
    let frames = drain_packets(packets, &mut buf);
    assert!(
        frames.len() >= 2,
        "expected ≥2 packets, got {}",
        frames.len()
    );

    let mut total_cells = 0_u32;
    for (i, frame) in frames.iter().enumerate() {
        let mut cells: Vec<WireCell<u32>> = Vec::new();
        let summary =
            decode_unauth_visit::<u32>(frame, limits, |_| true, |c| cells.push(c)).expect("decode");
        // Each packet must declare a node_dict with <=255 slots; reading
        // the body re-validates this via slot-index bounds checks during
        // the visit above, which never panicked. As a stronger structural
        // check, peek at the dict length byte after the header.
        let body_start = HEADER_LEN;
        // body layout: rule_dict_len, rule_dict, node_dict_len, node_dict, ...
        let rule_dict_len = frame[body_start] as usize;
        let node_dict_len_off = body_start + 1 + 16 * rule_dict_len;
        let node_dict_len = frame[node_dict_len_off] as usize;
        assert!(node_dict_len <= 255);
        assert!(rule_dict_len <= 255);

        total_cells += cells.len() as u32;
        let last = i + 1 == frames.len();
        let more = summary.flags & FLAG_MORE != 0;
        assert_eq!(more, !last, "FLAG_MORE mismatch on packet {i}");
    }
    assert_eq!(total_cells, n);
}

#[test]
fn packets_split_on_udp_budget() {
    let n: u32 = 1024;
    let (store, handles) = build_many_origins(n);
    assert_eq!(handles.len() as u32, n);

    // Hand-pick a budget that fits ~32 cells per packet so we get many
    // packets. Cells contribute 42+4=46 bytes plus dict cost; for the
    // wide-fanout fixture each cell carries a unique node id (16 B), so
    // per-cell amortized cost is ~62 bytes once the rule slot is shared.
    // Set a budget around 2 KiB and let the encoder choose.
    let limits = FrameLimits {
        max_payload_bytes: 2048,
        max_cells: 4096,
    };
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
        .expect("ctor");
    let frames = drain_packets(packets, &mut buf);
    assert!(
        frames.len() >= 2,
        "expected ≥2 packets, got {}",
        frames.len()
    );

    let mut total_cells = 0_u32;
    for (i, frame) in frames.iter().enumerate() {
        assert!(frame.len() <= limits.max_payload_bytes);
        let mut cells: Vec<WireCell<u32>> = Vec::new();
        let summary =
            decode_unauth_visit::<u32>(frame, limits, |_| true, |c| cells.push(c)).expect("decode");
        total_cells += cells.len() as u32;
        let last = i + 1 == frames.len();
        let more = summary.flags & FLAG_MORE != 0;
        assert_eq!(more, !last);
    }
    assert_eq!(total_cells, n);
}

#[test]
fn flag_more_unset_on_only_packet() {
    let rows: Vec<RawRow> = (0..2)
        .map(|i| RawRow {
            rule_fingerprint: 0xAB + i as u128,
            key_hash: 1 + i as u128,
            bucket: 0,
            origin_node_id: 0x500 + i as u128,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 1000,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        let pkt = packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(pkt.flags & FLAG_MORE, 0);
        assert_eq!(pkt.cells_emitted, handles.len() as u32);
        assert_eq!(packets.remaining(), 0);
    }
}

#[test]
fn mid_batch_packet_decodes_in_isolation() {
    // Use the high-fanout fixture so the split happens by node-dict cap.
    let n: u32 = 300;
    let (store, handles) = build_many_origins(n);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    let packets = Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
        .expect("ctor");
    let frames = drain_packets(packets, &mut buf);
    assert!(frames.len() >= 2);

    // Decode only the second packet — it must be fully usable on its own.
    let frame = &frames[1];
    let mut obs = ObservationBatch::<u32>::with_capacity(n as usize);
    let summary = decode_unauth::<u32>(frame, limits, &mut obs).expect("isolated decode ok");
    assert!(summary.cell_count > 0);
    assert_eq!(obs.len(), summary.cell_count as usize);
}

#[test]
fn decoder_rejects_bad_slot() {
    let rows: Vec<RawRow> = (0..3)
        .map(|i| RawRow {
            rule_fingerprint: 0x1234,
            key_hash: i as u128,
            bucket: 0,
            origin_node_id: 0x7777,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 100,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }
    let mut frame = buf.as_bytes().to_vec();

    // Reach the rule_slot column. Body starts at HEADER_LEN; layout is:
    // [rule_dict_len][rule_dict_bytes][node_dict_len][node_dict_bytes][rule_slot×N]...
    let body_start = HEADER_LEN;
    let rule_dict_len = frame[body_start] as usize;
    let node_dict_len_off = body_start + 1 + 16 * rule_dict_len;
    let node_dict_len = frame[node_dict_len_off] as usize;
    let rule_slot_off = node_dict_len_off + 1 + 16 * node_dict_len;
    // Set first cell's rule_slot to R (out of range).
    frame[rule_slot_off] = rule_dict_len as u8;

    let mut obs = ObservationBatch::<u32>::with_capacity(rows.len());
    let err = decode_unauth::<u32>(&frame, limits, &mut obs).unwrap_err();
    assert_eq!(err, DecodeError::BadSlot);
}

#[test]
fn decoder_rejects_truncated_dict() {
    let rows: Vec<RawRow> = (0..2)
        .map(|i| RawRow {
            rule_fingerprint: 0x9999,
            key_hash: i as u128,
            bucket: 0,
            origin_node_id: 0x1234,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 100,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }
    let mut frame = buf.as_bytes().to_vec();
    // Drop one byte from the dict region — header.body_len now disagrees
    // with the actual length.
    let drop_at = HEADER_LEN + 2; // middle of rule_dict
    frame.remove(drop_at);
    // Patch body_len in the header to match actual length so the outer
    // total-required check passes; the inner dict slicing will fail.
    let new_body_len = (frame.len() - HEADER_LEN) as u32;
    frame[52..56].copy_from_slice(&new_body_len.to_le_bytes());

    let mut obs = ObservationBatch::<u32>::with_capacity(rows.len());
    let err = decode_unauth::<u32>(&frame, limits, &mut obs).unwrap_err();
    assert_eq!(err, DecodeError::BodyLenMismatch);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "Packets dropped with")]
fn packets_drop_undrained_panics_in_debug() {
    let rows: Vec<RawRow> = (0..4)
        .map(|i| RawRow {
            rule_fingerprint: 0xAA,
            key_hash: i as u128,
            bucket: 0,
            origin_node_id: 0xBB,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 0,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let _packets = Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
        .expect("ctor");
    // Drop without calling next_into — Drop's debug-assert must fire.
}

// -- Outer guardrails -------------------------------------------------------

#[test]
fn budget_too_small_rejected_at_constructor() {
    let rows: Vec<RawRow> = vec![RawRow {
        rule_fingerprint: 1,
        key_hash: 2,
        bucket: 0,
        origin_node_id: 3,
        origin_incarnation: 1,
        count: 4,
        last_update_millis: 5,
    }];
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits {
        max_payload_bytes: HEADER_LEN, // no room for body
        max_cells: 4,
    };
    match Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits) {
        Err(EncodeError::BudgetTooSmall) => {}
        Ok(_) => panic!("expected BudgetTooSmall"),
    }
}

#[test]
fn empty_handles_emits_zero_packets() {
    let store = store_with_cap::<u32>(8, 8);
    let mut scratch = WireScratch::for_store(&store);
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    let mut packets =
        Packets::<u32>::unauth(sample_header(), &store, &[], &mut scratch, limits).expect("ctor");
    assert!(packets.next_into(&mut buf).expect("encode").is_none());
}

#[test]
fn count_width_mismatch_is_rejected() {
    let rows = vec![RawRow {
        rule_fingerprint: 0xAB,
        key_hash: 0xCD,
        bucket: 0,
        origin_node_id: 0xE0,
        origin_incarnation: 1,
        count: 7,
        last_update_millis: 99,
    }];
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }
    let mut obs = ObservationBatch::<u64>::with_capacity(1);
    let err = decode_unauth::<u64>(buf.as_bytes(), limits, &mut obs).unwrap_err();
    assert_eq!(err, DecodeError::BadCountWidth);
}

#[test]
fn visitor_accept_header_short_circuit() {
    let rows = vec![RawRow {
        rule_fingerprint: 1,
        key_hash: 2,
        bucket: 0,
        origin_node_id: 3,
        origin_incarnation: 1,
        count: 4,
        last_update_millis: 5,
    }];
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        let pkt = packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(pkt.cells_emitted, 1);
        assert_eq!(packets.remaining(), 0);
    }
    let mut visited = 0_usize;
    let _summary: DecodedSummary =
        decode_unauth_visit::<u32>(buf.as_bytes(), limits, |_h| false, |_c| visited += 1)
            .expect("decode");
    assert_eq!(visited, 0);
}

#[test]
fn frame_too_short_reported() {
    let mut obs = ObservationBatch::<u32>::with_capacity(0);
    let limits = FrameLimits::default();
    let err = decode_unauth::<u32>(&[0u8; HEADER_LEN - 1], limits, &mut obs).unwrap_err();
    assert_eq!(err, DecodeError::TooShort);
}

#[test]
fn frame_too_large_reported() {
    let limits = FrameLimits {
        max_payload_bytes: 16,
        max_cells: 4,
    };
    let mut obs = ObservationBatch::<u32>::with_capacity(0);
    let err = decode_unauth::<u32>(&[0u8; 128], limits, &mut obs).unwrap_err();
    assert_eq!(err, DecodeError::PayloadTooLarge);
}

#[test]
fn body_len_mismatch_reported() {
    let rows = vec![RawRow {
        rule_fingerprint: 1,
        key_hash: 2,
        bucket: 0,
        origin_node_id: 3,
        origin_incarnation: 1,
        count: 4,
        last_update_millis: 5,
    }];
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }
    let mut frame = buf.as_bytes().to_vec();
    frame.pop();
    let mut obs = ObservationBatch::<u32>::with_capacity(0);
    let err = decode_unauth::<u32>(&frame, limits, &mut obs).unwrap_err();
    assert!(matches!(
        err,
        DecodeError::BodyLenMismatch | DecodeError::Trailing
    ));
}

#[test]
fn header_flags_track_auth() {
    let rows: Vec<RawRow> = (0..2)
        .map(|i| RawRow {
            rule_fingerprint: 0xAABB ^ i as u128,
            key_hash: 1 + i as u128,
            bucket: 0,
            origin_node_id: 0x500 + i as u128,
            origin_incarnation: 1,
            count: 1,
            last_update_millis: 1000,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let key = HmacKey([3u8; 32]);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets = Packets::<u32>::auth(
            sample_header(),
            &store,
            &handles,
            &key,
            &mut scratch,
            limits,
        )
        .expect("ctor");
        let pkt = packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(pkt.flags & FLAG_AUTHENTICATED, FLAG_AUTHENTICATED);
        assert_eq!(pkt.flags & FLAG_MORE, 0);
        assert_eq!(packets.remaining(), 0);
    }

    let mut obs = ObservationBatch::<u32>::with_capacity(2);
    let summary = decode_auth::<u32>(buf.as_bytes(), &key, limits, &mut obs).expect("decode");
    assert_eq!(
        summary.header.flags & FLAG_AUTHENTICATED,
        FLAG_AUTHENTICATED
    );
    assert_eq!(summary.flags & FLAG_MORE, 0);
}

#[test]
fn auth_visit_works() {
    let key = HmacKey([9u8; 32]);
    let rows: Vec<RawRow> = (0..3)
        .map(|i| RawRow {
            rule_fingerprint: i as u128,
            key_hash: (i as u128) + 1,
            bucket: i,
            origin_node_id: 0x500 + i as u128,
            origin_incarnation: 1,
            count: 1 + i,
            last_update_millis: 1234,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets = Packets::<u32>::auth(
            sample_header(),
            &store,
            &handles,
            &key,
            &mut scratch,
            limits,
        )
        .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }

    let mut count = 0_usize;
    let summary =
        decode_auth_visit::<u32>(buf.as_bytes(), &key, limits, |_h| true, |_c| count += 1)
            .expect("decode");
    assert_eq!(count, summary.cell_count as usize);
    assert_eq!(count, rows.len());
}

#[test]
fn visitor_matches_allocating_for_curated_rows() {
    let rows: Vec<RawRow> = (0..5)
        .map(|i| RawRow {
            rule_fingerprint: 100 + i as u128,
            key_hash: 200 + i as u128,
            bucket: i,
            origin_node_id: 0x1000 + i as u128,
            origin_incarnation: 1,
            count: 7 + i,
            last_update_millis: 9000 + i as u64,
        })
        .collect();
    let (store, handles) = seed_store::<u32>(&rows, |c| c);
    let limits = FrameLimits::default();
    let mut scratch = WireScratch::for_store(&store);
    let mut buf = PacketBuf::for_limits(limits);
    {
        let mut packets =
            Packets::<u32>::unauth(sample_header(), &store, &handles, &mut scratch, limits)
                .expect("ctor");
        packets.next_into(&mut buf).expect("encode").expect("some");
        assert_eq!(packets.remaining(), 0);
    }

    let mut visited: Vec<WireCell<u32>> = Vec::new();
    decode_unauth_visit::<u32>(buf.as_bytes(), limits, |_h| true, |c| visited.push(c))
        .expect("decode");

    let mut obs = ObservationBatch::<u32>::with_capacity(5);
    decode_unauth::<u32>(buf.as_bytes(), limits, &mut obs).expect("decode");

    assert_eq!(visited.len(), obs.len());
    for (i, cell) in visited.iter().enumerate() {
        assert_eq!(cell.rule_fingerprint, obs.rule_fingerprints[i]);
        assert_eq!(cell.key_hash, obs.key_hashes[i]);
        assert_eq!(cell.bucket, obs.buckets[i]);
        assert_eq!(cell.origin_node_id, obs.origin_node_ids[i]);
        assert_eq!(cell.origin_incarnation, obs.incarnations[i]);
        assert_eq!(cell.count, obs.counts[i]);
        assert_eq!(cell.last_update_millis, obs.last_update_millis[i]);
    }
}
