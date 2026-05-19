use super::*;
use crate::core::{DescriptorMatcher, LocalEngine, Rule, RuleTable, hash_domain, hash_key};
use crate::{
    Decision, Descriptor, EnforcementMode, LimitRequest, OverflowPolicy, SafetyMargin, WindowSpec,
};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
struct CodecRoundTripCase {
    digest_count: u8,
    cell_count: u8,
}

impl Arbitrary for CodecRoundTripCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            digest_count: u8::arbitrary(g) % 8,
            cell_count: u8::arbitrary(g) % 32,
        }
    }
}

#[derive(Clone, Debug)]
struct AuthMutationCase {
    cell_count: u8,
    mutation_index: u16,
}

impl Arbitrary for AuthMutationCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            cell_count: (u8::arbitrary(g) % 8).max(1),
            mutation_index: u16::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct GeneratedMergeCell {
    rule_id: u8,
    key_hash: u8,
    bucket: u8,
    origin: u8,
    incarnation: u8,
    count: u8,
}

impl Arbitrary for GeneratedMergeCell {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rule_id: u8::arbitrary(g) % 4,
            key_hash: u8::arbitrary(g) % 16,
            bucket: u8::arbitrary(g) % 4,
            origin: u8::arbitrary(g) % 16,
            incarnation: u8::arbitrary(g) % 3,
            count: u8::arbitrary(g),
        }
    }
}

impl GeneratedMergeCell {
    fn into_cell(self) -> CounterCell {
        CounterCell {
            rule_id: u32::from(self.rule_id) + 1,
            key_hash: (u128::from(self.key_hash) + 10).into(),
            bucket_start_millis: i64::from(self.bucket) * 1_000,
            origin_node_id: (u128::from(self.origin) + 1).into(),
            origin_incarnation: u64::from(self.incarnation) + 1,
            count: u64::from(self.count) + 1,
            last_update_millis: u64::from(self.count) + 1,
            sequence: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct MergeLawCase {
    cells: Vec<GeneratedMergeCell>,
}

impl Arbitrary for MergeLawCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut cells = Vec::<GeneratedMergeCell>::arbitrary(g);
        cells.truncate(24);
        Self { cells }
    }
}

#[derive(Clone, Debug)]
struct DirtyRingCase {
    dirty_capacity: u8,
    cell_count: u8,
}

impl Arbitrary for DirtyRingCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            dirty_capacity: u8::arbitrary(g) % 8,
            cell_count: (u8::arbitrary(g) % 16).max(1),
        }
    }
}

#[derive(Clone, Debug)]
struct CodecCapacityCase {
    digest_count: u8,
    cell_count: u8,
    payload_selector: u16,
    initially_truncated: bool,
}

impl Arbitrary for CodecCapacityCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            digest_count: u8::arbitrary(g) % 12,
            cell_count: u8::arbitrary(g) % 48,
            payload_selector: u16::arbitrary(g),
            initially_truncated: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct DecodeLimitCase {
    digest_count: u8,
    cell_count: u8,
    digest_limit_delta: u8,
    cell_limit_delta: u8,
    payload_limit_delta: u16,
    payload_too_small: bool,
}

impl Arbitrary for DecodeLimitCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            digest_count: u8::arbitrary(g) % 12,
            cell_count: u8::arbitrary(g) % 48,
            digest_limit_delta: u8::arbitrary(g) % 4,
            cell_limit_delta: u8::arbitrary(g) % 4,
            payload_limit_delta: u16::arbitrary(g) % 128,
            payload_too_small: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct DigestModelCell {
    cell: GeneratedMergeCell,
    sequence: u8,
}

impl Arbitrary for DigestModelCell {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            cell: GeneratedMergeCell::arbitrary(g),
            sequence: u8::arbitrary(g),
        }
    }
}

impl DigestModelCell {
    fn into_cell(self) -> CounterCell {
        let mut cell = self.cell.into_cell();
        cell.sequence = u64::from(self.sequence);
        cell
    }
}

#[derive(Clone, Debug)]
struct DigestModelCase {
    cells: Vec<DigestModelCell>,
    shard_count: u8,
    shard_selector: u8,
}

impl Arbitrary for DigestModelCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut cells = Vec::<DigestModelCell>::arbitrary(g);
        cells.truncate(64);
        let shard_count = (u8::arbitrary(g) % 8).max(1);
        Self {
            cells,
            shard_count,
            shard_selector: u8::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct CellTableCapacityCase {
    capacity: u8,
    incoming_count: u8,
}

impl Arbitrary for CellTableCapacityCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            capacity: u8::arbitrary(g) % 12,
            incoming_count: u8::arbitrary(g) % 24,
        }
    }
}

fn cell(count: u64, origin: u64) -> CounterCell {
    CounterCell {
        rule_id: 1,
        key_hash: 20_u128.into(),
        bucket_start_millis: 0,
        origin_node_id: u128::from(origin).into(),
        origin_incarnation: 1,
        count,
        last_update_millis: count,
        sequence: 0,
    }
}

fn rule() -> Rule {
    Rule {
        id: 1,
        domain_hash: hash_domain("api"),
        descriptor_matcher: DescriptorMatcher::exact_keys(["tenant"]),
        limit: 10,
        window: WindowSpec {
            size_millis: 1_000,
            bucket_count: 10,
        },
        local_fallback_limit: 3,
        local_absolute_limit: 6,
        stale_after_millis: 500,
        safety_margin: SafetyMargin { hits: 0 },
        overflow_policy: OverflowPolicy::UseOverflowKey,
        mode: EnforcementMode::Enforce,
    }
}

#[test]
fn merge_remote_is_monotonic() {
    let mut table = CellTable::with_capacity(4, 8);

    let first = table.merge_remote(cell(5, 1), None, 0).expect("insert");
    let stale = table.merge_remote(cell(3, 1), None, 0).expect("stale");
    let newer = table.merge_remote(cell(8, 1), None, 0).expect("newer");

    assert_eq!(first.delta, 5);
    assert_eq!(stale.delta, 0);
    assert!(!stale.changed);
    assert_eq!(newer.delta, 3);
    assert_eq!(table.active_cell_count(), 1);
}

#[test]
fn merge_remote_example_orders_preserve_counts() {
    fn merged_counts(cells: &[CounterCell]) -> Vec<(u128, u64)> {
        let mut table = CellTable::with_capacity(8, 16);
        for cell in cells {
            table.merge_remote(*cell, None, 0).expect("merge");
        }
        let mut counts = table
            .cells()
            .map(|(_id, cell)| (cell.origin_node_id.value(), cell.count))
            .collect::<Vec<_>>();
        counts.sort();
        counts
    }

    let a1 = cell(3, 1);
    let a2 = cell(7, 1);
    let b1 = cell(5, 2);
    let b2 = cell(9, 2);

    assert_eq!(
        merged_counts(&[a1, a2, b1, b2]),
        merged_counts(&[a2, a2, b2, b2])
    );
    assert_eq!(
        merged_counts(&[a1, b1, a2, b2]),
        merged_counts(&[b1, b2, a1, a2])
    );
    assert_eq!(
        merged_counts(&[a1, b1, a2, b2]),
        merged_counts(&[a1, a2, b1, b2])
    );
}

#[test]
fn dirty_ring_records_overflow() {
    let mut table = CellTable::with_capacity(4, 1);

    table.merge_remote(cell(1, 1), None, 0).expect("insert one");
    table.merge_remote(cell(1, 2), None, 0).expect("insert two");

    assert!(table.dirty_overflowed());
    assert_eq!(table.dirty_cells().count(), 1);
}

#[test]
fn digest_changes_when_counts_change() {
    let mut table = CellTable::with_capacity(4, 8);
    table.merge_remote(cell(1, 1), None, 0).expect("insert");
    let before = table.digest(0, 1);
    table.merge_remote(cell(2, 1), None, 0).expect("update");
    let after = table.digest(0, 1);

    assert_eq!(before.active_cell_count, 1);
    assert_ne!(before.checksum, after.checksum);
    assert!(after.max_sequence > before.max_sequence);
}

#[test]
fn binary_round_trip_reuses_buffer_and_respects_capacity() {
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: vec![ShardDigest {
            shard_id: 0,
            active_cell_count: 1,
            max_sequence: 9,
            checksum: 11,
        }],
        cells: vec![cell(5, 1), cell(6, 2)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(512);
    let capacity = buffer.capacity();

    let truncated = encode_message(&message, &mut buffer, 512);
    let decoded = decode_message(&buffer, 4, 4).expect("decode");

    assert!(!truncated);
    assert_eq!(buffer.capacity(), capacity);
    assert_eq!(decoded.cells.len(), 2);
    assert_eq!(decoded.digests, message.digests);
    assert_eq!(
        decode_message(&buffer, 0, 4),
        Err(DecodeError::CapacityExceeded)
    );
}

#[test]
fn visitor_decode_reports_cells_without_allocating_message_vectors() {
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: vec![ShardDigest {
            shard_id: 0,
            active_cell_count: 1,
            max_sequence: 9,
            checksum: 11,
        }],
        cells: vec![cell(5, 1), cell(6, 2)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(512);
    let mut digest_count = 0;
    let mut cell_count = 0;
    let mut total = 0_u64;

    assert!(!encode_message(&message, &mut buffer, 512));
    let summary = decode_message_visit(
        &buffer,
        GossipLimits {
            max_payload_bytes: 512,
            max_digests: 4,
            max_cells: 4,
        },
        |_| digest_count += 1,
        |cell| {
            cell_count += 1;
            total = total.saturating_add(cell.count);
        },
    )
    .expect("decode");

    assert_eq!(summary.digest_count, 1);
    assert_eq!(summary.cell_count, 2);
    assert_eq!(digest_count, 1);
    assert_eq!(cell_count, 2);
    assert_eq!(total, 11);
}

#[test]
fn binary_encode_truncates_to_max_payload() {
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: Vec::new(),
        cells: vec![cell(5, 1), cell(6, 2)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(HEADER_LEN + CELL_LEN);

    assert!(encode_message(&message, &mut buffer, HEADER_LEN + CELL_LEN));
    let decoded = decode_message(&buffer, 0, 2).expect("decode");

    assert!(decoded.truncated);
    assert_eq!(decoded.cells.len(), 1);
}

#[test]
fn authenticated_message_rejects_tampering() {
    let key = HmacKey::new([7_u8; 32]);
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: Vec::new(),
        cells: vec![cell(5, 1)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(512);

    assert!(!encode_authenticated_message(
        &message,
        key,
        &mut buffer,
        GossipLimits::default()
    ));
    let decoded =
        decode_authenticated_message(&buffer, key, GossipLimits::default()).expect("decode");
    assert_eq!(decoded.cells.len(), 1);

    let last = buffer.len() - 1;
    buffer[last] ^= 1;
    assert_eq!(
        decode_authenticated_message(&buffer, key, GossipLimits::default()),
        Err(DecodeError::AuthenticationFailed)
    );
}

type NormalizedCell = (u32, u128, i64, u128, u64, u64);

fn merge_case_cells(case: MergeLawCase) -> Vec<CounterCell> {
    case.cells
        .into_iter()
        .map(GeneratedMergeCell::into_cell)
        .collect()
}

fn identity_key(cell: CounterCell) -> (u32, u128, i64, u128, u64) {
    (
        cell.rule_id,
        cell.key_hash.value(),
        cell.bucket_start_millis,
        cell.origin_node_id.value(),
        cell.origin_incarnation,
    )
}

fn normalized_counts(cells: impl IntoIterator<Item = CounterCell>) -> Vec<NormalizedCell> {
    let mut counts = cells
        .into_iter()
        .map(|cell| {
            (
                cell.rule_id,
                cell.key_hash.value(),
                cell.bucket_start_millis,
                cell.origin_node_id.value(),
                cell.origin_incarnation,
                cell.count,
            )
        })
        .collect::<Vec<_>>();
    counts.sort();
    counts
}

fn expected_counts_from_map(
    max_by_identity: &BTreeMap<(u32, u128, i64, u128, u64), u64>,
) -> Vec<NormalizedCell> {
    max_by_identity
        .iter()
        .map(
            |((rule_id, key_hash, bucket, origin, incarnation), count)| {
                (*rule_id, *key_hash, *bucket, *origin, *incarnation, *count)
            },
        )
        .collect()
}

fn merged_cells(cells: &[CounterCell]) -> Vec<CounterCell> {
    let mut table = CellTable::with_capacity(64, 64);
    for cell in cells {
        table.merge_remote(*cell, None, 0).expect("merge");
    }
    table.cells().map(|(_id, cell)| cell).collect()
}

fn generated_digest(index: usize) -> ShardDigest {
    ShardDigest {
        shard_id: index as u16,
        active_cell_count: (index + 1) as u32,
        max_sequence: index as u64 + 10,
        checksum: u128::from(index as u64 + 20),
    }
}

fn generated_wire_cell(index: usize) -> CounterCell {
    CounterCell {
        rule_id: (index as RuleId).saturating_add(1),
        key_hash: (u128::from(index as u64) + 1).into(),
        bucket_start_millis: (index as i64) * 1_000,
        origin_node_id: (u128::from(index as u64) + 1).into(),
        origin_incarnation: 1,
        count: index as u64 + 1,
        last_update_millis: index as u64 + 2,
        sequence: 0,
    }
}

fn generated_message(digest_count: usize, cell_count: usize, truncated: bool) -> GossipMessage {
    GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 8_000,
            flags: 0,
        },
        digests: (0..digest_count).map(generated_digest).collect(),
        cells: (0..cell_count).map(generated_wire_cell).collect(),
        truncated,
    }
}

#[quickcheck]
fn quickcheck_remote_merge_is_monotonic_per_full_cell_identity(case: MergeLawCase) -> TestResult {
    let cells = merge_case_cells(case);
    if cells.is_empty() {
        return TestResult::discard();
    }

    let mut table = CellTable::with_capacity(64, 64);
    let mut max_by_identity = BTreeMap::<_, u64>::new();
    for cell in cells {
        let previous = max_by_identity.get(&identity_key(cell)).copied();
        let expected_delta = previous
            .map(|count| cell.count.saturating_sub(count))
            .unwrap_or(cell.count);
        let outcome = table.merge_remote(cell, None, 0).expect("merge");

        if outcome.delta != expected_delta || outcome.changed != (expected_delta > 0) {
            return TestResult::error("remote merge outcome diverged from monotonic max model");
        }

        max_by_identity
            .entry(identity_key(cell))
            .and_modify(|count| *count = (*count).max(cell.count))
            .or_insert(cell.count);

        if normalized_counts(table.cells().map(|(_id, cell)| cell))
            != expected_counts_from_map(&max_by_identity)
        {
            return TestResult::error(
                "remote merge did not preserve the prefix maximum per full identity",
            );
        }
    }

    TestResult::passed()
}

#[quickcheck]
fn quickcheck_remote_merge_is_idempotent_for_duplicate_delivery(case: MergeLawCase) -> TestResult {
    let cells = merge_case_cells(case);
    if cells.is_empty() {
        return TestResult::discard();
    }

    let mut duplicated = cells.clone();
    duplicated.extend(cells.iter().copied());

    let merged = normalized_counts(merged_cells(&cells));
    if merged == normalized_counts(merged_cells(&duplicated)) {
        TestResult::passed()
    } else {
        TestResult::error("remote merge result changed after duplicate delivery")
    }
}

#[quickcheck]
fn quickcheck_remote_merge_is_commutative_for_delivery_order(case: MergeLawCase) -> TestResult {
    let cells = merge_case_cells(case);
    if cells.is_empty() {
        return TestResult::discard();
    }

    let mut sorted_by_identity = cells.clone();
    sorted_by_identity.sort_by_key(|cell| (identity_key(*cell), cell.count));
    let mut reversed = cells.clone();
    reversed.reverse();

    let merged = normalized_counts(merged_cells(&cells));
    if merged == normalized_counts(merged_cells(&reversed))
        && merged == normalized_counts(merged_cells(&sorted_by_identity))
    {
        TestResult::passed()
    } else {
        TestResult::error("remote merge result changed under reordered delivery")
    }
}

#[quickcheck]
fn quickcheck_remote_merge_is_associative_for_grouped_state_merges(
    case: MergeLawCase,
) -> TestResult {
    let cells = merge_case_cells(case);
    if cells.is_empty() {
        return TestResult::discard();
    }

    let first_split = cells.len() / 3;
    let second_split = (cells.len() * 2) / 3;
    let a = &cells[..first_split];
    let b = &cells[first_split..second_split];
    let c = &cells[second_split..];

    let mut ab_then_c = merged_cells(&merged_cells(a));
    ab_then_c.extend(merged_cells(b));
    ab_then_c = merged_cells(&ab_then_c);
    ab_then_c.extend(merged_cells(c));

    let mut a_then_bc = merged_cells(b);
    a_then_bc.extend(merged_cells(c));
    a_then_bc = merged_cells(&a_then_bc);
    let mut grouped_right = merged_cells(a);
    grouped_right.extend(a_then_bc);

    let all_at_once = normalized_counts(merged_cells(&cells));
    if all_at_once == normalized_counts(merged_cells(&ab_then_c))
        && all_at_once == normalized_counts(merged_cells(&grouped_right))
    {
        TestResult::passed()
    } else {
        TestResult::error("remote merge result changed under grouped state merges")
    }
}

#[quickcheck]
fn quickcheck_dirty_ring_is_bounded_and_reports_overflow(case: DirtyRingCase) -> TestResult {
    let dirty_capacity = usize::from(case.dirty_capacity);
    let cell_count = usize::from(case.cell_count);
    let mut table = CellTable::with_capacity(cell_count, dirty_capacity);

    for origin in 0..cell_count {
        if table
            .merge_remote(cell(1, origin as u64 + 1), None, 0)
            .is_err()
        {
            return TestResult::error("generated table filled before cell capacity");
        }
        if table.dirty_len() > dirty_capacity {
            return TestResult::error("dirty ring length exceeded configured capacity");
        }
    }

    let expected_overflow = dirty_capacity < cell_count;
    if table.dirty_overflowed() == expected_overflow {
        TestResult::passed()
    } else {
        TestResult::error("dirty ring overflow flag diverged from capacity model")
    }
}

#[quickcheck]
fn quickcheck_dirty_ring_retains_latest_dirty_cells_in_order(case: DirtyRingCase) -> TestResult {
    let dirty_capacity = usize::from(case.dirty_capacity);
    let cell_count = usize::from(case.cell_count);
    let mut table = CellTable::with_capacity(cell_count, dirty_capacity);
    let mut expected = Vec::with_capacity(dirty_capacity.min(cell_count));

    for origin in 0..cell_count {
        let cell = cell(1, origin as u64 + 1);
        if table.merge_remote(cell, None, 0).is_err() {
            return TestResult::error("generated table filled before cell capacity");
        }
        expected.push(identity_key(cell));
        if expected.len() > dirty_capacity {
            expected.remove(0);
        }
    }

    let actual = table.dirty_cells().map(identity_key).collect::<Vec<_>>();
    if actual == expected {
        TestResult::passed()
    } else {
        TestResult::error("dirty ring retained cells diverged from latest-N order model")
    }
}

#[quickcheck]
fn quickcheck_cell_table_reports_full_without_growing(case: CellTableCapacityCase) -> TestResult {
    let capacity = usize::from(case.capacity);
    let incoming_count = usize::from(case.incoming_count);
    let mut table = CellTable::with_capacity(capacity, incoming_count.max(1));
    let mut accepted = 0_usize;

    for origin in 0..incoming_count {
        match table.merge_remote(cell(1, origin as u64 + 1), None, 0) {
            Ok(_) if accepted < capacity => accepted += 1,
            Ok(_) => {
                return TestResult::error("cell table accepted more identities than capacity");
            }
            Err(CellTableFull) if accepted == capacity => {}
            Err(CellTableFull) => {
                return TestResult::error("cell table reported full before reaching capacity");
            }
        }
        if table.active_cell_count() > capacity || table.capacity() != capacity {
            return TestResult::error("cell table grew beyond its configured capacity");
        }
    }

    TestResult::passed()
}

#[quickcheck]
fn quickcheck_encoder_respects_payload_capacity_and_truncates_by_model(
    case: CodecCapacityCase,
) -> TestResult {
    let digest_count = usize::from(case.digest_count);
    let cell_count = usize::from(case.cell_count);
    let message = generated_message(digest_count, cell_count, case.initially_truncated);
    let full_len = HEADER_LEN + digest_count * DIGEST_LEN + cell_count * CELL_LEN;
    let max_payload_bytes = usize::from(case.payload_selector) % (full_len + CELL_LEN + 1);
    let mut buffer = Vec::with_capacity(full_len + CELL_LEN);

    let truncated = encode_message(&message, &mut buffer, max_payload_bytes);
    if buffer.len() > max_payload_bytes {
        return TestResult::error("encoder wrote past configured payload capacity");
    }

    let fixed_len = HEADER_LEN + digest_count * DIGEST_LEN;
    if fixed_len > max_payload_bytes {
        if truncated && buffer.is_empty() {
            return TestResult::passed();
        }
        return TestResult::error("encoder wrote a frame without enough room for fixed fields");
    }

    let expected_cells = cell_count.min((max_payload_bytes - fixed_len) / CELL_LEN);
    let expected_truncated = case.initially_truncated || expected_cells < cell_count;
    if truncated != expected_truncated {
        return TestResult::error("encoder truncation flag diverged from payload-size model");
    }

    let Ok(decoded) = decode_message(&buffer, digest_count, expected_cells) else {
        return TestResult::error("decoder rejected generated capacity-respecting frame");
    };
    if decoded.digests.len() != digest_count || decoded.cells.len() != expected_cells {
        return TestResult::error("encoded frame counts diverged from payload-size model");
    }
    if decoded.truncated != expected_truncated {
        return TestResult::error("encoded frame header did not preserve truncation state");
    }

    TestResult::passed()
}

#[quickcheck]
fn quickcheck_decoder_enforces_payload_and_count_limits_before_allocation(
    case: DecodeLimitCase,
) -> TestResult {
    let digest_count = usize::from(case.digest_count);
    let cell_count = usize::from(case.cell_count);
    let message = generated_message(digest_count, cell_count, false);
    let payload_capacity = HEADER_LEN + digest_count * DIGEST_LEN + cell_count * CELL_LEN;
    let mut buffer = Vec::with_capacity(payload_capacity);
    if encode_message(&message, &mut buffer, payload_capacity) {
        return TestResult::error("generated decode-limit message unexpectedly truncated");
    }

    let max_payload_bytes = if case.payload_too_small {
        buffer
            .len()
            .saturating_sub(usize::from(case.payload_limit_delta).max(1))
    } else {
        buffer.len()
    };
    let max_digests = digest_count.saturating_sub(usize::from(case.digest_limit_delta));
    let max_cells = cell_count.saturating_sub(usize::from(case.cell_limit_delta));
    let result = decode_message_with_limits(
        &buffer,
        GossipLimits {
            max_payload_bytes,
            max_digests,
            max_cells,
        },
    );
    let expected = if buffer.len() > max_payload_bytes {
        Err(DecodeError::PayloadTooLarge)
    } else if digest_count > max_digests || cell_count > max_cells {
        Err(DecodeError::CapacityExceeded)
    } else {
        Ok(())
    };

    match (result, expected) {
        (Ok(decoded), Ok(()))
            if decoded.digests.len() == digest_count && decoded.cells.len() == cell_count =>
        {
            TestResult::passed()
        }
        (Err(actual), Err(expected)) if actual == expected => TestResult::passed(),
        (Ok(_), Err(_)) => TestResult::error("decoder accepted a frame that exceeded limits"),
        (Err(_), Ok(())) => TestResult::error("decoder rejected a frame within limits"),
        (Err(_), Err(_)) => TestResult::error("decoder limit error precedence diverged"),
        (Ok(_), Ok(())) => TestResult::error("decoded frame counts diverged from encoded counts"),
    }
}

#[quickcheck]
fn quickcheck_digest_matches_shard_model(case: DigestModelCase) -> TestResult {
    let shard_count = u16::from(case.shard_count);
    let shard_id = u16::from(case.shard_selector) % shard_count;
    let cells = case
        .cells
        .into_iter()
        .map(DigestModelCell::into_cell)
        .collect::<Vec<_>>();
    let mut expected_count = 0_u32;
    let mut expected_max_sequence = 0_u64;
    let mut expected_checksum = 0_u128;

    for cell in cells.iter().copied() {
        if shard_for(cell, shard_count) != shard_id {
            continue;
        }
        expected_count = expected_count.saturating_add(1);
        expected_max_sequence = expected_max_sequence.max(cell.sequence);
        expected_checksum ^= cell_checksum(cell);
    }

    let actual = digest_cells(cells.iter().copied(), shard_id, shard_count);
    if actual.shard_id == shard_id
        && actual.active_cell_count == expected_count
        && actual.max_sequence == expected_max_sequence
        && actual.checksum == expected_checksum
    {
        TestResult::passed()
    } else {
        TestResult::error("digest diverged from shard/count/sequence/checksum model")
    }
}

#[quickcheck]
fn quickcheck_codec_roundtrip_and_visitor_decode_match(case: CodecRoundTripCase) -> TestResult {
    let digest_count = usize::from(case.digest_count);
    let cell_count = usize::from(case.cell_count);
    let mut digests = Vec::with_capacity(digest_count);
    for index in 0..digest_count {
        digests.push(ShardDigest {
            shard_id: index as u16,
            active_cell_count: (index + 1) as u32,
            max_sequence: index as u64 + 10,
            checksum: u128::from(index as u64 + 20),
        });
    }
    let mut cells = Vec::with_capacity(cell_count);
    for index in 0..cell_count {
        cells.push(CounterCell {
            rule_id: 1,
            key_hash: u128::from(index as u64 + 1).into(),
            bucket_start_millis: (index as i64) * 1_000,
            origin_node_id: u128::from(index as u64 + 1).into(),
            origin_incarnation: 1,
            count: index as u64 + 1,
            last_update_millis: index as u64 + 2,
            sequence: 0,
        });
    }
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 8_000,
            flags: 0,
        },
        digests,
        cells,
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(4096);
    if encode_message(&message, &mut buffer, 4096) {
        return TestResult::error("generated codec message unexpectedly truncated");
    }

    let Ok(decoded) = decode_message(&buffer, digest_count, cell_count) else {
        return TestResult::error("allocating decode failed for encoded message");
    };
    let mut visitor_digests = Vec::with_capacity(digest_count);
    let mut visitor_cells = Vec::with_capacity(cell_count);
    let Ok(summary) = decode_message_visit(
        &buffer,
        GossipLimits {
            max_payload_bytes: 4096,
            max_digests: digest_count,
            max_cells: cell_count,
        },
        |digest| visitor_digests.push(digest),
        |cell| visitor_cells.push(cell),
    ) else {
        return TestResult::error("visitor decode failed for encoded message");
    };

    if decoded != message {
        return TestResult::error("allocating decode did not round-trip encoded message");
    }
    if summary.digest_count != decoded.digests.len()
        || summary.cell_count != decoded.cells.len()
        || visitor_digests != decoded.digests
        || visitor_cells != decoded.cells
    {
        return TestResult::error("visitor decode content diverged from allocating decode");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_authenticated_frames_reject_single_byte_mutations(
    case: AuthMutationCase,
) -> TestResult {
    let key = HmacKey::new([7_u8; 32]);
    let cells = (0..case.cell_count)
        .map(|index| cell(u64::from(index) + 1, u64::from(index) + 1))
        .collect();
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: vec![ShardDigest {
            shard_id: 0,
            active_cell_count: 1,
            max_sequence: 2,
            checksum: 3,
        }],
        cells,
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(1024);

    if encode_authenticated_message(&message, key, &mut buffer, GossipLimits::default()) {
        return TestResult::error("generated authenticated message unexpectedly truncated");
    }
    let index = usize::from(case.mutation_index) % buffer.len();
    let mut mutated = buffer.clone();
    mutated[index] ^= 1;
    if decode_authenticated_message(&mutated, key, GossipLimits::default())
        == Err(DecodeError::AuthenticationFailed)
    {
        TestResult::passed()
    } else {
        TestResult::error("authenticated decode accepted a mutated frame")
    }
}

#[quickcheck]
fn quickcheck_authenticated_visitor_rejects_mutations_before_cell_callbacks(
    case: AuthMutationCase,
) -> TestResult {
    let key = HmacKey::new([7_u8; 32]);
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: Vec::new(),
        cells: vec![cell(u64::from(case.cell_count), 1)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(512);
    if encode_authenticated_message(&message, key, &mut buffer, GossipLimits::default()) {
        return TestResult::error("generated authenticated visitor message truncated");
    }
    let index = usize::from(case.mutation_index) % buffer.len();
    buffer[index] ^= 1;
    let mut callback_count = 0_u8;

    let result = decode_authenticated_message_visit_checked(
        &buffer,
        key,
        GossipLimits::default(),
        |_| true,
        |_| {},
        |_| callback_count = callback_count.saturating_add(1),
    );

    if result == Err(DecodeError::AuthenticationFailed) && callback_count == 0 {
        TestResult::passed()
    } else {
        TestResult::error("authenticated visitor decode ran callbacks for a mutated frame")
    }
}

#[test]
fn authenticated_visitor_decode_rejects_tampering_without_cell_callbacks() {
    let key = HmacKey::new([7_u8; 32]);
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 42,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 7,
            min_bucket: 0,
            max_bucket: 1000,
            flags: 0,
        },
        digests: Vec::new(),
        cells: vec![cell(5, 1)],
        truncated: false,
    };
    let mut buffer = Vec::with_capacity(512);
    let mut cells = 0_u8;

    assert!(!encode_authenticated_message(
        &message,
        key,
        &mut buffer,
        GossipLimits::default()
    ));
    let summary = decode_authenticated_message_visit_checked(
        &buffer,
        key,
        GossipLimits::default(),
        |_| true,
        |_| {},
        |_| cells = cells.saturating_add(1),
    )
    .expect("decode");
    assert_eq!(summary.cell_count, 1);
    assert_eq!(cells, 1);

    cells = 0;
    let last = buffer.len() - 1;
    buffer[last] ^= 1;
    assert_eq!(
        decode_authenticated_message_visit_checked(
            &buffer,
            key,
            GossipLimits::default(),
            |_| true,
            |_| {},
            |_| cells = cells.saturating_add(1),
        ),
        Err(DecodeError::AuthenticationFailed)
    );
    assert_eq!(cells, 0);
}

#[test]
fn payload_limit_is_enforced_before_decode() {
    let mut buffer = Vec::with_capacity(HEADER_LEN);
    let message = GossipMessage {
        header: GossipHeader {
            cluster_id_hash: 1,
            sender_node_id: ((1_u128 << 64) | 2).into(),
            sender_incarnation: 1,
            min_bucket: 0,
            max_bucket: 0,
            flags: 0,
        },
        digests: Vec::new(),
        cells: Vec::new(),
        truncated: false,
    };

    assert!(!encode_message(&message, &mut buffer, HEADER_LEN));
    assert_eq!(
        decode_message_with_limits(
            &buffer,
            GossipLimits {
                max_payload_bytes: HEADER_LEN - 1,
                max_digests: 0,
                max_cells: 0,
            }
        ),
        Err(DecodeError::PayloadTooLarge)
    );
}

#[test]
fn decoder_fuzz_smoke_returns_errors_without_panics() {
    let mut bytes = [0_u8; 96];
    let mut state = 0x1234_5678_9abc_def0_u64;

    for len in 0..bytes.len() {
        for byte in &mut bytes {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 32) as u8;
        }
        let _ = decode_message_with_limits(
            &bytes[..len],
            GossipLimits {
                max_payload_bytes: 128,
                max_digests: 4,
                max_cells: 4,
            },
        );
    }
}

#[test]
fn cell_table_reports_memory_pressure_without_growth() {
    let mut table = CellTable::with_capacity(1, 2);

    assert!(table.merge_remote(cell(1, 1), None, 0).is_ok());
    assert_eq!(table.merge_remote(cell(1, 2), None, 0), Err(CellTableFull));
    assert_eq!(table.capacity(), 1);
    assert_eq!(table.active_cell_count(), 1);
}

#[test]
fn simulation_merges_remote_counts_into_local_engine_estimates() {
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];
    let request = LimitRequest {
        domain: "api",
        descriptors: &descriptors,
        hits: 1,
    };
    let key_hash = hash_key(1, &request);
    let remote = CounterCell {
        rule_id: 1,
        key_hash,
        bucket_start_millis: 0,
        origin_node_id: 2_u128.into(),
        origin_incarnation: 1,
        count: 10,
        last_update_millis: 0,
        sequence: 0,
    };
    let mut table = CellTable::with_capacity(8, 8);
    let mut engine = LocalEngine::new(RuleTable::new(vec![rule()]), 16, 10);

    table
        .merge_remote(remote, Some(&mut engine), 0)
        .expect("merge");

    assert_eq!(
        engine.check_and_record(request, 1),
        Decision::Reject(crate::RejectReason::GlobalLimit)
    );
}
