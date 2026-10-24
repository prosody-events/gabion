use super::*;
use crate::wire::{FLAG_DIGEST, FLAG_MORE, FLAG_REPLY, FrameKind, frame_kind};

const LIMITS: FrameLimits = FrameLimits {
    max_payload_bytes: 400,
    max_cells: 64,
};
const KEY: HmacKey = HmacKey([9; 32]);

fn origin(id: u128) -> OriginKey {
    OriginKey {
        node_id: NodeId(id),
        incarnation: 1,
    }
}

fn entry(id: u128, max: u64, held: &[u64]) -> DigestEntry {
    let oldest = held.iter().copied().min().unwrap_or(max);
    let mut entry = DigestEntry::new(origin(id), max, oldest);
    for &sequence in held {
        entry.mark(sequence);
    }
    entry
}

fn header() -> Header {
    Header {
        sender_node_id: 0xAB,
        ..Header::default()
    }
}

fn encode_all(entries: &[DigestEntry], reply: bool, key: Option<&HmacKey>) -> Vec<Vec<u8>> {
    let mut packets = DigestPackets::new(header(), reply, entries, key, LIMITS).unwrap();
    let mut buf = PacketBuf::for_limits(LIMITS);
    let mut out = Vec::new();
    while packets.next_into(&mut buf) {
        out.push(buf.as_bytes().to_vec());
    }
    out
}

#[test]
fn entry_holds_exactly_what_was_marked_inside_the_window() {
    let max = 1_000;
    let entry = entry(1, max, &[max, max - 1, max - 5, 3]);
    assert!(entry.holds(max));
    assert!(entry.holds(max - 1));
    assert!(!entry.holds(max - 2));
    assert!(entry.holds(max - 5));
    assert!(!entry.holds(max + 1), "above the maximum is never held");
    assert!(!entry.holds(max - DIGEST_WINDOW + 1));
    assert!(
        entry.holds(max - DIGEST_WINDOW),
        "beyond the window is assumed held"
    );
    assert!(entry.holds(3));
}

#[test]
fn a_whole_entry_holds_nothing_older_than_its_bitmap() {
    let max = 1_000;
    let entry = entry(1, max, &[max, max - 2, max - 70]);
    assert!(entry.holds(max - 70));
    assert!(!entry.holds(max - 71));
    assert!(
        !entry.holds(max - DIGEST_WINDOW),
        "below the oldest held is lacked, not assumed held"
    );
}

#[test]
fn an_entry_costs_only_the_words_it_needs() {
    let one = encode_all(&[entry(1, 9, &[9, 3])], false, None);
    let two = encode_all(&[entry(1, 99, &[99, 3])], false, None);
    let window = encode_all(&[entry(1, 999, &[999, 3])], false, None);
    assert_eq!(one[0].len() + 8, two[0].len());
    assert_eq!(one[0].len() + 8 * (DIGEST_WORDS - 1), window[0].len());
    assert_eq!(
        one[0].len(),
        HEADER_LEN + BODY_OVERHEAD + ENTRY_FIXED_BYTES + 8
    );
}

#[test]
fn entries_of_every_reach_round_trip() {
    let entries: Vec<_> = (1..=12)
        .map(|id| {
            let max = id as u64 * 50;
            entry(id, max, &[max, max - 1, max.saturating_sub(id as u64 * 40)])
        })
        .collect();
    let mut decoded = Vec::new();
    for bytes in encode_all(&entries, false, None) {
        decode_digest_visit(&bytes, None, LIMITS, |e| decoded.push(e)).unwrap();
    }
    assert_eq!(decoded, entries);
}

#[test]
fn a_reach_out_of_range_is_rejected() {
    let packets = encode_all(&[entry(1, 5, &[5])], false, None);
    let reach_at = HEADER_LEN + BODY_OVERHEAD + ENTRY_FIXED_BYTES - 1;
    for bad in [0, DIGEST_WORDS as u8 + 1, REACH_CUT | 1] {
        let mut bytes = packets[0].clone();
        bytes[reach_at] = bad;
        assert_eq!(
            decode_digest_visit(&bytes, None, LIMITS, |_| ()).unwrap_err(),
            DecodeError::BadDigestReach,
            "reach byte {bad:#x}",
        );
    }
}

#[test]
fn packets_tile_the_key_space_and_round_trip() {
    let entries: Vec<_> = (1..=12)
        .map(|id| entry(id, id as u64 * 10, &[id as u64 * 10]))
        .collect();
    let packets = encode_all(&entries, false, None);
    assert!(
        packets.len() > 1,
        "twelve entries must not fit one 400 B packet"
    );

    let mut decoded = Vec::new();
    let mut scopes = Vec::new();
    for (i, bytes) in packets.iter().enumerate() {
        assert_eq!(frame_kind(bytes), FrameKind::Digest);
        let (header, scope) =
            decode_digest_visit(bytes, None, LIMITS, |e| decoded.push(e)).unwrap();
        assert_eq!(header.sender_node_id, 0xAB);
        assert_eq!(header.flags & FLAG_REPLY, 0);
        assert_eq!(header.flags & FLAG_MORE != 0, i + 1 < packets.len());
        scopes.push(scope);
    }
    assert_eq!(decoded, entries);
    assert_eq!(scopes[0].from, OriginKey::MIN);
    assert_eq!(scopes.last().unwrap().to, OriginKey::END);
    for pair in scopes.windows(2) {
        assert_eq!(pair[0].to, pair[1].from, "scopes leave no gap");
    }
    for entry in &entries {
        let covering = scopes.iter().filter(|s| s.contains(entry.origin)).count();
        assert_eq!(covering, 1, "each origin is in exactly one scope");
    }
}

#[test]
fn empty_digest_is_one_packet_speaking_for_every_origin() {
    let packets = encode_all(&[], true, None);
    assert_eq!(packets.len(), 1);
    let (header, scope) = decode_digest_visit(&packets[0], None, LIMITS, |_| panic!()).unwrap();
    assert_ne!(header.flags & FLAG_REPLY, 0);
    assert_ne!(header.flags & FLAG_DIGEST, 0);
    assert!(scope.contains(origin(1)));
    assert!(scope.contains(origin(u128::MAX - 1)));
}

#[test]
fn authenticated_digest_rejects_wrong_key_and_tampering() {
    let entries = [entry(1, 5, &[5])];
    let packets = encode_all(&entries, false, Some(&KEY));
    assert!(decode_digest_visit(&packets[0], Some(&KEY), LIMITS, |_| ()).is_ok());
    assert_eq!(
        decode_digest_visit(&packets[0], Some(&HmacKey([1; 32])), LIMITS, |_| ()).unwrap_err(),
        DecodeError::AuthenticationFailed
    );
    let mut tampered = packets[0].clone();
    tampered[HEADER_LEN + 3] ^= 1;
    assert_eq!(
        decode_digest_visit(&tampered, Some(&KEY), LIMITS, |_| ()).unwrap_err(),
        DecodeError::AuthenticationFailed
    );
}

#[test]
fn malformed_digests_are_rejected() {
    let packets = encode_all(&[entry(1, 5, &[5])], false, None);
    let mut short = packets[0].clone();
    short.truncate(short.len() - 1);
    assert!(decode_digest_visit(&short, None, LIMITS, |_| ()).is_err());

    let mut not_digest = packets[0].clone();
    not_digest[6] &= !(FLAG_DIGEST as u8);
    assert_eq!(
        decode_digest_visit(&not_digest, None, LIMITS, |_| ()).unwrap_err(),
        DecodeError::NotADigest
    );
}

#[test]
fn budget_too_small_for_one_entry_is_an_error() {
    let tiny = FrameLimits {
        max_payload_bytes: HEADER_LEN + BODY_OVERHEAD + MAX_ENTRY_BYTES - 1,
        max_cells: 1,
    };
    assert!(matches!(
        DigestPackets::new(header(), false, &[], None, tiny),
        Err(EncodeError::BudgetTooSmall)
    ));
}
