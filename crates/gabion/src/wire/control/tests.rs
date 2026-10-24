use super::*;
use crate::wire::{FrameLimits, HmacKey};

const KEY: HmacKey = HmacKey([9; 32]);

fn header() -> Header {
    Header {
        cluster_id_hash: 0xC1,
        sender_node_id: 0xAA,
        sender_incarnation: 3,
        ..Header::default()
    }
}

/// `n` entries of the shape `kind` carries.
fn entries(kind: ControlKind, n: u64) -> Vec<ControlEntry> {
    let wide = kind.entry_bytes() == CHANGE_ENTRY_BYTES;
    (0..n)
        .map(|i| ControlEntry {
            node_id: NodeId(0x1_0000_0000_0000_0000 + u128::from(i)),
            incarnation: i as u32,
            value: i * 1000,
            rule_fingerprint: if wide {
                0x2_0000_0000_0000_0000 + u128::from(i)
            } else {
                0
            },
            key_hash: KeyHash(if wide {
                0x3_0000_0000_0000_0000 + u128::from(i)
            } else {
                0
            }),
            bucket: if wide { 7 + i as u32 } else { 0 },
        })
        .collect()
}

fn limits() -> FrameLimits {
    FrameLimits {
        max_payload_bytes: 1400,
        max_cells: 4096,
    }
}

fn decode(
    buf: &PacketBuf,
    key: Option<&HmacKey>,
) -> Result<(DecodedControl, Vec<ControlEntry>), DecodeError> {
    let mut got = Vec::new();
    let frame = decode_control(buf.as_bytes(), key, limits(), |e| got.push(e))?;
    Ok((frame, got))
}

#[test]
fn every_kind_round_trips_with_and_without_auth() {
    for kind in [
        ControlKind::Ihave,
        ControlKind::Graft,
        ControlKind::Prune,
        ControlKind::Nack,
        ControlKind::Probe,
        ControlKind::Pull,
    ] {
        for key in [None, Some(&KEY)] {
            let mut buf = PacketBuf::for_limits(limits());
            let sent = entries(kind, 5);
            let n = encode_control(header(), kind, 2, &sent, key, limits(), &mut buf).unwrap();
            assert_eq!(n, 5);
            assert!(is_control(buf.as_bytes()));
            let (frame, got) = decode(&buf, key).unwrap();
            assert_eq!((frame.kind, frame.tree), (kind, 2));
            assert_eq!(frame.header.sender_node_id, 0xAA);
            assert_eq!(got, sent);
        }
    }
}

#[test]
fn a_change_entry_is_wider_than_an_origin_entry() {
    let mut buf = PacketBuf::for_limits(limits());
    let mut sizes = Vec::new();
    for kind in [ControlKind::Graft, ControlKind::Nack] {
        encode_control(
            header(),
            kind,
            0,
            &entries(kind, 3),
            None,
            limits(),
            &mut buf,
        )
        .unwrap();
        sizes.push(buf.as_bytes().len());
    }
    assert_eq!(
        sizes[0] - sizes[1],
        3 * (CHANGE_ENTRY_BYTES - CONTROL_ENTRY_BYTES)
    );
}

#[test]
fn a_frame_without_entries_is_valid() {
    let mut buf = PacketBuf::for_limits(limits());
    let n = encode_control(
        header(),
        ControlKind::Prune,
        1,
        &[],
        None,
        limits(),
        &mut buf,
    )
    .unwrap();
    assert_eq!(n, 0);
    let (frame, got) = decode(&buf, None).unwrap();
    assert_eq!(frame.kind, ControlKind::Prune);
    assert!(got.is_empty());
}

#[test]
fn entries_that_do_not_fit_one_packet_continue_in_the_next() {
    let sent = entries(ControlKind::Ihave, 120);
    let mut rest = &sent[..];
    let mut got = Vec::new();
    let mut frames = 0;
    while !rest.is_empty() {
        let mut buf = PacketBuf::for_limits(limits());
        let n = encode_control(
            header(),
            ControlKind::Ihave,
            0,
            rest,
            Some(&KEY),
            limits(),
            &mut buf,
        )
        .unwrap();
        assert!(n > 0 && buf.as_bytes().len() <= 1400);
        got.extend(decode(&buf, Some(&KEY)).unwrap().1);
        rest = &rest[n..];
        frames += 1;
    }
    assert!(frames > 1);
    assert_eq!(got, sent);
}

#[test]
fn a_budget_with_no_room_for_an_entry_is_an_error() {
    let tiny = FrameLimits {
        max_payload_bytes: HEADER_LEN + 4,
        max_cells: 1,
    };
    let mut buf = PacketBuf::for_limits(tiny);
    assert_eq!(
        encode_control(
            header(),
            ControlKind::Nack,
            0,
            &entries(ControlKind::Nack, 1),
            None,
            tiny,
            &mut buf
        ),
        Err(EncodeError::BudgetTooSmall)
    );
}

#[test]
fn a_tampered_or_wrongly_keyed_frame_is_rejected() {
    let mut buf = PacketBuf::for_limits(limits());
    encode_control(
        header(),
        ControlKind::Graft,
        1,
        &entries(ControlKind::Graft, 2),
        Some(&KEY),
        limits(),
        &mut buf,
    )
    .unwrap();
    assert_eq!(
        decode(&buf, Some(&HmacKey([1; 32]))).unwrap_err(),
        DecodeError::AuthenticationFailed
    );
    let mut bytes = buf.as_bytes().to_vec();
    bytes[HEADER_LEN + 6] ^= 1;
    assert_eq!(
        decode_control(&bytes, Some(&KEY), limits(), |_| {}).unwrap_err(),
        DecodeError::AuthenticationFailed
    );
}

#[test]
fn an_unknown_kind_or_a_wrong_entry_count_is_rejected() {
    let mut buf = PacketBuf::for_limits(limits());
    encode_control(
        header(),
        ControlKind::Ihave,
        0,
        &entries(ControlKind::Ihave, 2),
        None,
        limits(),
        &mut buf,
    )
    .unwrap();
    let mut bytes = buf.as_bytes().to_vec();
    bytes[HEADER_LEN] = 99;
    assert_eq!(
        decode_control(&bytes, None, limits(), |_| {}).unwrap_err(),
        DecodeError::BadControl
    );
    let mut bytes = buf.as_bytes().to_vec();
    bytes[HEADER_LEN + 2] = 3;
    assert_eq!(
        decode_control(&bytes, None, limits(), |_| {}).unwrap_err(),
        DecodeError::BadControl
    );
}

#[test]
fn a_frame_of_cells_is_not_a_control_frame() {
    let mut data = vec![0u8; HEADER_LEN];
    crate::wire::header::patch_header(&mut data, &header());
    assert!(!is_control(&data));
    assert!(!is_control(&data[..4]));
}

/// `n` changes of one origin and rule, in consecutive buckets and values.
fn changes_of(origin: u128, n: u64) -> Vec<ControlEntry> {
    (0..n)
        .map(|i| ControlEntry {
            node_id: NodeId(origin),
            incarnation: 2,
            value: 1_000 + i,
            rule_fingerprint: 0xABC,
            key_hash: KeyHash(0x5000 + u128::from(i)),
            bucket: 70 + (i / 4) as u32,
        })
        .collect()
}

fn ihave_bytes(sent: &[ControlEntry]) -> usize {
    let mut buf = PacketBuf::for_limits(limits());
    let n = encode_control(
        header(),
        ControlKind::Ihave,
        0,
        sent,
        None,
        limits(),
        &mut buf,
    )
    .unwrap();
    assert_eq!(n, sent.len());
    assert_eq!(decode(&buf, None).unwrap().1, sent);
    buf.as_bytes().len()
}

#[test]
fn ihave_entries_of_one_origin_and_rule_share_a_group() {
    let empty = ihave_bytes(&[]);
    assert_eq!(
        ihave_bytes(&changes_of(7, 1)) - empty,
        GROUP_HEAD + GROUP_ENTRY
    );
    assert_eq!(
        ihave_bytes(&changes_of(7, 10)) - empty,
        GROUP_HEAD + 10 * GROUP_ENTRY
    );
    let mut two = changes_of(7, 3);
    two.extend(changes_of(8, 3));
    assert_eq!(
        ihave_bytes(&two) - empty,
        2 * (GROUP_HEAD + 3 * GROUP_ENTRY)
    );
}

#[test]
fn an_ihave_entry_out_of_its_groups_reach_starts_a_new_one() {
    let empty = ihave_bytes(&[]);
    let first = changes_of(7, 1)[0];
    let behind = ControlEntry {
        bucket: first.bucket - 1,
        ..first
    };
    let far = ControlEntry {
        value: first.value + u64::from(u32::MAX) + 1,
        ..first
    };
    let other_rule = ControlEntry {
        rule_fingerprint: 0xDEF,
        ..first
    };
    for second in [behind, far, other_rule] {
        assert_eq!(
            ihave_bytes(&[first, second]) - empty,
            2 * (GROUP_HEAD + GROUP_ENTRY)
        );
    }
}

#[test]
fn a_truncated_ihave_group_is_rejected() {
    let mut buf = PacketBuf::for_limits(limits());
    encode_control(
        header(),
        ControlKind::Ihave,
        0,
        &changes_of(7, 3),
        None,
        limits(),
        &mut buf,
    )
    .unwrap();
    let mut bytes = buf.as_bytes().to_vec();
    bytes.truncate(bytes.len() - GROUP_ENTRY);
    let body_len = (bytes.len() - HEADER_LEN) as u32;
    bytes[52..56].copy_from_slice(&body_len.to_le_bytes());
    assert_eq!(
        decode_control(&bytes, None, limits(), |_| {}),
        Err(DecodeError::BadControl)
    );
}
