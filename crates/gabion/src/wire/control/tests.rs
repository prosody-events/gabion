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

fn entries(n: u64) -> Vec<ControlEntry> {
    (0..n)
        .map(|i| ControlEntry {
            node_id: NodeId(0x1_0000_0000_0000_0000 + u128::from(i)),
            incarnation: i as u32,
            value: i * 1000,
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
    ] {
        for key in [None, Some(&KEY)] {
            let mut buf = PacketBuf::for_limits(limits());
            let sent = entries(5);
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
    let sent = entries(120);
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
            &entries(1),
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
        &entries(2),
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
        &entries(2),
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
