use std::num::NonZeroU64;

use super::*;

fn sample() -> Header {
    Header {
        cluster_id_hash: 0x1111_2222_3333_4444_5555_6666_7777_8888,
        sender_node_id: 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111,
        sender_incarnation: 7,
        count_width: 4,
        cell_count: 12,
        body_len: 192,
        min_origin_sequence: 100,
        max_origin_sequence: 200,
        flags: FLAG_AUTHENTICATED | FLAG_MORE,
    }
}

#[test]
fn header_layout_is_72_bytes() {
    let mut buf = vec![0u8; HEADER_LEN];
    patch_header(&mut buf, &sample());
    assert_eq!(buf.len(), HEADER_LEN);
}

#[test]
fn header_roundtrip() {
    let h = sample();
    let mut buf = vec![0u8; HEADER_LEN];
    patch_header(&mut buf, &h);
    let got = read_header(&buf).expect("decode");
    assert_eq!(got, h);
}

#[test]
fn header_rejects_short() {
    let mut buf = vec![0u8; HEADER_LEN];
    patch_header(&mut buf, &sample());
    buf.truncate(HEADER_LEN - 1);
    assert_eq!(read_header(&buf), Err(DecodeError::TooShort));
}

#[test]
fn header_rejects_bad_magic() {
    let mut buf = vec![0u8; HEADER_LEN];
    patch_header(&mut buf, &sample());
    buf[0] ^= 0xFF;
    assert_eq!(read_header(&buf), Err(DecodeError::BadMagic));
}

#[test]
fn header_rejects_bad_version() {
    let mut buf = vec![0u8; HEADER_LEN];
    patch_header(&mut buf, &sample());
    buf[4] = 99;
    assert_eq!(read_header(&buf), Err(DecodeError::BadVersion));
}

#[test]
fn patch_header_preserves_layout() {
    let mut buf = vec![0_u8; HEADER_LEN];
    patch_header(&mut buf, &sample());
    let got = read_header(&buf).expect("decode");
    assert_eq!(got, sample());
}

#[test]
fn frame_kinds_round_trip_through_flags() {
    for kind in [
        FrameKind::Broadcast,
        FrameKind::Sync,
        FrameKind::Ack,
        FrameKind::Nack,
        FrameKind::Repair,
        FrameKind::Snapshot,
    ] {
        assert_eq!(FrameKind::from_flags(kind.flags()), Some(kind));
        // Unrelated flag bits do not change the kind.
        let noisy = kind.flags() | FLAG_MORE | FLAG_AUTHENTICATED;
        assert_eq!(FrameKind::from_flags(noisy), Some(kind));
    }
}

#[test]
fn two_kind_bits_are_not_a_kind() {
    assert_eq!(FrameKind::from_flags(FLAG_SYNC | FLAG_ACK), None);
    assert_eq!(FrameKind::from_flags(FLAG_NACK | FLAG_REPAIR), None);
    assert_eq!(FrameKind::from_flags(FLAG_SNAPSHOT | FLAG_SYNC), None);
}

#[test]
fn nacks_round_trip_through_the_sequence_fields() {
    let missed = Nack::Missed {
        latest: 41,
        missing: NonZeroU64::new(0b101).unwrap(),
    };
    for nack in [missed, Nack::Snapshot] {
        let (min, max) = nack.sequence_fields();
        let header = Header {
            min_origin_sequence: min,
            max_origin_sequence: max,
            ..sample()
        };
        assert_eq!(Nack::from_header(&header), nack);
    }
}
