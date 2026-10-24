use super::*;
use crate::crdt::ObservationBatch;
use crate::wire::decode_unauth;

const KEY: HmacKey = HmacKey([7; 32]);

fn header() -> Header {
    Header {
        cluster_id_hash: 9,
        sender_node_id: 42,
        sender_incarnation: 3,
        ..Header::default()
    }
}

fn round_trip(control: Control, ids: &[u64], key: Option<&HmacKey>) -> (Header, Control, Vec<u64>) {
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    encode_control(header(), control, ids, key, limits, &mut buf).expect("encode");
    let mut decoded = Vec::new();
    let (hdr, ctl) = decode_control(buf.as_bytes(), key, limits, &mut decoded).expect("decode");
    (hdr, ctl, decoded)
}

#[test]
fn every_control_kind_round_trips_with_and_without_auth() {
    for key in [None, Some(&KEY)] {
        let cases = [
            (Control::Graft, vec![]),
            (Control::Prune { backoff_secs: 60 }, vec![]),
            (Control::IHave, vec![1, 2, u64::MAX]),
            (Control::IWant, vec![]),
            (Control::IWant, vec![0, 7]),
            (Control::Sweep { cursor: 0 }, vec![]),
            (Control::CaughtUp, vec![]),
            (
                Control::Sweep {
                    cursor: u32::MAX - 1,
                },
                vec![],
            ),
            (
                Control::SweepDone {
                    cursor: 512,
                    next: SWEEP_COMPLETE,
                    frames: 19,
                },
                vec![],
            ),
        ];
        for (control, ids) in cases {
            let (hdr, ctl, out) = round_trip(control, &ids, key);
            assert_eq!(ctl, control);
            assert_eq!(out, ids);
            assert_eq!(hdr.sender_node_id, 42);
            assert_eq!(hdr.sender_incarnation, 3);
            assert_eq!(hdr.cell_count, 0);
            assert!(hdr.flags & FLAG_CONTROL != 0);
            assert_eq!(hdr.flags & FLAG_AUTHENTICATED != 0, key.is_some());
        }
    }
}

#[test]
fn control_frames_are_recognised_and_data_frames_are_not() {
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    encode_control(header(), Control::Graft, &[], None, limits, &mut buf).expect("encode");
    assert!(crate::wire::is_control(buf.as_bytes()));
    assert!(!crate::wire::is_control(&buf.as_bytes()[..HEADER_LEN - 1]));

    let mut obs = ObservationBatch::<u32>::with_capacity(1);
    assert!(decode_unauth::<u32>(buf.as_bytes(), limits, &mut obs).is_err());
}

#[test]
fn a_tampered_or_mis_keyed_control_frame_is_rejected() {
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    encode_control(
        header(),
        Control::IHave,
        &[1, 2],
        Some(&KEY),
        limits,
        &mut buf,
    )
    .expect("encode");
    let mut ids = Vec::new();

    let mut tampered = buf.as_bytes().to_vec();
    tampered[HEADER_LEN + 4] ^= 1;
    assert_eq!(
        decode_control(&tampered, Some(&KEY), limits, &mut ids),
        Err(DecodeError::AuthenticationFailed)
    );
    assert_eq!(
        decode_control(buf.as_bytes(), Some(&HmacKey([8; 32])), limits, &mut ids),
        Err(DecodeError::AuthenticationFailed)
    );
}

#[test]
fn a_truncated_id_list_is_rejected() {
    let limits = FrameLimits::default();
    let mut buf = PacketBuf::for_limits(limits);
    encode_control(header(), Control::IWant, &[1, 2], None, limits, &mut buf).expect("encode");
    let mut bytes = buf.as_bytes().to_vec();
    bytes.truncate(bytes.len() - 3);
    let mut hdr = read_header(&bytes).expect("header");
    hdr.body_len -= 3;
    patch_header(&mut bytes, &hdr);
    let mut ids = Vec::new();
    assert_eq!(
        decode_control(&bytes, None, limits, &mut ids),
        Err(DecodeError::BodyLenMismatch)
    );
}

#[test]
fn id_lists_over_one_datagram_are_refused() {
    let limits = FrameLimits {
        max_payload_bytes: 1400,
        max_cells: 4096,
    };
    let per_frame = max_ids_per_frame(limits, true);
    let ids = vec![0_u64; per_frame + 1];
    let mut buf = PacketBuf::for_limits(limits);
    assert_eq!(
        encode_control(header(), Control::IHave, &ids, Some(&KEY), limits, &mut buf),
        Err(EncodeError::BudgetTooSmall)
    );
    encode_control(
        header(),
        Control::IHave,
        &ids[..per_frame],
        Some(&KEY),
        limits,
        &mut buf,
    )
    .expect("a full frame fits");
    assert_eq!(
        buf.as_bytes().len(),
        limits.max_payload_bytes - (limits.max_payload_bytes - HEADER_LEN - AUTH_TAG_LEN - 3) % 8
    );
}
