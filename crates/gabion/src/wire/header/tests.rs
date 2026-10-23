use super::*;

fn sample() -> Header {
    Header {
        cluster_id_hash: 0x1111_2222_3333_4444_5555_6666_7777_8888,
        sender_node_id: 0xAAAA_BBBB_CCCC_DDDD_EEEE_FFFF_0000_1111,
        sender_incarnation: 7,
        count_width: 4,
        cell_count: 12,
        body_len: 192,
        ack_own: 100,
        ack_relay: 150,
        stream_lower: 200,
        stream_upper: 300,
        flags: FLAG_AUTHENTICATED | FLAG_MORE | FLAG_RELAY,
    }
}

#[test]
fn header_layout_is_88_bytes() {
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
