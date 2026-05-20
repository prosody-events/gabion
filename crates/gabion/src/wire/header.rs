//! Fixed-length frame header — hand-rolled little-endian, exactly 72 bytes.

use super::DecodeError;

pub const HEADER_LEN: usize = 72;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 1;
/// Set on every packet but the last in a multi-packet batch. Each packet is
/// independently decodable — `FLAG_MORE` is only an advisory hint for
/// receivers that want to defer downstream work until the batch settles.
pub const FLAG_MORE: u16 = 0x01;
pub const FLAG_AUTHENTICATED: u16 = 0x02;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Header {
    pub cluster_id_hash: u128,
    pub sender_node_id: u128,
    pub sender_incarnation: u32,
    pub count_width: u32,
    pub cell_count: u32,
    pub body_len: u32,
    pub min_origin_sequence: u64,
    pub max_origin_sequence: u64,
    pub flags: u16,
}

pub(crate) fn patch_header(buf: &mut [u8], hdr: &Header) {
    debug_assert!(buf.len() >= HEADER_LEN);
    let dst = &mut buf[..HEADER_LEN];
    dst[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    dst[4..6].copy_from_slice(&VERSION.to_le_bytes());
    dst[6..8].copy_from_slice(&hdr.flags.to_le_bytes());
    dst[8..24].copy_from_slice(&hdr.cluster_id_hash.to_le_bytes());
    dst[24..40].copy_from_slice(&hdr.sender_node_id.to_le_bytes());
    dst[40..44].copy_from_slice(&hdr.sender_incarnation.to_le_bytes());
    dst[44..48].copy_from_slice(&hdr.count_width.to_le_bytes());
    dst[48..52].copy_from_slice(&hdr.cell_count.to_le_bytes());
    dst[52..56].copy_from_slice(&hdr.body_len.to_le_bytes());
    dst[56..64].copy_from_slice(&hdr.min_origin_sequence.to_le_bytes());
    dst[64..72].copy_from_slice(&hdr.max_origin_sequence.to_le_bytes());
}

pub(crate) fn read_header(bytes: &[u8]) -> Result<Header, DecodeError> {
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::TooShort);
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(DecodeError::BadVersion);
    }
    let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let cluster_id_hash = u128::from_le_bytes(bytes[8..24].try_into().unwrap());
    let sender_node_id = u128::from_le_bytes(bytes[24..40].try_into().unwrap());
    let sender_incarnation = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
    let count_width = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
    let cell_count = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
    let body_len = u32::from_le_bytes(bytes[52..56].try_into().unwrap());
    let min_origin_sequence = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
    let max_origin_sequence = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
    Ok(Header {
        cluster_id_hash,
        sender_node_id,
        sender_incarnation,
        count_width,
        cell_count,
        body_len,
        min_origin_sequence,
        max_origin_sequence,
        flags,
    })
}

#[cfg(test)]
mod tests {
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
}
