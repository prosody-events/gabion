//! The control extension of a tree packet.
//!
//! ```text
//! data: [ 1 ][ root ][ seq u32 ][ lo ][ hi? ]      // cells follow in the frame
//! ack:  [ 2 ][ root ][ seq u32 ]                   // no cells
//! addr: [ 4 ][ ip 4 ][ port u16 ]  |  [ 6 ][ ip 16 ][ port u16 ]
//! hi?:  addr | [ 0 ]                               // 0: range runs to the end
//! ```
//!
//! `(root, seq)` names one broadcast. `[lo, hi)` is the range of the
//! root-rotated ring the receiver becomes responsible for.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const KIND_DATA: u8 = 1;
const KIND_ACK: u8 = 2;
const ADDR_NONE: u8 = 0;
const ADDR_V4: u8 = 4;
const ADDR_V6: u8 = 6;

const V6_ADDR_LEN: usize = 1 + 16 + 2;

/// Longest encoded extension: a data header with three IPv6 addresses.
pub(in crate::gossip) const CTRL_MAX_LEN: usize = 1 + 4 + 3 * V6_ADDR_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) enum Ctrl {
    Data {
        root: SocketAddr,
        seq: u32,
        lo: SocketAddr,
        hi: Option<SocketAddr>,
    },
    Ack {
        root: SocketAddr,
        seq: u32,
    },
}

impl Ctrl {
    /// Encode into `buf`, returning the extension bytes.
    pub fn encode<'a>(&self, buf: &'a mut [u8; CTRL_MAX_LEN]) -> &'a [u8] {
        let mut at = 0;
        match *self {
            Ctrl::Data { root, seq, lo, hi } => {
                buf[at] = KIND_DATA;
                at += 1;
                at += put_addr(&mut buf[at..], Some(root));
                buf[at..at + 4].copy_from_slice(&seq.to_le_bytes());
                at += 4;
                at += put_addr(&mut buf[at..], Some(lo));
                at += put_addr(&mut buf[at..], hi);
            }
            Ctrl::Ack { root, seq } => {
                buf[at] = KIND_ACK;
                at += 1;
                at += put_addr(&mut buf[at..], Some(root));
                buf[at..at + 4].copy_from_slice(&seq.to_le_bytes());
                at += 4;
            }
        }
        &buf[..at]
    }

    /// Decode an extension; `None` if it is malformed or has trailing bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let (&kind, rest) = bytes.split_first()?;
        let (root, rest) = get_addr(rest)?;
        let root = root?;
        let (seq, rest) = rest.split_first_chunk::<4>()?;
        let seq = u32::from_le_bytes(*seq);
        match kind {
            KIND_ACK => rest.is_empty().then_some(Ctrl::Ack { root, seq }),
            KIND_DATA => {
                let (lo, rest) = get_addr(rest)?;
                let (hi, rest) = get_addr(rest)?;
                rest.is_empty().then_some(Ctrl::Data {
                    root,
                    seq,
                    lo: lo?,
                    hi,
                })
            }
            _ => None,
        }
    }
}

fn put_addr(buf: &mut [u8], addr: Option<SocketAddr>) -> usize {
    match addr {
        None => {
            buf[0] = ADDR_NONE;
            1
        }
        Some(SocketAddr::V4(a)) => {
            buf[0] = ADDR_V4;
            buf[1..5].copy_from_slice(&a.ip().octets());
            buf[5..7].copy_from_slice(&a.port().to_le_bytes());
            7
        }
        Some(SocketAddr::V6(a)) => {
            buf[0] = ADDR_V6;
            buf[1..17].copy_from_slice(&a.ip().octets());
            buf[17..19].copy_from_slice(&a.port().to_le_bytes());
            V6_ADDR_LEN
        }
    }
}

/// `Some((addr, rest))`; the inner option is `None` for the "no address"
/// marker.
fn get_addr(bytes: &[u8]) -> Option<(Option<SocketAddr>, &[u8])> {
    let (&tag, rest) = bytes.split_first()?;
    match tag {
        ADDR_NONE => Some((None, rest)),
        ADDR_V4 => {
            let (ip, rest) = rest.split_first_chunk::<4>()?;
            let (port, rest) = rest.split_first_chunk::<2>()?;
            let ip = IpAddr::V4(Ipv4Addr::from(*ip));
            Some((Some(SocketAddr::new(ip, u16::from_le_bytes(*port))), rest))
        }
        ADDR_V6 => {
            let (ip, rest) = rest.split_first_chunk::<16>()?;
            let (port, rest) = rest.split_first_chunk::<2>()?;
            let ip = IpAddr::V6(Ipv6Addr::from(*ip));
            Some((Some(SocketAddr::new(ip, u16::from_le_bytes(*port))), rest))
        }
        _ => None,
    }
}
