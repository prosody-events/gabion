//! The control extension of a tree packet.
//!
//! ```text
//! data: [ 1 ][ root ][ seq u32 ][ lo ][ hi ]      // cells follow in the frame
//! ack:  [ 2 ][ root ][ seq u32 ]                   // no cells
//! pull: [ 3 ][ after u64 ][ before u64 ]           // no cells
//! fill: [ 4 ][ after u64 ][ through u64 ]          // cells follow in the frame
//! (kind 5 is the version digest, `digest.rs`)
//! sweep:  [ 6 ][ from u32 ]                         // no cells
//! swept:  [ 7 ][ from u32 ][ next u32 ]             // cells follow in the frame
//! addr: [ 4 ][ ip 4 ][ port u16 ]  |  [ 6 ][ ip 16 ][ port u16 ]
//! ```
//!
//! `(root, seq)` names one broadcast. `[lo, hi)` is the clockwise interval of
//! the sorted ring the receiver becomes responsible for; `hi` is excluded,
//! and for the outermost ranges it is the root itself.
//!
//! A pull asks the receiver for its own cells with an origin sequence
//! strictly between `after` and `before`; the fill that answers carries them
//! from just after `after` up to and including `through`. A sweep asks for
//! every live cell the receiver holds, in store order from slot `from`; the
//! answer says where to resume, [`SWEEP_DONE`] once it reached the end.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const KIND_DATA: u8 = 1;
const KIND_ACK: u8 = 2;
const KIND_PULL: u8 = 3;
const KIND_FILL: u8 = 4;
/// A version digest (see `digest.rs`), which [`Ctrl::decode`] does not parse.
pub(super) const KIND_DIGEST: u8 = 5;
const KIND_SWEEP: u8 = 6;
const KIND_SWEPT: u8 = 7;

/// The `next` of a sweep answer that reached the end of the store.
pub(in crate::gossip) const SWEEP_DONE: u32 = u32::MAX;
const ADDR_V4: u8 = 4;
const ADDR_V6: u8 = 6;

const V6_ADDR_LEN: usize = 1 + 16 + 2;
/// Longest encoded address.
pub(super) const ADDR_LEN_MAX: usize = V6_ADDR_LEN;

/// Longest encoded extension: a data header with three IPv6 addresses.
pub(in crate::gossip) const CTRL_MAX_LEN: usize = 1 + 4 + 3 * V6_ADDR_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) enum Ctrl {
    Data {
        root: SocketAddr,
        seq: u32,
        lo: SocketAddr,
        hi: SocketAddr,
    },
    Ack {
        root: SocketAddr,
        seq: u32,
    },
    Pull {
        after: u64,
        before: u64,
    },
    Fill {
        after: u64,
        through: u64,
    },
    Sweep {
        from: u32,
    },
    Swept {
        from: u32,
        next: u32,
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
                at += put_addr(&mut buf[at..], root);
                buf[at..at + 4].copy_from_slice(&seq.to_le_bytes());
                at += 4;
                at += put_addr(&mut buf[at..], lo);
                at += put_addr(&mut buf[at..], hi);
            }
            Ctrl::Ack { root, seq } => {
                buf[at] = KIND_ACK;
                at += 1;
                at += put_addr(&mut buf[at..], root);
                buf[at..at + 4].copy_from_slice(&seq.to_le_bytes());
                at += 4;
            }
            Ctrl::Pull { after, before: end }
            | Ctrl::Fill {
                after,
                through: end,
            } => {
                buf[at] = if matches!(self, Ctrl::Pull { .. }) {
                    KIND_PULL
                } else {
                    KIND_FILL
                };
                at += 1;
                for n in [after, end] {
                    buf[at..at + 8].copy_from_slice(&n.to_le_bytes());
                    at += 8;
                }
            }
            Ctrl::Sweep { from } => {
                buf[at] = KIND_SWEEP;
                buf[at + 1..at + 5].copy_from_slice(&from.to_le_bytes());
                at += 5;
            }
            Ctrl::Swept { from, next } => {
                buf[at] = KIND_SWEPT;
                buf[at + 1..at + 5].copy_from_slice(&from.to_le_bytes());
                buf[at + 5..at + 9].copy_from_slice(&next.to_le_bytes());
                at += 9;
            }
        }
        &buf[..at]
    }

    /// Decode an extension; `None` if it is malformed or has trailing bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let (&kind, rest) = bytes.split_first()?;
        if let KIND_SWEEP | KIND_SWEPT = kind {
            let (from, rest) = rest.split_first_chunk::<4>()?;
            let from = u32::from_le_bytes(*from);
            if kind == KIND_SWEEP {
                return rest.is_empty().then_some(Ctrl::Sweep { from });
            }
            let (next, rest) = rest.split_first_chunk::<4>()?;
            let next = u32::from_le_bytes(*next);
            return rest.is_empty().then_some(Ctrl::Swept { from, next });
        }
        if let KIND_PULL | KIND_FILL = kind {
            let (after, rest) = rest.split_first_chunk::<8>()?;
            let (end, rest) = rest.split_first_chunk::<8>()?;
            let (after, end) = (u64::from_le_bytes(*after), u64::from_le_bytes(*end));
            return rest.is_empty().then_some(if kind == KIND_PULL {
                Ctrl::Pull { after, before: end }
            } else {
                Ctrl::Fill {
                    after,
                    through: end,
                }
            });
        }
        let (root, rest) = get_addr(rest)?;
        let (seq, rest) = rest.split_first_chunk::<4>()?;
        let seq = u32::from_le_bytes(*seq);
        match kind {
            KIND_ACK => rest.is_empty().then_some(Ctrl::Ack { root, seq }),
            KIND_DATA => {
                let (lo, rest) = get_addr(rest)?;
                let (hi, rest) = get_addr(rest)?;
                rest.is_empty().then_some(Ctrl::Data { root, seq, lo, hi })
            }
            _ => None,
        }
    }
}

pub(super) fn put_addr(buf: &mut [u8], addr: SocketAddr) -> usize {
    match addr {
        SocketAddr::V4(a) => {
            buf[0] = ADDR_V4;
            buf[1..5].copy_from_slice(&a.ip().octets());
            buf[5..7].copy_from_slice(&a.port().to_le_bytes());
            7
        }
        SocketAddr::V6(a) => {
            buf[0] = ADDR_V6;
            buf[1..17].copy_from_slice(&a.ip().octets());
            buf[17..19].copy_from_slice(&a.port().to_le_bytes());
            V6_ADDR_LEN
        }
    }
}

pub(super) fn get_addr(bytes: &[u8]) -> Option<(SocketAddr, &[u8])> {
    let (&tag, rest) = bytes.split_first()?;
    match tag {
        ADDR_V4 => {
            let (ip, rest) = rest.split_first_chunk::<4>()?;
            let (port, rest) = rest.split_first_chunk::<2>()?;
            let ip = IpAddr::V4(Ipv4Addr::from(*ip));
            Some((SocketAddr::new(ip, u16::from_le_bytes(*port)), rest))
        }
        ADDR_V6 => {
            let (ip, rest) = rest.split_first_chunk::<16>()?;
            let (port, rest) = rest.split_first_chunk::<2>()?;
            let ip = IpAddr::V6(Ipv6Addr::from(*ip));
            Some((SocketAddr::new(ip, u16::from_le_bytes(*port)), rest))
        }
        _ => None,
    }
}
