//! The version digest a node pushes to one random peer every heartbeat.
//!
//! ```text
//! digest: [ 5 ][ entry ]*
//! entry:  [ root addr ][ seq u32 ][ high u64 ]
//! ```
//!
//! One entry per root the sender has heard from (itself included): the
//! highest broadcast number it holds from that root and the highest origin
//! sequence among the root's cells it holds. A receiver that is behind on a
//! root opens the gap to it and pulls it from the root. This catches what
//! the gap in a root's broadcast numbers cannot show: the last broadcasts
//! before a root falls quiet, and every broadcast routed through an interior
//! node that has died but is still in the views.

use std::net::SocketAddr;

use super::ctrl::{ADDR_LEN_MAX, KIND_DIGEST, get_addr, put_addr};

/// One root's position as the sender knows it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::gossip) struct Entry {
    pub root: SocketAddr,
    pub seq: u32,
    pub high: u64,
}

/// Longest encoded entry.
pub(in crate::gossip) const ENTRY_LEN_MAX: usize = ADDR_LEN_MAX + 4 + 8;

/// Start a digest extension in `buf`.
pub(in crate::gossip) fn begin(buf: &mut Vec<u8>) {
    buf.clear();
    buf.push(KIND_DIGEST);
}

/// Whether `buf` holds entries after the kind byte.
pub(in crate::gossip) fn has_entries(buf: &[u8]) -> bool {
    buf.len() > 1
}

/// Append `entry`.
pub(in crate::gossip) fn push(buf: &mut Vec<u8>, entry: Entry) {
    let mut addr = [0_u8; ADDR_LEN_MAX];
    let n = put_addr(&mut addr, entry.root);
    buf.extend_from_slice(&addr[..n]);
    buf.extend_from_slice(&entry.seq.to_le_bytes());
    buf.extend_from_slice(&entry.high.to_le_bytes());
}

/// Whether a control extension is a digest.
pub(in crate::gossip) fn is_digest(ext: &[u8]) -> bool {
    ext.first() == Some(&KIND_DIGEST)
}

/// The entries of a digest extension, up to the first malformed one.
pub(in crate::gossip) fn entries(ext: &[u8]) -> impl Iterator<Item = Entry> + '_ {
    let mut rest = ext.get(1..).unwrap_or_default();
    std::iter::from_fn(move || {
        let (root, after) = get_addr(rest)?;
        let (seq, after) = after.split_first_chunk::<4>()?;
        let (high, after) = after.split_first_chunk::<8>()?;
        rest = after;
        Some(Entry {
            root,
            seq: u32::from_le_bytes(*seq),
            high: u64::from_le_bytes(*high),
        })
    })
}
