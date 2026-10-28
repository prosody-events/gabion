//! The runtime's peer table: one dense row per discovered peer, sized once to
//! `gossip.peer_capacity` and indexed by address.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;

use crate::crdt::{CellIndex, NodeIdentity, hash_fingerprint, pow2_index_capacity_for};

/// What [`PeerTable::insert`] did with an address.
#[derive(Debug, Eq, PartialEq)]
pub(super) enum Insert {
    Added,
    Present,
    Full,
}

/// Rows `0..len` hold the peers in no particular order: a removed row is
/// filled by the last. Every column is sized at construction and never grows.
pub(super) struct PeerTable {
    len: usize,
    addr: Box<[SocketAddr]>,
    /// The identity from the peer's latest packet; `None` until its first.
    identity: Box<[Option<NodeIdentity>]>,
    /// Address hash to row; `addr[row]` confirms a match.
    by_addr: CellIndex,
}

impl PeerTable {
    pub(super) fn with_capacity(capacity: NonZeroU16) -> Self {
        let rows = usize::from(capacity.get());
        Self {
            len: 0,
            addr: vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)); rows].into_boxed_slice(),
            identity: vec![None; rows].into_boxed_slice(),
            by_addr: CellIndex::with_capacity(pow2_index_capacity_for(capacity.into())),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn addr(&self, row: usize) -> SocketAddr {
        self.addr[row]
    }

    pub(super) fn find(&self, addr: SocketAddr) -> Option<usize> {
        self.lookup(addr_hash(addr), addr).map(|row| row as usize)
    }

    /// A known address is `Present` even when the table is full, so
    /// re-announcing a peer is never a decline.
    pub(super) fn insert(&mut self, addr: SocketAddr) -> Insert {
        let hash = addr_hash(addr);
        if self.lookup(hash, addr).is_some() {
            return Insert::Present;
        }
        if self.len == self.addr.len() {
            return Insert::Full;
        }
        self.addr[self.len] = addr;
        self.identity[self.len] = None;
        self.by_addr.insert_unchecked(hash, self.len as u32);
        self.len += 1;
        Insert::Added
    }

    /// Swap-remove: the last row fills the hole and its index entry follows.
    pub(super) fn remove(&mut self, addr: SocketAddr) -> bool {
        let hash = addr_hash(addr);
        let Some(row) = self.lookup(hash, addr) else {
            return false;
        };
        self.by_addr.remove(hash, row);
        self.len -= 1;
        let (row, last) = (row as usize, self.len);
        if row != last {
            self.reindex(last, row);
            self.addr[row] = self.addr[last];
            self.identity[row] = self.identity[last];
        }
        true
    }

    /// Exchange two rows; the index follows both. The round's peer pick uses
    /// it.
    pub(super) fn swap(&mut self, a: usize, b: usize) {
        self.reindex(a, b);
        self.reindex(b, a);
        self.addr.swap(a, b);
        self.identity.swap(a, b);
    }

    /// Record `identity` for `addr` if it is a peer; an unknown sender changes
    /// nothing.
    pub(super) fn set_identity(&mut self, addr: SocketAddr, identity: NodeIdentity) {
        if let Some(row) = self.find(addr) {
            self.identity[row] = Some(identity);
        }
    }

    pub(super) fn rows(&self) -> impl Iterator<Item = (SocketAddr, Option<NodeIdentity>)> + '_ {
        let rows = ..self.len;
        self.addr[rows]
            .iter()
            .copied()
            .zip(self.identity[rows].iter().copied())
    }

    fn lookup(&self, hash: u64, addr: SocketAddr) -> Option<u32> {
        self.by_addr
            .find(hash, |row| self.addr[row as usize] == addr)
    }

    /// Point row `from`'s index entry at row `to`.
    fn reindex(&mut self, from: usize, to: usize) {
        let hash = addr_hash(self.addr[from]);
        self.by_addr.remove(hash, from as u32);
        self.by_addr.insert_unchecked(hash, to as u32);
    }
}

/// The address as one 128-bit key: an IPv4 address in its IPv6-mapped form,
/// with the port in bits 48..64, which are zero in that form.
fn addr_hash(addr: SocketAddr) -> u64 {
    let ip = match addr.ip() {
        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
        IpAddr::V6(ip) => ip,
    };
    hash_fingerprint(u128::from(ip) ^ (u128::from(addr.port()) << 48))
}

#[cfg(test)]
mod tests;
