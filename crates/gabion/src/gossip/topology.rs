//! Zone membership and gateway ranking: pure functions of peer addresses and
//! node identities, so every node ranks the same candidates the same way.

use std::net::{IpAddr, SocketAddr};

use crate::crdt::NodeId;

/// A zone (availability zone), named by the leading bits of its members'
/// gossip addresses.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct Zone(u128);

impl Zone {
    /// The zone of `addr`: its IP address truncated to `prefix_len` bits. A
    /// prefix longer than the address family clamps to the whole address.
    pub(super) fn of(addr: SocketAddr, prefix_len: u8) -> Self {
        let (bits, width) = match addr.ip() {
            IpAddr::V4(ip) => (u128::from(u32::from(ip)), 32),
            // Tag IPv6 so it cannot collide with an IPv4 prefix.
            IpAddr::V6(ip) => (u128::from(ip), 128),
        };
        let keep = u32::from(prefix_len).min(width);
        let zone = if keep == 0 { 0 } else { bits >> (width - keep) };
        Self(zone | if width == 128 { 1 << 127 } else { 0 })
    }
}

#[inline]
fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Rendezvous (highest-random-weight) weight of `member` for `origin`. The
/// member with the greatest weight within a zone is the origin's gateway
/// into that zone; ranking by descending weight gives the failover order.
pub(super) fn rendezvous_weight(origin: NodeId, member: SocketAddr) -> u64 {
    let addr_bits = match member.ip() {
        IpAddr::V4(ip) => u128::from(u32::from(ip)),
        IpAddr::V6(ip) => u128::from(ip),
    } ^ (u128::from(member.port()) << 112);
    let mixed =
        |x: u128| mix64(x as u64) ^ mix64(((x >> 64) as u64).wrapping_add(0x9E37_79B9_7F4A_7C15));
    mix64(mixed(origin.0) ^ mixed(addr_bits).rotate_left(21))
}

#[cfg(test)]
mod tests;
