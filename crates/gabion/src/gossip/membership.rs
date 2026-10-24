//! The cluster as discovery reports it, and the owner of every key.
//!
//! A `(rule, key)` is owned by the member with the highest rendezvous score
//! (highest random weight hashing), so ownership is a pure function of the
//! key and the set of members: two nodes that have discovered the same
//! members agree on every owner without exchanging a message, and a member
//! joining or leaving moves only the keys it wins or held.

use std::net::SocketAddr;

use crate::crdt::{KeyHash, NodeIdentity};

use super::repair::Heard;

/// A member's weight in the rendezvous hash: a hash of its address, the one
/// identity discovery gives every node for every other.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Token(u64);

impl Token {
    pub(super) fn of(addr: SocketAddr) -> Self {
        let port = u64::from(addr.port());
        let ip = match addr {
            SocketAddr::V4(v4) => u64::from(u32::from(*v4.ip())),
            SocketAddr::V6(v6) => {
                let bits = u128::from(*v6.ip());
                mix64(bits as u64) ^ (bits >> 64) as u64
            }
        };
        Self(mix64(ip ^ port << 48 ^ u64::from(addr.is_ipv6())))
    }
}

/// Where a key's hits are folded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Owner {
    Local,
    /// Index into [`Membership::members`].
    Peer(usize),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Member {
    pub(super) addr: SocketAddr,
    pub(super) token: Token,
    /// Learned from the packets the member sends.
    pub(super) identity: Option<NodeIdentity>,
    /// What this node has heard of the member's broadcasts as an owner.
    pub(super) heard: Heard,
}

impl Member {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            token: Token::of(addr),
            identity: None,
            heard: Heard::new(),
        }
    }
}

/// What [`Membership::add`] did with an address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Added {
    Joined,
    /// Already a member, or this node's own address.
    Known,
    /// The table is at capacity; the address was not recorded.
    Full,
}

/// This node and the peers discovery has reported, sized at construction.
pub(super) struct Membership {
    local: Member,
    peers: Vec<Member>,
    capacity: usize,
}

impl Membership {
    /// An empty table. The node's own address is not known until its
    /// transport is bound; [`Self::set_local`] supplies it before any owner
    /// is asked for.
    pub(super) fn new(peer_capacity: usize) -> Self {
        Self {
            local: Member::new(SocketAddr::from(([0, 0, 0, 0], 0))),
            peers: Vec::with_capacity(peer_capacity),
            capacity: peer_capacity,
        }
    }

    pub(super) fn set_local(&mut self, addr: SocketAddr) {
        self.local = Member::new(addr);
        self.peers.retain(|m| m.addr != addr);
    }

    pub(super) fn add(&mut self, addr: SocketAddr) -> Added {
        if addr == self.local.addr || self.peers.iter().any(|m| m.addr == addr) {
            return Added::Known;
        }
        if self.peers.len() == self.capacity {
            return Added::Full;
        }
        self.peers.push(Member::new(addr));
        Added::Joined
    }

    pub(super) fn remove(&mut self, addr: SocketAddr) -> bool {
        match self.peers.iter().position(|m| m.addr == addr) {
            Some(pos) => {
                self.peers.remove(pos);
                true
            }
            None => false,
        }
    }

    /// The peer at `addr`, which sent a packet as `identity`. A peer that
    /// restarted at the same address numbers its broadcasts afresh, so what
    /// was heard of its previous identity is forgotten.
    pub(super) fn heard_from(
        &mut self,
        addr: SocketAddr,
        identity: NodeIdentity,
    ) -> Option<&mut Member> {
        let member = self.peers.iter_mut().find(|m| m.addr == addr)?;
        if member.identity.is_some_and(|known| known != identity) {
            member.heard = Heard::new();
        }
        member.identity = Some(identity);
        Some(member)
    }

    pub(super) fn local_token(&self) -> Token {
        self.local.token
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn peers(&self) -> &[Member] {
        &self.peers
    }

    /// The member that owns `(fingerprint, key)`: highest score, with the
    /// lower address winning a (vanishingly rare) tie so the answer does not
    /// depend on the order peers were discovered in.
    pub(super) fn owner(&self, fingerprint: u128, key: KeyHash) -> Owner {
        let weight = key_weight(fingerprint, key);
        let mut best = (mix64(self.local.token.0 ^ weight), self.local.addr);
        let mut owner = Owner::Local;
        for (index, member) in self.peers.iter().enumerate() {
            let candidate = (mix64(member.token.0 ^ weight), member.addr);
            if candidate.0 > best.0 || (candidate.0 == best.0 && candidate.1 < best.1) {
                best = candidate;
                owner = Owner::Peer(index);
            }
        }
        owner
    }
}

fn key_weight(fingerprint: u128, key: KeyHash) -> u64 {
    let fold = |v: u128| mix64(v as u64 ^ mix64((v >> 64) as u64));
    mix64(fold(fingerprint).wrapping_add(fold(key.0).wrapping_mul(0x9E37_79B9_7F4A_7C15)))
}

/// The SplitMix64 finalizer: a bijective 64-bit mixer.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests;
