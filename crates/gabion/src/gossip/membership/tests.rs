use std::net::SocketAddr;

use super::*;

fn addr(last: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, last], 7946))
}

fn cluster(local: u8, peers: &[u8]) -> Membership {
    let mut m = Membership::new(64);
    m.set_local(addr(local));
    for &p in peers {
        assert_eq!(m.add(addr(p)), Added::Joined);
    }
    m
}

fn owner_addr(m: &Membership, key: u128) -> SocketAddr {
    match m.owner(7, KeyHash(key)) {
        Owner::Local => m.local.addr,
        Owner::Peer(i) => m.peers()[i].addr,
    }
}

#[test]
fn nodes_with_the_same_members_agree_on_every_owner() {
    let a = cluster(1, &[2, 3, 4, 5]);
    let b = cluster(3, &[5, 4, 2, 1]);
    for key in 0..500 {
        assert_eq!(owner_addr(&a, key), owner_addr(&b, key));
    }
}

#[test]
fn every_member_owns_a_share_of_the_keys() {
    let m = cluster(1, &[2, 3, 4]);
    let members = [addr(1), addr(2), addr(3), addr(4)];
    let mut owned = [0_u32; 4];
    for key in 0..4_000 {
        let owner = owner_addr(&m, key);
        owned[members.iter().position(|&a| a == owner).unwrap()] += 1;
    }
    for share in owned {
        assert!((700..1300).contains(&share), "uneven ownership: {owned:?}");
    }
}

#[test]
fn a_departure_moves_only_the_keys_that_member_owned() {
    let before = cluster(1, &[2, 3, 4, 5]);
    let mut after = cluster(1, &[2, 3, 4, 5]);
    assert!(after.remove(addr(4)));
    for key in 0..1_000 {
        let was = owner_addr(&before, key);
        if was != addr(4) {
            assert_eq!(owner_addr(&after, key), was);
        }
    }
}

#[test]
fn a_joiner_takes_keys_only_for_itself() {
    let before = cluster(1, &[2, 3]);
    let after = cluster(1, &[2, 3, 9]);
    for key in 0..1_000 {
        let now = owner_addr(&after, key);
        assert!(now == addr(9) || now == owner_addr(&before, key));
    }
}

#[test]
fn add_ignores_known_addresses_and_stops_at_capacity() {
    let mut m = Membership::new(2);
    m.set_local(addr(1));
    assert_eq!(m.add(addr(1)), Added::Known);
    assert_eq!(m.add(addr(2)), Added::Joined);
    assert_eq!(m.add(addr(2)), Added::Known);
    assert_eq!(m.add(addr(3)), Added::Joined);
    assert_eq!(m.add(addr(4)), Added::Full);
    assert_eq!(m.peers().len(), 2);
}

#[test]
fn a_node_alone_owns_everything() {
    let m = cluster(1, &[]);
    for key in 0..100 {
        assert_eq!(m.owner(7, KeyHash(key)), Owner::Local);
    }
}
