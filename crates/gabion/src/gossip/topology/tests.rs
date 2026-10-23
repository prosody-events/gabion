use std::net::SocketAddr;

use super::*;

fn addr(s: &str) -> SocketAddr {
    s.parse().expect("socket address")
}

#[test]
fn zone_groups_addresses_by_prefix() {
    let a = Zone::of(addr("10.1.0.5:7946"), 16);
    assert_eq!(a, Zone::of(addr("10.1.200.9:7946"), 16));
    assert_ne!(a, Zone::of(addr("10.2.0.5:7946"), 16));
    assert_eq!(
        Zone::of(addr("10.1.0.5:1"), 24),
        Zone::of(addr("10.1.0.200:1"), 24)
    );
    assert_ne!(
        Zone::of(addr("10.1.0.5:1"), 24),
        Zone::of(addr("10.1.1.5:1"), 24)
    );
}

#[test]
fn zone_prefix_zero_puts_everyone_in_one_zone() {
    assert_eq!(
        Zone::of(addr("10.1.0.5:1"), 0),
        Zone::of(addr("192.168.7.7:1"), 0)
    );
}

#[test]
fn zone_prefix_longer_than_family_clamps() {
    assert_ne!(
        Zone::of(addr("10.1.0.5:1"), 200),
        Zone::of(addr("10.1.0.6:1"), 200)
    );
}

#[test]
fn ipv6_zone_does_not_collide_with_ipv4() {
    assert_ne!(
        Zone::of(addr("[::]:1"), 16),
        Zone::of(addr("0.0.0.0:1"), 16)
    );
}

#[test]
fn rendezvous_weight_is_a_pure_function() {
    let origin = NodeId(0xABCD);
    let member = addr("10.1.0.5:7946");
    assert_eq!(
        rendezvous_weight(origin, member),
        rendezvous_weight(origin, member)
    );
}

#[test]
fn rendezvous_winners_spread_across_members() {
    let members: Vec<SocketAddr> = (0..8)
        .map(|i| addr(&format!("10.1.0.{}:7946", i + 1)))
        .collect();
    let mut wins = [0_u32; 8];
    for origin in 0..800_u128 {
        let winner = (0..members.len())
            .max_by_key(|&i| rendezvous_weight(NodeId(origin), members[i]))
            .expect("members");
        wins[winner] += 1;
    }
    assert!(wins.iter().all(|&w| (50..=150).contains(&w)), "{wins:?}");
}

#[test]
fn removing_a_member_moves_only_its_origins() {
    let members: Vec<SocketAddr> = (0..8)
        .map(|i| addr(&format!("10.1.0.{}:7946", i + 1)))
        .collect();
    for origin in 0..200_u128 {
        let best = |skip: Option<usize>| {
            (0..members.len())
                .filter(|&i| Some(i) != skip)
                .max_by_key(|&i| rendezvous_weight(NodeId(origin), members[i]))
        };
        let full = best(None).expect("members");
        let without_other = best(Some((full + 1) % members.len()));
        assert_eq!(without_other, Some(full));
    }
}
