use quickcheck_macros::quickcheck;

use super::*;
use crate::crdt::NodeId;

/// Twice the capacity: inserts hit Present and Full, and removes hit and miss.
const POOL: u8 = 16;
const CAPACITY: NonZeroU16 = NonZeroU16::new(8).unwrap();

/// Both families, so both arms of `addr_hash` run. Each odd address is the
/// IPv4-mapped twin of the even one before it, so the two hash equal but are
/// distinct peers.
fn addr(i: u8) -> SocketAddr {
    let ip = Ipv4Addr::new(10, 0, 0, i & !1);
    if i.is_multiple_of(2) {
        SocketAddr::from((ip, 9000))
    } else {
        SocketAddr::from((ip.to_ipv6_mapped(), 9000))
    }
}

/// After any mix of inserts, removes, shuffles and identity updates, `find`
/// returns exactly the row a linear scan finds, and the rows hold exactly the
/// model's peers with their identities.
#[quickcheck]
fn peer_table_index_matches_linear_scan(ops: Vec<(u8, u8, u8)>) {
    let mut table = PeerTable::with_capacity(CAPACITY);
    let mut model: Vec<(SocketAddr, Option<NodeIdentity>)> = Vec::new();
    for (op, x, y) in ops {
        let a = addr(x % POOL);
        match op % 4 {
            0 => {
                let expected = if model.iter().any(|p| p.0 == a) {
                    Insert::Present
                } else if model.len() == usize::from(CAPACITY.get()) {
                    Insert::Full
                } else {
                    model.push((a, None));
                    Insert::Added
                };
                assert_eq!(table.insert(a), expected);
            }
            1 => {
                let had = model.iter().any(|p| p.0 == a);
                model.retain(|p| p.0 != a);
                assert_eq!(table.remove(a), had);
            }
            2 => table.shuffle_front(usize::from(x), || u64::from(y)),
            3 => {
                let identity = NodeIdentity::new(NodeId(u128::from(y)), 1);
                table.set_identity(a, identity);
                if let Some(p) = model.iter_mut().find(|p| p.0 == a) {
                    p.1 = Some(identity);
                }
            }
            _ => {}
        }
        for i in 0..POOL {
            let a = addr(i);
            let scan = (0..table.len()).find(|&row| table.addr(row) == a);
            assert_eq!(table.find(a), scan, "{a}");
        }
        let mut rows: Vec<_> = table.rows().collect();
        rows.sort_by_key(|p| p.0);
        model.sort_by_key(|p| p.0);
        assert_eq!(rows, model);
    }
}
