use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use super::*;
use crate::crdt::NodeId;

/// Twice the capacity: inserts hit Present and Full, and removes hit and miss.
const POOL: u8 = 16;
const CAPACITY: NonZeroU16 = NonZeroU16::new(8).unwrap();
const RTO: u64 = 100;
const FLOOR: u64 = 5;

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

fn table() -> PeerTable {
    PeerTable::with_capacity(CAPACITY, 7, RTO)
}

/// A table holding one peer, at row 0.
fn one_row() -> PeerTable {
    let mut table = table();
    assert_eq!(table.insert(addr(0)), Insert::Added);
    table
}

/// After any mix of inserts, removes and contacts, `find` returns exactly the
/// row a linear scan finds, and the rows hold exactly the model's peers with
/// their identities.
#[quickcheck]
fn peer_table_index_matches_linear_scan(ops: Vec<(u8, u8, u8)>) {
    let mut table = table();
    let mut model: Vec<(SocketAddr, Option<NodeId>)> = Vec::new();
    for (op, x, y) in ops {
        let a = addr(x % POOL);
        match op % 3 {
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
                assert_eq!(table.remove(a).is_some(), had);
            }
            _ => {
                if let Some(row) = table.find(a) {
                    let id = NodeId(u128::from(y));
                    table.contact(row, NodeIdentity::new(id, 1));
                    if let Some(p) = model.iter_mut().find(|p| p.0 == a) {
                        p.1 = Some(id);
                    }
                }
            }
        }
        for i in 0..POOL {
            let a = addr(i);
            let scan = (0..table.len()).find(|&row| table.addr(row) == a);
            assert_eq!(table.find(a), scan, "{a}");
        }
        let mut rows: Vec<_> = table.entries().map(|e| (e.addr, e.node_id)).collect();
        rows.sort_by_key(|p| p.0);
        model.sort_by_key(|p| p.0);
        assert_eq!(rows, model);
    }
}

/// One step on a row's streams. Values are small, so sends, acks and gap
/// reports land on, below and above the head and the marks.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// The head rises by this much: new own cells.
    Issue(u8),
    /// A send closing through `sent + n`, capped at the head.
    Send(u8),
    Ack(u8),
    Gap(u8),
    /// The round's timeout check, rewinding a timed-out stream.
    Service,
    Advance(u8),
    /// A packet of the peer's own stream closing `(lower, lower + len]`.
    Receive(u8, u8),
    /// The round's gap-report check.
    Report,
}

impl Arbitrary for Op {
    fn arbitrary(g: &mut Gen) -> Self {
        let x = u8::arbitrary(g) % 8;
        let y = u8::arbitrary(g) % 4 + 1;
        match u8::arbitrary(g) % 8 {
            0 => Op::Issue(x),
            1 => Op::Send(x + 1),
            2 => Op::Ack(x * 2),
            3 => Op::Gap(x * 2),
            4 => Op::Service,
            5 => Op::Advance(x * 16),
            6 => Op::Receive(x, y),
            _ => Op::Report,
        }
    }
}

/// The sender rules the `peer` module documents on `StreamTx`, written out
/// on plain fields.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct TxModel {
    sent: u64,
    acked: u64,
    timer: u64,
}

impl TxModel {
    fn send(&mut self, upper: u64, now: u64) {
        if self.acked == self.sent {
            self.timer = now;
        }
        self.sent = self.sent.max(upper);
    }

    fn ack(&mut self, ack: u64, head: u64, now: u64) {
        if ack <= head && ack > self.acked {
            self.acked = ack;
            self.sent = self.sent.max(ack);
            if ack < self.sent {
                self.timer = now;
            }
        }
    }

    fn gap(&mut self, ack: u64, head: u64, now: u64) {
        if ack <= head {
            (self.acked, self.sent, self.timer) = (ack, ack, now);
        }
    }

    fn service(&mut self, now: u64) {
        if self.acked < self.sent && now - self.timer >= RTO {
            (self.sent, self.timer) = (self.acked, now);
        }
    }
}

/// The own stream's sender follows the model through any mix of sends,
/// acks, gap reports and timeouts, and keeps `acked ≤ sent ≤ head`: an ack
/// above the head changes nothing, a gap report below `acked` leaves
/// `acked == sent == ack`, a send while data is in flight leaves the timer,
/// and ack progress with data left restarts it.
#[quickcheck]
fn outbound_matches_model(ops: Vec<Op>) {
    let mut table = one_row();
    let (mut head, mut now) = (0_u64, 1_000_u64);
    let mut model = TxModel::default();
    for op in ops {
        let before = (table.own_sent[0], table.link[0].own);
        match op {
            Op::Issue(n) => head += u64::from(n),
            Op::Send(n) => {
                let upper = (table.own_sent[0] + u64::from(n)).min(head);
                if upper > table.own_sent[0] {
                    let in_flight = model.acked < model.sent;
                    table.own_tx(0).note_sent(upper, now);
                    model.send(upper, now);
                    if in_flight {
                        assert_eq!(table.link[0].own.timer_ms, before.1.timer_ms);
                    }
                }
            }
            Op::Ack(a) => {
                let ack = u64::from(a);
                table.own_tx(0).note_ack(ack, head, now);
                model.ack(ack, head, now);
                if ack > head {
                    assert_eq!((table.own_sent[0], table.link[0].own), before);
                }
                if ack > before.1.acked && ack <= head && ack < table.own_sent[0] {
                    assert_eq!(table.link[0].own.timer_ms, now);
                }
            }
            Op::Gap(a) => {
                let ack = u64::from(a);
                let owed = table.own_tx(0).gap_report(ack, head, now);
                model.gap(ack, head, now);
                if ack <= head {
                    assert_eq!((table.link[0].own.acked, table.own_sent[0]), (ack, ack));
                    assert_eq!(owed, ack < head);
                }
            }
            Op::Service => {
                if table.own_tx(0).timed_out(now) {
                    table.own_tx(0).rewind(now);
                }
                model.service(now);
            }
            Op::Advance(ms) => now += u64::from(ms),
            Op::Receive(..) | Op::Report => {}
        }
        let own = table.link[0].own;
        assert!(own.acked <= table.own_sent[0] && table.own_sent[0] <= head);
        let row = TxModel {
            sent: table.own_sent[0],
            acked: own.acked,
            timer: own.timer_ms,
        };
        assert_eq!(row, model, "after {op:?}");
    }
}

/// The receiver's gap rules the `peer` module documents on `Inbound`,
/// written out.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RxModel {
    have: u64,
    gap: Gap,
}

impl RxModel {
    fn receive(&mut self, lower: u64, upper: u64, now: u64) {
        let stash_of = |gap: Gap| match gap {
            Gap::Open { stash, .. } | Gap::Abandoned { stash } => Some(stash),
            Gap::Closed => None,
        };
        if lower > self.have {
            let stash = stash_of(self.gap).map_or((lower, upper), |s| {
                if lower <= s.1 && upper >= s.0 {
                    (s.0.min(lower), s.1.max(upper))
                } else if lower > s.0 {
                    (lower, upper)
                } else {
                    s
                }
            });
            self.gap = match self.gap {
                Gap::Closed => Gap::Open {
                    stash,
                    report_at_ms: now + FLOOR,
                    reports: 0,
                },
                Gap::Open {
                    report_at_ms,
                    reports,
                    ..
                } => Gap::Open {
                    stash,
                    report_at_ms,
                    reports,
                },
                Gap::Abandoned { .. } => Gap::Abandoned { stash },
            };
        } else if upper > self.have {
            self.have = upper;
            if let Some(stash) = stash_of(self.gap) {
                if stash.0 <= self.have {
                    self.have = self.have.max(stash.1);
                    self.gap = Gap::Closed;
                } else {
                    let report_at_ms = match self.gap {
                        Gap::Open { report_at_ms, .. } => report_at_ms,
                        _ => now + FLOOR,
                    };
                    self.gap = Gap::Open {
                        stash,
                        report_at_ms,
                        reports: 0,
                    };
                }
            }
        }
    }

    fn report(&mut self, now: u64) -> GapDue {
        match self.gap {
            Gap::Open {
                stash,
                report_at_ms,
                reports,
            } if now >= report_at_ms => {
                if reports == GOSSIP_GAP_REPORTS {
                    self.gap = Gap::Abandoned { stash };
                    GapDue::Abandoned {
                        missing: stash.0 - self.have,
                    }
                } else {
                    self.gap = Gap::Open {
                        stash,
                        report_at_ms: now + 4 * FLOOR,
                        reports: reports + 1,
                    };
                    GapDue::Report
                }
            }
            _ => GapDue::Nothing,
        }
    }
}

/// The own stream's receiver follows the model through any mix of ranges,
/// reports and time: `have` never decreases and never jumps a hole, an
/// abandoned gap never reports, and progress resets the report count.
#[quickcheck]
fn inbound_gap_matches_model(ops: Vec<Op>) {
    let mut inbound = Inbound::default();
    let mut model = RxModel::default();
    let mut now = 1_000_u64;
    for op in ops {
        let before = inbound;
        match op {
            Op::Receive(lower, len) => {
                let (lower, upper) = (u64::from(lower), u64::from(lower + len));
                inbound.receive(lower, upper, now, FLOOR);
                model.receive(lower, upper, now);
                if lower > before.have {
                    assert_eq!(inbound.have, before.have, "jumped a hole");
                }
                if inbound.have > before.have
                    && let Gap::Open { reports, .. } = inbound.gap
                {
                    assert_eq!(reports, 0);
                }
            }
            Op::Report => {
                let due = inbound.take_report(now, FLOOR);
                assert_eq!(due, model.report(now));
                if matches!(before.gap, Gap::Abandoned { .. }) {
                    assert_eq!(due, GapDue::Nothing);
                }
            }
            Op::Advance(ms) => now += u64::from(ms) / 4,
            _ => {}
        }
        assert!(inbound.have >= before.have);
        assert_eq!(inbound, Inbound::from(model), "after {op:?}");
    }
}

impl From<RxModel> for Inbound {
    fn from(model: RxModel) -> Self {
        Self {
            have: model.have,
            gap: model.gap,
        }
    }
}

/// After any mix of stream and gap events, a row's deadline is the earlier
/// of its stream timeout (while data is in flight) and its gap's next report,
/// and never when neither is pending.
#[quickcheck]
fn deadline_is_min_of_row_timers(ops: Vec<Op>) {
    let mut table = one_row();
    let (mut head, mut now) = (0_u64, 1_000_u64);
    for op in ops {
        match op {
            Op::Issue(n) => head += u64::from(n),
            Op::Send(n) => {
                let upper = (table.own_sent[0] + u64::from(n)).min(head);
                table.own_tx(0).note_sent(upper, now);
            }
            Op::Ack(a) => table.own_tx(0).note_ack(u64::from(a), head, now),
            Op::Gap(a) => {
                table.own_tx(0).gap_report(u64::from(a), head, now);
            }
            Op::Service => {
                if table.own_tx(0).timed_out(now) {
                    table.own_tx(0).rewind(now);
                }
            }
            Op::Advance(ms) => now += u64::from(ms),
            Op::Receive(lower, len) => {
                let (lower, upper) = (u64::from(lower), u64::from(lower + len));
                table.own_in(0).receive(lower, upper, now, FLOOR);
            }
            Op::Report => {
                table.own_in(0).take_report(now, FLOOR);
            }
        }
        table.refresh_deadline(0);
        let link = table.link[0];
        let timeout = (link.own.acked < table.own_sent[0]).then_some(link.own.timer_ms + RTO);
        let report = match link.own_in.gap {
            Gap::Open { report_at_ms, .. } => Some(report_at_ms),
            _ => None,
        };
        let expected = timeout.into_iter().chain(report).min();
        let deadline = table.deadline[0];
        assert_eq!(
            (deadline != Deadline::NEVER).then_some(deadline.0),
            expected
        );
        assert_eq!(table.flags(0).has(PeerFlags::GAP_OPEN), report.is_some());
    }
}

/// A peer's new incarnation resets its whole row; a late packet from the
/// incarnation before changes nothing, and the new one is then current.
#[test]
fn incarnation_change_resets_the_whole_row() {
    let mut table = one_row();
    let old = NodeIdentity::new(NodeId(9), 1);
    let new = NodeIdentity::new(NodeId(9), 2);
    assert_eq!(table.contact(0, old), Contact::First);
    table.own_tx(0).note_sent(5, 1_000);
    table.own_tx(0).note_ack(3, 5, 1_000);
    table.own_in(0).receive(0, 4, 1_000, FLOOR);
    table.own_in(0).receive(6, 7, 1_000, FLOOR);
    for bits in [
        PeerFlags::ACK_DUE,
        PeerFlags::REPLY_DUE,
        PeerFlags::RESEND_OWN,
    ] {
        table.set_flag(0, bits, true);
    }
    table.refresh_deadline(0);
    assert!(table.flags(0).has(PeerFlags::GAP_OPEN));

    assert_eq!(table.contact(0, new), Contact::Restarted);
    let link = *table.link(0);
    assert_eq!((link.own.acked, table.own_sent(0)), (0, 0));
    assert_eq!(link.own_in, Inbound::default());
    assert_eq!(table.flags(0), PeerFlags::default());
    assert_eq!(link.prev_incarnation, Some(1));
    assert_eq!(link.identity, Some(new));
    assert_eq!(table.deadline[0], Deadline::NEVER);

    table.own_tx(0).note_sent(2, 2_000);
    assert_eq!(table.contact(0, old), Contact::Late);
    assert_eq!((table.own_sent(0), table.link(0).identity), (2, Some(new)));
    assert_eq!(table.contact(0, new), Contact::Current);
}

/// Each node ranks the same peers by its own key: the top-weighted of the
/// distinct-hash addresses differs between local keys 1 and 2 (two keys
/// checked to disagree), so a weight that ignores the key or the address
/// ranks them alike.
#[test]
fn rendezvous_ranking_depends_on_the_local_key() {
    let top = |local_key| {
        (0..8)
            .map(|i| addr(2 * i))
            .max_by_key(|&peer| rendezvous_weight(local_key, peer))
    };
    assert_ne!(top(1), top(2));
}
