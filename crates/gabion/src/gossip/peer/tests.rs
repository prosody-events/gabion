use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use super::*;
use crate::crdt::NodeId;
use crate::gossip::transport::Sent;

/// Twice the capacity: inserts hit Present and Full, and removes hit and miss.
const POOL: u8 = 16;
const CAPACITY: NonZeroU16 = NonZeroU16::new(8).unwrap();
/// A few floors, so data in flight often times out between acks, and a whole
/// number of them, so `Op::Advance` lands exactly on the timeout.
const RTO: u64 = 4 * FLOOR;
const FLOOR: u64 = 5;
/// Four timeouts.
const SUSPECT_AFTER: u64 = 4 * RTO;

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
    PeerTable::with_capacity(CAPACITY, 7, RTO, SUSPECT_AFTER)
}

/// The stream range `(lower, upper]`; every caller passes `lower < upper`.
fn range(lower: u64, upper: u64) -> StreamRange {
    StreamRange::new(lower, upper).expect("lower < upper")
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
                    table.contact(row, NodeIdentity::new(id, 1), None);
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
        let mut rows: Vec<_> = table.entries(0).map(|e| (e.addr, e.node_id)).collect();
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
    /// Time moves this many relay floors. The timeout and the report times
    /// are whole floors, so time lands exactly on each.
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
            5 => Op::Advance(x),
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
                    table.tx(0, Stream::Own).note_sent(upper, now);
                    model.send(upper, now);
                    if in_flight {
                        assert_eq!(table.link[0].own.timer_ms, before.1.timer_ms);
                    }
                }
            }
            Op::Ack(a) => {
                let ack = u64::from(a);
                table.tx(0, Stream::Own).note_ack(ack, head, now);
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
                let owed = table.tx(0, Stream::Own).gap_report(ack, head, now);
                model.gap(ack, head, now);
                if ack <= head {
                    assert_eq!((table.link[0].own.acked, table.own_sent[0]), (ack, ack));
                    assert_eq!(owed, ack < head);
                }
            }
            Op::Service => {
                if table.own_timer(0).is_some_and(|at| at <= now) {
                    table.tx(0, Stream::Own).rewind(now);
                }
                model.service(now);
            }
            Op::Advance(n) => now += u64::from(n) * FLOOR,
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
                inbound.receive(range(lower, upper), now, FLOOR);
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
            Op::Advance(n) => now += u64::from(n) * FLOOR,
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
                table.tx(0, Stream::Own).note_sent(upper, now);
            }
            Op::Ack(a) => {
                table.tx(0, Stream::Own).note_ack(u64::from(a), head, now);
            }
            Op::Gap(a) => {
                table.tx(0, Stream::Own).gap_report(u64::from(a), head, now);
            }
            Op::Service => {
                if table.own_timer(0).is_some_and(|at| at <= now) {
                    table.tx(0, Stream::Own).rewind(now);
                }
            }
            Op::Advance(n) => now += u64::from(n) * FLOOR,
            Op::Receive(lower, len) => {
                let (lower, upper) = (u64::from(lower), u64::from(lower + len));
                table.link[0]
                    .own_in
                    .receive(range(lower, upper), now, FLOOR);
            }
            Op::Report => {
                table.link[0].own_in.take_report(now, FLOOR);
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

/// A zone hash for tests.
fn zone(raw: u32) -> ZoneHash {
    ZoneHash::from_wire(raw).expect("not 0")
}

/// A peer's new incarnation resets its whole row, its zone to the new
/// header's and its flags to one owed ack; a late packet from the
/// incarnation before changes nothing, and a zone-0 header never clears a
/// known zone.
#[test]
fn incarnation_change_resets_the_whole_row() {
    let mut table = one_row();
    let old = NodeIdentity::new(NodeId(9), 1);
    let new = NodeIdentity::new(NodeId(9), 2);
    assert_eq!(table.contact(0, old, Some(zone(1))), Contact::First);
    assert_eq!(table.contact(0, old, None), Contact::Current);
    assert_eq!(table.link(0).zone, Some(zone(1)));
    for stream in [Stream::Own, Stream::Relay] {
        table.tx(0, stream).note_sent(5, 1_000);
        table.tx(0, stream).note_ack(3, 5, 1_000);
    }
    table.receive_own(0, range(0, 4), false, 1_000, FLOOR);
    table.receive_own(0, range(6, 7), true, 1_000, FLOOR);
    table.receive_relay(0, range(2, 3), 1_000, FLOOR);
    table.note_send(
        0,
        Sent {
            bytes: 1,
            left: true,
        },
        true,
        1_000,
    );
    for bits in [PeerFlags::REPLY_DUE, PeerFlags::RESEND_OWN] {
        table.set_flag(0, bits, true);
    }
    table.refresh_deadline(0);
    assert!(table.flags(0).has(PeerFlags::GAP_OPEN));
    assert_eq!(table.link(0).answer, Answer::Owed { since_ms: 1_000 });

    assert_eq!(table.contact(0, new, Some(zone(2))), Contact::Restarted);
    let link = *table.link(0);
    assert_eq!((link.own, table.own_sent[0]), (Outbound::default(), 0));
    assert_eq!((link.relay, table.relay_sent[0]), (Outbound::default(), 0));
    for mark in [link.own_in, link.flagged_in, link.relay_in] {
        assert_eq!(mark, Inbound::default());
    }
    assert_eq!(
        (link.own_mode, link.answer),
        (OwnMode::Plain, Answer::Settled)
    );
    assert_eq!(link.zone, Some(zone(2)));
    assert_eq!(table.flags(0), PeerFlags::ACK_DUE);
    assert_eq!(link.prev_incarnation, Some(1));
    assert_eq!(link.identity, Some(new));
    assert_eq!(table.deadline[0], Deadline::NEVER);

    table.tx(0, Stream::Own).note_sent(2, 2_000);
    assert_eq!(table.contact(0, old, Some(zone(3))), Contact::Late);
    assert_eq!((table.own_sent[0], table.link(0).identity), (2, Some(new)));
    assert_eq!(table.link(0).zone, Some(zone(2)));
    assert_eq!(table.contact(0, new, None), Contact::Current);
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

/// A table of `members` peers at `addr(1..)`, all heard in zone 2, for a node
/// in zone 1 whose id hashes to `local_key`, roles refreshed.
fn zoned_table(local_key: u64, members: u8, capacity: u16) -> PeerTable {
    let capacity = NonZeroU16::new(capacity).unwrap();
    let mut table = PeerTable::with_capacity(capacity, local_key, RTO, SUSPECT_AFTER);
    table.set_local_zone(zone(1));
    for i in 1..=members {
        assert_eq!(table.insert(addr(2 * i)), Insert::Added);
        let row = table.find(addr(2 * i)).unwrap();
        table.contact(
            row,
            NodeIdentity::new(NodeId(u128::from(i)), 1),
            Some(zone(2)),
        );
    }
    table.refresh(0, |_| {});
    table
}

fn gateway(table: &PeerTable) -> Option<SocketAddr> {
    (0..table.len())
        .find(|&row| table.role(row) == Role::Gateway)
        .map(|row| table.addr(row))
}

/// Rendezvous gateways: removing a member moves only the origins whose
/// gateway it was, different origins pick different members, a suspect ranks
/// below every live member, an all-suspect zone picks the earliest
/// suspicion, and an answer restores the weight order.
#[quickcheck]
fn rendezvous_moves_only_the_removed_members_origins(members: u8, removed: u8) {
    let members = members % 11 + 2;
    let removed = addr(2 * (removed % members + 1));
    let mut gateways = Vec::new();
    for local_key in 1..=16 {
        let mut table = zoned_table(local_key, members, 16);
        let before = gateway(&table).unwrap();
        gateways.push(before);
        table.remove(removed);
        table.refresh(0, |_| {});
        assert_eq!(gateway(&table).unwrap() != before, before == removed);
    }
    gateways.sort();
    gateways.dedup();
    assert!(members < 4 || gateways.len() >= 2, "{gateways:?}");

    let mut table = zoned_table(1, members, 16);
    let first = gateway(&table).unwrap();
    let row = table.find(first).unwrap();
    table.link[row].answer = Answer::Owed { since_ms: 0 };
    table.judge(SUSPECT_AFTER, |_| {});
    table.refresh(SUSPECT_AFTER, |_| {});
    assert_ne!(gateway(&table), Some(first));
    for row in 0..table.len() {
        table.link[row].answer = Answer::Owed { since_ms: 0 };
    }
    let late = table.find(addr(2)).unwrap();
    table.link[late].answer = Answer::CannotRelay { since_ms: 5 };
    table.stale = true;
    table.judge(SUSPECT_AFTER, |_| {});
    table.refresh(SUSPECT_AFTER, |_| {});
    // `CannotRelay` from 5 began before `Owed` from 0 turns suspect.
    assert_eq!(gateway(&table), Some(addr(2)));
    for row in 0..table.len() {
        let answer = AckInfo {
            incarnation: 7,
            probe_reply: true,
            ..AckInfo::default()
        };
        table.credit(row, answer, 7, [0, 0], SUSPECT_AFTER);
    }
    table.refresh(SUSPECT_AFTER, |_| {});
    assert_eq!(gateway(&table), Some(first));
}

/// One row of a refresh case: its zone (0 = none), whether it speaks another
/// version, and its answer.
#[derive(Clone, Debug)]
struct RefreshCase {
    local: u32,
    rows: Vec<(u32, bool, Answer)>,
}

impl Arbitrary for RefreshCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let n = usize::arbitrary(g) % 64 + 1;
        // Up to one zone per peer.
        let zones = u32::arbitrary(g) % n as u32 + 1;
        let answer = |g: &mut Gen| match u8::arbitrary(g) % 4 {
            0 | 1 => Answer::Settled,
            2 => Answer::Owed {
                since_ms: u64::arbitrary(g) % 50,
            },
            _ => Answer::CannotRelay {
                since_ms: u64::arbitrary(g) % 50,
            },
        };
        Self {
            local: u32::arbitrary(g) % 3,
            rows: (0..n)
                .map(|_| {
                    (
                        u32::arbitrary(g) % (zones + 1),
                        bool::arbitrary(g) && bool::arbitrary(g),
                        answer(g),
                    )
                })
                .collect(),
        }
    }
}

/// The two-pass refresh gives each row the role an O(n²) reference gives:
/// a row of a known remote zone is its gateway iff no other row of that
/// zone that speaks this version outranks it, by the reference's own rank:
/// live members by weight, then suspects by the earliest suspicion, then the
/// address. The zone table is sized to the rows, so the one-zone-per-peer
/// case fills it exactly.
#[quickcheck]
fn role_refresh_equals_brute_force(case: RefreshCase) {
    // At `NOW` an `Owed` answer from 40 ms or earlier is suspect, and every
    // `CannotRelay` is.
    const NOW: u64 = SUSPECT_AFTER + 40;
    let n = case.rows.len();
    let capacity = NonZeroU16::new(n as u16).unwrap();
    let mut table = PeerTable::with_capacity(capacity, 3, RTO, SUSPECT_AFTER);
    if case.local > 0 {
        table.set_local_zone(zone(case.local));
    }
    let addr_of = |row: usize| SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 1000 + row as u16));
    for (i, &(z, muted, answer)) in case.rows.iter().enumerate() {
        table.insert(addr_of(i));
        table.contact(
            i,
            NodeIdentity::new(NodeId(i as u128), 1),
            ZoneHash::from_wire(z),
        );
        if muted {
            table.mute(i);
        }
        table.link[i].answer = answer;
    }
    table.judge(NOW, |_| {});
    table.stale = true;
    table.refresh(NOW, |_| {});
    let eligible = |row: usize| {
        let z = case.rows[row].0;
        !case.rows[row].1 && z != 0 && case.local != 0 && z != case.local
    };
    let rank = |row: usize| {
        let suspect_from = match case.rows[row].2 {
            Answer::Settled => None,
            Answer::Owed { since_ms } => Some(since_ms + SUSPECT_AFTER),
            Answer::CannotRelay { since_ms } => Some(since_ms),
        };
        match suspect_from.filter(|&from| from <= NOW) {
            Some(from) => (false, u64::MAX - from, addr_of(row)),
            None => (true, rendezvous_weight(3, addr_of(row)), addr_of(row)),
        }
    };
    for row in 0..n {
        let expected = if case.rows[row].1 {
            Role::Incompatible
        } else if !eligible(row) {
            Role::Zone
        } else if (0..n).any(|other| {
            other != row
                && eligible(other)
                && case.rows[other].0 == case.rows[row].0
                && rank(other) > rank(row)
        }) {
            Role::Remote
        } else {
            Role::Gateway
        };
        assert_eq!(table.role(row), expected, "row {row} of {case:?}");
    }
}

/// One event on a row's answer state.
#[derive(Clone, Copy, Debug)]
enum AnswerOp {
    /// A send that asks or not, left or held.
    Send {
        asks: bool,
        left: bool,
    },
    /// Own data goes out: the stream is owed.
    Data,
    /// An ack of everything sent, or a repeat of the stuck one, flagged
    /// `CANNOT_RELAY` or not.
    Ack {
        progress: bool,
        cannot_relay: bool,
    },
    ProbeReply,
    Role(u8),
    Mute,
    Restart,
    /// A held packet leaves after this many floors, on a round whose `now`
    /// may trail the round that held it by up to a floor (a late heartbeat),
    /// so the shift can reach past `now`.
    Shift(u8),
    Advance(u8),
}

impl Arbitrary for AnswerOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 10 {
            0 => Self::Send {
                asks: bool::arbitrary(g),
                left: bool::arbitrary(g),
            },
            1 => Self::Data,
            2 | 3 => Self::Ack {
                progress: bool::arbitrary(g),
                cannot_relay: bool::arbitrary(g),
            },
            4 => Self::ProbeReply,
            5 => Self::Role(u8::arbitrary(g) % 3),
            6 if u8::arbitrary(g) % 4 == 0 => Self::Mute,
            7 if u8::arbitrary(g) % 4 == 0 => Self::Restart,
            8 => Self::Shift(u8::arbitrary(g) % 8),
            _ => Self::Advance(u8::arbitrary(g) % 8),
        }
    }
}

/// A row's answer state follows the transition table of the design (§3.7)
/// through any mix of sends, acks, probe answers, role changes, restarts and
/// held time, and it is suspect exactly when `suspect_after` has passed since
/// its oldest unanswered packet or it said it cannot relay; for two
/// `suspect_after` values.
#[quickcheck]
fn answer_matches_its_model(ops: Vec<AnswerOp>) {
    for suspect_after in [4 * RTO, 3 * RTO] {
        let mut table = PeerTable::with_capacity(CAPACITY, 7, RTO, suspect_after);
        table.insert(addr(0));
        let mut identity = NodeIdentity::new(NodeId(9), 1);
        table.contact(0, identity, None);
        let (mut model, mut now, mut head) = (Answer::Settled, 1_000_u64, 0_u64);
        for op in &ops {
            let suspect = |answer: Answer, judged| match answer {
                Answer::Settled => false,
                Answer::Owed { since_ms } => since_ms + suspect_after <= judged,
                Answer::CannotRelay { since_ms } => since_ms <= judged,
            };
            let was = suspect(model, table.judged_ms);
            let owed = |table: &mut PeerTable| table.owed(0) != [false; 2];
            let acks = |table: &PeerTable, ack, probe_reply, cannot_relay| AckInfo {
                incarnation: 7,
                own: if table.role(0) == Role::Gateway {
                    OwnAck::Flagged(ack)
                } else {
                    OwnAck::Plain(ack)
                },
                relay: table.relay_sent[0],
                probe_reply,
                cannot_relay,
                ..AckInfo::default()
            };
            match *op {
                AnswerOp::Send { asks, left } => {
                    table.note_send(0, Sent { bytes: 0, left }, asks, now);
                    if asks && left && model == Answer::Settled {
                        model = Answer::Owed { since_ms: now };
                    }
                }
                AnswerOp::Data => {
                    head += 1;
                    table.tx(0, Stream::Own).note_sent(head, now);
                    table.note_send(
                        0,
                        Sent {
                            bytes: 1,
                            left: true,
                        },
                        true,
                        now,
                    );
                    if model == Answer::Settled {
                        model = Answer::Owed { since_ms: now };
                    }
                }
                AnswerOp::Ack {
                    progress,
                    cannot_relay,
                } => {
                    let ack = if progress {
                        table.own_sent[0]
                    } else {
                        table.link[0].own.acked
                    };
                    let moved =
                        progress && ack > table.link[0].own.acked && table.role(0).gets_own();
                    let counts =
                        cannot_relay && matches!(table.role(0), Role::Gateway | Role::Remote);
                    table.credit(0, acks(&table, ack, false, cannot_relay), 7, [head, 0], now);
                    // A counting `CANNOT_RELAY` packet's acks are no progress.
                    if counts {
                        let from = match model {
                            Answer::Settled => now,
                            Answer::Owed { since_ms } => since_ms + suspect_after,
                            Answer::CannotRelay { since_ms } => since_ms,
                        };
                        model = Answer::CannotRelay {
                            since_ms: from.min(now),
                        };
                    } else if moved {
                        model = if owed(&mut table) {
                            Answer::Owed { since_ms: now }
                        } else {
                            Answer::Settled
                        };
                    }
                }
                AnswerOp::ProbeReply => {
                    let owed_before = owed(&mut table);
                    let ack = table.link[0].own.acked;
                    table.credit(0, acks(&table, ack, true, false), 7, [head, 0], now);
                    if !owed_before {
                        model = Answer::Settled;
                    } else if was {
                        // A suspect's answer rewinds; it is not progress.
                        assert_eq!(table.own_sent[0], table.link[0].own.acked);
                    }
                }
                AnswerOp::Role(role) => {
                    let role = [Role::Zone, Role::Gateway, Role::Remote][usize::from(role)];
                    if table.role(0) != Role::Incompatible
                        && table.apply_role(0, role)
                        && role == Role::Remote
                        && !was
                    {
                        model = Answer::Settled;
                    }
                }
                AnswerOp::Mute => {
                    table.mute(0);
                    model = Answer::Settled;
                }
                AnswerOp::Restart => {
                    identity.incarnation += 1;
                    table.contact(0, identity, None);
                    model = Answer::Settled;
                    head = 0;
                }
                AnswerOp::Shift(held) => {
                    let held = u64::from(held) * FLOOR;
                    now += held.saturating_sub(FLOOR);
                    table.shift_owed(held, now);
                    if let Answer::Owed { since_ms } = &mut model {
                        *since_ms = (*since_ms + held).min(now);
                    }
                }
                AnswerOp::Advance(n) => {
                    now += u64::from(n) * FLOOR;
                    table.judge(now, |_| {});
                }
            }
            assert_eq!(table.link[0].answer, model, "after {op:?}");
            assert_eq!(
                table.suspect(0),
                suspect(model, table.judged_ms),
                "after {op:?}"
            );
        }
    }
}

/// An own-stream ack counts on a `Zone` row only plain and on a `Gateway`
/// row only flagged, and a `GAP_OWN` report rewinds a forgotten stream only
/// when its kind matches the role; becoming the gateway rewinds the own
/// stream to 0.
#[test]
fn own_acks_count_only_in_the_kind_the_role_receives() {
    for (role, counted, ignored) in [
        (Role::Zone, OwnAck::Plain(3), OwnAck::Flagged(3)),
        (Role::Gateway, OwnAck::Flagged(3), OwnAck::Plain(3)),
    ] {
        let mut table = one_row();
        table.contact(0, NodeIdentity::new(NodeId(9), 1), None);
        table.apply_role(0, role);
        table.tx(0, Stream::Own).note_sent(5, 1_000);
        let acks = |own, gap_own| AckInfo {
            incarnation: 7,
            own,
            gap_own,
            ..AckInfo::default()
        };
        table.credit(0, acks(ignored, false), 7, [5, 0], 1_000);
        assert_eq!(table.link[0].own.acked, 0, "{role:?}");
        table.credit(0, acks(counted, false), 7, [5, 0], 1_000);
        assert_eq!(table.link[0].own.acked, 3, "{role:?}");
        // A forgotten stream: a gap report below `acked`.
        let below = |own: OwnAck| match own {
            OwnAck::Plain(_) => OwnAck::Plain(1),
            OwnAck::Flagged(_) => OwnAck::Flagged(1),
        };
        table.credit(0, acks(below(ignored), true), 7, [5, 0], 1_000);
        assert_eq!(table.link[0].own.acked, 3, "{role:?}");
        table.credit(0, acks(below(counted), true), 7, [5, 0], 1_000);
        assert_eq!((table.link[0].own.acked, table.own_sent[0]), (1, 1));
    }
    let mut table = one_row();
    table.tx(0, Stream::Own).note_sent(5, 1_000);
    table.tx(0, Stream::Own).note_ack(5, 5, 1_000);
    assert!(table.apply_role(0, Role::Gateway));
    assert_eq!((table.link[0].own.acked, table.own_sent[0]), (0, 0));
    assert!(table.flags(0).has(PeerFlags::RESEND_OWN));
}

/// Flagged own ranges move their own mark: a lost first flagged packet opens
/// a flagged gap although the plain mark is past it, freshness is judged
/// against the flagged mark, and only the gap of the kind received last is
/// reported, with the ack of that kind.
#[test]
fn flagged_ranges_keep_their_own_mark() {
    let mut table = one_row();
    assert!(!table.receive_own(0, range(0, 10), false, 1_000, FLOOR));
    assert!(table.receive_own(0, range(3, 6), true, 1_000, FLOOR));
    let link = table.link[0];
    assert_eq!((link.own_in.have, link.flagged_in.have), (10, 0));
    assert_eq!(link.own_ack(), OwnAck::Flagged(0));
    assert!(table.flags(0).has(PeerFlags::GAP_OPEN));
    let reports = table.take_reports(0, 1_000 + FLOOR, FLOOR);
    assert_eq!(reports, [GapDue::Report, GapDue::Nothing]);
    // The lost packet arrives late: fresh, as it lies above the flagged mark.
    assert!(table.receive_own(0, range(0, 3), true, 1_010, FLOOR));
    assert_eq!(table.link[0].flagged_in.have, 6);
    assert!(!table.receive_own(0, range(2, 6), true, 1_010, FLOOR));
    // A plain packet makes the plain mark the one acked and reported.
    table.receive_own(0, range(12, 14), false, 1_010, FLOOR);
    assert_eq!(table.link[0].own_ack(), OwnAck::Plain(10));
    let reports = table.take_reports(0, 1_010 + FLOOR, FLOOR);
    assert_eq!(reports, [GapDue::Report, GapDue::Nothing]);
}

/// A row's deadline also holds the relay stream's timeout, the relay gap's
/// report, and the moment an awaited answer turns the row suspect; once
/// judged suspect, that moment leaves it.
#[test]
fn deadline_holds_the_relay_and_suspicion_timers() {
    let mut table = one_row();
    table.tx(0, Stream::Relay).note_sent(2, 1_000);
    table.refresh_deadline(0);
    assert_eq!(table.deadline[0], Deadline(1_000 + RTO));
    table.receive_relay(0, range(4, 5), 1_000, FLOOR);
    assert_eq!(table.deadline[0], Deadline(1_000 + FLOOR));
    assert!(table.flags(0).has(PeerFlags::GAP_OPEN));

    let mut table = one_row();
    table.apply_role(0, Role::Remote);
    table.note_send(
        0,
        Sent {
            bytes: 1,
            left: true,
        },
        true,
        1_000,
    );
    table.tx(0, Stream::Own).restart(1_000 + SUSPECT_AFTER);
    table.refresh_deadline(0);
    assert_eq!(table.deadline[0], Deadline(1_000 + SUSPECT_AFTER));
    table.judge(1_000 + SUSPECT_AFTER, |_| {});
    assert!(table.suspect(0));
    // Only the suspect's next probe remains.
    assert_eq!(table.deadline[0], Deadline(1_000 + SUSPECT_AFTER + RTO));
}
