//! A bounded capture of every message the cluster sends, for the page's
//! Messages view: fixed columns of plain data and a byte arena for bodies,
//! all allocated in [`Capture::new`], so recording a message never allocates.
//!
//! The logging transport [`records`](Capture::record) each message as it is
//! sent, with its raw header and body; the engine
//! [`settles`](Capture::settle) it when it stamps the event, with its instant
//! and, for a lost one, why. The page reads a filtered window of rows through
//! one preallocated `f64` buffer, and decodes one message only when the reader
//! selects it ([`Capture::select`]).

use std::cell::RefCell;
use std::rc::Rc;

use gabion::wire::{
    CellPayload, FrameLimits, HEADER_LEN, Header, OwnAck, Payload, decode, read_header,
};
use serde::{Deserialize, Serialize};

use crate::engine::CLUSTER_ID_HASH;
use crate::event::{
    ListedRepeat, MessageRole, MessageType, PacketKind, Riders, Seq, Tier, TypeSet,
};
use crate::hex::u128_hex;

/// Messages the capture keeps: the newest this many of the run.
pub const CAPTURE_PACKETS: usize = 32_768;
/// Bytes of message bodies the capture keeps.
pub const CAPTURE_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Markers the capture keeps.
pub const CAPTURE_MARKERS: usize = 1_024;
/// Delivered gap reports waiting for the repeat they ask for.
const PENDING_REPEATS: usize = 256;
/// A silence longer than this between two entries of the view is listed as
/// an interval row: a fifth of the default heartbeat.
pub const INTERVAL_ROW_MS: u64 = 100;
/// Slots of the rows buffer's head.
pub const HEAD_SLOTS: usize = 40;
/// Slots of one row.
pub const ROW_SLOTS: usize = 11;
/// Rows one read writes at most.
pub const MAX_ROWS: usize = 64;
/// Related messages a selection marks at most.
const MAX_RELATED: usize = 16;

/// The capture as the transports and the engine share it: everything runs on
/// one thread, and no borrow is held across an `.await`.
pub type SharedCapture = Rc<RefCell<Capture>>;

/// Why a lost message was lost.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum LostBy {
    /// Its link was cut when it was sent.
    Blocked,
    /// It was dropped on a lossy link (rarely, a full queue on that link).
    Loss,
    /// A full queue on a link with no loss: the only drop left there.
    QueueFull,
}

/// What a message's sending came to, decided once when it settles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    Accepted,
    Lost(LostBy),
}

/// What the page shows of a message's outcome: derived, never stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind")]
pub enum Fate {
    /// Accepted, and held with its sub-step, not yet delivered.
    InFlight,
    Delivered,
    Lost {
        by: LostBy,
    },
}

impl Fate {
    fn code(self) -> u32 {
        match self {
            Self::Delivered => 0,
            Self::InFlight => 1,
            Self::Lost {
                by: LostBy::Blocked,
            } => 2,
            Self::Lost { by: LostBy::Loss } => 3,
            Self::Lost {
                by: LostBy::QueueFull,
            } => 4,
        }
    }
}

/// Something that happened between messages, listed as a paragraph break.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkerKind {
    /// `node` decided a burst: `admitted` requests admitted, `rejected`
    /// refused.
    Request {
        node: u32,
        admitted: u64,
        rejected: u64,
    },
    /// `origin`'s gateway into `zone` changed.
    GatewayChanged {
        origin: u32,
        zone: u8,
        from: Option<u32>,
        to: Option<u32>,
    },
    Added {
        node: u32,
    },
    Removed {
        node: u32,
    },
    /// Every link got the scenario's own network back.
    Heal,
}

impl MarkerKind {
    /// Whether the marker names a node `admits` accepts; a heal names all.
    fn names(self, admits: impl Fn(u32) -> bool) -> bool {
        match self {
            Self::Request { node, .. } | Self::Added { node } | Self::Removed { node } => {
                admits(node)
            }
            Self::GatewayChanged {
                origin, from, to, ..
            } => admits(origin) || from.is_some_and(&admits) || to.is_some_and(&admits),
            Self::Heal => true,
        }
    }

    /// The marker's row: kind code then four operands, absent ones -1.
    fn slots(self) -> [f64; 5] {
        let opt = |id: Option<u32>| id.map_or(-1.0, f64::from);
        match self {
            Self::Request {
                node,
                admitted,
                rejected,
            } => [0.0, f64::from(node), admitted as f64, rejected as f64, -1.0],
            Self::GatewayChanged {
                origin,
                zone,
                from,
                to,
            } => [1.0, f64::from(origin), f64::from(zone), opt(from), opt(to)],
            Self::Added { node } => [2.0, f64::from(node), -1.0, -1.0, -1.0],
            Self::Removed { node } => [3.0, f64::from(node), -1.0, -1.0, -1.0],
            Self::Heal => [4.0, -1.0, -1.0, -1.0, -1.0],
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Marker {
    /// The first message seq settled after it.
    before: u64,
    at_ms: u64,
    what: MarkerKind,
}

/// Which way messages through the scoped node go.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum Way {
    #[default]
    Both,
    Sent,
    Received,
}

/// Which way messages between a pair go.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum PairWay {
    #[default]
    Both,
    AToB,
    BToA,
}

/// Which tier of messages a scope lists.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub enum TierFilter {
    #[default]
    Any,
    InZone,
    CrossZone,
}

impl TierFilter {
    fn admits(self, tier: Tier) -> bool {
        match self {
            Self::Any => true,
            Self::InZone => tier == Tier::InZone,
            Self::CrossZone => tier == Tier::CrossZone,
        }
    }
}

/// Two distinct nodes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "[u32; 2]")]
pub struct Pair {
    a: u32,
    b: u32,
}

impl Pair {
    /// `None` unless `a != b`.
    pub fn new(a: u32, b: u32) -> Option<Self> {
        (a != b).then_some(Self { a, b })
    }
}

impl TryFrom<[u32; 2]> for Pair {
    type Error = &'static str;

    fn try_from([a, b]: [u32; 2]) -> Result<Self, Self::Error> {
        Self::new(a, b).ok_or("a pair names two different nodes")
    }
}

/// Whose messages the view lists.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "kind")]
pub enum Scope {
    All {
        #[serde(default)]
        tier: TierFilter,
    },
    Node {
        node: u32,
        #[serde(default)]
        way: Way,
        #[serde(default)]
        tier: TierFilter,
    },
    Pair {
        pair: Pair,
        #[serde(default)]
        way: PairWay,
    },
}

impl Scope {
    fn admits(self, src: u32, dst: u32, tier: Tier) -> bool {
        match self {
            Self::All { tier: t } => t.admits(tier),
            Self::Node { node, way, tier: t } => {
                t.admits(tier)
                    && match way {
                        Way::Both => src == node || dst == node,
                        Way::Sent => src == node,
                        Way::Received => dst == node,
                    }
            }
            Self::Pair {
                pair: Pair { a, b },
                way,
            } => match way {
                PairWay::Both => (src, dst) == (a, b) || (src, dst) == (b, a),
                PairWay::AToB => (src, dst) == (a, b),
                PairWay::BToA => (src, dst) == (b, a),
            },
        }
    }

    /// Whether a marker belongs in this scope. A request's marker shows only
    /// in a scope that names its node: in every node's scope at once it would
    /// interleave every row under a sustained feed.
    fn shows(self, what: MarkerKind) -> bool {
        match self {
            Self::All { .. } => !matches!(what, MarkerKind::Request { .. }),
            Self::Node { node, .. } => what.names(|id| id == node),
            Self::Pair {
                pair: Pair { a, b },
                ..
            } => what.names(|id| id == a || id == b),
        }
    }
}

/// The view's filter: a scope, the types to list (none means every type),
/// and whether only lost messages are listed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct Filter {
    pub scope: Scope,
    #[serde(default)]
    pub types: u16,
    #[serde(default)]
    pub lost_only: bool,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            scope: Scope::All {
                tier: TierFilter::Any,
            },
            types: 0,
            lost_only: false,
        }
    }
}

/// One entry of the view, packed: a two-bit tag above a payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Entry {
    Message(u64),
    Marker(u64),
    Interval(u64),
}

const TAG_SHIFT: u32 = 62;
const PAYLOAD: u64 = (1 << TAG_SHIFT) - 1;

impl Entry {
    fn pack(self) -> u64 {
        match self {
            Self::Message(seq) => seq,
            Self::Marker(index) => (1 << TAG_SHIFT) | index,
            Self::Interval(ms) => (2 << TAG_SHIFT) | ms.min(PAYLOAD),
        }
    }

    fn unpack(word: u64) -> Self {
        let payload = word & PAYLOAD;
        match word >> TAG_SHIFT {
            0 => Self::Message(payload),
            1 => Self::Marker(payload),
            _ => Self::Interval(payload),
        }
    }
}

/// The filtered list the page reads, appended as messages settle and rebuilt
/// in one pass when the filter changes. Entries are addressed by ordinal,
/// their place since the filter was set, which never changes while it holds.
struct View {
    filter: Filter,
    entries: Box<[u64]>,
    /// Ordinals appended: the next entry's ordinal.
    len: u64,
    /// The instant of the last entry, for interval rows.
    last_ms: Option<u64>,
    /// No entry before this ordinal is still kept.
    first_kept: u64,
    /// Per [`MessageType`], the messages in scope, and the lost ones.
    counts: [u32; MessageType::ALL.len()],
    lost: [u32; MessageType::ALL.len()],
    scope_count: u32,
    scope_lost: u32,
    /// Messages listed, lost ones listed, and their bytes.
    shown: u32,
    shown_lost: u32,
    shown_bytes: u64,
}

impl View {
    fn new(capacity: usize) -> Self {
        Self {
            filter: Filter::default(),
            entries: vec![0; capacity].into_boxed_slice(),
            len: 0,
            last_ms: None,
            first_kept: 0,
            counts: [0; MessageType::ALL.len()],
            lost: [0; MessageType::ALL.len()],
            scope_count: 0,
            scope_lost: 0,
            shown: 0,
            shown_lost: 0,
            shown_bytes: 0,
        }
    }

    fn clear(&mut self) {
        let filter = self.filter;
        let entries = std::mem::take(&mut self.entries);
        *self = Self {
            filter,
            entries,
            ..Self::new(0)
        };
    }

    fn push(&mut self, entry: Entry, at_ms: u64) {
        if let Some(last) = self.last_ms
            && at_ms > last + INTERVAL_ROW_MS
            && matches!(entry, Entry::Message(_))
        {
            self.put(Entry::Interval(at_ms - last));
        }
        self.last_ms = Some(at_ms);
        self.put(entry);
    }

    fn put(&mut self, entry: Entry) {
        let slot = (self.len % self.entries.len() as u64) as usize;
        self.entries[slot] = entry.pack();
        self.len += 1;
    }

    /// The entry at `ordinal`, unless the ring has overwritten it.
    fn at(&self, ordinal: u64) -> Option<Entry> {
        let cap = self.entries.len() as u64;
        (ordinal < self.len && ordinal + cap >= self.len)
            .then(|| Entry::unpack(self.entries[(ordinal % cap) as usize]))
    }
}

/// A delivered gap report's ask: `from` should repeat its `stream` to `to`
/// from after `mark`.
#[derive(Clone, Copy, Debug, Default)]
struct PendingRepeat {
    from: u32,
    to: u32,
    stream: Stream,
    mark: u64,
}

/// One of a sender's two streams.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub enum Stream {
    #[default]
    Own,
    Relay,
}

/// The fields a link scan reads from one kept header.
#[derive(Clone, Copy, Debug)]
struct Info {
    seq: u64,
    at_ms: u64,
    src: u32,
    dst: u32,
    role: MessageRole,
    riders: Riders,
    lost: bool,
    /// The data stream and range `(lower, upper]` it carries.
    data: Option<(Stream, u64, u64)>,
    gap_own: bool,
    gap_relay: bool,
    own_mark: u64,
    relay_mark: u64,
    incarnation: u32,
    probe: bool,
    probe_reply: bool,
}

impl Info {
    fn gap(&self, stream: Stream) -> Option<u64> {
        match stream {
            Stream::Own => self.gap_own.then_some(self.own_mark),
            Stream::Relay => self.gap_relay.then_some(self.relay_mark),
        }
    }

    fn mark(&self, stream: Stream) -> u64 {
        match stream {
            Stream::Own => self.own_mark,
            Stream::Relay => self.relay_mark,
        }
    }
}

/// How one message relates to the selected one, said from the selected
/// one's side.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Relation {
    /// A probe answer to the selected probe, or a sweep page to its request.
    AnsweredBy,
    Answers,
    /// The first ack whose mark reaches the selected data's top.
    AcknowledgedBy,
    Acknowledges,
    /// The first later gap report that covers the selected lost data.
    ReportedMissingBy,
    ReportsMissing,
    /// The delivered data whose arrival showed the selected report's gap.
    RevealedBy,
    /// The first later data on the link and stream that covers it.
    RepeatedAs,
    Repeats,
}

/// Why a data message repeats what it already sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RepeatCause {
    AfterTimeout,
    AfterGapReport,
    /// Its peer's probe answer showed the positions still owed.
    AfterAnswer,
}

impl From<ListedRepeat> for RepeatCause {
    fn from(repeat: ListedRepeat) -> Self {
        match repeat {
            ListedRepeat::AfterTimeout => Self::AfterTimeout,
            ListedRepeat::AfterGapReport => Self::AfterGapReport,
        }
    }
}

/// One link from the selected message to a related one.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Link {
    pub relation: Relation,
    pub seq: Seq,
    pub role: MessageRole,
    /// The related message's instant less the selected one's.
    pub offset_ms: i64,
}

/// The header's fields, decoded by the library's own reader.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct HeaderFields {
    pub kind: PacketKind,
    pub flags: u8,
    pub cells: u16,
    pub sender: Option<u32>,
    pub sender_incarnation: u32,
    pub sender_zone: u32,
    pub ack_incarnation: u32,
    pub own_ack: u64,
    pub own_ack_as_gateway: bool,
    pub relay_ack: u64,
    /// `(lower, upper]` of a stream packet.
    pub range: Option<[u64; 2]>,
}

/// One decoded cell.
#[derive(Clone, Debug, Serialize)]
pub struct CellRow {
    /// The origin's stable id.
    pub origin: Option<u32>,
    #[serde(with = "u128_hex")]
    pub rule: u128,
    #[serde(with = "u128_hex")]
    pub key: u128,
    pub bucket: u32,
    pub count: u32,
    pub incarnation: u32,
    pub updated_ms: u64,
}

/// One message, read: a copy the page owns, so it stays whole after the
/// message leaves the capture.
#[derive(Clone, Debug, Serialize)]
pub struct Detail {
    pub seq: Seq,
    pub at_ms: u64,
    pub src: u32,
    pub dst: u32,
    pub wire_len: u16,
    pub role: MessageRole,
    pub riders: Riders,
    pub repeat: Option<RepeatCause>,
    pub tier: Tier,
    pub fate: Fate,
    /// The header, then the body when it is kept.
    pub bytes: Vec<u8>,
    pub body_kept: bool,
    pub header: HeaderFields,
    pub cells: Vec<CellRow>,
    pub links: Vec<Link>,
    /// Whether it asks its receiver for an answer.
    pub asks_answer: bool,
}

/// The stable id behind a gossip node id: the engine's `id·256 + 1`.
pub(crate) fn stable_id(node_id: u128) -> Option<u32> {
    (node_id % 0x100 == 1)
        .then(|| u32::try_from(node_id / 0x100).ok())
        .flatten()
}

/// The capture. See the module note.
pub struct Capture {
    at_ms: Box<[u64]>,
    src: Box<[u32]>,
    dst: Box<[u32]>,
    wire_len: Box<[u16]>,
    header: Box<[[u8; HEADER_LEN]]>,
    body_at: Box<[u64]>,
    role: Box<[MessageRole]>,
    riders: Box<[Riders]>,
    repeat: Box<[Option<ListedRepeat>]>,
    tier: Box<[Tier]>,
    outcome: Box<[Outcome]>,
    arena: Box<[u8]>,
    /// Bytes ever written to the arena, skips included: the next body's
    /// absolute position.
    body_written: u64,
    /// The newest seq recorded, and settled.
    recorded: u64,
    settled: u64,
    /// The first seq of the open sub-step, while one is open.
    open_from: Option<u64>,
    markers: Box<[Marker]>,
    markers_written: u64,
    pending: Box<[PendingRepeat]>,
    pending_len: usize,
    view: View,
    rows: Box<[f64]>,
    related: [u64; MAX_RELATED],
    related_len: usize,
    selected: Option<u64>,
}

impl Capture {
    /// A capture of the newest `packets` messages and `body_bytes` of their
    /// bodies: everything it will ever hold, allocated here.
    pub fn new(packets: usize, body_bytes: usize) -> Self {
        let marker = Marker {
            before: 0,
            at_ms: 0,
            what: MarkerKind::Heal,
        };
        Self {
            at_ms: vec![0; packets].into_boxed_slice(),
            src: vec![0; packets].into_boxed_slice(),
            dst: vec![0; packets].into_boxed_slice(),
            wire_len: vec![0; packets].into_boxed_slice(),
            header: vec![[0; HEADER_LEN]; packets].into_boxed_slice(),
            body_at: vec![0; packets].into_boxed_slice(),
            role: vec![MessageRole::Ack; packets].into_boxed_slice(),
            riders: vec![Riders::NONE; packets].into_boxed_slice(),
            repeat: vec![None; packets].into_boxed_slice(),
            tier: vec![Tier::InZone; packets].into_boxed_slice(),
            outcome: vec![Outcome::Accepted; packets].into_boxed_slice(),
            arena: vec![0; body_bytes].into_boxed_slice(),
            body_written: 0,
            recorded: 0,
            settled: 0,
            open_from: None,
            markers: vec![marker; CAPTURE_MARKERS].into_boxed_slice(),
            markers_written: 0,
            pending: vec![PendingRepeat::default(); PENDING_REPEATS].into_boxed_slice(),
            pending_len: 0,
            view: View::new(2 * packets + CAPTURE_MARKERS),
            rows: vec![0.0; HEAD_SLOTS + MAX_ROWS * ROW_SLOTS].into_boxed_slice(),
            related: [0; MAX_RELATED],
            related_len: 0,
            selected: None,
        }
    }

    /// The production capture, shared.
    pub fn shared() -> SharedCapture {
        Rc::new(RefCell::new(Self::new(CAPTURE_PACKETS, CAPTURE_BODY_BYTES)))
    }

    /// Bytes the capture holds on the heap, all allocated by [`Capture::new`].
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of_val;
        size_of_val(&*self.at_ms)
            + size_of_val(&*self.src)
            + size_of_val(&*self.dst)
            + size_of_val(&*self.wire_len)
            + size_of_val(&*self.header)
            + size_of_val(&*self.body_at)
            + size_of_val(&*self.role)
            + size_of_val(&*self.riders)
            + size_of_val(&*self.repeat)
            + size_of_val(&*self.tier)
            + size_of_val(&*self.outcome)
            + size_of_val(&*self.arena)
            + size_of_val(&*self.markers)
            + size_of_val(&*self.pending)
            + size_of_val(&*self.view.entries)
            + size_of_val(&*self.rows)
    }

    fn capacity(&self) -> u64 {
        self.at_ms.len() as u64
    }

    fn slot(&self, seq: u64) -> usize {
        (seq % self.capacity()) as usize
    }

    /// Whether `seq`'s slot still holds it.
    pub fn kept(&self, seq: u64) -> bool {
        seq >= 1 && seq <= self.recorded && seq + self.capacity() > self.recorded
    }

    /// Whether `seq`'s body is still in the arena.
    fn body_kept(&self, seq: u64) -> bool {
        let slot = self.slot(seq);
        usize::from(self.wire_len[slot]) <= HEADER_LEN
            || self.body_written <= self.body_at[slot] + self.arena.len() as u64
    }

    /// The newest seq settled, which is the run's message count.
    pub fn total(&self) -> u64 {
        self.settled
    }

    /// Start a new run: the next message is seq 1, and nothing before it is
    /// kept or counted.
    pub fn mark_run_start(&mut self) {
        self.recorded = 0;
        self.settled = 0;
        self.body_written = 0;
        self.open_from = None;
        self.markers_written = 0;
        self.pending_len = 0;
        self.related_len = 0;
        self.selected = None;
        self.view.clear();
    }

    /// Record one message `buf` from `src` to `dst`, as it is sent, with its
    /// header and cell count as the library read them, and return its seq,
    /// role, riders and repeat. `accepted` is whether the router took it.
    pub fn record(
        &mut self,
        buf: &[u8],
        (header, cells): (&Header, u16),
        (src, dst): (u32, u32),
        tier: Tier,
        accepted: bool,
    ) -> (Seq, MessageRole, Riders, Option<ListedRepeat>) {
        let (role, riders, mut repeat) = MessageRole::of(header, cells, tier);
        if let Some((stream, lower, _)) = data_of(header.payload)
            && let Some(at) = self.pending[..self.pending_len]
                .iter()
                .position(|p| p.from == src && p.to == dst && p.stream == stream)
        {
            let mark = self.pending[at].mark;
            self.pending_len -= 1;
            self.pending.copy_within(at + 1..=self.pending_len, at);
            if repeat.is_none() && lower <= mark {
                repeat = Some(ListedRepeat::AfterGapReport);
            }
        }
        self.recorded += 1;
        let seq = self.recorded;
        let slot = self.slot(seq);
        let len = buf.len().min(usize::from(u16::MAX));
        self.src[slot] = src;
        self.dst[slot] = dst;
        self.wire_len[slot] = len as u16;
        if let Some(head) = buf.first_chunk::<HEADER_LEN>() {
            self.header[slot] = *head;
        }
        self.role[slot] = role;
        self.riders[slot] = riders;
        self.repeat[slot] = repeat;
        self.tier[slot] = tier;
        self.outcome[slot] = if accepted {
            Outcome::Accepted
        } else {
            Outcome::Lost(LostBy::Loss)
        };
        self.body_at[slot] = self.write_body(&buf[HEADER_LEN.min(len)..len]);
        (Seq(seq), role, riders, repeat)
    }

    /// Copy `body` whole into the arena, skipping to its start when the
    /// tail is too short, and return its absolute position.
    fn write_body(&mut self, body: &[u8]) -> u64 {
        let size = self.arena.len() as u64;
        if body.is_empty() || body.len() as u64 > size {
            return self.body_written;
        }
        let offset = self.body_written % size;
        if offset + body.len() as u64 > size {
            self.body_written += size - offset;
        }
        let at = self.body_written;
        let start = (at % size) as usize;
        self.arena[start..start + body.len()].copy_from_slice(body);
        self.body_written += body.len() as u64;
        at
    }

    /// Settle `seq` at `at_ms`: lost by `lost`, or delivered. Its view entry
    /// and counts follow, and a delivered gap report waits for its repeat.
    pub fn settle(&mut self, seq: Seq, at_ms: u64, lost: Option<LostBy>) {
        let seq = seq.0;
        // A message recorded before the run's start is not this run's.
        if seq != self.settled + 1 || seq > self.recorded {
            return;
        }
        self.settled = seq;
        let slot = self.slot(seq);
        self.at_ms[slot] = at_ms;
        if let Some(by) = lost {
            self.outcome[slot] = Outcome::Lost(by);
        } else if let Ok((header, _)) = read_header(&self.header[slot], CLUSTER_ID_HASH) {
            let acks = header.acks;
            let marks = [
                (acks.gap_own, Stream::Own, own_mark(acks.own)),
                (acks.gap_relay, Stream::Relay, acks.relay),
            ];
            for (gap, stream, mark) in marks {
                if gap {
                    self.push_pending(PendingRepeat {
                        from: self.dst[slot],
                        to: self.src[slot],
                        stream,
                        mark,
                    });
                }
            }
        }
        self.count(seq, 1);
        if self.listed(seq) {
            self.view.push(Entry::Message(seq), at_ms);
        }
    }

    fn push_pending(&mut self, pending: PendingRepeat) {
        if self.pending_len == self.pending.len() {
            self.pending.copy_within(1.., 0);
            self.pending_len -= 1;
        }
        self.pending[self.pending_len] = pending;
        self.pending_len += 1;
    }

    /// Open a sub-step: messages from the next recorded on are in flight
    /// until [`Capture::close`].
    pub fn open(&mut self) {
        self.open_from = Some(self.recorded + 1);
    }

    pub fn close(&mut self) {
        self.open_from = None;
    }

    /// Write a marker at `at_ms`, before the next message to settle.
    pub fn mark(&mut self, at_ms: u64, what: MarkerKind) {
        let index = self.markers_written;
        let len = self.markers.len() as u64;
        self.markers[(index % len) as usize] = Marker {
            before: self.settled + 1,
            at_ms,
            what,
        };
        self.markers_written += 1;
        if self.view.filter.scope.shows(what) {
            self.view.push(Entry::Marker(index), at_ms);
        }
    }

    fn marker(&self, index: u64) -> Option<Marker> {
        let len = self.markers.len() as u64;
        (index < self.markers_written && index + len >= self.markers_written)
            .then(|| self.markers[(index % len) as usize])
    }

    fn fate(&self, seq: u64) -> Fate {
        match self.outcome[self.slot(seq)] {
            Outcome::Lost(by) => Fate::Lost { by },
            Outcome::Accepted if self.open_from.is_some_and(|from| from <= seq) => Fate::InFlight,
            Outcome::Accepted => Fate::Delivered,
        }
    }

    fn in_scope(&self, seq: u64) -> bool {
        let slot = self.slot(seq);
        self.view
            .filter
            .scope
            .admits(self.src[slot], self.dst[slot], self.tier[slot])
    }

    /// Whether `seq` passes the whole filter.
    fn listed(&self, seq: u64) -> bool {
        let slot = self.slot(seq);
        let filter = self.view.filter;
        self.in_scope(seq)
            && TypeSet(filter.types).admits(self.role[slot], self.riders[slot], self.repeat[slot])
            && (!filter.lost_only || matches!(self.outcome[slot], Outcome::Lost(_)))
    }

    /// Add `seq` into the view's counts.
    fn count(&mut self, seq: u64, n: u32) {
        if !self.in_scope(seq) {
            return;
        }
        let slot = self.slot(seq);
        let lost = matches!(self.outcome[slot], Outcome::Lost(_));
        let (role, riders, repeat) = (self.role[slot], self.riders[slot], self.repeat[slot]);
        let view = &mut self.view;
        view.scope_count += n;
        view.scope_lost += n * u32::from(lost);
        for (i, t) in MessageType::ALL.iter().enumerate() {
            if t.matches(role, riders, repeat) {
                view.counts[i] += n;
                view.lost[i] += n * u32::from(lost);
            }
        }
        let filter = view.filter;
        if TypeSet(filter.types).admits(role, riders, repeat) && (!filter.lost_only || lost) {
            view.shown += n;
            view.shown_lost += n * u32::from(lost);
            view.shown_bytes += u64::from(n) * u64::from(self.wire_len[slot]);
        }
    }

    /// Set the view's filter and rebuild it in one pass over what is kept.
    pub fn set_filter(&mut self, filter: Filter) {
        self.view.filter = filter;
        self.view.clear();
        let first = (self.settled + 1).saturating_sub(self.capacity()).max(1);
        let markers_len = self.markers.len() as u64;
        let mut marker = self.markers_written.saturating_sub(markers_len);
        for seq in first..=self.settled {
            while let Some(m) = self.marker(marker).filter(|m| m.before <= seq) {
                if filter.scope.shows(m.what) {
                    self.view.push(Entry::Marker(marker), m.at_ms);
                }
                marker += 1;
            }
            self.count(seq, 1);
            if self.listed(seq) {
                let at = self.at_ms[self.slot(seq)];
                self.view.push(Entry::Message(seq), at);
            }
        }
        while let Some(m) = self.marker(marker) {
            if filter.scope.shows(m.what) {
                self.view.push(Entry::Marker(marker), m.at_ms);
            }
            marker += 1;
        }
    }

    /// The rows buffer: [`HEAD_SLOTS`] head slots, then [`MAX_ROWS`] rows of
    /// [`ROW_SLOTS`]. Never reallocated, so its address is stable.
    pub fn rows(&self) -> &[f64] {
        &self.rows
    }

    /// Write the head and the rows of ordinals `first..first + count` (at
    /// most [`MAX_ROWS`]) into the rows buffer, and return the rows written.
    ///
    /// Head: total, kept, view length, first kept ordinal, in flight listed,
    /// in flight in all, lost listed, bytes listed, messages listed, in
    /// scope, lost in scope, then each [`MessageType`]'s count in scope and
    /// its lost count.
    ///
    /// Row: seq (a marker's kind), instant (an interval's length), from, to,
    /// wire length, cells, code, relation (2 selected, 1 related), the flags
    /// byte, then a data message's range `(lower, upper]` or another's own
    /// and relay ack marks. The code
    /// packs the role in bits 0–7, the riders in 8–15, the fate in 16–19,
    /// the tier in 20–21, the repeat in 22–23, and the entry kind in 24–25:
    /// message, marker, interval, or no longer kept.
    pub fn write_rows(&mut self, first: u64, count: usize) -> usize {
        let (in_flight, in_flight_all) = self.in_flight();
        let cap = self.view.entries.len() as u64;
        while self.view.first_kept < self.view.len
            && (self.view.first_kept + cap < self.view.len
                || matches!(self.view.at(self.view.first_kept), Some(Entry::Message(seq)) if !self.kept(seq)))
        {
            self.view.first_kept += 1;
        }
        let view = &self.view;
        let mut head = [0.0; HEAD_SLOTS];
        let fixed = [
            self.settled as f64,
            self.settled.min(self.capacity()) as f64,
            view.len as f64,
            view.first_kept as f64,
            f64::from(in_flight),
            f64::from(in_flight_all),
            f64::from(view.shown_lost),
            view.shown_bytes as f64,
            f64::from(view.shown),
            f64::from(view.scope_count),
            f64::from(view.scope_lost),
        ];
        head[..fixed.len()].copy_from_slice(&fixed);
        let n = MessageType::ALL.len();
        for i in 0..n {
            head[fixed.len() + i] = f64::from(view.counts[i]);
            head[fixed.len() + n + i] = f64::from(view.lost[i]);
        }
        let end = first
            .saturating_add(count.min(MAX_ROWS) as u64)
            .min(view.len);
        self.rows[..HEAD_SLOTS].copy_from_slice(&head);
        let mut written = 0;
        for ordinal in first..end {
            let row = self.row(ordinal);
            let at = HEAD_SLOTS + written * ROW_SLOTS;
            self.rows[at..at + ROW_SLOTS].copy_from_slice(&row);
            written += 1;
        }
        written
    }

    fn row(&self, ordinal: u64) -> [f64; ROW_SLOTS] {
        const KIND: u32 = 24;
        let gone = [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            f64::from(3u32 << KIND),
            0.0,
            0.0,
            0.0,
            0.0,
        ];
        match self.view.at(ordinal) {
            Some(Entry::Message(seq)) if self.kept(seq) => {
                let slot = self.slot(seq);
                let Ok((header, cells)) = read_header(&self.header[slot], CLUSTER_ID_HASH) else {
                    return gone;
                };
                let (a, b) = match data_of(header.payload) {
                    Some((_, lower, upper)) => (lower, upper),
                    None => (own_mark(header.acks.own), header.acks.relay),
                };
                let repeat = match self.repeat[slot] {
                    None => 0,
                    Some(ListedRepeat::AfterTimeout) => 1,
                    Some(ListedRepeat::AfterGapReport) => 2,
                };
                let tier = match self.tier[slot] {
                    Tier::InZone => 0,
                    Tier::CrossZone => 1,
                };
                let code = self.role[slot] as u32
                    | u32::from(self.riders[slot].bits()) << 8
                    | self.fate(seq).code() << 16
                    | tier << 20
                    | repeat << 22;
                let relation = if self.selected == Some(seq) {
                    2.0
                } else if self.related[..self.related_len].contains(&seq) {
                    1.0
                } else {
                    0.0
                };
                [
                    seq as f64,
                    self.at_ms[slot] as f64,
                    f64::from(self.src[slot]),
                    f64::from(self.dst[slot]),
                    f64::from(self.wire_len[slot]),
                    f64::from(cells),
                    f64::from(code),
                    relation,
                    f64::from(self.header[slot][7]),
                    a as f64,
                    b as f64,
                ]
            }
            Some(Entry::Marker(index)) => match self.marker(index) {
                Some(m) => {
                    let [kind, a, b, c, d] = m.what.slots();
                    let code = f64::from(1u32 << KIND);
                    [kind, m.at_ms as f64, a, b, c, d, code, 0.0, 0.0, 0.0, 0.0]
                }
                None => gone,
            },
            Some(Entry::Interval(ms)) => {
                let code = f64::from(2u32 << KIND);
                [0.0, ms as f64, 0.0, 0.0, 0.0, 0.0, code, 0.0, 0.0, 0.0, 0.0]
            }
            Some(Entry::Message(_)) | None => gone,
        }
    }

    /// Messages in flight that the view lists, and in all.
    fn in_flight(&self) -> (u32, u32) {
        let Some(from) = self.open_from else {
            return (0, 0);
        };
        let (mut listed, mut all) = (0, 0);
        for seq in from.max(1)..=self.settled {
            if self.kept(seq) && self.fate(seq) == Fate::InFlight {
                all += 1;
                listed += u32::from(self.listed(seq));
            }
        }
        (listed, all)
    }

    /// The fields a link scan reads from `seq`, if it is kept.
    fn info(&self, seq: u64) -> Option<Info> {
        if !self.kept(seq) || seq > self.settled {
            return None;
        }
        let slot = self.slot(seq);
        let (header, _) = read_header(&self.header[slot], CLUSTER_ID_HASH).ok()?;
        let acks = header.acks;
        let probe = match header.payload {
            Payload::Ack { probe }
            | Payload::Cells(CellPayload::Own { probe, .. } | CellPayload::Relay { probe, .. }) => {
                probe
            }
            Payload::Cells(CellPayload::Sweep) | Payload::SweepRequest => false,
        };
        Some(Info {
            seq,
            at_ms: self.at_ms[slot],
            src: self.src[slot],
            dst: self.dst[slot],
            role: self.role[slot],
            riders: self.riders[slot],
            lost: matches!(self.outcome[slot], Outcome::Lost(_)),
            data: data_of(header.payload),
            gap_own: acks.gap_own,
            gap_relay: acks.gap_relay,
            own_mark: own_mark(acks.own),
            relay_mark: acks.relay,
            incarnation: acks.incarnation,
            probe,
            probe_reply: acks.probe_reply,
        })
    }

    /// Select `seq`, or clear the selection with `None`, and return its
    /// detail: decoded now, with its links found by bounded scans of the
    /// kept messages of the same pair. Rows related to it are marked in the
    /// next read.
    pub fn select(&mut self, seq: Option<u64>) -> Option<Detail> {
        self.selected = None;
        self.related_len = 0;
        let me = self.info(seq?)?;
        self.selected = Some(me.seq);
        let links = self.links(&me);
        for link in links.iter().take(MAX_RELATED) {
            self.related[self.related_len] = link.seq.0;
            self.related_len += 1;
        }
        let slot = self.slot(me.seq);
        let wire_len = self.wire_len[slot];
        let body_kept = self.body_kept(me.seq);
        let mut bytes = Vec::with_capacity(usize::from(wire_len));
        bytes.extend_from_slice(&self.header[slot]);
        let body_len = usize::from(wire_len).saturating_sub(HEADER_LEN);
        let mut cells = Vec::new();
        if body_len > 0 && body_kept {
            let start = (self.body_at[slot] % self.arena.len() as u64) as usize;
            bytes.extend_from_slice(&self.arena[start..start + body_len]);
            let decoded = decode(&bytes, FrameLimits::default(), CLUSTER_ID_HASH, |obs| {
                cells.push(CellRow {
                    origin: stable_id(obs.origin.0),
                    rule: obs.rule_fingerprint,
                    key: obs.key_hash.0,
                    bucket: obs.bucket.0,
                    count: obs.count,
                    incarnation: obs.incarnation,
                    updated_ms: obs.last_update_millis,
                });
            });
            debug_assert!(
                decoded.is_ok(),
                "the capture keeps what was sent: {decoded:?}"
            );
        }
        let (header, cell_count) = read_header(&bytes, CLUSTER_ID_HASH).ok()?;
        let (own_ack, own_ack_as_gateway) = match header.acks.own {
            OwnAck::Direct(mark) => (mark, false),
            OwnAck::AsGateway(mark) => (mark, true),
        };
        let repeat = self.repeat[slot].map(RepeatCause::from).or_else(|| {
            links
                .iter()
                .any(|l| l.relation == Relation::Repeats)
                .then_some(RepeatCause::AfterAnswer)
        });
        Some(Detail {
            seq: Seq(me.seq),
            at_ms: me.at_ms,
            src: me.src,
            dst: me.dst,
            wire_len,
            role: me.role,
            riders: me.riders,
            repeat,
            tier: self.tier[slot],
            fate: self.fate(me.seq),
            bytes,
            body_kept,
            header: HeaderFields {
                kind: header.payload.into(),
                flags: self.header[slot][7],
                cells: cell_count,
                sender: stable_id(header.sender.node_id.0),
                sender_incarnation: header.sender.incarnation,
                sender_zone: header.sender_zone.map_or(0, |z| z.get()),
                ack_incarnation: header.acks.incarnation,
                own_ack,
                own_ack_as_gateway,
                relay_ack: header.acks.relay,
                range: data_of(header.payload).map(|(_, l, u)| [l, u]),
            },
            cells,
            links,
            asks_answer: header.payload.asks_answer(),
        })
    }

    /// The kept messages of `me`'s pair before it (newest first) and after
    /// it (oldest first).
    fn pair_before<'a>(&'a self, me: &'a Info) -> impl Iterator<Item = Info> + 'a {
        let first = (self.recorded + 1).saturating_sub(self.capacity()).max(1);
        (first..me.seq)
            .rev()
            .filter(|&seq| self.same_pair(seq, me))
            .filter_map(|seq| self.info(seq))
    }

    fn pair_after<'a>(&'a self, me: &'a Info) -> impl Iterator<Item = Info> + 'a {
        (me.seq + 1..=self.settled)
            .filter(|&seq| self.same_pair(seq, me))
            .filter_map(|seq| self.info(seq))
    }

    /// Whether `seq` travels between `me`'s two nodes, either way, from the
    /// route columns alone: `seq` must be kept, as every seq the pair scans
    /// visit is.
    fn same_pair(&self, seq: u64, me: &Info) -> bool {
        let slot = self.slot(seq);
        let pair = (self.src[slot], self.dst[slot]);
        pair == (me.src, me.dst) || pair == (me.dst, me.src)
    }

    /// Every link from `me`, in a fixed order.
    fn links(&self, me: &Info) -> Vec<Link> {
        let mut links = Vec::new();
        let mut link = |relation, other: Option<Info>| {
            if let Some(o) = other {
                links.push(Link {
                    relation,
                    seq: Seq(o.seq),
                    role: o.role,
                    offset_ms: o.at_ms as i64 - me.at_ms as i64,
                });
            }
        };
        let back = |i: &Info| i.src == me.dst;
        let forth = |i: &Info| i.src == me.src;
        // Asking and answering.
        if me.probe {
            link(
                Relation::AnsweredBy,
                self.pair_after(me).find(|i| back(i) && i.probe_reply),
            );
        }
        if me.probe_reply {
            link(
                Relation::Answers,
                self.pair_before(me).find(|i| back(i) && i.probe),
            );
        }
        if me.role == MessageRole::SweepRequest {
            link(
                Relation::AnsweredBy,
                self.pair_after(me)
                    .find(|i| back(i) && i.role == MessageRole::SweepPage),
            );
        }
        if me.role == MessageRole::SweepPage {
            link(
                Relation::Answers,
                self.pair_before(me)
                    .find(|i| back(i) && i.role == MessageRole::SweepRequest),
            );
        }
        // Data: its ack, the report of its loss, its repeat, what it repeats.
        if let Some((stream, lower, upper)) = me.data {
            if !me.lost {
                link(
                    Relation::AcknowledgedBy,
                    self.pair_after(me).find(|i| {
                        back(i) && !i.lost && i.incarnation != 0 && i.mark(stream) >= upper
                    }),
                );
            } else {
                link(
                    Relation::ReportedMissingBy,
                    self.pair_after(me)
                        .find(|i| back(i) && i.gap(stream).is_some_and(|h| h <= lower)),
                );
            }
            link(
                Relation::RepeatedAs,
                self.pair_after(me).find(|i| {
                    forth(i)
                        && i.data
                            .is_some_and(|(s, l, u)| s == stream && l <= lower && u >= upper)
                }),
            );
            // What it repeats: the newest lost data it covers, else the
            // newest it covers.
            let covered = |i: &Info| {
                forth(i)
                    && i.data
                        .is_some_and(|(s, l, u)| s == stream && lower <= l && u <= upper)
            };
            link(
                Relation::Repeats,
                self.pair_before(me)
                    .find(|i| covered(i) && i.lost)
                    .or_else(|| self.pair_before(me).find(covered)),
            );
        }
        // A report: what it reports missing, what showed the gap, its repeat.
        for stream in [Stream::Own, Stream::Relay] {
            let Some(mark) = me.gap(stream) else {
                continue;
            };
            let on = |i: &Info| back(i) && i.data.is_some_and(|(s, ..)| s == stream);
            let mut missing = 0;
            for i in self.pair_before(me).filter(on).take(MAX_RELATED) {
                let (_, l, _) = i.data.unwrap_or_default();
                if l < mark {
                    break;
                }
                if i.lost && missing < MAX_RELATED / 2 {
                    missing += 1;
                    link(Relation::ReportsMissing, Some(i));
                }
            }
            link(
                Relation::RevealedBy,
                self.pair_before(me)
                    .filter(on)
                    .find(|i| !i.lost && i.data.is_some_and(|(_, l, _)| l > mark)),
            );
            link(
                Relation::RepeatedAs,
                self.pair_after(me)
                    .filter(on)
                    .find(|i| i.data.is_some_and(|(_, l, _)| l <= mark)),
            );
        }
        // An ack: the newest data of its stream that it acknowledges.
        if matches!(me.role, MessageRole::Ack | MessageRole::ProbeAnswer) && me.incarnation != 0 {
            link(
                Relation::Acknowledges,
                self.pair_before(me).find(|i| {
                    back(i) && !i.lost && i.data.is_some_and(|(s, _, u)| u <= me.mark(s))
                }),
            );
        }
        links
    }
}

/// The stream and range a data payload carries.
fn data_of(payload: Payload) -> Option<(Stream, u64, u64)> {
    match payload {
        Payload::Cells(CellPayload::Own { range, .. }) => {
            Some((Stream::Own, range.lower(), range.upper()))
        }
        Payload::Cells(CellPayload::Relay { range, .. }) => {
            Some((Stream::Relay, range.lower(), range.upper()))
        }
        Payload::Ack { .. } | Payload::Cells(CellPayload::Sweep) | Payload::SweepRequest => None,
    }
}

fn own_mark(own: OwnAck) -> u64 {
    match own {
        OwnAck::Direct(mark) | OwnAck::AsGateway(mark) => mark,
    }
}

#[cfg(test)]
mod tests;
