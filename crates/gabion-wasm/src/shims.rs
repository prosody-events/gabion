//! Observation shims that turn the runtime's existing push surfaces into the
//! visualizer's event log. Neither shim changes gossip behavior — they mirror
//! the precedents already in tree (`gossip-bench`'s `BenchAggregateStore` and
//! `CountingTransport`) and only record what flows past.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::rc::Rc;

use gabion::crdt::{BucketEpoch, Count, DeltaSink, ExpirationSink, KeyHash};
use gabion::gossip::AggregateStore;
use gabion::gossip::GossipTransport;
use gabion::gossip::sim::{SimRouter, SimTransport};

use crate::event::EventKind;

/// Shared, single-threaded event sink. Shims push raw [`EventKind`]s here as
/// they observe them; the engine drains and time-stamps them. `Rc<RefCell<…>>`
/// because everything runs on one `LocalSet` (wasm is single-threaded).
pub type EventLog = Rc<RefCell<Vec<EventKind>>>;

/// Maps each node's gossip address to its dense `0..N` index. Built once by
/// the engine and shared with every transport so packet events carry indices,
/// not `SocketAddr`s.
pub type AddressBook = Rc<HashMap<SocketAddr, u32>>;

/// Downstream aggregate store that doubles as the cluster-aggregate read
/// surface (like the production `DashMapStore`) and the cell-event source.
/// Each `apply` folds the rows into a per-key total and emits a
/// `CellCreated` / `CellUpdated` / `CellExpired` event per row.
pub struct EventEmittingAggregateStore<C: Count> {
    node: u32,
    log: EventLog,
    inner: RefCell<HashMap<(u128, KeyHash, BucketEpoch), u64>>,
    _marker: PhantomData<C>,
}

impl<C: Count> EventEmittingAggregateStore<C> {
    pub fn new(node: u32, log: EventLog) -> Self {
        Self {
            node,
            log,
            inner: RefCell::new(HashMap::new()),
            _marker: PhantomData,
        }
    }

    /// This node's cluster-aggregate total across all cells — what its local
    /// admission decision reads.
    pub fn total(&self) -> u64 {
        self.inner.borrow().values().copied().sum()
    }

    /// Total stored count for one rule fingerprint. The visualizer watches a
    /// single rule in v1, so this is the per-node line the convergence fan
    /// draws.
    pub fn total_for_rule(&self, rule: u128) -> u64 {
        self.inner
            .borrow()
            .iter()
            .filter(|((r, ..), _)| *r == rule)
            .map(|(_, c)| *c)
            .sum()
    }
}

impl<C: Count> AggregateStore<C> for EventEmittingAggregateStore<C> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        let mut map = self.inner.borrow_mut();
        let mut log = self.log.borrow_mut();
        for i in 0..deltas.len() {
            let key = &deltas.keys[i];
            let delta: u64 = deltas.deltas[i].into();
            let previous: u64 = deltas.previous[i].into();
            let current: u64 = deltas.current[i].into();
            *map.entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0) += delta;
            // Previous stored count zero ⇒ this is the cell's first sighting
            // on this node; anything else is a count rising on a known cell.
            let event = if previous == 0 {
                EventKind::CellCreated {
                    node: self.node,
                    rule: key.rule_fingerprint,
                    key: key.key_hash.0,
                    bucket: key.bucket,
                    count: current,
                }
            } else {
                EventKind::CellUpdated {
                    node: self.node,
                    rule: key.rule_fingerprint,
                    key: key.key_hash.0,
                    bucket: key.bucket,
                    count: current,
                }
            };
            log.push(event);
        }
        for i in 0..expirations.len() {
            let key = &expirations.keys[i];
            let last: u64 = expirations.last_counts[i].into();
            let entry = map
                .entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0);
            *entry = entry.saturating_sub(last);
            if *entry == 0 {
                map.remove(&(key.rule_fingerprint, key.key_hash, key.bucket));
            }
            log.push(EventKind::CellExpired {
                node: self.node,
                rule: key.rule_fingerprint,
                key: key.key_hash.0,
                bucket: key.bucket,
            });
        }
    }
}

/// Transport wrapper that logs one packet event per send/receive. Wraps a
/// `SimTransport` and consults the shared `SimRouter` to tell a delivered
/// send (the packet was enqueued at `dst`) from a dropped one — the router's
/// `received_count` increments only on actual delivery, so a diff of one
/// across the inner send is exactly "did this packet land".
pub struct LoggingSimTransport {
    inner: SimTransport,
    router: SimRouter,
    src: u32,
    addresses: AddressBook,
    log: EventLog,
}

impl LoggingSimTransport {
    pub fn new(
        inner: SimTransport,
        router: SimRouter,
        src: u32,
        addresses: AddressBook,
        log: EventLog,
    ) -> Self {
        Self {
            inner,
            router,
            src,
            addresses,
            log,
        }
    }
}

impl GossipTransport for LoggingSimTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn writable<'a>(&'a self) -> impl Future<Output = io::Result<()>> + 'a {
        self.inner.writable()
    }

    fn try_send_to(&self, buf: &[u8], dst: SocketAddr) -> io::Result<usize> {
        let before = self.router.received_count(dst);
        let result = self.inner.try_send_to(buf, dst);
        // Only a successful (non-`WouldBlock`) send is a network event; the
        // re-queued `WouldBlock` path is backpressure, not a packet.
        if let Ok(n) = result
            && let Some(&dst_index) = self.addresses.get(&dst)
        {
            let delivered = self.router.received_count(dst) > before;
            let bytes = n as u32;
            let event = if delivered {
                EventKind::PacketSent {
                    src: self.src,
                    dst: dst_index,
                    bytes,
                }
            } else {
                EventKind::PacketDropped {
                    src: self.src,
                    dst: dst_index,
                    bytes,
                }
            };
            self.log.borrow_mut().push(event);
        }
        result
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let result = self.inner.recv_from(buf).await;
        if let Ok((n, src_addr)) = result
            && let Some(&src_index) = self.addresses.get(&src_addr)
        {
            self.log.borrow_mut().push(EventKind::PacketDelivered {
                src: src_index,
                dst: self.src,
                bytes: n as u32,
            });
        }
        result
    }
}
