//! Observation shims that turn the runtime's existing push surfaces into the
//! visualizer's event log. Neither shim changes gossip behavior — they only
//! record what flows past.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;

use gabion::aggregate::{Aggregate, AggregateTable};
use gabion::crdt::{DeltaSink, ExpirationSink, ValidStorageConfig};
use gabion::gossip::AggregateStore;
use gabion::gossip::GossipTransport;
use gabion::gossip::sim::{SimRouter, SimTransport};
use gabion::wire::read_header;

use crate::engine::CLUSTER_ID_HASH;
use crate::event::{EventKind, Tier};

/// Shared, single-threaded event sink. Shims push raw [`EventKind`]s here as
/// they observe them; the engine drains and time-stamps them. `Rc<RefCell<…>>`
/// because everything runs on one `LocalSet` (wasm is single-threaded).
pub type EventLog = Rc<RefCell<Vec<EventKind>>>;

/// A live member as the transports resolve it: its stable id and the zone it
/// is placed in.
#[derive(Clone, Copy, Debug)]
pub struct Member {
    pub id: u32,
    pub zone: u8,
}

/// Maps each node's gossip address to its [`Member`]. Shared with every
/// transport so packet events carry ids and tiers, not `SocketAddr`s. Mutable
/// (`RefCell`) because nodes join and leave at runtime: the engine inserts a
/// newcomer's address on add and removes a departed one on delete, and the
/// already-running transports resolve against the live map.
pub type AddressBook = Rc<RefCell<HashMap<SocketAddr, Member>>>;

/// A node's count table, gabion's own [`Aggregate`]: the table admission's
/// `window_total` reads. Every `apply` goes to it first; then one
/// `CellCreated` / `CellUpdated` / `CellExpired` event per row goes to the log.
pub struct EventEmittingAggregateStore {
    node: u32,
    log: EventLog,
    inner: Aggregate,
}

impl EventEmittingAggregateStore {
    pub fn new(node: u32, log: EventLog, storage: &ValidStorageConfig) -> Self {
        Self {
            node,
            log,
            inner: Aggregate::new(storage),
        }
    }

    /// The table this node admits against.
    pub fn table(&self) -> AggregateTable<'_> {
        self.inner.table()
    }
}

impl AggregateStore for EventEmittingAggregateStore {
    fn apply(&self, deltas: &DeltaSink, expirations: &ExpirationSink) {
        self.inner.apply(deltas, expirations);
        let mut log = self.log.borrow_mut();
        for i in 0..deltas.len() {
            let key = &deltas.keys[i];
            let previous: u64 = deltas.previous[i].into();
            let current: u64 = deltas.current[i].into();
            // Previous stored count zero ⇒ this is the cell's first sighting
            // on this node; anything else is a count rising on a known cell.
            let event = if previous == 0 {
                EventKind::CellCreated {
                    node: self.node,
                    rule: key.rule_fingerprint,
                    key: key.key_hash.0,
                    bucket: key.bucket.0,
                    count: current,
                }
            } else {
                EventKind::CellUpdated {
                    node: self.node,
                    rule: key.rule_fingerprint,
                    key: key.key_hash.0,
                    bucket: key.bucket.0,
                    count: current,
                }
            };
            log.push(event);
        }
        for key in expirations.keys.iter().take(expirations.len()) {
            log.push(EventKind::CellExpired {
                node: self.node,
                rule: key.rule_fingerprint,
                key: key.key_hash.0,
                bucket: key.bucket.0,
            });
        }
    }
}

/// Transport wrapper that logs one packet event per send/receive. Each sent
/// packet is tagged with its kind, read with the library's own header reader,
/// and its tier, by placement. Wraps a `SimTransport` and consults the shared
/// `SimRouter` to tell a delivered send (the packet was enqueued at `dst`)
/// from a dropped one — the router's `received_count` increments only on
/// actual delivery, so a diff of one across the inner send is exactly "did
/// this packet land".
pub struct LoggingSimTransport {
    inner: SimTransport,
    router: SimRouter,
    src: u32,
    /// The sender's placement.
    zone: u8,
    addresses: AddressBook,
    log: EventLog,
}

impl LoggingSimTransport {
    pub fn new(
        inner: SimTransport,
        router: SimRouter,
        src: u32,
        zone: u8,
        addresses: AddressBook,
        log: EventLog,
    ) -> Self {
        Self {
            inner,
            router,
            src,
            zone,
            addresses,
            log,
        }
    }
}

impl GossipTransport for LoggingSimTransport {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn try_send_to(&self, buf: &[u8], dst_addr: SocketAddr) -> io::Result<usize> {
        let before = self.router.received_count(dst_addr);
        let result = self.inner.try_send_to(buf, dst_addr);
        // Only a successful send is a network event; a refused
        // (`WouldBlock`) send is held by the runtime and retried, not a packet.
        if let Ok(n) = result
            && let Some(&dst) = self.addresses.borrow().get(&dst_addr)
        {
            let header = read_header(buf, CLUSTER_ID_HASH);
            debug_assert!(
                header.is_ok(),
                "the runtime sends only what its encoder wrote: {header:?}"
            );
            if let Ok((header, _)) = header {
                let delivered = self.router.received_count(dst_addr) > before;
                let (src, dst_id, bytes) = (self.src, dst.id, n as u32);
                let (kind, tier) = (header.payload.into(), Tier::between(self.zone, dst.zone));
                let event = if delivered {
                    EventKind::PacketSent {
                        src,
                        dst: dst_id,
                        bytes,
                        kind,
                        tier,
                    }
                } else {
                    EventKind::PacketDropped {
                        src,
                        dst: dst_id,
                        bytes,
                        kind,
                        tier,
                    }
                };
                self.log.borrow_mut().push(event);
            }
        }
        result
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let result = self.inner.recv_from(buf).await;
        if let Ok((n, src_addr)) = result
            && let Some(&src) = self.addresses.borrow().get(&src_addr)
        {
            self.log.borrow_mut().push(EventKind::PacketDelivered {
                src: src.id,
                dst: self.src,
                bytes: n as u32,
            });
        }
        result
    }
}
