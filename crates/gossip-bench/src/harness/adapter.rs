//! The one place the harness names the library under test beyond the
//! stable trait surface (`gabion::gossip::{Clock, GossipTransport, Ticker}`,
//! `gabion::discovery::{Peer, PeerEvent}`). It builds each node the way
//! `gabiond` does at boot, records hits through its client, reads its admin
//! snapshot, and keeps the aggregate the node writes through. The judge
//! exempts this file from the harness identity check: a baseline arm builds
//! with H1's copy against its own library, and a phase that changes an API
//! used here changes this file in the same commit.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;

use ahash::AHashMap;
use anyhow::{Result, bail};
pub use gabion::crdt::BucketEpoch;
use gabion::crdt::{
    CellStore, CellStoreConfig, DeltaSink, ExpirationSink, KeyHash, NodeId, NodeIdentity,
    RuleDescriptor,
};
use gabion::defaults as d;
use gabion::gossip::{
    AdminCommand, AdminSnapshot, AggregateStore, GossipClient, GossipError, GossipRuntime,
    GossipTransport, Ticker,
};
use gabion::rules::{Descriptor, DescriptorPattern, EnforcementMode, Rule, hash_key};
use gabion_server::config::GossipSettings;
use tokio::sync::{mpsc, oneshot};

use super::driver::Counters;
use super::node::{KeySpace, NOT_TRACKED, Sizing, Slot, ring_window_total};
use super::plan::node_addr;
use super::regime::Regime;
use super::rng::{Rng, Stream, mix64};
use super::world::{PeerStream, SimClock, SimSocket, SimTicker, World};

const DOMAIN: &str = "study";
const DESCRIPTOR_KEY: &str = "site";
const RULE_ID: u32 = 1;

impl Ticker for SimTicker {
    async fn tick(&mut self) {
        self.next_tick().await;
    }
}

/// The library's identity of the regime's one rule and of every key.
struct Identity {
    rule: RuleDescriptor,
    hashes: Box<[KeyHash]>,
    /// Key hash → key index.
    index: AHashMap<u128, u32>,
}

impl Identity {
    fn build(regime: &Regime, keys: usize) -> Self {
        let rule = Rule::new(
            RULE_ID,
            DOMAIN,
            vec![DescriptorPattern {
                key: DESCRIPTOR_KEY.into(),
                value: "*".into(),
            }],
            regime.rule.limit,
            regime.rule.window_ms,
            regime.rule.bucket_ms,
            EnforcementMode::Enforce,
        );
        let hashes: Box<[KeyHash]> = (0..keys)
            .map(|i| {
                let value = i.to_string();
                hash_key(
                    RULE_ID,
                    DOMAIN,
                    &[Descriptor {
                        key: DESCRIPTOR_KEY,
                        value: &value,
                    }],
                )
            })
            .collect();
        let index = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (h.0, i as u32))
            .collect();
        Self {
            rule: RuleDescriptor {
                fingerprint: rule.fingerprint,
                window_millis: rule.window_millis.min(u64::from(u32::MAX)) as u32,
                bucket_millis: rule.bucket_millis.min(u64::from(u32::MAX)) as u32,
                limit: rule.limit,
                flags: 0,
                local_rule_id: RULE_ID,
            },
            hashes,
            index,
        }
    }
}

/// A node's view of the cluster aggregate: per key, a ring of bucket totals
/// summed across origins. Semantically the server adapter's `DashMapStore`
/// (`crates/server/src/store.rs`) — deltas add, expirations subtract the
/// origin's last count, `window_total` sums the live buckets — laid out as
/// fixed rings because the key population is known up front.
pub struct NodeStore {
    keys: Rc<KeySpace>,
    identity: Rc<Identity>,
    ring: RefCell<Box<[Slot]>>,
    /// `(tracked index, bucket)` rows touched since the driver last looked.
    touched: RefCell<Vec<(u32, BucketEpoch)>>,
    /// Rows for a rule or key the regime never issued. Always zero for a
    /// correct solution.
    pub foreign_rows: Cell<u64>,
}

impl NodeStore {
    fn new(keys: Rc<KeySpace>, identity: Rc<Identity>) -> Self {
        let slots = keys.len() * keys.ring_len as usize;
        Self {
            keys,
            identity,
            ring: RefCell::new(vec![Slot::default(); slots].into_boxed_slice()),
            touched: RefCell::new(Vec::new()),
            foreign_rows: Cell::new(0),
        }
    }

    /// What the adapter's admission check reads: the live-window total for
    /// `key` at `now_millis`.
    pub fn window_total(&self, key: u32, now_millis: u64) -> u64 {
        ring_window_total(
            &self.keys,
            &self.ring.borrow(),
            key,
            self.keys.bucket_of(now_millis),
        )
    }

    pub fn bucket_count(&self, key: u32, bucket: BucketEpoch) -> u32 {
        self.ring.borrow()[self.keys.slot(key, bucket)].count_for(bucket)
    }

    pub fn take_touched(&self, into: &mut Vec<(u32, BucketEpoch)>) {
        into.clear();
        std::mem::swap(into, &mut self.touched.borrow_mut());
    }

    fn key_of(&self, fingerprint: u128, key_hash: KeyHash) -> Option<u32> {
        let key = (fingerprint == self.identity.rule.fingerprint)
            .then(|| self.identity.index.get(&key_hash.0).copied())
            .flatten();
        if key.is_none() {
            self.foreign_rows.set(self.foreign_rows.get() + 1);
        }
        key
    }

    fn touch(&self, key: u32, bucket: BucketEpoch) {
        let tracked = self.keys.tracked_of[key as usize];
        if tracked != NOT_TRACKED {
            self.touched.borrow_mut().push((tracked, bucket));
        }
    }
}

impl AggregateStore<u32> for NodeStore {
    fn apply(&self, deltas: &DeltaSink<u32>, expirations: &ExpirationSink<u32>) {
        let mut ring = self.ring.borrow_mut();
        for i in 0..deltas.len() {
            if deltas.applies_locally[i] == 0 || deltas.deltas[i] == 0 {
                continue;
            }
            let cell = deltas.keys[i];
            let Some(key) = self.key_of(cell.rule_fingerprint, cell.key_hash) else {
                continue;
            };
            if ring[self.keys.slot(key, cell.bucket)].add(cell.bucket, deltas.deltas[i]) {
                self.touch(key, cell.bucket);
            }
        }
        for i in 0..expirations.len() {
            if expirations.applies_locally[i] == 0 {
                continue;
            }
            let cell = expirations.keys[i];
            let Some(key) = self.key_of(cell.rule_fingerprint, cell.key_hash) else {
                continue;
            };
            let slot = &mut ring[self.keys.slot(key, cell.bucket)];
            if slot.bucket == cell.bucket {
                slot.count = slot.count.saturating_sub(expirations.last_counts[i]);
                self.touch(key, cell.bucket);
            }
        }
    }
}

/// A started node, as the driver holds it.
pub struct Launched<F> {
    pub future: F,
    pub node: Node,
    pub store: Rc<NodeStore>,
}

/// The driver's handle on a running node: its client and admin channel.
pub struct Node {
    client: GossipClient<u32>,
    admin: mpsc::Sender<AdminCommand>,
    identity: Rc<Identity>,
}

impl Node {
    /// Record one admitted hit of `key`, as `SharedLimiter::evaluate` does
    /// after admitting it. Resolves to whether the node took the hit.
    pub fn record(
        &self,
        key: u32,
        bucket: BucketEpoch,
        now_millis: u64,
    ) -> impl Future<Output = bool> + use<> {
        let client = self.client.clone();
        let rule = self.identity.rule;
        let hash = self.identity.hashes[key as usize];
        async move {
            client
                .record(rule.fingerprint, hash, bucket, 1, rule.limit, now_millis)
                .await
                .is_ok()
        }
    }

    /// Ask for the node's health counters; `None` when the request cannot
    /// be queued. The node answers the next time it runs.
    pub fn ask_health(&self) -> Option<HealthReply> {
        let (reply, rx) = oneshot::channel();
        self.admin
            .try_send(AdminCommand::Snapshot { reply })
            .ok()
            .map(|()| HealthReply(rx))
    }
}

/// A pending [`Node::ask_health`].
pub struct HealthReply(oneshot::Receiver<AdminSnapshot>);

impl HealthReply {
    /// The counters, once the node has answered.
    pub fn take(mut self) -> Option<Counters> {
        let snapshot = self.0.try_recv().ok()?;
        let stats = snapshot.store_stats;
        Some(Counters {
            active_cells: stats.active_cells,
            cell_store_full_rejects: stats.cell_store_full_rejects,
            rule_dictionary_full_rejects: stats.rule_dictionary_full_rejects,
            node_dictionary_full_rejects: stats.node_dictionary_full_rejects,
            decode_rejects: snapshot.decode_reject_count,
        })
    }
}

/// Starts nodes on a world: everything they share is built once.
pub struct Launcher {
    world: Rc<World>,
    keys: Rc<KeySpace>,
    identity: Rc<Identity>,
    cells: CellStoreConfig,
    seed: u64,
}

impl Launcher {
    pub fn new(
        regime: &Regime,
        sizing: &Sizing,
        world: &Rc<World>,
        keys: &Rc<KeySpace>,
        seed: u64,
    ) -> Self {
        // Never below production; above it, keep the production ratio of the
        // ring to the cell store (`STORAGE_*_DIRTY_CAPACITY` over
        // `STORAGE_MAX_CELLS`).
        let ring = |production: usize| {
            production
                .max((sizing.cell_capacity as usize * production).div_ceil(d::STORAGE_MAX_CELLS))
        };
        let fit = |floor: u16, n: usize| floor.max((n + 16).min(u16::MAX as usize) as u16);
        Self {
            world: Rc::clone(world),
            keys: Rc::clone(keys),
            identity: Rc::new(Identity::build(regime, keys.len())),
            cells: CellStoreConfig {
                cell_capacity: sizing.cell_capacity,
                rule_dictionary_capacity: d::STORAGE_RULE_DICTIONARY_CAPACITY,
                node_dictionary_capacity: fit(
                    d::STORAGE_NODE_DICTIONARY_CAPACITY,
                    sizing.total_nodes,
                ),
                local_dirty_capacity: ring(d::STORAGE_LOCAL_DIRTY_CAPACITY),
                forwarded_dirty_capacity: ring(d::STORAGE_FORWARDED_DIRTY_CAPACITY),
                peer_capacity: fit(d::STORAGE_PEER_CAPACITY, sizing.max_running_nodes),
            },
            seed,
        }
    }

    /// Start node `idx` on its own [`SimSocket`].
    pub fn launch(
        &self,
        idx: usize,
    ) -> Result<Launched<impl Future<Output = Result<(), GossipError>> + use<>>> {
        let socket = SimSocket {
            world: Rc::clone(&self.world),
            idx,
            addr: node_addr(idx),
        };
        self.launch_on(socket, idx)
    }

    /// Start node `idx` on `transport` exactly as `gabiond` would at boot
    /// (production gossip settings, the regime's rule interned before the
    /// runtime starts); a test wraps the node's [`SimSocket`] to observe
    /// what it sends.
    pub fn launch_on<T: GossipTransport>(
        &self,
        transport: T,
        idx: usize,
    ) -> Result<Launched<impl Future<Output = Result<(), GossipError>> + use<T>>> {
        let id = (u128::from(mix64(idx as u64 ^ 0x6761_6269_6f6e)) << 64) | idx as u128 | 1 << 63;
        let identity = NodeIdentity::new(NodeId(id), 1);
        let rng_seed = Rng::stream(self.seed, Stream::Gossip, idx as u64).next_u64();
        let config = GossipSettings::default().into_runtime_config(identity, rng_seed);
        let mut cell_store = CellStore::<u32>::new(self.cells, identity);
        if cell_store.intern_rule(self.identity.rule).is_none() {
            bail!("node {idx}: the cell store refused to register the regime's rule");
        }
        let store = Rc::new(NodeStore::new(
            Rc::clone(&self.keys),
            Rc::clone(&self.identity),
        ));
        let (admin, admin_rx) = mpsc::channel(4);
        let (runtime, client) = GossipRuntime::from_parts_with_admin(
            transport,
            SimClock {
                world: Rc::clone(&self.world),
            },
            config,
            cell_store,
            Rc::clone(&store),
            Some(admin_rx),
        );
        let future = runtime.run(PeerStream {
            world: Rc::clone(&self.world),
            idx,
        });
        Ok(Launched {
            future,
            node: Node {
                client,
                admin,
                identity: Rc::clone(&self.identity),
            },
            store,
        })
    }
}
