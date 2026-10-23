//! Assembling one gabion node, and the aggregate store it writes through.
//!
//! This module is the whole of the harness's contact with the solution
//! under test (the "solution contract" in the crate README): it builds the
//! production [`GossipConfig`] the way `gabiond` does, builds and seeds a
//! [`CellStore`], constructs the runtime with
//! [`GossipRuntime::from_parts_with_admin`], and reads the cluster aggregate
//! through [`AggregateStore::apply`].

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;

use ahash::AHashMap;
use anyhow::{Result, bail};
use gabion::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, DeltaSink, ExpirationSink, KeyHash, NodeId,
    NodeIdentity, RuleDescriptor,
};
use gabion::gossip::{AdminCommand, AggregateStore, GossipClient, GossipError, GossipRuntime};
use gabion::rules::{Descriptor, DescriptorPattern, EnforcementMode, Rule, hash_key};
use gabion_server::config::GossipSettings;
use serde::Serialize;
use tokio::sync::mpsc;

use super::plan::{Plan, node_addr};
use super::regime::Regime;
use super::rng::{Rng, Stream, mix64};
use super::world::{PeerStream, SimClock, SimSocket, World};

const DOMAIN: &str = "study";
const DESCRIPTOR_KEY: &str = "site";
const RULE_ID: u32 = 1;
pub const NOT_TRACKED: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    Site,
    Hot,
    Burst,
}

/// The regime's key population and its single rule, shared by every node
/// store and the oracle.
pub struct KeySpace {
    pub fingerprint: u128,
    pub limit: u64,
    pub window_ms: u64,
    pub bucket_ms: u64,
    pub live_buckets: u32,
    /// Ring slots kept per key: the live window plus two, so a bucket that
    /// left the window is never confused with a live one.
    pub ring_len: u32,
    pub hashes: Vec<KeyHash>,
    pub kinds: Vec<KeyKind>,
    index: AHashMap<u128, u32>,
    /// Key → tracked index, or [`NOT_TRACKED`].
    pub tracked_of: Vec<u32>,
    /// Tracked index → key.
    pub tracked: Vec<u32>,
}

impl KeySpace {
    pub fn build(regime: &Regime, seed: u64) -> Self {
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
        let sites = regime.site_keys();
        let mut kinds = vec![KeyKind::Site; sites as usize];
        kinds.extend(regime.traffic.hot_keys.iter().map(|_| KeyKind::Hot));
        kinds.extend(regime.traffic.bursts.iter().map(|_| KeyKind::Burst));
        let hashes: Vec<KeyHash> = (0..kinds.len())
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

        // Every hot and burst key, plus a seeded sample of sites (partial
        // Fisher-Yates), in ascending key order.
        let mut rng = Rng::stream(seed, Stream::Oracle, 0);
        let mut pool: Vec<u32> = (0..sites).collect();
        let sample = regime.tracked_sample_keys.min(sites) as usize;
        for i in 0..sample {
            let j = i + rng.below((pool.len() - i) as u64) as usize;
            pool.swap(i, j);
        }
        let mut tracked: Vec<u32> = pool[..sample].to_vec();
        tracked.extend(sites..kinds.len() as u32);
        tracked.sort_unstable();
        let mut tracked_of = vec![NOT_TRACKED; kinds.len()];
        for (t, &k) in tracked.iter().enumerate() {
            tracked_of[k as usize] = t as u32;
        }

        Self {
            fingerprint: rule.fingerprint,
            limit: rule.limit,
            window_ms: rule.window_millis,
            bucket_ms: rule.bucket_millis,
            live_buckets: rule.live_buckets,
            ring_len: rule.live_buckets + 2,
            hashes,
            kinds,
            index,
            tracked_of,
            tracked,
        }
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn bucket_of(&self, now_millis: u64) -> BucketEpoch {
        (now_millis / self.bucket_ms) as BucketEpoch
    }

    pub fn rule_descriptor(&self) -> RuleDescriptor {
        RuleDescriptor {
            fingerprint: self.fingerprint,
            window_millis: self.window_ms.min(u64::from(u32::MAX)) as u32,
            bucket_millis: self.bucket_ms.min(u64::from(u32::MAX)) as u32,
            limit: self.limit,
            flags: 0,
            local_rule_id: RULE_ID,
        }
    }

    fn slot(&self, key: u32, bucket: BucketEpoch) -> usize {
        key as usize * self.ring_len as usize + (bucket % self.ring_len) as usize
    }
}

/// One `(bucket, count)` cell of a per-key ring.
#[derive(Clone, Copy, Debug, Default)]
pub struct Slot {
    pub bucket: BucketEpoch,
    pub count: u32,
}

impl Slot {
    /// Add `v` to bucket `bucket`, recycling the slot when it held an older
    /// bucket. Returns `false` when `bucket` is older than the slot's and so
    /// already outside every live window.
    pub fn add(&mut self, bucket: BucketEpoch, v: u32) -> bool {
        if self.bucket != bucket {
            if bucket < self.bucket && self.count != 0 {
                return false;
            }
            *self = Self { bucket, count: 0 };
        }
        self.count = self.count.saturating_add(v);
        true
    }

    pub fn count_for(self, bucket: BucketEpoch) -> u32 {
        if self.bucket == bucket { self.count } else { 0 }
    }
}

/// Sum of `ring` over the `live` buckets ending at `current`.
pub fn ring_window_total(keys: &KeySpace, ring: &[Slot], key: u32, current: BucketEpoch) -> u64 {
    (0..keys.live_buckets)
        .filter_map(|offset| current.checked_sub(offset))
        .map(|b| u64::from(ring[keys.slot(key, b)].count_for(b)))
        .sum()
}

/// A node's view of the cluster aggregate: per key, a ring of bucket totals
/// summed across origins. Semantically the server adapter's `DashMapStore`
/// (`crates/server/src/store.rs`) — deltas add, expirations subtract the
/// origin's last count, `window_total` sums the live buckets — laid out as
/// fixed rings because the key population is known up front.
pub struct NodeStore {
    keys: Rc<KeySpace>,
    ring: RefCell<Box<[Slot]>>,
    /// `(tracked index, bucket)` rows touched since the driver last looked.
    touched: RefCell<Vec<(u32, BucketEpoch)>>,
    /// Rows for a rule or key the regime never issued. Always zero for a
    /// correct solution.
    pub foreign_rows: Cell<u64>,
}

impl NodeStore {
    pub fn new(keys: Rc<KeySpace>) -> Self {
        let slots = keys.len() * keys.ring_len as usize;
        Self {
            keys,
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
        let key = (fingerprint == self.keys.fingerprint)
            .then(|| self.keys.index.get(&key_hash.0).copied())
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

/// Per-node capacities, derived from the regime and identical for every
/// solution.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Sizing {
    pub cell_capacity: u32,
    pub local_dirty_capacity: usize,
    pub forwarded_dirty_capacity: usize,
    pub node_dictionary_capacity: u16,
    pub peer_capacity: u16,
    pub rule_dictionary_capacity: u16,
    pub total_nodes: usize,
    pub max_running_nodes: usize,
    pub keys: usize,
    pub tracked_keys: usize,
    pub peak_offered_rps: f64,
}

impl Sizing {
    pub fn new(regime: &Regime, plan: &Plan, keys: &KeySpace) -> Self {
        use gabion::defaults as d;
        let cells = (1.5 * live_cell_bound(regime, plan.max_running)).ceil() as u64;
        let cell_capacity = cells
            .max(16_384)
            .next_multiple_of(1024)
            .min(u64::from(u32::MAX)) as u32;
        // Never below production; above it, keep the production ratio of the
        // ring to the cell store (`STORAGE_*_DIRTY_CAPACITY` over
        // `STORAGE_MAX_CELLS`).
        let ring = |production: usize| {
            production.max((cell_capacity as usize * production).div_ceil(d::STORAGE_MAX_CELLS))
        };
        let local_dirty_capacity = ring(d::STORAGE_LOCAL_DIRTY_CAPACITY);
        let forwarded_dirty_capacity = ring(d::STORAGE_FORWARDED_DIRTY_CAPACITY);
        let total_nodes = plan.nodes.len();
        let fit = |floor: u16, n: usize| floor.max((n + 16).min(u16::MAX as usize) as u16);
        Self {
            cell_capacity,
            local_dirty_capacity,
            forwarded_dirty_capacity,
            node_dictionary_capacity: fit(d::STORAGE_NODE_DICTIONARY_CAPACITY, total_nodes),
            peer_capacity: fit(d::STORAGE_PEER_CAPACITY, plan.max_running),
            rule_dictionary_capacity: d::STORAGE_RULE_DICTIONARY_CAPACITY,
            total_nodes,
            max_running_nodes: plan.max_running,
            keys: keys.len(),
            tracked_keys: keys.tracked.len(),
            peak_offered_rps: regime.peak_offered_rps(),
        }
    }

    pub fn cell_store_config(&self) -> CellStoreConfig {
        CellStoreConfig {
            cell_capacity: self.cell_capacity,
            rule_dictionary_capacity: self.rule_dictionary_capacity,
            node_dictionary_capacity: self.node_dictionary_capacity,
            local_dirty_capacity: self.local_dirty_capacity,
            forwarded_dirty_capacity: self.forwarded_dirty_capacity,
            peer_capacity: self.peer_capacity,
        }
    }
}

/// Upper bound on the cells one node holds at once. A request creates at
/// most one cell, its `(key, bucket, origin)`, which lives one window plus
/// expiry slack (two buckets); and a key holds at most one cell per running
/// pod per bucket. Uses offered rather than admitted load, so it holds for
/// any solution that expires a cell within two buckets of its leaving the
/// window.
fn live_cell_bound(regime: &Regime, pods: usize) -> f64 {
    let rule = regime.rule;
    let lifetime_s = (rule.window_ms + 2 * rule.bucket_ms) as f64 / 1000.0;
    let per_key = (pods * (regime.live_buckets() as usize + 2)) as f64;
    let sustainable = rule.limit as f64 * 1000.0 / rule.window_ms as f64;
    let traffic = &regime.traffic;
    let site = f64::from(regime.site_keys())
        * (lifetime_s * 1000.0 / traffic.site_period_ms as f64).min(per_key);
    let hot: f64 = traffic
        .hot_keys
        .iter()
        .map(|h| (h.limit_multiple * sustainable * lifetime_s).min(per_key))
        .sum();
    let bursts: f64 = traffic
        .bursts
        .iter()
        .map(|b| (b.rate_rps * lifetime_s.min(b.duration_s as f64)).min(per_key))
        .sum();
    site + hot + bursts
}

/// A started node, as the driver holds it.
pub struct Launched<F> {
    pub future: F,
    pub client: GossipClient<u32>,
    pub admin: mpsc::Sender<AdminCommand>,
    pub store: Rc<NodeStore>,
}

pub fn node_identity(idx: usize) -> NodeIdentity {
    let id = (u128::from(mix64(idx as u64 ^ 0x6761_6269_6f6e)) << 64) | idx as u128 | 1 << 63;
    NodeIdentity::new(NodeId(id), 1)
}

/// Build node `idx` exactly as `gabiond` would at boot (production gossip
/// settings, configured rule pinned in the cell store before the runtime
/// starts) and return its event-loop future.
pub fn launch_node(
    world: &Rc<World>,
    keys: &Rc<KeySpace>,
    cells: CellStoreConfig,
    idx: usize,
    seed: u64,
) -> Result<Launched<impl Future<Output = Result<(), GossipError>> + use<>>> {
    let identity = node_identity(idx);
    let rng_seed = Rng::stream(seed, Stream::Gossip, idx as u64).next_u64();
    let config = GossipSettings::default().into_runtime_config(identity, rng_seed);
    let mut cell_store = CellStore::<u32>::new(cells, identity);
    if cell_store.intern_rule(keys.rule_descriptor()).is_none() {
        bail!("node {idx}: the cell store refused to register the regime's rule");
    }
    let store = Rc::new(NodeStore::new(Rc::clone(keys)));
    let (admin, admin_rx) = mpsc::channel(4);
    let (runtime, client) = GossipRuntime::from_parts_with_admin(
        SimSocket {
            world: Rc::clone(world),
            idx,
            addr: node_addr(idx),
        },
        SimClock {
            world: Rc::clone(world),
        },
        config,
        cell_store,
        Rc::clone(&store),
        Some(admin_rx),
    );
    let future = runtime.run(PeerStream {
        world: Rc::clone(world),
        idx,
    });
    Ok(Launched {
        future,
        client,
        admin,
        store,
    })
}
