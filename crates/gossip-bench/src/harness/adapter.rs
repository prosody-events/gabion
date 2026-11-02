//! The one place the harness names the library under test beyond the
//! stable trait surface (`gabion::gossip::{Clock, GossipTransport, Ticker}`,
//! `gabion::discovery::{Peer, PeerEvent}`). It builds each node the way
//! `gabiond` does at boot, gives its discovery stream each added peer's zone,
//! as Kubernetes EndpointSlices do, records hits through its client, reads its
//! admin snapshot, and keeps the aggregate the node writes through. The judge
//! exempts this file from the harness identity check: a baseline arm builds
//! with H1's copy against its own library, and a phase that changes an API
//! used here changes this file in the same commit.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use ahash::AHashMap;
use anyhow::Result;
use futures::StreamExt;
use gabion::aggregate::Aggregate;
pub use gabion::crdt::BucketEpoch;
use gabion::crdt::{
    CellStore, DeltaSink, ExpirationSink, KeyHash, NodeId, NodeIdentity, ValidStorageConfig,
};
pub(super) use gabion::defaults as d;
use gabion::discovery::{Peer, PeerEvent};
use gabion::gossip::{
    AdminCommand, AdminSnapshot, AggregateStore, GossipClient, GossipError, GossipRuntime,
    GossipTransport, Ticker, ValidGossipConfig,
};
use gabion::rules::{Descriptor, EnforcementMode, RuleDef, RuleSpec, RuleTable, hash_key};
use gabion::settings::Settings;
use gabion::zone::{ZoneHash, ZoneName};
use tokio::sync::{mpsc, oneshot};

use super::driver::Counters;
use super::node::{KeySpace, NOT_TRACKED, Sizing};
use super::plan::{node_addr, node_az};
use super::regime::Regime;
use super::rng::{Rng, Stream};
use super::world::{PeerStream, SimClock, SimSocket, SimTicker, World};

const DOMAIN: &str = "study";
const DESCRIPTOR_KEY: &str = "site";

impl Ticker for SimTicker {
    async fn tick(&mut self) -> u64 {
        self.next_tick().await
    }
}

/// The library's identity of the regime's one rule and of every key.
struct Identity {
    /// The regime's rule, which every node's cell store interns.
    rules: RuleTable,
    rule: RuleSpec,
    hashes: Box<[KeyHash]>,
    /// Key hash → key index.
    index: AHashMap<u128, u32>,
}

impl Identity {
    fn build(regime: &Regime, keys: usize, cells: &ValidStorageConfig) -> Result<Self> {
        let rule = RuleDef {
            name: "study",
            domain: DOMAIN,
            patterns: &[(DESCRIPTOR_KEY, "*")],
            rate: &format!("{}r/{}ms", regime.rule.limit, regime.rule.window_ms),
            window: None,
            bucket: Some(Duration::from_millis(regime.rule.bucket_ms)),
            mode: EnforcementMode::Enforce,
        }
        .build(&cells.cardinality)?;
        let spec = rule.spec();
        let hashes: Box<[KeyHash]> = (0..keys)
            .map(|i| {
                let value = i.to_string();
                hash_key(
                    DOMAIN,
                    [Descriptor {
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
        Ok(Self {
            rules: RuleTable::new(Box::new([rule]), cells.rule_dictionary_capacity())?,
            rule: spec,
            hashes,
            index,
        })
    }
}

/// A node's view of the cluster aggregate: the library's aggregate table,
/// sized from the node's validated `storage.max_cells` as `gabiond` sizes
/// it, plus the driver's bookkeeping of which tracked rows changed.
pub struct NodeStore {
    keys: Rc<KeySpace>,
    identity: Rc<Identity>,
    counts: Aggregate,
    /// `(tracked index, bucket)` rows touched since the driver last looked.
    touched: RefCell<Vec<(u32, BucketEpoch)>>,
    /// Rows under the regime's rule for a key the regime never issued. Always
    /// zero for a correct solution.
    pub foreign_rows: Cell<u64>,
}

impl NodeStore {
    fn new(keys: Rc<KeySpace>, identity: Rc<Identity>, cells: &ValidStorageConfig) -> Self {
        Self {
            keys,
            identity,
            counts: Aggregate::new(cells),
            touched: RefCell::new(Vec::new()),
            foreign_rows: Cell::new(0),
        }
    }

    /// What the adapter's admission check reads: the live-window total for
    /// `key` at `now_millis`.
    pub fn window_total(&self, key: u32, now_millis: u64) -> u64 {
        let hash = self.identity.hashes[key as usize];
        self.counts
            .table()
            .window_total(self.identity.rule, hash, now_millis)
    }

    pub fn bucket_count(&self, key: u32, bucket: BucketEpoch) -> u32 {
        let hash = self.identity.hashes[key as usize];
        self.counts
            .table()
            .get(self.identity.rule.fingerprint, hash.0, bucket)
            .map_or(0, |cell| cell.count.min(u64::from(u32::MAX)) as u32)
    }

    pub fn take_touched(&self, into: &mut Vec<(u32, BucketEpoch)>) {
        into.clear();
        std::mem::swap(into, &mut self.touched.borrow_mut());
    }

    /// Note a row the node applied: count it as foreign when the regime
    /// never issued its key, else mark a tracked key's bucket touched.
    fn note(&self, fingerprint: u128, key_hash: KeyHash, bucket: BucketEpoch) {
        let key = (fingerprint == self.identity.rule.fingerprint)
            .then(|| self.identity.index.get(&key_hash.0).copied())
            .flatten();
        let Some(key) = key else {
            self.foreign_rows.set(self.foreign_rows.get() + 1);
            return;
        };
        let tracked = self.keys.tracked_of[key as usize];
        if tracked != NOT_TRACKED {
            self.touched.borrow_mut().push((tracked, bucket));
        }
    }
}

impl AggregateStore for NodeStore {
    fn apply(&self, deltas: &DeltaSink, expirations: &ExpirationSink) {
        self.counts.apply(deltas, expirations);
        for i in 0..deltas.len() {
            if deltas.applies_locally[i] != 0 && deltas.deltas[i] != 0 {
                let cell = deltas.keys[i];
                self.note(cell.rule_fingerprint, cell.key_hash, cell.bucket);
            }
        }
        for i in 0..expirations.len() {
            if expirations.applies_locally[i] != 0 {
                let cell = expirations.keys[i];
                self.note(cell.rule_fingerprint, cell.key_hash, cell.bucket);
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
    client: GossipClient,
    admin: mpsc::Sender<AdminCommand>,
    identity: Rc<Identity>,
}

impl Node {
    /// Record one admitted hit of `key`, as `SharedLimiter` does after
    /// admitting it. Resolves to whether the node took the hit.
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
            decode_rejects: snapshot.counters.decode_rejects(),
        })
    }
}

/// The production settings with the store sized for the regime and the peer
/// table for every node it starts, which the launcher validates once as
/// `gabiond` validates them.
pub(super) fn settings(sizing: &Sizing) -> Result<Settings> {
    Ok(Settings {
        max_cells: sizing.cell_capacity.try_into()?,
        peer_capacity: u16::try_from(sizing.total_nodes)?
            .max(d::GOSSIP_PEER_CAPACITY)
            .try_into()?,
        ..Settings::default()
    })
}

/// Starts nodes on a world: everything they share is built once.
pub struct Launcher {
    world: Rc<World>,
    keys: Rc<KeySpace>,
    identity: Rc<Identity>,
    gossip: ValidGossipConfig,
    cells: ValidStorageConfig,
    /// Each node's zone by address, as discovery reports it.
    zones: Rc<AHashMap<SocketAddr, ZoneHash>>,
    seed: u64,
}

/// The zone the plan places node `idx` in.
fn zone_name(idx: usize) -> Result<ZoneName> {
    Ok(ZoneName::parse(&format!("az-{}", node_az(idx)))?)
}

impl Launcher {
    pub fn new(
        regime: &Regime,
        sizing: &Sizing,
        world: &Rc<World>,
        keys: &Rc<KeySpace>,
        seed: u64,
    ) -> Result<Self> {
        let (gossip, cells) = settings(sizing)?.validate()?;
        let zones = (0..sizing.total_nodes)
            .map(|idx| Ok((node_addr(idx), zone_name(idx)?.hash())))
            .collect::<Result<_>>()?;
        Ok(Self {
            world: Rc::clone(world),
            keys: Rc::clone(keys),
            identity: Rc::new(Identity::build(regime, keys.len(), &cells)?),
            gossip,
            cells,
            zones: Rc::new(zones),
            seed,
        })
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
    /// (production gossip settings, the regime's rule interned as the cell
    /// store is built); a test wraps the node's [`SimSocket`] to observe
    /// what it sends.
    pub fn launch_on<T: GossipTransport>(
        &self,
        transport: T,
        idx: usize,
    ) -> Result<Launched<impl Future<Output = Result<(), GossipError>> + use<T>>> {
        // The node id's hash ranks each node's join-sweep sources, the only
        // per-node gossip randomness this library has: draw it from the run's
        // gossip stream.
        let draw = Rng::stream(self.seed, Stream::Gossip, idx as u64).next_u64();
        let id = (u128::from(draw) << 64) | idx as u128 | 1 << 63;
        let identity = NodeIdentity::new(NodeId(id), 1);
        let cell_store = CellStore::new(self.cells, identity, &self.identity.rules);
        let store = Rc::new(NodeStore::new(
            Rc::clone(&self.keys),
            Rc::clone(&self.identity),
            &self.cells,
        ));
        // Each node announces the zone the plan places it in.
        let mut gossip = self.gossip.clone();
        gossip.zone = Some(zone_name(idx)?);
        let (admin, admin_rx) = mpsc::channel(4);
        let (runtime, client) = GossipRuntime::from_parts_with_admin(
            transport,
            SimClock {
                world: Rc::clone(&self.world),
            },
            gossip,
            cell_store,
            Rc::clone(&store),
            Some(admin_rx),
        );
        // Discovery names each added peer's zone, as an EndpointSlice
        // endpoint's `zone` does.
        let zones = Rc::clone(&self.zones);
        let peers = PeerStream {
            world: Rc::clone(&self.world),
            idx,
        }
        .map(move |event| match event {
            PeerEvent::Added(peer) => PeerEvent::Added(Peer {
                zone: zones.get(&peer.addr).copied(),
                ..peer
            }),
            other => other,
        });
        let future = runtime.run(peers);
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
