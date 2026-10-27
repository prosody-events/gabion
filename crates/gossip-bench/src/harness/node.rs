//! What every node and the oracle share: the regime's key population, the
//! per-key bucket rings, and the per-node sizing. Nothing here names the
//! library; [`super::adapter`] maps it onto gabion's types.

use serde::Serialize;

use super::adapter::BucketEpoch;
use super::plan::Plan;
use super::regime::Regime;
use super::rng::{Rng, Stream};

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
    pub limit: u64,
    pub window_ms: u64,
    pub bucket_ms: u64,
    pub live_buckets: u32,
    /// Ring slots kept per key: the live window plus two, so a bucket that
    /// left the window is never confused with a live one.
    pub ring_len: u32,
    pub kinds: Vec<KeyKind>,
    /// Key → tracked index, or [`NOT_TRACKED`].
    pub tracked_of: Vec<u32>,
    /// Tracked index → key.
    pub tracked: Vec<u32>,
}

impl KeySpace {
    pub fn build(regime: &Regime, seed: u64) -> Self {
        let sites = regime.site_keys();
        let mut kinds = vec![KeyKind::Site; sites as usize];
        kinds.extend(regime.traffic.hot_keys.iter().map(|_| KeyKind::Hot));
        kinds.extend(regime.traffic.bursts.iter().map(|_| KeyKind::Burst));
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

        let live_buckets = regime.live_buckets();
        Self {
            limit: regime.rule.limit,
            window_ms: regime.rule.window_ms,
            bucket_ms: regime.rule.bucket_ms,
            live_buckets,
            ring_len: live_buckets + 2,
            kinds,
            tracked_of,
            tracked,
        }
    }

    pub fn len(&self) -> usize {
        self.kinds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }

    pub fn bucket_of(&self, now_millis: u64) -> BucketEpoch {
        (now_millis / self.bucket_ms) as BucketEpoch
    }

    pub fn slot(&self, key: u32, bucket: BucketEpoch) -> usize {
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

/// Per-node capacities, derived from the regime and identical for every
/// solution.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Sizing {
    pub cell_capacity: u32,
    pub total_nodes: usize,
    pub max_running_nodes: usize,
    pub keys: usize,
    pub tracked_keys: usize,
    pub peak_offered_rps: f64,
}

impl Sizing {
    pub fn new(regime: &Regime, plan: &Plan, keys: &KeySpace) -> Self {
        let cells = (1.5 * live_cell_bound(regime, plan.max_running)).ceil() as u64;
        let cell_capacity = cells
            .max(16_384)
            .next_multiple_of(1024)
            .min(u64::from(u32::MAX)) as u32;
        let total_nodes = plan.nodes.len();
        Self {
            cell_capacity,
            total_nodes,
            max_running_nodes: plan.max_running,
            keys: keys.len(),
            tracked_keys: keys.tracked.len(),
            peak_offered_rps: regime.peak_offered_rps(),
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
