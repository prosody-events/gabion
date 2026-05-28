//! [`DashMapStore`] — the server-side aggregate store the gossip runtime
//! writes through.
//!
//! Keys on the portable cell identity `(rule_fingerprint, key_hash, bucket)`.
//! Per-bucket totals accumulate contributions from every origin; on
//! expiration the dying origin's last count is subtracted, mirroring the
//! per-origin CRDT semantics at the cluster level.

use std::marker::PhantomData;

use dashmap::DashMap;

use gabion::crdt::{BucketEpoch, Count, DeltaSink, ExpirationSink, KeyHash};
use gabion::gossip::AggregateStore;
use gabion::rules::RuleSpec;

/// Lock-sharded per-bucket counter table. `&self` everywhere — clone an
/// `Arc<DashMapStore<C>>` between the gossip writer and the read path.
#[derive(Debug)]
pub struct DashMapStore<C: Count> {
    cells: DashMap<(u128, KeyHash, BucketEpoch), u64>,
    _marker: PhantomData<fn() -> C>,
}

impl<C: Count> Default for DashMapStore<C> {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

impl<C: Count> DashMapStore<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cells: DashMap::with_capacity(capacity),
            _marker: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// Sum live buckets for `(rule_fingerprint, key_hash)`. `live_buckets` is
    /// the rule's window expressed in bucket counts (≥ 1). The current
    /// bucket and the preceding `live_buckets − 1` are summed; older buckets
    /// (already aged out of the rule's window) are skipped.
    pub fn window_total(
        &self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        now_millis: u64,
        bucket_millis: u64,
        live_buckets: u32,
    ) -> u64 {
        // `Rule::new` clamps `bucket_millis` and `live_buckets` to ≥ 1, so
        // every caller routed through `Rule::spec()` is safe. Defensive
        // clamps here would only hide caller bugs.
        let current = (now_millis / bucket_millis) as BucketEpoch;
        let mut total: u64 = 0;
        for offset in 0..live_buckets {
            let Some(bucket) = current.checked_sub(offset) else {
                break;
            };
            if let Some(value) = self.cells.get(&(rule_fingerprint, key_hash, bucket)) {
                total = total.saturating_add(*value.value());
            }
        }
        total
    }

    /// Wall-clock ms until a request of weight `hits` for `(spec, key_hash)`
    /// would be admitted under the sliding-window model. One DashMap lookup
    /// per walked bucket; typical case is one lookup.
    pub fn time_until_admit_millis(
        &self,
        spec: RuleSpec,
        key_hash: KeyHash,
        now_millis: u64,
        total: u64,
        hits: u64,
    ) -> u64 {
        gabion::window::time_until_admit_millis(
            now_millis,
            spec.bucket_millis,
            spec.live_buckets,
            spec.limit,
            total,
            hits,
            |bucket| {
                self.cells
                    .get(&(spec.fingerprint, key_hash, bucket))
                    .map_or(0, |v| *v.value())
            },
        )
    }
}

impl<C: Count> AggregateStore<C> for DashMapStore<C> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        for i in 0..deltas.len() {
            if deltas.applies_locally[i] == 0 {
                continue;
            }
            let key = &deltas.keys[i];
            let v: u64 = deltas.deltas[i].into();
            if v == 0 {
                continue;
            }
            let mut entry = self
                .cells
                .entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0);
            *entry = entry.saturating_add(v);
        }
        for i in 0..expirations.len() {
            if expirations.applies_locally[i] == 0 {
                continue;
            }
            let key = &expirations.keys[i];
            let v: u64 = expirations.last_counts[i].into();
            let composite = (key.rule_fingerprint, key.key_hash, key.bucket);
            // Subtract this origin's last contribution. If the bucket total
            // hits zero (all origins have expired), drop the row entirely.
            // DashMap's `remove_if` keeps the per-shard lock for the whole
            // check; no torn read.
            let mut drop_row = false;
            if let Some(mut entry) = self.cells.get_mut(&composite) {
                let next = entry.saturating_sub(v);
                *entry = next;
                drop_row = next == 0;
            }
            if drop_row {
                self.cells.remove_if(&composite, |_, v| *v == 0);
            }
        }
    }
}
