//! Column-oriented IO containers: [`ObservationBatch`] and [`DeltaSink`].

use super::{
    BucketEpoch, CellDelta, CellHandle, CompactCellKey, Count, Incarnation, KeyHash, NodeId,
};

/// SoA input for `ingest_local` / `merge_remote`. For `ingest_local`, the
/// `counts` column carries the hit delta to saturating-add; for
/// `merge_remote`, it carries the observed absolute count to max-merge.
#[derive(Clone, Debug, Default)]
pub struct ObservationBatch<C: Count> {
    pub rule_fingerprints: Vec<u128>,
    pub key_hashes: Vec<KeyHash>,
    pub buckets: Vec<BucketEpoch>,
    pub origin_node_ids: Vec<NodeId>,
    pub incarnations: Vec<Incarnation>,
    pub counts: Vec<C>,
    pub last_update_millis: Vec<u64>,
}

impl<C: Count> ObservationBatch<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rule_fingerprints: Vec::with_capacity(capacity),
            key_hashes: Vec::with_capacity(capacity),
            buckets: Vec::with_capacity(capacity),
            origin_node_ids: Vec::with_capacity(capacity),
            incarnations: Vec::with_capacity(capacity),
            counts: Vec::with_capacity(capacity),
            last_update_millis: Vec::with_capacity(capacity),
        }
    }

    pub fn assert_consistent(&self) {
        let n = self.rule_fingerprints.len();
        debug_assert_eq!(self.key_hashes.len(), n);
        debug_assert_eq!(self.buckets.len(), n);
        debug_assert_eq!(self.origin_node_ids.len(), n);
        debug_assert_eq!(self.incarnations.len(), n);
        debug_assert_eq!(self.counts.len(), n);
        debug_assert_eq!(self.last_update_millis.len(), n);
    }

    pub fn len(&self) -> usize {
        self.rule_fingerprints.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        self.rule_fingerprints.clear();
        self.key_hashes.clear();
        self.buckets.clear();
        self.origin_node_ids.clear();
        self.incarnations.clear();
        self.counts.clear();
        self.last_update_millis.clear();
    }

    /// Append one observation row. Caller must keep all columns the same length.
    pub fn push(
        &mut self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        origin: NodeId,
        incarnation: Incarnation,
        count: C,
        last_update_millis: u64,
    ) {
        self.rule_fingerprints.push(rule_fingerprint);
        self.key_hashes.push(key_hash);
        self.buckets.push(bucket);
        self.origin_node_ids.push(origin);
        self.incarnations.push(incarnation);
        self.counts.push(count);
        self.last_update_millis.push(last_update_millis);
    }
}

/// SoA delta channel. The CRDT module appends one column row per cell whose
/// stored count rose. The aggregator folds without per-record dispatch.
#[derive(Clone, Debug, Default)]
pub struct DeltaSink<C: Count> {
    pub handles: Vec<CellHandle>,
    pub keys: Vec<CompactCellKey>,
    pub previous: Vec<C>,
    pub current: Vec<C>,
    pub deltas: Vec<C>,
    /// `0`/`1` flags — kept as a byte column rather than `Vec<bool>`.
    pub applies_locally: Vec<u8>,
}

impl<C: Count> DeltaSink<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            handles: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
            previous: Vec::with_capacity(capacity),
            current: Vec::with_capacity(capacity),
            deltas: Vec::with_capacity(capacity),
            applies_locally: Vec::with_capacity(capacity),
        }
    }
    pub fn len(&self) -> usize {
        self.handles.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        self.handles.clear();
        self.keys.clear();
        self.previous.clear();
        self.current.clear();
        self.deltas.clear();
        self.applies_locally.clear();
    }

    pub fn row(&self, i: usize) -> Option<CellDelta<C>> {
        if i >= self.len() {
            return None;
        }
        Some(CellDelta {
            handle: self.handles[i],
            key: self.keys[i],
            previous_count: self.previous[i],
            current_count: self.current[i],
            delta: self.deltas[i],
            applies_locally: self.applies_locally[i] != 0,
        })
    }

    pub(super) fn push(
        &mut self,
        handle: CellHandle,
        key: CompactCellKey,
        previous: C,
        current: C,
        delta: C,
        applies_locally: bool,
    ) {
        self.handles.push(handle);
        self.keys.push(key);
        self.previous.push(previous);
        self.current.push(current);
        self.deltas.push(delta);
        self.applies_locally.push(applies_locally as u8);
    }
}
