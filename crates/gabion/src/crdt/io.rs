//! Column-oriented IO containers: [`ObservationBatch`], [`DeltaSink`], and
//! [`ExpirationSink`].

use super::{
    BucketEpoch, CellDelta, CellExpiration, CellHandle, CompactCellKey, Count, Incarnation,
    KeyHash, NodeId,
};

/// One row to push into an [`ObservationBatch`]. Bundled so callers can
/// construct an observation in one expression rather than passing seven
/// columns positionally.
#[derive(Clone, Copy, Debug)]
pub struct Observation<C: Count> {
    pub rule_fingerprint: u128,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub origin: NodeId,
    pub incarnation: Incarnation,
    pub count: C,
    pub last_update_millis: u64,
}

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

    /// Append one observation row. Caller must keep all columns the same
    /// length.
    pub fn push(&mut self, row: Observation<C>) {
        self.rule_fingerprints.push(row.rule_fingerprint);
        self.key_hashes.push(row.key_hash);
        self.buckets.push(row.bucket);
        self.origin_node_ids.push(row.origin);
        self.incarnations.push(row.incarnation);
        self.counts.push(row.count);
        self.last_update_millis.push(row.last_update_millis);
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

/// SoA expiration channel. The CRDT module appends one column row per cell
/// freed by [`super::CellStore::expire`]. Mirrors [`DeltaSink`] so external
/// aggregate stores can fold both signal halves without per-record dispatch.
#[derive(Clone, Debug, Default)]
pub struct ExpirationSink<C: Count> {
    pub handles: Vec<CellHandle>,
    pub keys: Vec<CompactCellKey>,
    pub last_counts: Vec<C>,
    pub last_update_millis: Vec<u64>,
    /// `0`/`1` flags — kept as a byte column rather than `Vec<bool>`.
    pub applies_locally: Vec<u8>,
}

impl<C: Count> ExpirationSink<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            handles: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
            last_counts: Vec::with_capacity(capacity),
            last_update_millis: Vec::with_capacity(capacity),
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
        self.last_counts.clear();
        self.last_update_millis.clear();
        self.applies_locally.clear();
    }

    pub fn row(&self, i: usize) -> Option<CellExpiration<C>> {
        if i >= self.len() {
            return None;
        }
        Some(CellExpiration {
            handle: self.handles[i],
            key: self.keys[i],
            last_count: self.last_counts[i],
            last_update_millis: self.last_update_millis[i],
            applies_locally: self.applies_locally[i] != 0,
        })
    }

    pub(super) fn push(
        &mut self,
        handle: CellHandle,
        key: CompactCellKey,
        last_count: C,
        last_update_millis: u64,
        applies_locally: bool,
    ) {
        self.handles.push(handle);
        self.keys.push(key);
        self.last_counts.push(last_count);
        self.last_update_millis.push(last_update_millis);
        self.applies_locally.push(applies_locally as u8);
    }
}
