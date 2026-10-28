//! The gossip leader's writer for the shared-memory aggregate table: the
//! library's [`AggregateWriter`] over the zone's slots, fenced by the leader
//! lease. Every worker reads the same table on the
//! request hot path through [`super::ShmRegion::aggregate`].

use std::sync::atomic::Ordering;

use gabion::aggregate::AggregateWriter;
use gabion::crdt::{DeltaSink, ExpirationSink};
use gabion::gossip::AggregateStore;

use super::ShmRegion;

/// Every slot is an atomic, so a second writer could not cause undefined
/// behaviour, only wrong counts; the gossip leader lease keeps the writer
/// single across processes.
pub struct ShmAggregateStore {
    region: ShmRegion,
    /// The lease epoch this leader won; a later one means it lost the lease.
    epoch: u64,
}

impl ShmAggregateStore {
    /// The writer for `region`'s table, which it empties first. Each leader
    /// builds its counts afresh, from its own hits and peers' gossip, so
    /// counts an earlier leader left behind would be added a second time and
    /// never expire. Readers under-count until gossip refills the table.
    pub fn new(region: ShmRegion) -> Self {
        let cleared = writer(&region).clear();
        if cleared > 0 {
            tracing::info!(
                code = "leader_takeover",
                cleared,
                "gabion: this worker took over gossip and emptied the previous leader's counts, \
                 so that peers' gossip refills them without counting them twice. Until then, \
                 usually a gossip tick or two, this node under-counts and may admit requests over \
                 their limits; a node with no peers instead under-counts until each rule's window \
                 has passed. Nothing to do unless it lasts more than a few gossip ticks; if it \
                 does, check that this node receives gossip from its peers (\"Verifying \
                 convergence\" in the gabion-nginx README).",
            );
        }
        Self {
            region,
            epoch: region.lease().epoch.load(Ordering::Acquire),
        }
    }
}

impl AggregateStore<u32> for ShmAggregateStore {
    fn apply(&self, deltas: &DeltaSink<u32>, expirations: &ExpirationSink<u32>) {
        // A leader that stalled past its lease may still be applying gossip
        // after its successor emptied the table, which would count it twice.
        if self.region.lease().epoch.load(Ordering::Acquire) != self.epoch {
            return;
        }
        writer(&self.region).apply(deltas, expirations);
    }
}

fn writer(region: &ShmRegion) -> AggregateWriter<'_> {
    AggregateWriter::new(
        region.aggregate(),
        &region.header().aggregate_write_seq,
        &region.stats().aggregate_dropped,
    )
}

#[cfg(test)]
mod tests;
