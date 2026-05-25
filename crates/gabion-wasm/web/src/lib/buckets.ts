// Bucket-window math mirroring the CRDT (`crates/gabion/src/crdt.rs`). One
// source of truth for "how many buckets does a window of this width hold", so
// the rule-control readout (ControlRail) and the per-bucket strips (the Strata)
// can never disagree about how many bars a window shows.
//
// The engine keeps a cell while `bucket_epoch + liveBuckets >= currentEpoch`
// (the expiry predicate at `crdt.rs`: a cell is freed once
// `bucket + live < current`). So the live epochs are
// `[currentEpoch - liveBuckets, currentEpoch]` — `liveBuckets + 1` distinct
// buckets, one *more* than the nominal `window / bucket`, because the oldest
// bucket only partially overlaps the trailing edge of the sliding window and is
// retained until it falls fully out. We show every bucket the engine retains so
// the strip's Σ matches the node's aggregate total exactly.

/** The CRDT's `live_buckets` for a window: `div_ceil(window, bucket)`. */
export function nominalBuckets(windowMs: number, bucketMs: number): number {
  return Math.max(1, Math.ceil(windowMs / Math.max(bucketMs, 1)));
}

/** How many bucket bars a window shows: the nominal count plus the
 *  partially-overlapping oldest bucket the engine also retains. */
export function visibleBuckets(windowMs: number, bucketMs: number): number {
  return nominalBuckets(windowMs, bucketMs) + 1;
}

/** The slot layout for a strip at virtual time `virtualMs`. `slotCount` bars
 *  span absolute epochs `[oldestEpoch, currentEpoch]`; a cell in bucket-epoch
 *  `e` belongs to slot `e - oldestEpoch`. Keying bars by their absolute epoch
 *  (`oldestEpoch + i`) makes the strip scroll and the oldest bucket age out
 *  fall out of Svelte transitions as `virtualMs` advances. */
export interface BucketSlots {
  currentEpoch: number;
  oldestEpoch: number;
  slotCount: number;
}

export function bucketSlots(windowMs: number, bucketMs: number, virtualMs: number): BucketSlots {
  const bm = Math.max(bucketMs, 1);
  const live = nominalBuckets(windowMs, bm);
  const currentEpoch = Math.floor(virtualMs / bm);
  return {
    currentEpoch,
    // Mirror the engine's oldest retained epoch (`current - live`), not the
    // nominal `current - live + 1`: the off-by-one would drop the oldest kept
    // bucket and undercount Σ versus the node's aggregate total.
    oldestEpoch: currentEpoch - live,
    slotCount: live + 1,
  };
}
