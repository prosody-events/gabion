// Forward-looking "Window N s · M buckets" preview for the ControlRail. This is
// the *only* consumer left: it labels a knob value the user is about to apply,
// before any node has interned the rule, so there is no live CRDT state to read
// and TS can't call into Rust for a label. It mirrors
// `RuleDescriptor::live_buckets() + 1` — the nominal `ceil(window / bucket)`
// plus the partially-overlapping oldest bucket the engine retains until it
// falls fully out of the sliding window.
//
// Live CRDT state is *not* rendered from this file: the Strata right-anchors its
// fixed-width grid on the CRDT-reported `bucket_epoch_now` off the snapshot, so
// it can never drift from production expiry.

/** The CRDT's `live_buckets` for a window: `div_ceil(window, bucket)`. */
export function nominalBuckets(windowMs: number, bucketMs: number): number {
  return Math.max(1, Math.ceil(windowMs / Math.max(bucketMs, 1)));
}

/** How many bucket bars a window shows: the nominal count plus the
 *  partially-overlapping oldest bucket the engine also retains. Mirrors
 *  `RuleDescriptor::live_buckets() + 1`. */
export function visibleBuckets(windowMs: number, bucketMs: number): number {
  return nominalBuckets(windowMs, bucketMs) + 1;
}
