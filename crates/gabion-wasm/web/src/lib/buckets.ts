// The window's bucket count for the ControlRail's "Window N s · M buckets"
// preview and the Strata's grid. The preview labels a knob value the user is
// about to apply, before any node has interned the rule, so there is no live
// state to read and TS can't call into Rust for a label.
//
// Live CRDT state is *not* rendered from this file: the Strata right-anchors its
// fixed-width grid on the CRDT-reported `bucket_epoch_now` off the snapshot, so
// it can never drift from production expiry.

/** The rule's `live_buckets` for a window, `div_ceil(window, bucket)`: the
 *  buckets admission's `window_total` counts, the current one and those before
 *  it. */
export function nominalBuckets(windowMs: number, bucketMs: number): number {
  return Math.max(1, Math.ceil(windowMs / Math.max(bucketMs, 1)));
}
