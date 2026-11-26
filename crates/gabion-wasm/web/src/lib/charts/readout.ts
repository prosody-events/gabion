// The sentence the readout line under the charts prints for the item under the
// cursor, the finger or a focused chart's arrow keys: pure, so a test can
// recompute it.

import { fmtPercent } from '../format';

/** How long ago a slot or a bucket `ms` before the newest began: "3.2 s ago",
 *  or "now" for the newest. */
export const ago = (ms: number): string => (ms <= 0 ? 'now' : `${(ms / 1000).toFixed(1)} s ago`);

/** R1, bucket `k` of `buckets` (oldest first). */
export function windowLine(k: number, buckets: number, bucketMs: number, node: number, hits: number, truth: number, total: number, limit: number): string {
  const age = (buckets - 1 - k) * bucketMs;
  const which = age === 0 ? 'the current bucket' : `bucket from ${ago(age)}`;
  const cluster = truth === hits ? '' : ` · the cluster counted ${truth}`;
  return `${which} · ${hits} hits${cluster} · node ${node}'s window ${total} of ${limit}`;
}

/** What a request or a bin admitted and rejected: "25 admitted · 10
 *  rejected", leaving out a zero part unless both are. */
function split(admitted: number, rejected: number): string {
  if (rejected === 0) return `${admitted} admitted`;
  if (admitted === 0) return `${rejected} rejected`;
  return `${admitted} admitted · ${rejected} rejected`;
}

/** R2 in order: the `index`-th Send kept, counting from 1. */
export const requestLine = (index: number, node: number, admitted: number, rejected: number): string =>
  `request ${index} · node ${node} · ${split(admitted, rejected)}`;

/** R2 in time: one bin. */
export const binLine = (ageMs: number, admitted: number, rejected: number): string => `${ago(ageMs)} · ${split(admitted, rejected)}`;

/** L: one slot, with the nodes behind in it and by how much, in (zone, id)
 *  order. */
export function behindLine(ageMs: number, behind: readonly (readonly [node: number, hits: number])[]): string {
  if (behind.length === 0) return `${ago(ageMs)} · every node caught up`;
  return `${ago(ageMs)} · ${behind.map(([node, hits]) => `node ${node} behind by ${hits}`).join(', ')}`;
}

/** S: one slot's share. */
export const shareLine = (ageMs: number, share: number | null): string =>
  share === null
    ? `${ago(ageMs)} · no data bytes sent in the window`
    : `${ago(ageMs)} · ${fmtPercent(share)} of the window's data bytes crossed a zone`;
