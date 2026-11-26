// What the time charts read: the last rule window of virtual time, in `SLOTS`
// slots of `W / SLOTS` each, kept in preallocated typed rings with a head
// index. A slot opens when virtual time crosses its start, never per frame, and
// a snapshot only adds into the open slot, so `push` allocates nothing once
// every node has a row (a node's row is made when it is first seen).
//
// Series: the requests the cluster admitted and rejected in each slot (the
// differences of `ClusterState`'s session counters, which never fall), each
// node's hits behind in each slot (NaN while caught up), and the cumulative data
// bytes in all and across zones at each slot's start, kept for two windows so
// the share over the window ending at any slot of the last window is exact at
// slot boundaries.

import type { ClusterState } from '../sim/types';

/** Slots a window is cut into: 100 ms each at the default 10 s window. */
export const SLOTS = 100;
/** Cumulative marks kept: the starts of two windows of slots, and the open
 *  slot's. */
const MARKS = 2 * SLOTS + 1;

/** The snapshot fields one sample reads. */
export type Sample = Pick<
  ClusterState,
  'virtual_ms' | 'nodes' | 'bytes_by_tier' | 'oracle_total' | 'admitted_hits' | 'rejected_hits'
>;

/** One node's row: its hits behind in each slot (NaN while it was caught up
 *  all slot long), and its state at the last sample, which holds until a later
 *  sample shows otherwise. */
interface Row {
  readonly node: number;
  readonly behind: Float64Array;
  last: number;
  seen: number;
}

/** The ring index of slot `k`. */
export const slotIndex = (k: number): number => ((k % SLOTS) + SLOTS) % SLOTS;

export class ChartHistory {
  /** Virtual ms per slot. */
  slotMs = 100;
  /** The open slot, `floor(t / slotMs)` at the last sample; −1 before any. */
  head = -1;
  /** The first slot sampled since the reset. */
  first = 0;
  /** Requests admitted and rejected in each slot, by `slotIndex`. */
  readonly admitted = new Float64Array(SLOTS);
  readonly rejected = new Float64Array(SLOTS);
  readonly #allStart = new Float64Array(MARKS);
  readonly #crossStart = new Float64Array(MARKS);
  readonly #rows = new Map<number, Row>();
  // The same rows in a list, walked by index when slots open.
  readonly #list: Row[] = [];
  #samples = 0;
  #admitted = 0;
  #rejected = 0;
  #all = 0;
  #cross = 0;

  /** The rule window, in virtual ms. */
  get windowMs(): number {
    return this.slotMs * SLOTS;
  }

  /** Forget every sample and cut a window of `windowMs` into slots. Called on a
   *  new run; a membership change keeps the history. */
  reset(windowMs: number): void {
    this.slotMs = windowMs / SLOTS;
    this.head = -1;
    this.#rows.clear();
    this.#list.length = 0;
    this.#samples = 0;
  }

  /** Node `node`'s hits behind per slot, by `slotIndex`, or undefined if it
   *  was never seen. */
  behind(node: number): Float64Array | undefined {
    return this.#rows.get(node)?.behind;
  }

  /** Fold one snapshot into the open slot, opening every slot whose start
   *  virtual time has crossed since the last sample. */
  push(snap: Sample): void {
    const k = Math.floor(snap.virtual_ms / this.slotMs);
    const cross = snap.bytes_by_tier.cross_zone.data;
    const all = snap.bytes_by_tier.in_zone.data + cross;
    const seen = ++this.#samples;
    if (this.head < 0) {
      this.head = k;
      this.first = k;
      this.admitted.fill(0);
      this.rejected.fill(0);
      this.#allStart[k % MARKS] = all;
      this.#crossStart[k % MARKS] = cross;
      this.#admitted = snap.admitted_hits;
      this.#rejected = snap.rejected_hits;
    }
    // Open each slot crossed since the last sample: a state holds until a
    // sample shows it changed, and what happened since the last sample counts
    // in the slot this one falls in.
    for (let j = Math.max(this.head + 1, k - MARKS + 1); j <= k; j++) {
      this.#allStart[j % MARKS] = this.#all;
      this.#crossStart[j % MARKS] = this.#cross;
      const i = slotIndex(j);
      this.admitted[i] = 0;
      this.rejected[i] = 0;
      for (let r = 0; r < this.#list.length; r++) this.#list[r].behind[i] = this.#list[r].last;
    }
    if (k > this.head) this.head = k;
    const open = slotIndex(this.head);
    this.admitted[open] += snap.admitted_hits - this.#admitted;
    this.rejected[open] += snap.rejected_hits - this.#rejected;
    this.#admitted = snap.admitted_hits;
    this.#rejected = snap.rejected_hits;
    this.#all = all;
    this.#cross = cross;

    for (const node of snap.nodes) {
      let row = this.#rows.get(node.id);
      if (row === undefined) {
        row = { node: node.id, behind: new Float64Array(SLOTS).fill(Number.NaN), last: Number.NaN, seen };
        this.#rows.set(node.id, row);
        this.#list.push(row);
      }
      row.seen = seen;
      row.last = node.lag.kind === 'CaughtUp' ? Number.NaN : Math.max(snap.oracle_total - node.window_total, 0);
      // The slot keeps the most a node was behind in it.
      if (!Number.isNaN(row.last) && !(row.behind[open] >= row.last)) row.behind[open] = row.last;
    }
    // A departed node's row goes. Membership changes are rare, so this runs
    // only when one has.
    if (this.#list.length > snap.nodes.length) {
      for (let r = this.#list.length - 1; r >= 0; r--) {
        if (this.#list[r].seen === seen) continue;
        this.#rows.delete(this.#list[r].node);
        this.#list.splice(r, 1);
      }
    }
  }

  /** The mark index of slot `j`'s start: the first sample's for a slot before
   *  it. */
  #mark(j: number): number {
    return Math.max(j, this.first) % MARKS;
  }

  /** The share of the data bytes sent in the window ending at slot `j`'s end
   *  (for the open slot: now) that crossed zones, or null when none were sent
   *  or `j` is outside the last window. */
  shareAt(j: number): number | null {
    if (this.head < 0 || j > this.head || j < this.head - SLOTS) return null;
    const from = this.#mark(j - SLOTS + 1);
    const open = j === this.head;
    const to = this.#mark(j + 1);
    const all = (open ? this.#all : this.#allStart[to]) - this.#allStart[from];
    const cross = (open ? this.#cross : this.#crossStart[to]) - this.#crossStart[from];
    return all > 0 ? cross / all : null;
  }
}
