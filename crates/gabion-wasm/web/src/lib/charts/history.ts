// The rolling time-series the dashboard charts read from. One sample per engine
// snapshot: the cluster's true total (the simulator-only oracle), each zone's
// least and most informed view, the share of data bytes that crossed zones over
// the rule window, and every node's own view. Stored column-major, the shape
// uPlot's `setData` wants, so a redraw is a reference pass, not a transform.
//
// Deliberately a plain class, not Svelte `$state`: 600 samples × (N + 4Z)
// numbers under a deep reactive proxy would cost on every frame. The owner
// mutates the columns here and bumps a single `$state` counter to trigger the
// charts' redraw effect (see `App.svelte` / `Dashboard.svelte`).

import type { ClusterState, NodeState } from '../sim/types';

/** Rolling-window depth: at the visualizer's tick rate, 600 samples is
 *  comfortably past convergence. */
export const HISTORY_CAP = 600;

/** The window's cumulative byte marks are kept this many to a window, however
 *  fast the play speed, so the ring is bounded. */
const MARKS_PER_WINDOW = 128;

/** One node's line, keyed by its *stable id* (not rank): its view of the total
 *  at each retained sample, aligned to `times`. `null` before the node joined
 *  and after it left: uPlot draws those as a gap, so a newcomer's line begins
 *  mid-chart and a departed node's line ends there rather than the whole window
 *  restarting. `samples` counts the non-null entries so a column can be dropped
 *  once its last real value scrolls out of the window, with no scan. */
interface NodeColumn {
  values: (number | null)[];
  samples: number;
}

/** The cumulative data bytes, and every node's own-stream bytes, at one instant:
 *  what a window's start is measured against. */
export interface Mark {
  t: number;
  inData: number;
  crossData: number;
  nodes: readonly NodeState[];
}

/** The session's start: nothing sent yet. */
const SESSION_START: Mark = { t: 0, inData: 0, crossData: 0, nodes: [] };

export class ChartHistory {
  /** x-axis: virtual milliseconds since the session began. */
  readonly times: number[] = [];
  /** Engine tick at each sample, so "converged in N rounds" can name the round. */
  readonly ticks: number[] = [];
  /** Ground-truth cluster total, the line every node chases. */
  readonly oracle: number[] = [];
  /** max − min of every node's view: the disagreement that decays to zero. */
  readonly disagreement: number[] = [];
  /** The share of data bytes that crossed zones over the window ending at each
   *  sample; `null` when the window carried no data bytes. */
  readonly share: (number | null)[] = [];
  /** Per zone, the least and most informed view among its live members at each
   *  sample, and their difference; `null` while the zone has no live node. */
  readonly zoneMin: (number | null)[][] = [];
  readonly zoneMax: (number | null)[][] = [];
  readonly zoneSpread: (number | null)[][] = [];
  /** Per-node columns keyed by stable id, insertion-ordered. Keying by id (not
   *  rank) is what lets a join or leave thread through the existing window
   *  instead of restarting it: a join appends a back-filled column, a leave
   *  stops feeding one (its line gaps), and the shared time axis never resets. */
  readonly #columns = new Map<number, NodeColumn>();
  // The cumulative marks the share window is measured from: at most one per
  // `windowMs / MARKS_PER_WINDOW`, from the newest mark at or before the window's
  // start up to now, so at most 129 whatever the play speed.
  #marks: Mark[] = [];
  #windowMs = 0;
  #blank: null[] = [];

  get length(): number {
    return this.times.length;
  }

  /** The rule window the share is measured over, in virtual ms. */
  get windowMs(): number {
    return this.#windowMs;
  }

  /** `length` nulls: the line a chart draws when it has nothing to show. */
  get blank(): null[] {
    if (this.#blank.length !== this.times.length) {
      this.#blank = new Array<null>(this.times.length).fill(null);
    }
    return this.#blank;
  }

  /** The line of node `id`, or `undefined` once it has scrolled out. */
  column(id: number): (number | null)[] | undefined {
    return this.#columns.get(id)?.values;
  }

  /** Discard every sample and column and measure the share over `windowMs`.
   *  Called on a full rebuild (a new preset or Reset). A *membership* change
   *  does not reset: `push` reshapes in place, which is the whole point: the
   *  time axis survives a join or leave. */
  reset(windowMs: number): void {
    for (const column of [this.times, this.ticks, this.oracle, this.disagreement, this.share]) {
      column.length = 0;
    }
    this.zoneMin.length = 0;
    this.zoneMax.length = 0;
    this.zoneSpread.length = 0;
    this.#columns.clear();
    this.#marks = [];
    this.#windowMs = windowMs;
  }

  /** Append one sample from a fresh snapshot, deriving the disagreement, the
   *  per-zone spread and the share. Drops the oldest sample once the window is
   *  full so memory and redraw cost stay bounded. */
  push(snap: ClusterState): void {
    const sampleIdx = this.times.length;
    const zones = snap.zones.length;
    while (this.zoneMin.length < zones) {
      for (const columns of [this.zoneMin, this.zoneMax, this.zoneSpread]) {
        columns.push(new Array<number | null>(sampleIdx).fill(null));
      }
    }
    const min = new Array<number>(zones).fill(Number.POSITIVE_INFINITY);
    const max = new Array<number>(zones).fill(-1);
    let low = Number.POSITIVE_INFINITY;
    let high = -1;
    for (const node of snap.nodes) {
      const total = node.aggregate_total;
      min[node.zone] = Math.min(min[node.zone], total);
      max[node.zone] = Math.max(max[node.zone], total);
      low = Math.min(low, total);
      high = Math.max(high, total);
      let col = this.#columns.get(node.id);
      if (col === undefined) {
        // A newcomer: back-fill the samples before it joined with null, so its
        // line starts at this x rather than the window's left edge.
        col = { values: new Array<number | null>(sampleIdx).fill(null), samples: 0 };
        this.#columns.set(node.id, col);
      }
      col.values.push(total);
      col.samples += 1;
    }
    // Every column still at the old length is a departed node: extend it with a
    // null so its line gaps rather than mis-aligning with a later node's samples.
    for (const col of this.#columns.values()) {
      if (col.values.length === sampleIdx) col.values.push(null);
    }
    for (let z = 0; z < zones; z++) {
      const live = max[z] >= 0;
      this.zoneMin[z].push(live ? min[z] : null);
      this.zoneMax[z].push(live ? max[z] : null);
      this.zoneSpread[z].push(live ? max[z] - min[z] : null);
    }

    this.#commit(snap);
    this.times.push(snap.virtual_ms);
    this.ticks.push(snap.tick);
    this.oracle.push(snap.oracle_total);
    this.disagreement.push(high < 0 ? 0 : high - low);
    this.share.push(this.shareOf(snap));

    if (this.times.length > HISTORY_CAP) this.#dropOldest();
  }

  /** The round at which the cluster first agreed (disagreement hit zero on a
   *  real total), or `null` if it has not converged yet. The seeded burst lands
   *  at tick 0, so this tick *is* the round count. */
  convergedRound(): number | null {
    for (let i = 0; i < this.disagreement.length; i++) {
      if (this.disagreement[i] === 0 && this.oracle[i] > 0) return this.ticks[i];
    }
    return null;
  }

  /** The mark the window ending at `t` is measured from: the newest mark at or
   *  before `t − W`, or the session's start when the window reaches back past
   *  it. */
  windowStart(t: number): Mark {
    if (t <= this.#windowMs) return SESSION_START;
    for (let i = this.#marks.length - 1; i >= 0; i--) {
      if (this.#marks[i].t <= t - this.#windowMs) return this.#marks[i];
    }
    return SESSION_START;
  }

  /** σ: the share of the data bytes sent in the window ending at `snap` that
   *  crossed zones, or `null` when none were sent. */
  shareOf(snap: ClusterState): number | null {
    const start = this.windowStart(snap.virtual_ms);
    const cross = snap.bytes_by_tier.cross_zone.data - start.crossData;
    const total = cross + snap.bytes_by_tier.in_zone.data - start.inData;
    return total > 0 ? cross / total : null;
  }

  /** Keep `snap`'s cumulative bytes as a mark: a new one once `windowMs / 128`
   *  of virtual time has passed since the last, else replacing it; and drop the
   *  marks no window can start from any more. */
  #commit(snap: ClusterState): void {
    const mark: Mark = {
      t: snap.virtual_ms,
      inData: snap.bytes_by_tier.in_zone.data,
      crossData: snap.bytes_by_tier.cross_zone.data,
      nodes: snap.nodes,
    };
    const marks = this.#marks;
    const last = marks.length - 1;
    if (last >= 0 && mark.t - marks[last].t < this.#windowMs / MARKS_PER_WINDOW) marks[last] = mark;
    else marks.push(mark);
    // The second mark at or before `t − W` means the first can never be a start.
    while (marks.length > 1 && marks[1].t <= mark.t - this.#windowMs) marks.shift();
  }

  #dropOldest(): void {
    for (const column of [this.times, this.ticks, this.oracle, this.disagreement, this.share]) {
      column.shift();
    }
    for (const columns of [this.zoneMin, this.zoneMax, this.zoneSpread]) {
      for (const column of columns) column.shift();
    }
    for (const [id, col] of this.#columns) {
      if (col.values.shift() !== null) col.samples -= 1;
      // A departed node whose last real sample has now scrolled out leaves an
      // all-null column. Drop it so the series count returns to the live set.
      if (col.samples === 0) this.#columns.delete(id);
    }
  }
}
