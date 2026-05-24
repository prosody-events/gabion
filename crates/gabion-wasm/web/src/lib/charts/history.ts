// The rolling time-series the dashboard charts read from. One sample per engine
// advance: the cluster's true total (the simulator-only oracle), every node's
// view of it, and their spread. Stored column-major — the exact shape uPlot's
// `setData` wants — so a redraw is a reference pass, not a transform.
//
// Deliberately a plain class, not Svelte `$state`: 600 samples × (N + 3)
// numbers under a deep reactive proxy would cost on every frame. The owner
// mutates the columns here and bumps a single `$state` counter to trigger the
// charts' redraw effect (see `App.svelte` / `Dashboard.svelte`).

import type { ClusterState } from '../sim/types';

/** Rolling-window depth. Doubles as the scrub-back history in Phase 6; at the
 *  visualizer's tick rate, 600 samples is comfortably past convergence. */
export const HISTORY_CAP = 600;

/** One node's fan line, keyed by its *stable id* (not rank): its view of the
 *  total at each retained sample, aligned to `times`. `null` before the node
 *  joined and after it left — uPlot draws those as a gap, so a newcomer's line
 *  begins mid-chart and a departed node's line ends there rather than the whole
 *  window restarting. `samples` counts the non-null entries so a column can be
 *  dropped once its last real value scrolls out of the window, with no scan. */
interface NodeColumn {
  values: (number | null)[];
  samples: number;
}

export class ChartHistory {
  /** x-axis: virtual milliseconds since the session began. */
  readonly times: number[] = [];
  /** Engine tick at each sample, so "converged in N rounds" can name the round. */
  readonly ticks: number[] = [];
  /** Ground-truth cluster total — the heavy dashed line every node chases. */
  readonly oracle: number[] = [];
  /** max − min of the per-node views: the disagreement that decays to zero. */
  readonly disagreement: number[] = [];
  /** Per-node fan columns keyed by stable id, insertion-ordered. Keying by id
   *  (not rank) is what lets a join or leave thread through the existing window
   *  instead of restarting it: a join appends a back-filled column, a leave
   *  stops feeding one (its line gaps), and the shared time axis never resets. */
  readonly #columns = new Map<number, NodeColumn>();

  get length(): number {
    return this.times.length;
  }

  /** The fan's per-node series count: live nodes plus any departed ones whose
   *  history is still inside the window. The dashboard sizes the fan chart from
   *  this (not the live node count), so the series and the columns always agree. */
  get nodeCount(): number {
    return this.#columns.size;
  }

  /** The fan columns in insertion order — the per-node `y` series uPlot wants,
   *  with `null` gaps where a node had not yet joined or had already left. */
  get nodes(): (number | null)[][] {
    const out: (number | null)[][] = [];
    for (const col of this.#columns.values()) out.push(col.values);
    return out;
  }

  /** Discard every sample and column. Called on a full rebuild (a new preset or
   *  Reset). A *membership* change does not reset — `push` reshapes in place,
   *  which is the whole point: the time axis survives a join or leave. */
  reset(): void {
    this.times.length = 0;
    this.ticks.length = 0;
    this.oracle.length = 0;
    this.disagreement.length = 0;
    this.#columns.clear();
  }

  /** Append one sample from a fresh snapshot, deriving the disagreement. Drops
   *  the oldest sample once the window is full so memory and redraw cost stay
   *  bounded. Reshapes the fan columns for the snapshot's live membership
   *  without touching the time-series backbone. */
  push(snap: ClusterState): void {
    const sampleIdx = this.times.length;

    let max = 0;
    let min = snap.nodes.length === 0 ? 0 : Number.POSITIVE_INFINITY;
    for (const node of snap.nodes) {
      const total = node.aggregate_total;
      if (total > max) max = total;
      if (total < min) min = total;
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
    // Every column still at the old length is a departed (or not-yet-rejoined)
    // node — extend it with a null so its line gaps rather than mis-aligning
    // with a later node's samples. (A live node grew above, so it is skipped.)
    for (const col of this.#columns.values()) {
      if (col.values.length === sampleIdx) col.values.push(null);
    }

    this.times.push(snap.virtual_ms);
    this.ticks.push(snap.tick);
    this.oracle.push(snap.oracle_total);
    this.disagreement.push(max - min);

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

  #dropOldest(): void {
    this.times.shift();
    this.ticks.shift();
    this.oracle.shift();
    this.disagreement.shift();
    for (const [id, col] of this.#columns) {
      if (col.values.shift() !== null) col.samples -= 1;
      // A departed node whose last real sample has now scrolled out leaves an
      // all-null column. Drop it so the fan's series count returns to the live
      // set instead of carrying dead lines for the rest of the session.
      if (col.samples === 0) this.#columns.delete(id);
    }
  }
}
