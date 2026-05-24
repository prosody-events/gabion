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

export class ChartHistory {
  /** x-axis: virtual milliseconds since the session began. */
  readonly times: number[] = [];
  /** Engine tick at each sample, so "converged in N rounds" can name the round. */
  readonly ticks: number[] = [];
  /** Ground-truth cluster total — the heavy dashed line every node chases. */
  readonly oracle: number[] = [];
  /** max − min of the per-node views: the disagreement that decays to zero. */
  readonly disagreement: number[] = [];
  /** One column per node: `nodes[rank]` is the view over time (the fan) of the
   *  node at that rank in the snapshot's live, insertion-ordered node list — a
   *  positional series, not keyed by stable id. The count is reshaped (and the
   *  window cleared) whenever membership changes, so a join or leave restarts
   *  the fan rather than threading a per-id line through the gap. */
  readonly nodes: number[][] = [];

  get length(): number {
    return this.times.length;
  }

  get nodeCount(): number {
    return this.nodes.length;
  }

  /** Discard every sample and re-shape for a cluster of `nodeCount` nodes.
   *  Called on reset and whenever the node count changes under us. */
  reset(nodeCount: number): void {
    this.times.length = 0;
    this.ticks.length = 0;
    this.oracle.length = 0;
    this.disagreement.length = 0;
    this.nodes.length = 0;
    for (let i = 0; i < nodeCount; i++) this.nodes.push([]);
  }

  /** Append one sample from a fresh snapshot, deriving the disagreement. Drops
   *  the oldest sample once the window is full so memory and redraw cost stay
   *  bounded. */
  push(snap: ClusterState): void {
    if (snap.nodes.length !== this.nodes.length) this.reset(snap.nodes.length);

    let max = 0;
    let min = snap.nodes.length === 0 ? 0 : Number.POSITIVE_INFINITY;
    // Index by rank (position in the live node list), not by stable id: after
    // a removal ids have gaps, but the reshaped `nodes` array is dense 0..N.
    snap.nodes.forEach((node, rank) => {
      const total = node.aggregate_total;
      if (total > max) max = total;
      if (total < min) min = total;
      this.nodes[rank].push(total);
    });

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
    for (const column of this.nodes) column.shift();
  }
}
