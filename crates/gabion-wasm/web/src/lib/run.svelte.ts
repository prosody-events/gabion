// One run of one scenario: the engine and everything the page keeps about it
// (the selection, Hits per Send, the staged knobs, the chart history, the feed,
// the requests it admitted, its place in its story). Undo swaps a whole run
// back, so a scenario change, a Restart or a Rebuild is never a loss.

import { ChartHistory } from './charts/history';
import type { Cut } from './cuts';
import { START, type Progress } from './next';
import type { Knobs, Preset } from './presets';
import { RequestLog } from './requests';
import type { Sim } from './sim/sim';
import type { ClusterState, SimConfig } from './sim/types';

/** How long the previous run stays to come back to, wall ms. */
export const UNDO_MS = 30_000;

export class Run {
  readonly sim: Sim;
  readonly preset: Preset;
  readonly config: SimConfig;
  /** The knobs the engine was built from. */
  readonly applied: Knobs;
  /** The knobs the Tune controls stage for the next Rebuild. */
  knobs: Knobs = $state() as Knobs;
  /** The selected node: always a live one. */
  selected = $state(0);
  /** Hits per Send: the last value committed valid. */
  hits = $state(25);
  cluster: ClusterState = $state.raw() as ClusterState;
  cuts: readonly Cut[] = $state.raw([]);
  /** The faults the scenario's own seed made: Restart discards only the
   *  reader's. */
  readonly seededCuts: number;
  progress: Progress = $state.raw(START);
  /** Ticks with each chart sample: the one signal the dashboard redraws on. */
  chartVersion = $state(0);
  readonly history = new ChartHistory();
  readonly requests = new RequestLog();
  /** The feed's start, the instant the build was handed to the reader, and
   *  the hits it has injected since. */
  feedFrom: number;
  trafficInjected = 0;

  constructor(sim: Sim, preset: Preset, config: SimConfig, applied: Knobs, cluster: ClusterState, hits: number) {
    this.sim = sim;
    this.preset = preset;
    this.config = config;
    this.applied = applied;
    this.knobs = { ...applied };
    this.selected = preset.select;
    this.hits = hits;
    this.cluster = cluster;
    this.cuts = [...sim.cuts()];
    this.seededCuts = this.cuts.length;
    this.feedFrom = cluster.virtual_ms;
    this.history.reset(applied.rule_window_ms);
  }
}
