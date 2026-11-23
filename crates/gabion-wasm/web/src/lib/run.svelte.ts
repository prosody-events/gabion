// One run of one scenario: the engine and everything the page keeps about it
// (the selection, Requests per Send, the staged knobs, the chart history, the
// feed, the reader's Sends, the hits a node holds for its heartbeat, its
// place in its story). Undo swaps a whole run
// back, so a scenario change, a Restart or a Rebuild is never a loss.

import { ChartHistory } from './charts/history';
import type { Cut } from './cuts';
import type { Story } from './next';
import { ORIGIN, type Gateways, type Held, type Knobs, type Preset, type SustainedTraffic } from './presets';
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
  selected = $state() as number;
  /** Requests per Send: the last value committed valid. */
  requests = $state() as number;
  cluster: ClusterState = $state.raw() as ClusterState;
  cuts: readonly Cut[] = $state.raw([]);
  /** The faults the scenario's own seed made: Restart discards only the
   *  reader's. */
  readonly seededCuts: number;
  /** Where the run stands in its preset's story. */
  story: Story = $state.raw() as Story;
  /** Ticks with each chart sample: the one signal the charts redraw on. */
  chartVersion = $state(0);
  readonly history = new ChartHistory();
  /** The reader's Sends: node, bucket, admitted and rejected. */
  readonly sends = new RequestLog();
  /** The hits a node holds for its next heartbeat, whose sweep and countdown
   *  the page shows. */
  held: Held | null = $state.raw(null);
  /** The feed, once it has started (`SustainedTraffic.starts`), its start
   *  and the hits it has injected since. */
  traffic: SustainedTraffic | undefined = $state.raw();
  feedFrom = 0;
  trafficInjected = 0;
  /** Each built node's gateway into each other zone as the cluster warmed:
   *  the gateway a gateway story offers to break. */
  readonly gateways: Gateways;

  constructor(
    sim: Sim,
    preset: Preset,
    config: SimConfig,
    applied: Knobs,
    cluster: ClusterState,
    requests: number,
    held: Held | null,
    gateways: Gateways,
  ) {
    this.sim = sim;
    this.preset = preset;
    this.config = config;
    this.applied = applied;
    this.knobs = { ...applied };
    this.selected = ORIGIN;
    this.requests = requests;
    this.held = held;
    this.gateways = gateways;
    this.cluster = cluster;
    this.cuts = sim.cuts();
    this.seededCuts = this.cuts.length;
    this.story = preset.story;
    if (preset.traffic?.starts === 'load') this.startFeed();
    this.history.reset(applied.rule_window_ms);
  }

  /** Start the preset's feed now, unless it runs already. */
  startFeed(): void {
    if (this.traffic !== undefined || this.preset.traffic === undefined) return;
    this.traffic = this.preset.traffic;
    this.feedFrom = this.cluster.virtual_ms;
  }
}
