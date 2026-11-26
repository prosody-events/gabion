// Scenario presets: the canned starting conditions the control rail offers,
// in four groups: what gabion does (Rate limiting, Propagation, Sustained
// overload), what faults do to it (Zone partition, Packet loss, Gateway
// failure, Gateway cut), the study's measurement (Steady state, No zone
// configured) and Sandbox.
//
// A preset is a config, its feed, the charts its pane shows, the story its
// caption tells (`lib/next.ts`), the scenario its story hands over to, and an
// `async seed(sim, config)` that fires its opening commands (cuts,
// seed requests) against a freshly built cluster. It is not a timed script:
// the reader drives every step the caption names.
//
// Every preset opens on a warmed, settled cluster (`warm`): before its seed,
// the page steps whole heartbeat periods until every node has a gateway into
// every other zone and one period has passed with no packet, so nothing left
// over from the cold start is drawn when the reader first presses anything. The
// gateway map `warm` gathers is how the gateway stories name the node whose
// fault they offer: both open healthy, and the reader breaks the gateway.
//
// Rate limiting, Zone partition and Sustained overload set a low limit, so
// the vessels draw it and admission refuses past it; at limit 100 with twelve
// nodes gabion's error budget ε is 1, so every Send admitting two or more is
// flushed at once to the node's zone and one gateway in each other zone. The
// rest pin the limit far off (`NARRATIVE_LIMIT`, 1 000 000), where ε is in the
// hundreds, so a Send's hits wait for its node's next heartbeat: the
// propagation story. A seed reports the hits it leaves waiting at the selected
// node, so the page shows that node's heartbeat coming from the first frame.
//
// The feed exists only where traffic is the subject: Steady state, No zone
// configured and Sustained overload, plus Gateway cut's trickle at node 0
// from its fault on, which keeps node 0 sending data that a gateway cut off
// from its zone must answer with "cannot relay". A feed injects ⌊rate·T⌋
// one-hit requests by virtual time T, counted from the moment it starts (the
// build handed to the reader, or the fault), so it is deterministic under
// Step and repeatable from load.

import type { PresetId, Story } from './next';
import type { Member, Sim } from './sim/sim';
import { waves, type SimConfig, type SimEvent, type ZoneMode } from './sim/types';

/** The single watched rule's key. Every preset seed and every Send targets
 *  this key, so they all grow the same counter and the story stays about one
 *  number. */
export const WATCHED_KEY = 1;

/** The cluster size the presets build: three zones of four. */
const NODES = 12;

/** The zones the presets place the cluster in. */
const ZONES = 3;

/** Requests in the Send the seeded presets open with. */
const SEED_REQUESTS = 50;

/** The node every preset selects on load, and the node the seeded presets
 *  seed and read the gateways of. */
export const ORIGIN = 0;

/** The zone whose gateway the gateway stories break: zone b. */
const GATEWAY_ZONE = 1;

/** The limit the far-off presets pin: far above their volumes, so ε stays
 *  large and a request spreads by heartbeat. */
const NARRATIVE_LIMIT = 1_000_000;

/** Requests per Send every preset opens with, and the range the field
 *  accepts. Each request is one hit. */
export const BURST_REQUESTS = 25;
export const BURST_REQUESTS_MIN = 1;
export const BURST_REQUESTS_MAX = 1000;

/** The limit Rate limiting and Zone partition enforce: four default Sends
 *  reach it exactly. */
const RATE_LIMIT = 4 * BURST_REQUESTS;

/** The highest rule limit the Tune controls accept. */
export const RULE_LIMIT_MAX = 1_000_000;

/** Steady state's and No zone configured's feed (hits/sec, round-robin over
 *  the live nodes). The windowed oracle plateaus at roughly
 *  `rate · window_seconds` ≈ 30 hits. */
const BACKGROUND_RATE_PER_SEC = 3;

/** The `overload` preset's feed (hits/sec). It offers ≈ `rate · window` ≈ 800
 *  hits a window, twice the 400 limit; each node admits what expiry frees and
 *  rejects the rest. */
const OVERLOAD_RATE_PER_SEC = 80;

/** Gateway cut's trickle at node 0 (hits/sec), so node 0 keeps sending data
 *  its cut gateway answers with "cannot relay". */
const TRICKLE_RATE_PER_SEC = 2;

/** The most heartbeat periods `warm` and `settle` step: best effort, so a lossy
 *  or tiny cluster still opens. A healthy cluster needs a handful. */
const WARM_PERIODS_MAX = 40;

/** The subset of `SimConfig` the rebuild controls own. */
export interface Knobs {
  nodes: number;
  zones: number;
  zone_mode: ZoneMode;
  target_err_bps: number;
  uniform_loss: number;
  rule_limit: number;
  rule_window_ms: number;
  tick_interval_ms: number;
}

/** Seat the controls from a preset: its pinned fields win, and every field it
 *  leaves open falls to the Rust-reported `defaults` (`Sim.defaultConfig()`), so
 *  a control at rest builds the same cluster as an omitted field and the rest
 *  positions follow `gabion::defaults` with no hand-typed mirror. Only `nodes`
 *  falls to a local constant (`NODES`, the presets' cluster size: a viz choice,
 *  not a Rust tunable). The bucket width is not a knob; it tracks
 *  `defaults.rule_bucket_ms` directly in `App.svelte` so the
 *  window-is-whole-buckets invariant holds. Called on every preset switch (e.g.
 *  Packet loss seats the loss slider at 0.3). */
export function knobsFromPreset(preset: Preset, defaults: SimConfig): Knobs {
  return {
    nodes: preset.config.nodes ?? NODES,
    zones: preset.config.zones ?? defaults.zones,
    zone_mode: preset.config.zone_mode ?? defaults.zone_mode,
    target_err_bps: preset.config.target_err_bps ?? defaults.target_err_bps,
    uniform_loss: preset.config.uniform_loss ?? defaults.uniform_loss,
    rule_limit: preset.config.rule_limit ?? defaults.rule_limit,
    rule_window_ms: preset.config.rule_window_ms ?? defaults.rule_window_ms,
    tick_interval_ms: preset.config.tick_interval_ms ?? defaults.tick_interval_ms,
  };
}

/** A steady, uncapped feed of one-hit requests for the watched key: round-robin
 *  over the live nodes by global hit index (`'every-node'`), or all at one node
 *  (a node that has left takes none). ⌊rate·T⌋ hits are injected by virtual
 *  time T since the build was handed to the reader, so the feed is the same
 *  however play or Step chunks time. There is no cap: the windowed oracle
 *  (`engine.rs`) decays with expiry, so the aggregate plateaus at
 *  ≈ `rate · window_seconds` on its own. */
export interface SustainedTraffic {
  readonly rate_per_sec: number;
  readonly at: 'every-node' | { readonly node: number };
  /** When it starts: as the build is handed to the reader, or at the
   *  gateway story's fault, so the story's "before" is quiet. */
  readonly starts: 'load' | 'fault';
}

/** `gateways[node][zone]`: the gateway each built node's runtime reported into
 *  each other zone while the cluster warmed up. */
export type Gateways = ReadonlyArray<ReadonlyArray<number | undefined>>;

/** Hits admitted at `node` that wait for its next heartbeat. */
export interface Held {
  readonly node: number;
  readonly hits: number;
}

/** A chart the detail pane shows, by what it plots: the selected node's
 *  window by bucket against the limit (R1), the requests admitted and
 *  rejected (R2), who is behind and for how long (L), and the share of data
 *  bytes that crossed a zone (S). The pane draws them in this order. */
export type ChartId = 'window' | 'requests' | 'behind' | 'share';

export type Preset = {
  readonly id: PresetId;
  readonly label: string;
  /** The rail's group: what gabion does, faults, measurement, Sandbox. */
  readonly group: 0 | 1 | 2 | 3;
  readonly config: Partial<SimConfig>;
  /** The feed the play loop and Step drive, where traffic is the subject (the
   *  module header names them). */
  readonly traffic?: SustainedTraffic;
  /** The charts the detail pane shows; it draws them in `ChartId`'s order. */
  readonly charts: readonly ChartId[];
  /** The story the caption tells, as it opens, with the scenario it hands
   *  over to. */
  readonly story: Story;
  /** Fire the preset's opening commands on the warmed cluster, at the current
   *  virtual time, and report the hits they leave held for the selected
   *  node's heartbeat, or null. A preset with none opens as warmed. */
  seed?(sim: Sim, config: SimConfig): Promise<Held | null>;
};

/** The nodes a build places: node `i` in zone `i mod zones`. */
export function buildNodes(c: SimConfig): Member[] {
  return Array.from({ length: c.nodes }, (_, id) => ({ id, zone: id % c.zones }));
}

/** The node that seeds zone `z` in the spread-out presets: the zone's `z`-th
 *  member, so the seeds sit at different ranks around the stage (nodes 0, 4 and
 *  8 of twelve in three zones). */
function seedNode(c: SimConfig, z: number): number {
  const zone = buildNodes(c).filter((n) => n.zone === z);
  return zone[z]?.id ?? zone[0]?.id ?? z;
}

/** Step whole heartbeat periods, at most `WARM_PERIODS_MAX`, until a period
 *  that sent or lost no packet ends with `enough` true. `enough` sees every
 *  period's events. Returns at the cap all the same. */
async function quiet(sim: Sim, c: SimConfig, enough: (events: readonly SimEvent[]) => boolean): Promise<void> {
  for (let period = 0; period < WARM_PERIODS_MAX; period++) {
    const { events } = await sim.step(c.tick_interval_ms);
    const done = enough(events);
    if (done && !events.some((e) => e.kind.type === 'PacketSent' || e.kind.type === 'PacketDropped')) return;
  }
}

/** Warm a freshly built cluster: step until every built node has a gateway
 *  into every other non-empty zone (none with no zone configured) and the
 *  cluster has gone quiet, reading the gateways from the steps' own
 *  `GatewayChanged` events, and return them. */
export async function warm(sim: Sim, c: SimConfig): Promise<Gateways> {
  const gateways = Array.from({ length: c.nodes }, () => new Array<number | undefined>(c.zones).fill(undefined));
  const need = c.zone_mode === 'Configured' ? c.nodes * (Math.min(c.zones, c.nodes) - 1) : 0;
  let found = 0;
  await quiet(sim, c, (events) => {
    for (const { kind } of events) {
      if (kind.type !== 'GatewayChanged') continue;
      const row = gateways[kind.origin];
      if (row === undefined) continue;
      found += Number(kind.to !== undefined) - Number(row[kind.zone] !== undefined);
      row[kind.zone] = kind.to;
    }
    return found === need;
  });
  return gateways;
}

/** Step until one heartbeat period passes with no packet, so a seed has spread
 *  and been acknowledged before the first paint. */
const settle = (sim: Sim, c: SimConfig): Promise<void> => quiet(sim, c, () => true);

/** Send `requests` at `node`: the hits it holds for its next heartbeat when
 *  they wait for it, or null when they flushed at once. */
async function opening(sim: Sim, node: number, requests: number): Promise<Held | null> {
  const batch = await sim.submitRequest(node, WATCHED_KEY, requests);
  return waves(batch) === 0 ? { node, hits: requests } : null;
}

/** Steady state's and No zone configured's seed: ten requests at one node of
 *  each zone, the first at node 0, whose held hits it reports. */
async function spreadOut(sim: Sim, c: SimConfig): Promise<Held | null> {
  const held = await opening(sim, seedNode(c, 0), 10);
  for (let z = 1; z < c.zones; z++) await opening(sim, seedNode(c, z), 10);
  return held;
}

/** The scenarios, in the rail's order. */
export const PRESETS: readonly Preset[] = [
  {
    id: 'rate-limiting',
    label: 'Rate limiting',
    group: 0,
    config: { nodes: NODES, zones: ZONES, rule_limit: RATE_LIMIT },
    charts: ['window', 'requests'],
    story: { kind: 'rate-limiting', next: 'partition', act: 'open', zones: 0, sendBucket: null },
    // Opens settled at 0: the reader's Sends are the story.
  },
  {
    id: 'propagation',
    label: 'Propagation',
    group: 0,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    charts: ['behind'],
    story: { kind: 'propagation', next: 'rate-limiting', act: 'open' },
    async seed(sim, c) {
      await sim.submitRequest(ORIGIN, WATCHED_KEY, SEED_REQUESTS);
      await settle(sim, c);
      return null;
    },
  },
  {
    id: 'overload',
    label: 'Sustained overload',
    group: 0,
    // A low limit so the window fills within seconds; ε is then 1, so eager
    // flushing keeps every node's view close behind the truth. The feed offers
    // ≈ rate·window ≈ 800 hits a window, twice the limit, so once the window
    // fills each node admits what expiry frees and rejects the rest.
    config: { nodes: NODES, zones: ZONES, rule_limit: 400 },
    traffic: { rate_per_sec: OVERLOAD_RATE_PER_SEC, at: 'every-node', starts: 'load' },
    charts: ['window', 'requests', 'behind'],
    story: { kind: 'watch', watched: 'overload', next: 'rate-limiting' },
    // No opening request: the sustained feed (driven during play) is the story.
  },
  {
    id: 'partition',
    label: 'Zone partition',
    group: 1,
    config: { nodes: NODES, zones: ZONES, rule_limit: RATE_LIMIT },
    charts: ['window', 'requests', 'behind'],
    story: { kind: 'partition', next: 'gateway-failure', act: 'open' },
    async seed(sim, c) {
      await sim.cut({ kind: 'zone', zone: c.zones - 1 }, buildNodes(c));
      return null;
    },
  },
  {
    id: 'loss',
    label: 'Packet loss',
    group: 1,
    config: { nodes: NODES, zones: ZONES, uniform_loss: 0.3, rule_limit: NARRATIVE_LIMIT },
    charts: ['behind'],
    story: { kind: 'watch', watched: 'loss', next: 'rate-limiting' },
    seed: (sim) => opening(sim, ORIGIN, SEED_REQUESTS),
  },
  {
    id: 'gateway-failure',
    label: 'Gateway failure',
    group: 1,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    charts: ['behind'],
    story: { kind: 'gateway', fault: 'silenced', next: 'gateway-cut', origin: ORIGIN, zone: GATEWAY_ZONE, act: 'open' },
    // Opens healthy: the reader's Send, then the reader's fault.
  },
  {
    id: 'gateway-cut',
    label: 'Gateway cut',
    group: 1,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: TRICKLE_RATE_PER_SEC, at: { node: ORIGIN }, starts: 'fault' },
    charts: ['behind'],
    story: { kind: 'gateway', fault: 'unheard', next: 'sandbox', origin: ORIGIN, zone: GATEWAY_ZONE, act: 'open' },
  },
  {
    id: 'steady',
    label: 'Steady state',
    group: 2,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC, at: 'every-node', starts: 'load' },
    charts: ['behind', 'share'],
    story: { kind: 'watch', watched: 'steady', next: 'rate-limiting' },
    seed: spreadOut,
  },
  {
    id: 'no-zone',
    label: 'No zone configured',
    group: 2,
    config: { nodes: NODES, zones: ZONES, zone_mode: 'Unset', rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC, at: 'every-node', starts: 'load' },
    charts: ['behind', 'share'],
    story: { kind: 'watch', watched: 'no-zone', next: 'rate-limiting' },
    seed: spreadOut,
  },
  {
    id: 'sandbox',
    label: 'Sandbox',
    group: 3,
    // No feed and an empty seed: the cluster starts silent and the reader
    // drives every hit. The far-off limit, so a request spreads at its node's
    // next heartbeat, one event at a time under Step.
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    charts: ['requests', 'behind'],
    story: { kind: 'sandbox' },
    // No seed: the reader sends the first hit.
  },
];

/** The preset the page opens on: the product's job, first. */
export const DEFAULT_PRESET = PRESETS[0];

/** The preset with id `id`. */
export const presetById = (id: PresetId): Preset => PRESETS.find((p) => p.id === id) ?? DEFAULT_PRESET;
