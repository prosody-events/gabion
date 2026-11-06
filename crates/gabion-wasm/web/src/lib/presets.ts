// Scenario presets — the canned starting conditions the control rail offers.
//
// A preset is a config, a blurb, a `pacing`, an optional selected node, an
// optional feed and an `async seed(sim, config, gateways)` that fires its
// opening commands (partitions, seed requests) against a freshly built
// cluster. It is deliberately *not* a timed script: the partition→heal
// narrative is a user gesture during play (the rail's Heal button).
//
// Every preset opens on a warmed, settled cluster (`warm`): before its seed,
// the page steps whole heartbeat periods until every node has a gateway into
// every other zone and one period has passed with no packet, so nothing left
// over from the cold start flies when the reader first presses anything. The
// gateway map `warm` gathers is how the gateway presets name the node they
// fault.
//
// Most presets pin the rule limit high (`NARRATIVE_LIMIT`, 1 000 000) on
// purpose — well above the seeded volumes. A request then stays well under
// gabion's threshold anti-entropy budget (ε ≈ limit·bps/(10⁴·N), ~833 at this
// limit with N=12), so it waits for its node's next heartbeat, which sends it
// to the node's zone and one gateway in each other zone. The `overload` preset
// tells the limit-crossing story instead.
//
// The feed exists only where traffic is the subject: Steady state, No zone
// configured and Sustained overload, plus Gateway cut's trickle at node 0,
// which keeps node 0 sending data that a gateway cut off from its zone must
// answer with "cannot relay". The one-Send presets (Propagation, Zone
// partition, Packet loss, Gateway failure, Sandbox) have none, so their hits
// leave the window visibly. A feed injects ⌊rate·T⌋ one-hit requests by
// virtual time T, counted from the moment the build is handed to the reader,
// so it is deterministic under Step and replayable from load.
//
// The `overload` preset tells the opposite — and the reason gabion exists —
// story. A single request that approaches the limit can't: to keep each node's
// local delta under ε while twelve nodes' sum exceeds the limit you'd need
// L/N < ε = L·bps/(10⁴·N), i.e. bps > 10⁴, an absurd error budget. So overload
// feeds the cluster fast against a low limit: it offers ≈ `rate · window` ≈ 800
// hits a window, twice the 400 limit; each node admits what expiry frees and
// rejects the rest. There, eager threshold flushing is the healthy behaviour
// on display, and `showsLimitChart` surfaces the Aggregate-vs-Limit panel. It
// alone is `pacing: 'live'`: its feed submits many requests per frame.

import { blurb, noun, type Blurb } from './format';
import type { Sim } from './sim/sim';
import type { SimConfig, SimEvent, ZoneMode } from './sim/types';

/** The single watched rule's key. Every preset seed and every interactive Send
 *  (stage click, rail Send) targets this key, so they all grow the same
 *  counter and the convergence story stays about one number. */
export const WATCHED_KEY = 1;

/** The cluster size the presets build: three zones of four. */
const NODES = 12;

/** The zones the presets place the cluster in. */
const ZONES = 3;

/** Hits in the request the one-Send presets seed. */
const BURST = 50;

/** The node the one-Send presets seed, select on load and read the gateways of. */
const ORIGIN = 0;

/** The pods in the production peak the Steady blurb compares with. */
const PROD_PEAK = 183;

/** The rule limit every preset but Sustained overload pins. Far above their
 *  seeded volumes, so ε stays large and a request spreads by heartbeat: the
 *  propagation story. Overrides the much lower viz-friendly default
 *  (`SimConfig::default`), which exists for the limit-crossing story the
 *  `overload` preset tells. */
const NARRATIVE_LIMIT = 1_000_000;

/** Steady state's and No zone configured's feed (hits/sec, round-robin over
 *  the live nodes). The windowed oracle plateaus at roughly
 *  `rate · window_seconds` ≈ 30 hits. */
const BACKGROUND_RATE_PER_SEC = 3;

/** The `overload` preset's feed (hits/sec). It offers ≈ `rate · window` ≈ 800
 *  hits a window, twice the 400 limit; each node admits what expiry frees and
 *  rejects the rest. */
const OVERLOAD_RATE_PER_SEC = 80;

/** Gateway cut's trickle at node 0 (hits/sec): one hit per 500 ms period, so
 *  node 0 keeps sending data its cut gateway answers with "cannot relay". */
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

/** How the page meets a wave of data packets. `'held'`: it does not step the
 *  engine while the wave flies, and each count changes when its packet lands.
 *  `'live'`: it steps by time and shows every snapshot at once (Sustained
 *  overload, whose feed submits many requests per frame). */
export type Pacing = 'held' | 'live';

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
}

/** `gateways[node][zone]`: the gateway each built node's runtime reported into
 *  each other zone while the cluster warmed up. */
export type Gateways = ReadonlyArray<ReadonlyArray<number | undefined>>;

export interface Preset {
  readonly id: string;
  readonly label: string;
  /** What the scenario shows, built from the config the cluster is built with. */
  blurb(config: SimConfig): Blurb;
  readonly config: Partial<SimConfig>;
  readonly pacing: Pacing;
  /** The node the scenario opens with selected, so its gateways show on load. */
  readonly select?: number;
  /** A feed the play loop and Step drive: Steady state, No zone configured and
   *  Sustained overload, whose subject is traffic, and Gateway cut's trickle. */
  readonly traffic?: SustainedTraffic;
  /** Surfaces the Aggregate-vs-Limit dashboard panel. Only legible when the
   *  scenario drives the aggregate toward a *low* limit (the overload preset):
   *  against a 1 000 000 limit the panel would read as a flat line. */
  readonly showsLimitChart?: boolean;
  /** Fire the preset's opening commands on the warmed cluster, at the current
   *  virtual time. */
  seed(sim: Sim, config: SimConfig, gateways: Gateways): Promise<void>;
}

/** The nodes the build places in zone `z`: node `i` is in zone `i mod zones`. */
function members(c: SimConfig, z: number): number[] {
  return Array.from({ length: c.nodes }, (_, i) => i).filter((i) => i % c.zones === z);
}

/** How many of an origin's N − 1 data packets cross a zone with no zone set,
 *  averaged over the build's origins: N − n_z for an origin in a zone of n_z,
 *  so N − Σ n_z² / N, the unweighted mean chart 1's no-zone reference starts
 *  from. Two decimals at most. */
function acrossNoZone(c: SimConfig): number {
  let squares = 0;
  for (let z = 0; z < c.zones; z++) squares += members(c, z).length ** 2;
  return Math.round((c.nodes - squares / c.nodes) * 100) / 100;
}

/** Every build node not in `group`. */
function except(c: SimConfig, group: readonly number[]): number[] {
  return Array.from({ length: c.nodes }, (_, i) => i).filter((i) => !group.includes(i));
}

/** The node that seeds zone `z` in the spread-out presets: the zone's `z`-th
 *  member, so the seeds sit at different ranks around the stage (nodes 0, 4 and
 *  8 of twelve in three zones). */
function seedNode(c: SimConfig, z: number): number {
  const zone = members(c, z);
  return zone[z] ?? zone[0] ?? z;
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

/** Throw: node 0 never found a gateway into zone `zone`. */
function noGateway(zone: number): never {
  throw new Error(
    `node ${ORIGIN} found no gateway into zone ${String.fromCharCode(97 + zone)} while the cluster warmed up, ` +
      `so the gateway scenario cannot start. A zone needs at least one live node node ${ORIGIN} can hear; ` +
      'rebuild with more nodes or fewer zones.',
  );
}

export const PRESETS: readonly Preset[] = [
  {
    id: 'propagation',
    label: 'Propagation',
    blurb: (c) =>
      blurb`${c.zones} ${noun(c.zones, 'zone')}, far below the limit: a request waits for its node's next heartbeat, which sends it to the node's own zone and to one gateway in each other zone, and the gateways relay it one relay floor (${c.min_emit_interval_ms} ms) later.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    select: ORIGIN,
    async seed(sim, c) {
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
      await settle(sim, c);
    },
  },
  {
    id: 'steady',
    label: 'Steady state',
    blurb: (c) =>
      blurb`Light traffic across zones. Each change reaches every other node once, and ${c.zones - 1} of those ${c.nodes - 1} data packets cross a zone, so the data-byte share settles on its expected-with-zones line: ${c.zones - 1} of ${c.nodes - 1} here, and ${c.zones - 1} of ${PROD_PEAK - 1} at ${PROD_PEAK} nodes, which is why the saving grows with cluster size.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC, at: 'every-node' },
    async seed(sim, c) {
      for (let z = 0; z < c.zones; z++) await sim.submitRequest(seedNode(c, z), WATCHED_KEY, 10);
    },
  },
  {
    id: 'overload',
    label: 'Sustained overload',
    blurb: () =>
      blurb`Steady traffic floods the cluster against a low limit. Play to watch each node admit hits up to the limit and reject the rest.`,
    // A low limit so the window fills within seconds; ε is then 1, so eager
    // flushing keeps every node's view close behind the truth. The feed offers
    // ≈ rate·window ≈ 800 hits a window, twice the limit, so once the window
    // fills each node admits what expiry frees and rejects the rest.
    config: { nodes: NODES, zones: ZONES, rule_limit: 400 },
    pacing: 'live',
    traffic: { rate_per_sec: OVERLOAD_RATE_PER_SEC, at: 'every-node' },
    showsLimitChart: true,
    async seed() {
      // No opening request: the sustained feed (driven during play) is the story.
    },
  },
  {
    id: 'partition',
    label: 'Zone partition',
    blurb: () =>
      blurb`The last zone is cut off from the rest and ${BURST} hits land on each side. Nodes cannot hear across the cut until you heal the network; the disagreement of every zone then returns to zero.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    async seed(sim, c) {
      const cut = members(c, c.zones - 1);
      await sim.cutZone(cut, except(c, cut));
      await sim.submitRequest(0, WATCHED_KEY, BURST);
      await sim.submitRequest(cut[0], WATCHED_KEY, BURST);
    },
  },
  {
    id: 'loss',
    label: 'Packet loss',
    blurb: (c) =>
      blurb`Every link drops ${Math.round(c.uniform_loss * 100)}% of packets. Convergence still arrives, just over more rounds.`,
    config: { nodes: NODES, zones: ZONES, uniform_loss: 0.3, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    async seed(sim) {
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'gateway-failure',
    label: 'Gateway failure',
    blurb: () =>
      blurb`The origin's gateway into the second zone is silenced without telling anyone, then a request lands at the origin. After the suspect timeout the origin picks another gateway, and the zone catches up.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    select: ORIGIN,
    async seed(sim, c, gateways) {
      const g = gateways[ORIGIN]?.[1] ?? noGateway(1);
      await sim.silence(g, except(c, [g]));
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'gateway-cut',
    label: 'Gateway cut from its zone',
    blurb: () =>
      blurb`The origin's gateway into the second zone can hear its zone but not be heard by it. The origin keeps sending, ${TRICKLE_RATE_PER_SEC} hits a second, so the gateway says it cannot relay, and the origin moves to another gateway.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    select: ORIGIN,
    traffic: { rate_per_sec: TRICKLE_RATE_PER_SEC, at: { node: ORIGIN } },
    async seed(sim, c, gateways) {
      const g = gateways[ORIGIN]?.[1] ?? noGateway(1);
      await sim.cutFromZone(g, members(c, 1).filter((i) => i !== g));
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'no-zone',
    label: 'No zone configured',
    blurb: (c) =>
      blurb`Nodes are placed in ${c.zones} zones but told nothing, so every node sends to every peer directly: on average ${acrossNoZone(c)} of ${c.nodes - 1} data packets cross a zone.`,
    config: { nodes: NODES, zones: ZONES, zone_mode: 'Unset', rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC, at: 'every-node' },
    async seed(sim, c) {
      for (let z = 0; z < c.zones; z++) await sim.submitRequest(seedNode(c, z), WATCHED_KEY, 10);
    },
  },
  {
    id: 'sandbox',
    label: 'Sandbox',
    blurb: () =>
      blurb`A blank, quiet cluster with no background traffic. Click a node, send it a request, then step to watch the change spread one event at a time. You drive every step.`,
    // No feed and an empty seed: the cluster starts silent and the user drives
    // every hit by hand, advancing one event at a time with Step. The narrative
    // limit (not the low default) so the eager threshold flush never fires: a
    // request then spreads only at its node's next heartbeat, which is the
    // whole point of this scenario.
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    pacing: 'held',
    async seed() {
      // Deliberately empty: the user injects the first hit.
    },
  },
];

/** The preset the page opens on. */
export const DEFAULT_PRESET = PRESETS[0];
