// Scenario presets — the canned starting conditions the control rail offers.
//
// A preset is a config plus an `async seed(sim, config)` that fires its opening
// commands against a freshly built cluster (partitions, seed bursts); the
// gateway presets first step heartbeats until the origin has found the gateway
// they cut. It is deliberately *not* a timed script: the partition→heal
// narrative is a user gesture during play (the rail's Heal button).
//
// Most presets pin the rule limit high (`NARRATIVE_LIMIT`, 1 000 000) on
// purpose — well above the seeded volumes. A burst then stays well under
// gabion's threshold anti-entropy budget (ε ≈ limit·bps/(10⁴·N), ~833 at this
// limit with N=12), so it waits for the next heartbeat, which sends it to the
// origin's zone and one gateway in each other zone. The viz-friendly default
// limit (1 000, see `SimConfig::default` in `config.rs`) gives ε ≈ 0.83, so
// every hit would eager-flush and a burst would flood at t=0 — which is why the
// narrative presets override it back up. The default limit's limit-crossing
// story is told instead by the `overload` and `sandbox` presets.
//
// A real rate limiter is never idle, and neither is the demo: every narrative
// preset carries a low, *uncapped* background `traffic` feed (a few hits/sec,
// spread round-robin). It keeps ≥1 bucket live at all times, so new counts
// keep arriving and beams flow perpetually — instead of the cluster falling
// silent once the opening burst ages out of the window.
// The feed is a faint hum the preset's distinctive event (the burst spike, the
// partition) layers on top of. Because the convergence oracle is
// now windowed (sum of live local cells, `engine.rs`), the feed plateaus at
// `rate · window` rather than climbing forever.
//
// The `overload` preset tells the opposite — and the reason gabion exists —
// story. A single burst that approaches the limit can't: to keep each node's
// local delta under ε while twelve nodes' sum exceeds the limit you'd need
// L/N < ε = L·bps/(10⁴·N), i.e. bps > 10⁴, an absurd error budget. So overload
// feeds the cluster fast against a low limit, so the windowed aggregate plateaus
// well inside the REJECTING band (≈ `rate · window` ≈ 2× the limit). There,
// eager threshold flushing is the healthy behavior on display — every node's
// view tracks the aggregate tightly — and `showsLimitChart` surfaces the
// Aggregate-vs-Limit panel. The `sandbox` preset is the complement: *no* feed at
// all, a blank cluster the user drives by hand (inject + Step) to watch one cell
// gossip out, converge, and age back to quiet.
//
// Every feed is folded into the play loop as a carry-save rate (App.svelte), not
// a timed script: the total injected by virtual time T is exactly ⌊rate·T⌋
// regardless of how play chunked the steps, so it stays deterministic.

import { blurb, noun, type Blurb } from './format';
import type { Sim } from './sim/sim';
import type { SimConfig, ZoneMode } from './sim/types';

/** The single watched rule's key. Every preset seed and every interactive burst
 *  (stage click, rail Send) targets this key, so they all grow the same
 *  counter and the convergence story stays about one number. */
export const WATCHED_KEY = 1;

/** The cluster size the presets build: three zones of four. */
const NODES = 12;

/** The zones the presets place the cluster in. */
const ZONES = 3;

/** Hits in the burst the presets seed. */
const BURST = 50;

/** The node the burst presets seed, select on load and read the gateways of. */
const ORIGIN = 0;

/** The pods in the production peak the Steady blurb compares with. */
const PROD_PEAK = 183;

/** The rule limit the narrative presets pin (burst, steady, partition, loss,
 *  gateway failure). Far above their seeded volumes, so ε stays large and a
 *  burst spreads lazily by heartbeat: the convergence story. Overrides the much
 *  lower viz-friendly default (`SimConfig::default`), which exists for the
 *  limit-crossing story the `overload` preset and the no-preset path tell. */
const NARRATIVE_LIMIT = 1_000_000;

/** The faint background feed every narrative preset carries (hits/sec, spread
 *  round-robin). Low enough that the preset's headline event still dominates,
 *  but enough to keep the window populated so gossip never falls silent. The
 *  windowed oracle plateaus at roughly `rate · window_seconds` ≈ 30 hits. */
const BACKGROUND_RATE_PER_SEC = 3;

/** The `overload` preset's feed (hits/sec). Uncapped against the low overload
 *  limit, so the windowed aggregate plateaus at ≈ `rate · window_seconds` ≈ 800
 *  — twice the 400 limit, well inside the REJECTING band, and perpetually so. */
const OVERLOAD_RATE_PER_SEC = 80;

/** The most heartbeats the gateway presets wait for the origin to find its
 *  gateway into the remote zone. A cluster finds it on the first or second. */
const WARM_TICKS_MAX = 10;

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

/** A steady, uncapped traffic feed the play loop drives while a preset is active
 *  — the background hum that keeps the cluster alive (and the engine of the
 *  overload story). The hits are spread round-robin across the cluster (by
 *  global hit index, so the spread is chunk-independent), targeting the watched
 *  key. App.svelte folds `rate_per_sec` into each advance as a carry-save
 *  accumulator, so ⌊rate·T⌋ hits have landed by virtual time T regardless of
 *  step chunking — deterministic. There is no cap: the windowed oracle
 *  (`engine.rs`) decays with expiry, so the aggregate plateaus at
 *  ≈ `rate · window_seconds` on its own. */
export interface SustainedTraffic {
  readonly rate_per_sec: number;
}

export interface Preset {
  readonly id: string;
  readonly label: string;
  /** What the scenario shows, built from the config the cluster is built with. */
  blurb(config: SimConfig): Blurb;
  readonly config: Partial<SimConfig>;
  /** The node the scenario opens with selected, so its gateways show on load. */
  readonly select?: number;
  /** A steady background feed the play loop drives. Present on the narrative
   *  presets (a faint hum that keeps gossip perpetual) and the overload preset
   *  (a flood); absent on the user-driven `sandbox` and the gateway presets,
   *  whose catch-up is an exact equality. */
  readonly traffic?: SustainedTraffic;
  /** Surfaces the Aggregate-vs-Limit dashboard panel. Only legible when the
   *  scenario drives the aggregate toward a *low* limit (the overload preset),
   *  so it is decoupled from `traffic`: every narrative preset has a feed, but
   *  against a 1 000 000 limit the panel would read as a flat line. */
  readonly showsLimitChart?: boolean;
  /** Fire the preset's opening commands at the current virtual time. */
  seed(sim: Sim, config: SimConfig): Promise<void>;
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

/** Step one heartbeat at a time until `origin` has a gateway into `zone`, and
 *  return it. The gateway scenarios cut that node, so the origin must have
 *  picked it first. */
async function warmUntilGateway(
  sim: Sim,
  origin: number,
  zone: number,
  tickMs: number,
): Promise<number> {
  for (let tick = 0; tick < WARM_TICKS_MAX; tick++) {
    await sim.step(tickMs);
    const peers = (await sim.snapshot(origin)).inspected?.peers ?? [];
    const gateway = peers.find((p) => p.role === 'gateway' && p.zone === zone)?.id;
    if (gateway !== undefined) return gateway;
  }
  throw new Error(
    `the origin found no gateway into zone ${String.fromCharCode(97 + zone)} within ${WARM_TICKS_MAX} heartbeats, ` +
      'so the gateway scenario cannot start; rebuild with more nodes or zones',
  );
}

export const PRESETS: readonly Preset[] = [
  {
    id: 'burst',
    label: 'Traffic burst',
    blurb: (c) =>
      blurb`${c.zones} ${noun(c.zones, 'zone')}, a ${BURST}-hit burst at node ${ORIGIN}: one heartbeat reaches its zone and one gateway in each other zone, and the relays follow one relay floor (${c.min_emit_interval_ms} ms) later.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    select: ORIGIN,
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'steady',
    label: 'Steady state',
    blurb: (c) =>
      blurb`Light traffic across zones. Each change reaches every other node once, and ${c.zones - 1} of those ${c.nodes - 1} data packets cross a zone, so the data-byte share settles on its expected-with-zones line: ${c.zones - 1} of ${c.nodes - 1} here, and ${c.zones - 1} of ${PROD_PEAK - 1} at ${PROD_PEAK} nodes, which is why the saving grows with cluster size.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim, c) {
      for (let z = 0; z < c.zones; z++) await sim.submitRequest(seedNode(c, z), WATCHED_KEY, 10);
    },
  },
  {
    id: 'overload',
    label: 'Sustained overload',
    blurb: () =>
      blurb`Steady traffic floods the cluster against a low limit. Play to watch the aggregate climb past it and stay in the rejecting band.`,
    // A low limit so the aggregate crosses it within seconds; the default
    // budget then keeps ε well under 1, so eager flushing tracks the climb
    // tightly. The uncapped feed plateaus at ≈ rate·window ≈ 800 — twice the
    // limit — once expiry balances injection, so it sits in the band perpetually.
    config: { nodes: NODES, zones: ZONES, rule_limit: 400 },
    traffic: { rate_per_sec: OVERLOAD_RATE_PER_SEC },
    showsLimitChart: true,
    async seed() {
      // No opening burst: the sustained feed (driven during play) is the story.
    },
  },
  {
    id: 'partition',
    label: 'Zone partition',
    blurb: () =>
      blurb`The last zone is cut off from the rest and ${BURST} hits land on each side. Nodes cannot hear across the cut until you heal the network; the disagreement of every zone then returns to zero.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
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
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'gateway-failure',
    label: 'Gateway failure',
    blurb: () =>
      blurb`The origin's gateway into the second zone is silenced without telling anyone, then a burst lands at the origin. After the suspect timeout the origin picks another gateway, and the zone catches up.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    select: ORIGIN,
    async seed(sim, c) {
      const g = await warmUntilGateway(sim, ORIGIN, 1, c.tick_interval_ms);
      await sim.silence(g, except(c, [g]));
      await sim.submitRequest(ORIGIN, WATCHED_KEY, BURST);
    },
  },
  {
    id: 'gateway-cut',
    label: 'Gateway cut from its zone',
    blurb: () =>
      blurb`The origin's gateway into the second zone can hear its zone but not be heard by it. The gateway says it cannot relay, and the origin moves to another gateway.`,
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    select: ORIGIN,
    async seed(sim, c) {
      const g = await warmUntilGateway(sim, ORIGIN, 1, c.tick_interval_ms);
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
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim, c) {
      for (let z = 0; z < c.zones; z++) await sim.submitRequest(seedNode(c, z), WATCHED_KEY, 10);
    },
  },
  {
    id: 'sandbox',
    label: 'Sandbox',
    blurb: () =>
      blurb`A blank, quiet cluster with no background traffic. Click a node (or send a burst), then step to gossip the change out one round at a time. You drive every tick.`,
    // No traffic feed and an empty seed: the cluster starts silent and the user
    // drives every hit by hand, advancing one gossip tick at a time with Step.
    // The narrative limit (not the low default) so the eager threshold flush
    // never fires: a single burst then spreads only on Step, at the next
    // heartbeat, which is the whole point of this scenario.
    config: { nodes: NODES, zones: ZONES, rule_limit: NARRATIVE_LIMIT },
    async seed() {
      // Deliberately empty: the user injects the first hit.
    },
  },
];

/** The preset the page opens on. */
export const DEFAULT_PRESET = PRESETS[0];
