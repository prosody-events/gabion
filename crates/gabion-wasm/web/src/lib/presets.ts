// Scenario presets — the canned starting conditions the control rail offers.
//
// A preset is a config plus an `async seed(sim)` that fires its opening
// commands against a freshly built, not-yet-advanced cluster (partitions, seed
// bursts). It is deliberately *not* a timed script: every preset's initial
// state is "build the cluster, run these t=0 commands"; the partition→heal
// narrative is a user gesture during play (the rail's Heal button), not a
// scripted timeline. A timed-event driver only becomes load-bearing for the
// scrubber's replay-from-zero, so it waits for that phase.
//
// Most presets pin the rule limit high (`NARRATIVE_LIMIT`, 1 000 000) on
// purpose — well above the seeded volumes. A burst then stays well under
// gabion's threshold anti-entropy budget (ε ≈ limit·bps/(10⁴·N), ~833 at this
// limit with N=12), so it spreads by lazy heartbeat over several rounds: the
// multi-hop propagation these views exist to show. The viz-friendly default
// limit (1 000, see `SimConfig::default` in `config.rs`) gives ε ≈ 0.83, so
// every hit would eager-flush and a burst would flood at t=0 — which is why the
// narrative presets override it back up. The default limit's limit-crossing
// story is told instead by the `overload` and `sandbox` presets.
//
// A real rate limiter is never idle, and neither is the demo: every narrative
// preset carries a low, *uncapped* background `traffic` feed (a few hits/sec,
// spread round-robin). It keeps ≥1 bucket live at all times, so the gossip
// runtime never hits its empty-store guard and beams flow perpetually — instead
// of the cluster falling silent once the opening burst ages out of the window.
// The feed is a faint hum the preset's distinctive event (the burst spike, the
// partition, the isolation) layers on top of. Because the convergence oracle is
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
// regardless of how play chunked the steps, so it stays deterministic without
// the scrubber's driver.

import type { Sim } from './sim/sim';
import type { SimConfig } from './sim/types';

/** The single watched rule's key. Every preset seed and every interactive burst
 *  (stage click, rail Send) targets this key, so they all grow the same
 *  counter and the convergence story stays about one number. */
export const WATCHED_KEY = 1;

/** The cluster size the presets build. A ring of 12 reads cleanly and gabion's
 *  adaptive fanout still saturates it in a couple of rounds. */
const NODES = 12;

/** The rule limit the narrative presets pin (burst, steady, partition, loss,
 *  isolation). Far above their seeded volumes, so ε stays large and a burst
 *  spreads lazily by heartbeat — the convergence story. Overrides the much
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

/** The subset of `SimConfig` the rebuild sliders own. */
export interface Knobs {
  nodes: number;
  fanout: number;
  target_err_bps: number;
  uniform_loss: number;
  rule_limit: number;
  rule_window_ms: number;
  tick_interval_ms: number;
}

/** Seat the sliders from a preset: its pinned fields win, and every field it
 *  leaves open falls to the Rust-reported `defaults` (`Sim.defaultConfig()`), so
 *  a slider at rest builds the same cluster as an omitted field and the rest
 *  positions follow `gabion::defaults` with no hand-typed mirror. Only `nodes`
 *  falls to a local constant (`NODES`, the presets' ring size — a viz choice, not
 *  a Rust tunable). The bucket width is not a knob; it tracks `defaults.rule_bucket_ms`
 *  directly in `App.svelte` so the window-is-whole-buckets invariant holds.
 *  Called on every preset switch (e.g. Packet loss seats the loss slider at 0.3). */
export function knobsFromPreset(preset: Preset, defaults: SimConfig): Knobs {
  return {
    nodes: preset.config.nodes ?? NODES,
    fanout: preset.config.fanout ?? defaults.fanout,
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
 *  step chunking — deterministic, and it composes with scrub-from-zero. There is
 *  no cap: the windowed oracle (`engine.rs`) decays with expiry, so the
 *  aggregate plateaus at ≈ `rate · window_seconds` on its own. */
export interface SustainedTraffic {
  readonly rate_per_sec: number;
}

export interface Preset {
  readonly id: string;
  readonly label: string;
  readonly blurb: string;
  readonly config: Partial<SimConfig>;
  /** Whether the scenario severs links (a partition or isolation) — the rail
   *  surfaces its Heal control only for these, where it does something. */
  readonly usesNetwork?: boolean;
  /** A steady background feed the play loop drives. Present on every narrative
   *  preset (a faint hum that keeps gossip perpetual) and the overload preset
   *  (a flood); absent only on the user-driven `sandbox`. */
  readonly traffic?: SustainedTraffic;
  /** Surfaces the Aggregate-vs-Limit dashboard panel. Only legible when the
   *  scenario drives the aggregate toward a *low* limit (the overload preset),
   *  so it is decoupled from `traffic` — every narrative preset now has a feed,
   *  but against a 1 000 000 limit the panel would read as a flat line. */
  readonly showsLimitChart?: boolean;
  /** Fire the preset's opening commands at the current (t = 0) virtual time. */
  seed(sim: Sim): Promise<void>;
}

/** Even split of `0..NODES` into two halves, for the partition preset. */
function halves(): [number[], number[]] {
  const mid = Math.floor(NODES / 2);
  const all = Array.from({ length: NODES }, (_, i) => i);
  return [all.slice(0, mid), all.slice(mid)];
}

export const PRESETS: readonly Preset[] = [
  {
    id: 'burst',
    label: 'Traffic burst',
    blurb:
      'One node takes a 50-hit burst on top of a faint background hum. Play to watch the burst gossip out — and the cluster keep gossiping long after it ages out.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'steady',
    label: 'Steady state',
    blurb: 'Light traffic, scattered across the cluster. It converges at once — the calm baseline.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      for (const node of [0, 3, 6, 9]) {
        await sim.submitRequest(node, WATCHED_KEY, 10);
      }
    },
  },
  {
    id: 'overload',
    label: 'Sustained overload',
    blurb:
      'Steady traffic floods the cluster against a low limit. Play to watch the aggregate climb past it and stay in the rejecting band.',
    // A low limit so the aggregate crosses it within seconds; the default
    // budget then keeps ε well under 1, so eager flushing tracks the climb
    // tightly. The uncapped feed plateaus at ≈ rate·window ≈ 800 — twice the
    // limit — once expiry balances injection, so it sits in the band perpetually.
    config: { nodes: NODES, rng_seed: 1, rule_limit: 400 },
    traffic: { rate_per_sec: OVERLOAD_RATE_PER_SEC },
    showsLimitChart: true,
    async seed() {
      // No opening burst: the sustained feed (driven during play) is the story.
    },
  },
  {
    id: 'partition',
    label: 'Network partition',
    blurb:
      'The cluster splits in two; one half bursts. The severed half never hears the burst — heal the network to watch the halves reconcile.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    usesNetwork: true,
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      const [a, b] = halves();
      await sim.partition(a, b);
      await sim.submitRequest(a[0], WATCHED_KEY, 50);
    },
  },
  {
    id: 'loss',
    label: 'Packet loss',
    blurb: 'Every link drops 30% of packets. Convergence still arrives — just over more rounds.',
    config: { nodes: NODES, rng_seed: 1, uniform_loss: 0.3, rule_limit: NARRATIVE_LIMIT },
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'isolation',
    label: 'Node isolation & heal',
    blurb:
      'One node is cut off while a burst spreads. Heal it and it re-syncs by gossip catch-up — it kept its state, so no counts are lost.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    usesNetwork: true,
    traffic: { rate_per_sec: BACKGROUND_RATE_PER_SEC },
    async seed(sim) {
      const isolated = NODES - 1;
      const rest = Array.from({ length: NODES - 1 }, (_, i) => i);
      await sim.partition([isolated], rest);
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'sandbox',
    label: 'Sandbox',
    blurb:
      'A blank, quiet cluster — no background traffic. Click a node (or Send a burst), then Step to gossip the cell out one round at a time: watch it spread hop by hop, converge, and age back to quiet once the window slides past it. You drive every tick.',
    // No traffic feed and an empty seed: the cluster starts silent and the user
    // drives every hit by hand, advancing one gossip tick at a time with Step.
    // The narrative limit (not the low default) so the eager threshold flush
    // never fires — a single burst then spreads *only* on Step, one lazy
    // heartbeat round per tick, which is the whole point of this scenario. (The
    // limit-crossing / rejection story is the `overload` preset's job, where the
    // aggregate genuinely climbs past a low limit.)
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    async seed() {
      // Deliberately empty — the user injects the first hit.
    },
  },
];

/** The preset the page opens on. */
export const DEFAULT_PRESET = PRESETS[0];
