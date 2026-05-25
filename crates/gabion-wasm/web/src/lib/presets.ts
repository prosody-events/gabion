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
// story is told instead by the no-preset "Tune the cluster" path.
//
// The exception is the `overload` preset, which tells the opposite — and the
// reason gabion exists — story. A single burst that approaches the limit can't:
// to keep each node's local delta under ε while twelve nodes' sum exceeds the
// limit you'd need L/N < ε = L·bps/(10⁴·N), i.e. bps > 10⁴, an absurd error
// budget. So overload *feeds* the cluster at a steady rate (see `traffic`),
// dropping the limit low enough that the cluster aggregate climbs across it
// within seconds. There, eager threshold flushing is the healthy behavior on
// display — every node's view tracks the climbing aggregate tightly — and the
// Aggregate-vs-Limit chart's REJECTING band fills. The sustained feed is folded
// into the play loop as a carry-save rate (App.svelte), not a timed script: the
// total injected by virtual time T is exactly ⌊rate·T⌋ regardless of how play
// chunked the steps, so it stays deterministic without the scrubber's driver.

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

/** The rebuild knobs the control rail exposes, and the positions they take when
 *  a preset doesn't pin the field. These mirror `gabion::defaults` /
 *  `SimConfig::default` — the values the Rust side applies to an omitted field —
 *  so a slider at its default produces the same cluster as no slider at all. (A
 *  hand-mirror, like `sim/types.ts`; if the Rust defaults move, move these.) */
export const KNOB_DEFAULTS = {
  nodes: NODES,
  fanout: 6,
  target_err_bps: 100,
  uniform_loss: 0,
  // The rule + gossip knobs mirror `SimConfig::default` (`config.rs`): a
  // viz-friendly 1 000 limit / 10 s window, and the production 100 ms gossip
  // tick. A narrative preset seats `rule_limit` at `NARRATIVE_LIMIT` instead
  // (via `knobsFromPreset`); none pins window or tick, so those stay here.
  rule_limit: 1_000,
  rule_window_ms: 10_000,
  tick_interval_ms: 100,
} as const;

/** The bucket width within the window, fixed (not a knob) so the
 *  window-is-a-whole-number-of-buckets invariant `SimConfig::validate` enforces
 *  holds by construction for every window the slider can pick. Mirrors
 *  `SimConfig::default`'s `rule_bucket_ms`. */
export const RULE_BUCKET_MS = 1_000;

/** The subset of `SimConfig` the rebuild sliders own. */
export type Knobs = { [K in keyof typeof KNOB_DEFAULTS]: number };

/** Seat the sliders from a preset: its pinned fields win, the rest fall to the
 *  defaults above. Called on every preset switch so the knobs mirror the active
 *  scenario (e.g. Packet loss seats the loss slider at 0.3). */
export function knobsFromPreset(preset: Preset): Knobs {
  return {
    nodes: preset.config.nodes ?? KNOB_DEFAULTS.nodes,
    fanout: preset.config.fanout ?? KNOB_DEFAULTS.fanout,
    target_err_bps: preset.config.target_err_bps ?? KNOB_DEFAULTS.target_err_bps,
    uniform_loss: preset.config.uniform_loss ?? KNOB_DEFAULTS.uniform_loss,
    rule_limit: preset.config.rule_limit ?? KNOB_DEFAULTS.rule_limit,
    rule_window_ms: preset.config.rule_window_ms ?? KNOB_DEFAULTS.rule_window_ms,
    tick_interval_ms: preset.config.tick_interval_ms ?? KNOB_DEFAULTS.tick_interval_ms,
  };
}

/** A steady traffic feed the play loop drives while a preset is active —
 *  the engine of the overload story. The hits are spread round-robin across the
 *  cluster (by global hit index, so the spread is chunk-independent), targeting
 *  the watched key, until `cap` total have been injected; then the aggregate
 *  plateaus. App.svelte folds `rate_per_sec` into each advance as a carry-save
 *  accumulator, so ⌊rate·T⌋ hits have landed by virtual time T regardless of
 *  step chunking — deterministic, and it composes with scrub-from-zero. */
export interface SustainedTraffic {
  readonly rate_per_sec: number;
  readonly cap: number;
}

export interface Preset {
  readonly id: string;
  readonly label: string;
  readonly blurb: string;
  readonly config: Partial<SimConfig>;
  /** Whether the scenario severs links (a partition or isolation) — the rail
   *  surfaces its Heal control only for these, where it does something. */
  readonly usesNetwork?: boolean;
  /** A steady feed the play loop drives (the overload scenario). Its presence
   *  also surfaces the Aggregate-vs-Limit chart, which is only legible when the
   *  aggregate actually approaches the (low) limit this preset sets. */
  readonly traffic?: SustainedTraffic;
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
    blurb: 'One node takes a 50-hit burst. Play to watch it gossip out until every node agrees.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    async seed(sim) {
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'steady',
    label: 'Steady state',
    blurb: 'Light traffic, scattered across the cluster. It converges at once — the calm baseline.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
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
      'Steady traffic floods the cluster against a low limit. Play to watch the aggregate climb past it into the rejecting band.',
    // A low limit so the aggregate crosses it within seconds; the default
    // budget then keeps ε = 1, so eager flushing tracks the climb tightly. The
    // feed caps at 1.6× the limit, so the aggregate plateaus inside the band.
    config: { nodes: NODES, rng_seed: 1, rule_limit: 400 },
    traffic: { rate_per_sec: 150, cap: 640 },
    async seed() {
      // No opening burst: the sustained feed (driven during play) is the story.
    },
  },
  {
    id: 'partition',
    label: 'Network partition',
    blurb:
      'The cluster splits in two; one half bursts. Heal the network to watch the halves reconcile.',
    config: { nodes: NODES, rng_seed: 1, rule_limit: NARRATIVE_LIMIT },
    usesNetwork: true,
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
    async seed(sim) {
      const isolated = NODES - 1;
      const rest = Array.from({ length: NODES - 1 }, (_, i) => i);
      await sim.partition([isolated], rest);
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
];

/** The preset the page opens on. */
export const DEFAULT_PRESET = PRESETS[0];
