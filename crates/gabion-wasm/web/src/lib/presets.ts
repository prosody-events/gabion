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
// Every preset leaves the rule limit at the production default — far above the
// seeded volumes — on purpose. A burst stays well under gabion's threshold
// anti-entropy budget (ε ≈ limit·bps/(10⁴·N), ~900 at the default limit), so it
// spreads by lazy heartbeat over several rounds: the multi-hop propagation
// these views exist to show. A burst large enough to approach the limit trips
// the eager threshold flush and converges in one round; that overload regime,
// and the Aggregate-vs-Limit chart it powers, get their own preset later.

import type { Sim } from './sim/sim';
import type { SimConfig } from './sim/types';

/** The single watched rule's key. Every preset seed and every interactive burst
 *  (stage click, rail Send) targets this key, so they all grow the same
 *  counter and the convergence story stays about one number. */
export const WATCHED_KEY = 1;

/** The cluster size the presets build. A ring of 12 reads cleanly and gabion's
 *  adaptive fanout still saturates it in a couple of rounds. */
const NODES = 12;

export interface Preset {
  readonly id: string;
  readonly label: string;
  readonly blurb: string;
  readonly config: Partial<SimConfig>;
  /** Whether the scenario severs links (a partition or isolation) — the rail
   *  surfaces its Heal control only for these, where it does something. */
  readonly usesNetwork?: boolean;
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
    config: { nodes: NODES, rng_seed: 1 },
    async seed(sim) {
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'steady',
    label: 'Steady state',
    blurb: 'Light traffic, scattered across the cluster. It converges at once — the calm baseline.',
    config: { nodes: NODES, rng_seed: 1 },
    async seed(sim) {
      for (const node of [0, 3, 6, 9]) {
        await sim.submitRequest(node, WATCHED_KEY, 10);
      }
    },
  },
  {
    id: 'partition',
    label: 'Network partition',
    blurb:
      'The cluster splits in two; one half bursts. Heal the network to watch the halves reconcile.',
    config: { nodes: NODES, rng_seed: 1 },
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
    config: { nodes: NODES, rng_seed: 1, uniform_loss: 0.3 },
    async seed(sim) {
      await sim.submitRequest(0, WATCHED_KEY, 50);
    },
  },
  {
    id: 'isolation',
    label: 'Node isolation & heal',
    blurb:
      'One node is cut off while a burst spreads. Heal it and it re-syncs by gossip catch-up — it kept its state, so no counts are lost.',
    config: { nodes: NODES, rng_seed: 1 },
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
