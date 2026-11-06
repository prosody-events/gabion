// The stage heading: the cluster's state in one line, the page's largest
// type. Every number comes from the engine's snapshot (each node's windowed
// count and lag, the true total, the selected node's next decision) and the
// page's own record of the run (its cuts, the waves since the last Send). Each
// form has a template id, published for the audit.

import { cutState, repairLabel, type Cut } from './cuts';
import { blurb, claim, fmtDuration, fmtSeconds, joined, type Blurb } from './format';
import type { Admission, NodeState } from './sim/types';

/** The heading's forms. On the scale: `rl-0` nothing in the window yet,
 *  `rl-1` some nodes behind, `rl-2` every node agrees with the badge, `rl-3`
 *  every node agrees and the selected node is refusing (the countdown),
 *  `rl-4` the window emptied, `rl-5` every node agrees. Off the scale
 *  `hd-far`, with a feed `hd-feed`, and `hd-stranded` while a cut keeps some
 *  nodes from ever catching up. */
export type Template = 'rl-0' | 'rl-1' | 'rl-2' | 'rl-3' | 'rl-4' | 'rl-5' | 'hd-far' | 'hd-feed' | 'hd-stranded';

/** "Agreed in h hops": the data waves delivered between the last admitted
 *  Send and every node agreeing, and how long that took when the Send waited
 *  for its node's heartbeat. */
export interface Badge {
  readonly hops: number;
  readonly afterMs: number | null;
}

export interface HeadingView {
  readonly nodes: readonly NodeState[];
  readonly oracle: number;
  readonly limit: number;
  readonly onScale: boolean;
  /** The feed's hits younger than the grace, or null with no feed. */
  readonly newer: number | null;
  readonly cuts: readonly Cut[];
  readonly zones: readonly { name: string }[];
  readonly selected: number;
  readonly nextSend: Admission | undefined;
  readonly sendHits: number;
  /** Whether the window emptied since the last admitted Send. */
  readonly emptied: boolean;
  readonly badge: Badge | null;
  readonly tickMs: number;
}

export interface Heading {
  readonly id: Template;
  readonly parts: Blurb;
  /** The repair the heading offers, by its label, or null. */
  readonly repair: Blurb | null;
}

/** The heading for `h`. */
export function heading(h: HeadingView): Heading {
  const n = h.nodes.length;
  const t = h.oracle;
  let up = 0;
  let stranded = 0;
  let behind = 0;
  let gap = 0;
  let missing = 0;
  for (const node of h.nodes) {
    const lag = node.lag;
    if (lag.kind === 'Stranded') stranded += 1;
    const caughtUp = h.newer === null ? lag.kind === 'CaughtUp' : node.window_total >= t - h.newer;
    if (caughtUp) up += 1;
    if (lag.kind === 'Behind') {
      behind += 1;
      missing = Math.max(missing, lag.missing_for_ms);
    }
    if (lag.kind !== 'Stranded') gap = Math.max(gap, t - node.window_total);
  }
  const repair = h.cuts.length > 0 && stranded > 0 && behind === 0 ? repairLabel(h.cuts, h.zones) : null;
  const over: Blurb = h.onScale && t > h.limit ? blurb` · ${claim(t, 'cluster', 'admitted')} admitted against the limit of ${claim(h.limit, 'cluster', 'limit')}` : [];
  const late: Blurb =
    missing >= h.tickMs && behind > 0 ? [' · not caught up (≥ ', claim(fmtDuration(missing), 'cluster', 'behind'), ')'] : [];

  if (h.emptied && t === 0) return { id: 'rl-4', parts: ['the window is empty · no hits are counted'], repair };
  if (stranded > 0 && up === n - stranded) {
    const names = joined(h.cuts.map((c) => cutState(c, h.zones)));
    return {
      id: 'hd-stranded',
      parts: [...blurb`✓ All ${n - stranded} reachable nodes see ${claim(t, 'cluster', 'view')} hits · `, ...names, ...over],
      repair,
    };
  }
  if (h.newer !== null) {
    const tail: Blurb = h.newer > 0 ? blurb` · ${h.newer} newer hits on their way` : [];
    const head: Blurb =
      up === n
        ? blurb`✓ All ${n} nodes have every hit older than one heartbeat`
        : blurb`${up} of ${n} nodes have every hit older than one heartbeat`;
    return { id: 'hd-feed', parts: [...head, ...tail, ...over, ...late], repair };
  }
  const agreed = h.badge === null ? [] : badgeOf(h.badge);
  if (!h.onScale) {
    if (up < n) {
      return {
        id: 'hd-far',
        parts: [...blurb`${up} of ${n} nodes ${up === 1 ? 'sees' : 'see'} all ${claim(t, 'cluster', 'view')} hits · the rest are up to ${claim(gap, 'cluster', 'behind')} behind`, ...late],
        repair,
      };
    }
    const all: Blurb = t === 0 ? blurb`All ${n} nodes see no hits yet` : blurb`✓ All ${n} nodes see all ${claim(t, 'cluster', 'view')} hits`;
    return { id: 'hd-far', parts: [...all, ...agreed], repair };
  }
  const limit = claim(h.limit, 'cluster', 'limit');
  if (up < n) {
    return {
      id: 'rl-1',
      parts: [
        ...blurb`${up} of ${n} nodes ${up === 1 ? 'sees' : 'see'} ${claim(t, 'cluster', 'view')} of the ${limit} limit · the rest are up to ${claim(gap, 'cluster', 'behind')} behind`,
        ...over,
        ...late,
      ],
      repair,
    };
  }
  if (t === 0) return { id: 'rl-0', parts: blurb`All ${n} nodes see ${claim(0, 'cluster', 'view')} of the ${limit} limit`, repair };
  const all = blurb`All ${n} nodes see ${claim(t, 'cluster', 'view')} of the ${limit} limit`;
  const next = h.nextSend;
  if (next?.kind === 'Rejected') {
    const s = h.selected;
    const countdown: Blurb =
      next.reopens.kind === 'In'
        ? blurb` · node ${s} admits ${h.sendHits} again in ${claim(fmtSeconds(next.reopens.ms), s, 'reopens')}`
        : blurb` · node ${s} can never admit ${h.sendHits}: the limit is ${h.limit}`;
    return { id: 'rl-3', parts: [...all, ...countdown, ...over], repair };
  }
  if (h.badge !== null) return { id: 'rl-2', parts: ['✓ ', ...all, ...agreed, ...over], repair };
  return { id: 'rl-5', parts: ['✓ ', ...all, ...over], repair };
}

/** " · agreed in h hops[, t after the send]". */
function badgeOf(b: Badge): Blurb {
  const after: Blurb = b.afterMs === null ? [] : [', ', claim(fmtDuration(b.afterMs), 'cluster', 'behind'), ' after the send'];
  return [...blurb` · agreed in ${b.hops} ${b.hops === 1 ? 'hop' : 'hops'}`, ...after];
}
