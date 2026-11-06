// The one next action in every state of every scenario (design language,
// "The next action"): a pure function of the run, which the caption's action
// line names verbatim and whose control alone is filled. The scenarios'
// stories move through acts in `observe`, the one place a run's act changes;
// `nextAction` reads the act and the run's state; `goal` is the act's
// declarative sentence above the line; `progress` is a wait's meter.

import { blurb, type Blurb } from './format';
import type { Admission, Lag } from './sim/types';

/** What a run is: how long it plays on its own. `'settled'` plays until every
 *  node that waiting can reach has the hits and their acknowledgements have
 *  flown; `'per-wave'` (Propagation's Send) also stops before each wave. */
export type UntilSettled = 'off' | 'settled' | 'per-wave';

/** A wave the engine holds: none, held unflown until Step, flying, or frozen
 *  mid-flight by Pause. */
export type Flight = 'none' | 'waiting' | 'flying' | 'frozen';

/** The transport's controls a press can name. */
export type Control = 'play' | 'step' | 'send';

/** What a wait waits for. */
export type WaitReason = 'starting' | 'run' | 'window' | 'bucket' | 'oldest-bucket';

/** A story-only action: a button in the action line. */
export type InlineAction =
  | { readonly kind: 'select'; readonly node: number; readonly zone: number }
  | { readonly kind: 'use-hits'; readonly hits: number };

/** The scenarios a story hands over to, by preset id. */
export type PresetId =
  | 'rate-limiting'
  | 'propagation'
  | 'overload'
  | 'partition'
  | 'loss'
  | 'gateway-failure'
  | 'gateway-cut'
  | 'steady'
  | 'no-zone'
  | 'sandbox';

/** The one next action. */
export type NextAction =
  | { readonly kind: 'press'; readonly control: Control }
  | { readonly kind: 'inline'; readonly action: InlineAction }
  | { readonly kind: 'repair' }
  | { readonly kind: 'go'; readonly to: PresetId }
  | { readonly kind: 'edit' }
  | { readonly kind: 'wait'; readonly reason: WaitReason }
  | { readonly kind: 'watch'; readonly next: PresetId };

/** Each scenario's story: the acts it moves through. Gateway failure and
 *  Gateway cut are watched until their stories are told step by step. */
export type Story = 'rate-limiting' | 'propagation' | 'partition' | 'watch' | 'sandbox';

/** Where a run stands in its story. Rate limiting: `open`, `filling` (an
 *  admitted Send), `refused`, `reopened`, `sliding-refused`, `drained`, `done`.
 *  Propagation: `open`, `sent`, `done`. Zone partition: `open`, `far-side`
 *  (one side refused), `repair` (both refused), `counted` (reconnected),
 *  `done`. */
export type Act =
  | 'open'
  | 'filling'
  | 'refused'
  | 'reopened'
  | 'sliding-refused'
  | 'drained'
  | 'done'
  | 'sent'
  | 'far-side'
  | 'repair'
  | 'counted';

/** A run's place in its story: its act, a zone bitmask (Rate limiting: zones
 *  with an admitted Send; Zone partition: bit 0 the near side refused, bit 1
 *  the cut zone refused) and the bucket of the last admitted Send. */
export interface Progress {
  readonly act: Act;
  readonly zones: number;
  readonly sendBucket: number;
}

export const START: Progress = { act: 'open', zones: 0, sendBucket: -1 };

/** What one applied snapshot tells a story. */
export interface Observation {
  /** The Send this command decided, if any. */
  readonly decided: { readonly zone: number; readonly admission: Admission } | null;
  /** The selected node's decision on its next Send, after. */
  readonly nextSend: Admission | undefined;
  readonly oracle: number;
  readonly bucket: number;
  readonly cuts: number;
  /** The zone the scenario cut off, if it cut one. */
  readonly cutZone: number | null;
}

/** The act `p` moves to on observation `o`: the one place a run's act moves. */
export function observe(p: Progress, story: Story, o: Observation): Progress {
  const d = o.decided;
  const admitted = d?.admission.kind === 'Admitted';
  // A request heavier than the limit is refused for its weight, not by the
  // window: it moves no story.
  const refused = d?.admission.kind === 'Rejected' && d.admission.reopens.kind === 'In';
  const opens = o.nextSend?.kind === 'Admitted';
  switch (story) {
    case 'rate-limiting':
      switch (p.act) {
        case 'open':
        case 'filling':
          if (admitted) return { act: 'filling', zones: p.zones | (1 << d!.zone), sendBucket: o.bucket };
          return refused ? { ...p, act: 'refused' } : p;
        case 'refused':
          return opens ? { ...p, act: 'reopened' } : p;
        case 'reopened':
          if (admitted) return { ...p, sendBucket: o.bucket };
          return refused ? { ...p, act: 'sliding-refused' } : p;
        case 'sliding-refused':
          return opens ? { ...p, act: 'drained' } : p;
        case 'drained':
          return admitted ? { ...p, act: 'done' } : p;
        default:
          return p;
      }
    case 'propagation':
      if (p.act === 'open' && admitted) return { ...p, act: 'sent' };
      return p.act === 'sent' && o.oracle === 0 ? { ...p, act: 'done' } : p;
    case 'partition': {
      if (p.act === 'done') return p;
      if (p.act === 'counted') return o.oracle === 0 ? { ...p, act: 'done' } : p;
      if (o.cuts === 0) return { ...p, act: 'counted' };
      const zones = refused ? p.zones | (d!.zone === o.cutZone ? 2 : 1) : p.zones;
      return { ...p, zones, act: zones === 3 ? 'repair' : zones === 0 ? 'open' : 'far-side' };
    }
    case 'watch':
    case 'sandbox':
      return p;
  }
}

/** Everything the next action, the goal and a wait's meter read. */
export interface RunView {
  readonly story: Story;
  readonly next: PresetId | null;
  readonly progress: Progress;
  /** The selected node, and its zone. */
  readonly selected: number;
  readonly selectedZone: number;
  readonly nodes: readonly { readonly id: number; readonly zone: number; readonly lag: Lag }[];
  /** The selected node's decision on its next Send, and its non-empty
   *  buckets. */
  readonly nextSend: Admission | undefined;
  readonly subjectStrata: number;
  /** Whether the selected node is at its limit. */
  readonly subjectAtLimit: boolean;
  readonly cuts: number;
  readonly cutZone: number | null;
  readonly untilSettled: UntilSettled;
  readonly flight: Flight;
  readonly playing: boolean;
  readonly capped: boolean;
  readonly busy: boolean;
  /** Hits per Send: committed and valid, committed and invalid, or being
   *  typed. */
  readonly hits: 'valid' | 'invalid' | 'editing';
  /** The last Send's decision, until the next command. */
  readonly lastDecision: Admission | null;
  /** The scenario's own Hits per Send, and the one a Send carries now. */
  readonly burstHits: number;
  readonly sendHits: number;
  readonly oracle: number;
  /** Simulated time, the window bucket it falls in and the bucket's width. */
  readonly virtualMs: number;
  readonly bucketNow: number;
  readonly bucketMs: number;
  readonly limit: number;
  readonly windowMs: number;
  readonly zoneCount: number;
  readonly loss: number;
}

/** The one next action in the state `v` describes. */
export function nextAction(v: RunView): NextAction {
  if (v.hits === 'editing') return { kind: 'edit' };
  if (v.busy) return { kind: 'wait', reason: 'starting' };
  const never = v.lastDecision?.kind === 'Rejected' && v.lastDecision.reopens.kind === 'Never';
  if (v.hits === 'invalid' || never) return { kind: 'inline', action: { kind: 'use-hits', hits: v.burstHits } };
  if (v.flight === 'frozen' || v.capped) return press('play');
  if (v.flight === 'waiting') return press('step');
  if (v.untilSettled !== 'off' || v.flight === 'flying') return wait('run');
  return storyAction(v);
}

/** The action that names `control`. */
const press = (control: Control): NextAction => ({ kind: 'press', control });
/** The wait for `reason`. */
const wait = (reason: WaitReason): NextAction => ({ kind: 'wait', reason });

/** The action the story's current act wants, once nothing runs. */
function storyAction(v: RunView): NextAction {
  const { act } = v.progress;
  switch (v.story) {
    case 'rate-limiting':
      switch (act) {
        case 'open':
        case 'filling':
          return selectNextZone(v) ?? press('send');
        case 'refused':
          return v.playing ? wait('window') : press('play');
        case 'reopened':
          if (v.progress.sendBucket !== v.bucketNow) return press('send');
          return v.playing ? wait('bucket') : press('play');
        case 'sliding-refused':
          if (!v.playing) return press('play');
          return wait(v.subjectStrata >= 2 ? 'oldest-bucket' : 'window');
        case 'drained':
          return press('send');
        default:
          return v.next === null ? press('send') : { kind: 'go', to: v.next };
      }
    case 'propagation':
      if (act === 'open') return press('send');
      if (act === 'sent') return press('step');
      return v.next === null ? press('send') : { kind: 'go', to: v.next };
    case 'partition':
      switch (act) {
        case 'open':
          return press('send');
        case 'far-side':
          return selectOtherSide(v) ?? press('send');
        case 'repair':
          return { kind: 'repair' };
        case 'counted':
          return v.playing ? wait('window') : press('play');
        default:
          return v.next === null ? press('send') : { kind: 'go', to: v.next };
      }
    case 'watch':
      return v.playing && v.next !== null ? { kind: 'watch', next: v.next } : press('play');
    case 'sandbox':
      return press('send');
  }
}

/** In Rate limiting, once the selected node's zone has an admitted Send: the
 *  lowest live node of the next zone, in zone order, that has none. */
function selectNextZone(v: RunView): NextAction | null {
  if ((v.progress.zones & (1 << v.selectedZone)) === 0) return null;
  for (let zone = 0; zone < v.zoneCount; zone++) {
    if ((v.progress.zones & (1 << zone)) !== 0) continue;
    const node = lowest(v, (z) => z === zone);
    if (node !== null) return { kind: 'inline', action: { kind: 'select', node: node.id, zone } };
  }
  return null;
}

/** In Zone partition, once one side has refused: the lowest live node of the
 *  side that has not, while the selection is on the side that has. */
function selectOtherSide(v: RunView): NextAction | null {
  const cut = v.cutZone;
  if (cut === null) return null;
  const wantCut = (v.progress.zones & 1) !== 0;
  if ((v.selectedZone === cut) === wantCut) return null;
  const node = lowest(v, (z) => (z === cut) === wantCut);
  return node === null ? null : { kind: 'inline', action: { kind: 'select', node: node.id, zone: node.zone } };
}

/** The lowest live node whose zone passes `inZone`. */
function lowest(v: RunView, inZone: (zone: number) => boolean): { id: number; zone: number } | null {
  let best: { id: number; zone: number } | null = null;
  for (const n of v.nodes) if (inZone(n.zone) && (best === null || n.id < best.id)) best = n;
  return best;
}

/** Whether the run's story has been told: Sandbox has none to tell. */
export const storyDone = (v: Pick<RunView, 'story' | 'progress'>): boolean =>
  v.story === 'sandbox' || v.progress.act === 'done';

/** Where a wait started: its reason, the simulated instant and the reopening
 *  the selected node then waited for. Replaced only when the reason changes. */
export interface WaitStart {
  readonly reason: WaitReason;
  readonly virtualMs: number;
  readonly reopensMs: number;
}

/** A wait's progress in [0, 1], or null when it has no end to measure (the
 *  engine starting). */
export function progress(v: RunView, reason: WaitReason, from: WaitStart): number | null {
  switch (reason) {
    case 'starting':
      return null;
    case 'run': {
      let reachable = 0;
      let caughtUp = 0;
      for (const n of v.nodes) {
        if (n.lag.kind === 'Stranded') continue;
        reachable += 1;
        if (n.lag.kind === 'CaughtUp') caughtUp += 1;
      }
      return reachable === 0 ? 1 : caughtUp / reachable;
    }
    case 'window':
    case 'oldest-bucket': {
      const left = v.nextSend?.kind === 'Rejected' && v.nextSend.reopens.kind === 'In' ? v.nextSend.reopens.ms : 0;
      return from.reopensMs <= 0 ? 1 : clamp01(1 - left / from.reopensMs);
    }
    case 'bucket': {
      const end = (v.bucketNow + 1) * v.bucketMs;
      return end <= from.virtualMs ? 1 : clamp01((v.virtualMs - from.virtualMs) / (end - from.virtualMs));
    }
  }
}

const clamp01 = (x: number): number => Math.min(Math.max(x, 0), 1);

/** The act's goal: what it is for, declaratively. It never begins a clause
 *  with a verb a control could carry, and never quotes a control's label, so
 *  it cannot contradict the action line beneath it. */
export function goal(v: RunView, scenario: string): Blurb {
  const s = v.selected;
  const seconds = v.windowMs / 1000;
  switch (v.story) {
    case 'rate-limiting':
      switch (v.progress.act) {
        case 'open':
        case 'filling':
          return v.subjectAtLimit
            ? blurb`Every node now sees the limit, so node ${s} decides on its own whether the next request fits.`
            : blurb`The limit of ${v.limit} hits per ${seconds} s is shared by every node in all ${v.zoneCount} zones, and each request carries ${v.sendHits} hits.`;
        case 'refused':
          return blurb`A hit counts for ${seconds} s. When the oldest hits leave the window, node ${s} admits again.`;
        case 'done':
          return ['That is the limit: one window, counted on every node, and each node decides alone.'];
        default:
          return ['The window slides one second at a time, so the oldest hits leave first and room opens one bucket at a time.'];
      }
    case 'propagation':
      return v.progress.act === 'done'
        ? ['Hits count only inside the window: every node stopped counting them at the same instant, with no packet.']
        : ['Far from the limit, a node keeps a small change until its next heartbeat, then two waves of packets carry it.'];
    case 'partition':
      switch (v.progress.act) {
        case 'repair':
        case 'counted':
          return blurb`The cluster let ${v.oracle} hits through a limit of ${v.limit}; once the zone is back, every node counts them.`;
        case 'done':
          return ['A cut costs a rate limiter its limit: each side admitted on what it could hear.'];
        default:
          return ['The last zone is cut off, so each side decides on what it can hear, and each side can reach the limit on its own.'];
      }
    case 'sandbox':
      return ['Nothing here is scripted: requests, nodes and cuts are yours to try.'];
    case 'watch':
      return watchGoal(v, scenario);
  }
}

/** The goal of a scenario told by watching it, by its name. */
function watchGoal(v: RunView, scenario: string): Blurb {
  switch (scenario) {
    case 'overload':
      return ['Requests arrive faster than the limit allows; counts update as the engine computes them.'];
    case 'loss':
      return blurb`Every link drops ${Math.round(v.loss * 100)}% of its packets, and resends still bring every node the hits.`;
    case 'steady':
      return ['The share of bytes that cross a zone sits on its with-zones line, far below the no-zone line.'];
    case 'no-zone':
      return ['With no zone set, every node sends to every peer directly.'];
    case 'gateway-failure':
      return blurb`node ${v.selected}'s gateway into the next zone is silenced, so node ${v.selected} waits for an answer that never comes, then picks another gateway.`;
    default:
      return blurb`node ${v.selected}'s gateway into the next zone hears its zone but cannot reach it, so it answers that it cannot relay, and node ${v.selected} picks another gateway.`;
  }
}

/** What the scenario a story hands over to shows, after its Go button. */
export const GOES_TO: Readonly<Record<PresetId, string>> = {
  'rate-limiting': 'the same cluster, with a limit it reaches',
  propagation: 'how a small change spreads in two waves',
  overload: 'the limit under a steady flood',
  partition: 'what a cut costs a rate limiter',
  loss: 'resends through a lossy network',
  'gateway-failure': 'a gateway that goes silent',
  'gateway-cut': 'a gateway its zone cannot hear',
  steady: 'what zones save in bytes',
  'no-zone': 'the same cluster with no zones',
  sandbox: 'a cluster that is yours to drive',
};

/** What a watch state watches, by scenario. */
export const WATCHES: Readonly<Partial<Record<PresetId, string>>> = {
  overload: 'admitted hits hold at the limit; the rest are rejected',
  loss: 'lost packets are resent until every node has the hits',
  steady: 'the share of data bytes that cross a zone, against its two lines',
  'no-zone': 'every change goes to every peer directly',
  'gateway-failure': 'the silent gateway goes unanswered, then another takes over',
  'gateway-cut': 'the cut gateway says it cannot relay, then another takes over',
};

/** What a wait waits for, as its clause. */
export function waitClause(reason: WaitReason, subject: number): Blurb {
  switch (reason) {
    case 'starting':
      return ['starting the simulator'];
    case 'run':
      return ['the hits are spreading · the run pauses itself when every node has them'];
    case 'window':
      return blurb`the heading counts down to node ${subject}'s reopening`;
    case 'bucket':
      return ["the next one-second bucket opens, so the next request's hits form their own layer"];
    case 'oldest-bucket':
      return blurb`the bottom layer leaves the window first, and node ${subject} admits again`;
  }
}

/** What follows a press's label on the action line: the press's consequence
 *  where the story names one. */
export function pressClause(v: RunView, control: Control, wave: number): Blurb {
  switch (control) {
    case 'play':
      if (v.story === 'rate-limiting' && (v.progress.act === 'refused' || v.progress.act === 'sliding-refused')) {
        return [' and the window starts to slide'];
      }
      if (v.story === 'partition' && v.progress.act === 'counted') {
        return blurb` and every node counts ${v.oracle} until the window empties`;
      }
      return [];
    case 'step':
      if (v.flight === 'waiting') return wave <= 1 ? blurb` to send wave ${1}` : wave === 2 ? blurb` for wave ${2}, the relays` : blurb` for wave ${wave}`;
      return v.story === 'propagation' ? [" to reach the window's edge"] : [' to the next event'];
    case 'send':
      return v.story === 'rate-limiting' && v.progress.act === 'drained'
        ? blurb` · node ${v.selected} admits while it still counts ${v.oracle}`
        : [];
  }
}
