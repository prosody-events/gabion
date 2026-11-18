// The one next action in every state of every scenario (design language,
// "The next action"): a pure function of the run, which the caption's action
// line names verbatim and whose control alone is filled. Each scenario's story
// is a value of its own type, moved through its acts in `observe`, the one
// place a story changes; `nextAction` reads it and the run's state; `goal` is
// the act's declarative sentence above the line; `progress` is a wait's meter.
// Each story's acts are decided by a switch of their own with no default, so
// an act cannot go without an action, a goal or a move.

import { blurb, type Blurb } from './format';
import type { Admission, Lag } from './sim/types';

/** How a command's bounded run plays on its own: `'settled'` until every
 *  node that waiting can reach has the hits and their acknowledgements have
 *  been sent; `'per-wave'` (Propagation's Send) also stops before each wave. */
export type UntilSettled = 'settled' | 'per-wave';

/** What time does: paused, playing freely, paused where a bounded run hit its
 *  cap, a bounded run playing, or Propagation's run stopped before a wave the
 *  engine holds, until Step draws it. */
export type Clock = 'paused' | 'playing' | 'capped' | UntilSettled | 'before-wave';

/** The transport's controls a press can name. */
export type Control = 'play' | 'step' | 'send';

/** What a wait waits for. */
export type WaitReason = 'starting' | 'run' | 'window' | 'bucket' | 'oldest-bucket';

/** A story-only action: a button in the action line. */
type InlineAction =
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

/** The scenarios told by watching them. */
type WatchedPreset = 'overload' | 'loss' | 'gateway-failure' | 'gateway-cut' | 'steady' | 'no-zone';

/** The one next action. A repair carries the heading's label, so it names a
 *  control that is on screen. */
export type NextAction =
  | { readonly kind: 'press'; readonly control: Control }
  | { readonly kind: 'inline'; readonly action: InlineAction }
  | { readonly kind: 'repair'; readonly label: Blurb }
  | { readonly kind: 'go'; readonly to: PresetId }
  | { readonly kind: 'edit' }
  | { readonly kind: 'wait'; readonly reason: WaitReason }
  | { readonly kind: 'watch'; readonly what: WatchedPreset; readonly next: PresetId };

/** Rate limiting: `open`, `filling` (an admitted Send), `refused`,
 *  `reopened`, `sliding-refused`, `drained`, `done`. `zones` is a bitmask of
 *  the zones with an admitted Send; `sendBucket` the bucket of the last
 *  admitted Send. */
export interface RateLimiting {
  readonly kind: 'rate-limiting';
  readonly next: PresetId;
  readonly act: 'open' | 'filling' | 'refused' | 'reopened' | 'sliding-refused' | 'drained' | 'done';
  readonly zones: number;
  readonly sendBucket: number | null;
}

/** Propagation: `open`, `sent` (an admitted Send), `done` (the window
 *  emptied). */
export interface Propagation {
  readonly kind: 'propagation';
  readonly next: PresetId;
  readonly act: 'open' | 'sent' | 'done';
}

/** A side of Zone partition's cut: the zones that can hear each other, or
 *  the zone cut off. */
type Side = 'near' | 'cut';

/** Zone partition: `open`, `far-side` (one side refused), `repair` (both
 *  refused), `counted` (reconnected, `both` saying whether both sides had
 *  refused first) and `done` (the window emptied). */
type Partition =
  | { readonly kind: 'partition'; readonly next: PresetId; readonly act: 'open' | 'repair' }
  | { readonly kind: 'partition'; readonly next: PresetId; readonly act: 'far-side'; readonly refused: Side }
  | { readonly kind: 'partition'; readonly next: PresetId; readonly act: 'counted' | 'done'; readonly both: boolean };

/** A scenario told by watching it. */
interface Watched {
  readonly kind: 'watch';
  readonly watched: WatchedPreset;
  readonly next: PresetId;
}

/** Each scenario's story, as it stands: a preset declares its opening. */
export type Story = RateLimiting | Propagation | Partition | Watched | { readonly kind: 'sandbox' };

/** A decided Send as a story reads it: admitted, or refused by the window,
 *  in the zone of the node that decided it. */
export interface Verdict {
  readonly kind: 'admitted' | 'refused';
  readonly zone: number;
}

/** What Send `admission`, decided in `zone`, means to a story; null for a
 *  request heavier than the limit, which is refused for its weight and moves
 *  no story. */
export function verdictOf(admission: Admission, zone: number): Verdict | null {
  if (admission.kind === 'Admitted') return { kind: 'admitted', zone };
  return admission.reopens.kind === 'In' ? { kind: 'refused', zone } : null;
}

/** What one applied snapshot tells a story. */
export interface Observation {
  /** The verdict on the Send this command decided, if any. */
  readonly decided: Verdict | null;
  /** The selected node's decision on its next Send, after. */
  readonly nextSend: Admission | undefined;
  readonly oracle: number;
  readonly bucket: number;
  readonly cuts: number;
  /** The zone the scenario cut off, if it cut one. */
  readonly cutZone: number | null;
}

/** The story `s` moves to on observation `o`: the one place a story moves. */
export function observe(s: Story, o: Observation): Story {
  switch (s.kind) {
    case 'rate-limiting':
      return observeRate(s, o);
    case 'propagation':
      return observePropagation(s, o);
    case 'partition':
      return observePartition(s, o);
    case 'watch':
    case 'sandbox':
      return s;
  }
}

function observeRate(s: RateLimiting, o: Observation): RateLimiting {
  const d = o.decided;
  const verdict = d?.kind;
  const opens = o.nextSend?.kind === 'Admitted';
  switch (s.act) {
    case 'open':
    case 'filling':
      if (d?.kind === 'admitted') return { ...s, act: 'filling', zones: s.zones | (1 << d.zone), sendBucket: o.bucket };
      return verdict === 'refused' ? { ...s, act: 'refused' } : s;
    case 'refused':
      return opens ? { ...s, act: 'reopened' } : s;
    case 'reopened':
      if (verdict === 'admitted') return { ...s, sendBucket: o.bucket };
      return verdict === 'refused' ? { ...s, act: 'sliding-refused' } : s;
    case 'sliding-refused':
      return opens ? { ...s, act: 'drained' } : s;
    case 'drained':
      return verdict === 'admitted' ? { ...s, act: 'done' } : s;
    case 'done':
      return s;
  }
}

function observePropagation(s: Propagation, o: Observation): Propagation {
  switch (s.act) {
    case 'open':
      return o.decided?.kind === 'admitted' ? { ...s, act: 'sent' } : s;
    case 'sent':
      return o.oracle === 0 ? { ...s, act: 'done' } : s;
    case 'done':
      return s;
  }
}

function observePartition(s: Partition, o: Observation): Partition {
  const { next } = s;
  switch (s.act) {
    case 'done':
      return s;
    case 'counted':
      return o.oracle === 0 ? { ...s, act: 'done' } : s;
    case 'open':
    case 'far-side':
    case 'repair': {
      if (o.cuts === 0) return { kind: 'partition', next, act: 'counted', both: s.act === 'repair' };
      const d = o.decided;
      if (d?.kind !== 'refused') return s;
      const side: Side = d.zone === o.cutZone ? 'cut' : 'near';
      if (s.act === 'open') return { kind: 'partition', next, act: 'far-side', refused: side };
      return s.act === 'far-side' && s.refused !== side ? { kind: 'partition', next, act: 'repair' } : s;
    }
  }
}

/** Everything the next action, the goal and a wait's meter read. */
export interface RunView {
  readonly story: Story;
  /** The selected node, and its zone. */
  readonly selected: number;
  readonly selectedZone: number;
  readonly nodes: readonly { readonly id: number; readonly zone: number; readonly lag: Lag }[];
  /** The selected node's decision on its next Send, its count and its
   *  non-empty buckets. */
  readonly nextSend: Admission | undefined;
  readonly subjectCount: number;
  readonly subjectStrata: number;
  /** Whether the selected node is at its limit. */
  readonly subjectAtLimit: boolean;
  readonly cutZone: number | null;
  /** The repair the heading offers, by its label, or null. */
  readonly repair: Blurb | null;
  readonly clock: Clock;
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
  if (v.busy) return wait('starting');
  const never = v.lastDecision?.kind === 'Rejected' && v.lastDecision.reopens.kind === 'Never';
  if (v.hits === 'invalid' || never) return { kind: 'inline', action: { kind: 'use-hits', hits: v.burstHits } };
  if (v.clock === 'capped') return press('play');
  if (v.clock === 'before-wave') return press('step');
  if (v.clock === 'settled' || v.clock === 'per-wave') return wait('run');
  const playing = v.clock === 'playing';
  const s = v.story;
  switch (s.kind) {
    case 'rate-limiting':
      return rateAction(v, s, playing);
    case 'propagation':
      return propagationAction(s);
    case 'partition':
      return partitionAction(v, s, playing);
    case 'watch':
      return playing ? { kind: 'watch', what: s.watched, next: s.next } : press('play');
    case 'sandbox':
      return press('send');
  }
}

/** The action that names `control`. */
const press = (control: Control): NextAction => ({ kind: 'press', control });
/** The wait for `reason`. */
const wait = (reason: WaitReason): NextAction => ({ kind: 'wait', reason });

function rateAction(v: RunView, s: RateLimiting, playing: boolean): NextAction {
  switch (s.act) {
    case 'open':
    case 'filling':
      return selectNextZone(v, s.zones) ?? press('send');
    case 'refused':
      return playing ? wait('window') : press('play');
    case 'reopened':
      if (s.sendBucket !== v.bucketNow) return press('send');
      return playing ? wait('bucket') : press('play');
    case 'sliding-refused':
      if (!playing) return press('play');
      return wait(v.subjectStrata >= 2 ? 'oldest-bucket' : 'window');
    case 'drained':
      return press('send');
    case 'done':
      return { kind: 'go', to: s.next };
  }
}

function propagationAction(s: Propagation): NextAction {
  switch (s.act) {
    case 'open':
      return press('send');
    case 'sent':
      return press('step');
    case 'done':
      return { kind: 'go', to: s.next };
  }
}

function partitionAction(v: RunView, s: Partition, playing: boolean): NextAction {
  switch (s.act) {
    case 'open':
      return press('send');
    case 'far-side':
      return selectOtherSide(v, s.refused) ?? press('send');
    case 'repair':
      // The heading offers the repair once no relay is left to send; until
      // then the run is what to wait for, or what to play.
      if (v.repair !== null) return { kind: 'repair', label: v.repair };
      if (!v.nodes.some((n) => n.lag.kind === 'Behind')) return press('send');
      return playing ? wait('run') : press('play');
    case 'counted':
      return playing ? wait('window') : press('play');
    case 'done':
      return { kind: 'go', to: s.next };
  }
}

/** In Rate limiting, once the selected node's zone has an admitted Send: the
 *  first live node, in zone order, of a zone that has none. */
function selectNextZone(v: RunView, zones: number): NextAction | null {
  if ((zones & (1 << v.selectedZone)) === 0) return null;
  const node = first(v, (zone) => (zones & (1 << zone)) === 0);
  return node === null ? null : { kind: 'inline', action: { kind: 'select', node: node.id, zone: node.zone } };
}

/** In Zone partition, once side `refused` has refused: the first live node
 *  of the other side, while the selection is on the side that refused. */
function selectOtherSide(v: RunView, refused: Side): NextAction | null {
  const cut = v.cutZone;
  if (cut === null) return null;
  const wantCut = refused === 'near';
  if ((v.selectedZone === cut) === wantCut) return null;
  const node = first(v, (zone) => (zone === cut) === wantCut);
  return node === null ? null : { kind: 'inline', action: { kind: 'select', node: node.id, zone: node.zone } };
}

/** The first live node, in zone order and then by id, whose zone passes
 *  `inZone`. */
function first(v: RunView, inZone: (zone: number) => boolean): { id: number; zone: number } | null {
  let best: { id: number; zone: number } | null = null;
  for (const n of v.nodes) {
    if (!inZone(n.zone)) continue;
    if (best === null || n.zone < best.zone || (n.zone === best.zone && n.id < best.id)) best = n;
  }
  return best;
}

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
export function goal(v: RunView): Blurb {
  const s = v.story;
  switch (s.kind) {
    case 'rate-limiting':
      return rateGoal(v, s);
    case 'propagation':
      return propagationGoal(s);
    case 'partition':
      return partitionGoal(v, s);
    case 'watch':
      return watchGoal(v, s.watched);
    case 'sandbox':
      return ['Nothing here is scripted: requests, nodes and cuts are yours to try.'];
  }
}

function rateGoal(v: RunView, s: RateLimiting): Blurb {
  const seconds = v.windowMs / 1000;
  switch (s.act) {
    case 'open':
    case 'filling':
      return v.subjectAtLimit
        ? blurb`Every node now sees the limit, so node ${v.selected} decides on its own whether the next request fits.`
        : blurb`The limit of ${v.limit} hits per ${seconds} s is shared by every node in all ${v.zoneCount} zones, and each request carries ${v.sendHits} hits.`;
    case 'refused':
      return blurb`A hit counts for ${seconds} s. When the oldest hits leave the window, node ${v.selected} admits again.`;
    case 'reopened':
    case 'sliding-refused':
    case 'drained':
      return ['The window slides one second at a time, so the oldest hits leave first and room opens one bucket at a time.'];
    case 'done':
      return ['That is the limit: one window, counted on every node, and each node decides alone.'];
  }
}

function propagationGoal(s: Propagation): Blurb {
  switch (s.act) {
    case 'open':
    case 'sent':
      return ['Far from the limit, a node keeps a small change until its next heartbeat, then two waves of packets carry it.'];
    case 'done':
      return ['Hits count only inside the window: every node stopped counting them at the same instant, with no packet.'];
  }
}

function partitionGoal(v: RunView, s: Partition): Blurb {
  switch (s.act) {
    case 'open':
    case 'far-side':
      return ['The last zone is cut off, so each side decides on what it can hear, and each side can reach the limit on its own.'];
    case 'repair':
      return blurb`The cluster let ${v.oracle} hits through a limit of ${v.limit}; once the zone is back, every node counts them.`;
    case 'counted':
      return s.both
        ? blurb`The cluster let ${v.oracle} hits through a limit of ${v.limit}; once the zone is back, every node counts them.`
        : blurb`The zone is back before both sides refused, so every node now counts the ${v.oracle} hits the cluster let through.`;
    case 'done':
      return s.both
        ? ['A cut costs a rate limiter its limit: each side admitted on what it could hear.']
        : ['One window is counted on every node again; a cut costs the limit only while each side reaches it on its own.'];
  }
}

/** The goal of a scenario told by watching it. */
function watchGoal(v: RunView, watched: WatchedPreset): Blurb {
  switch (watched) {
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
    case 'gateway-cut':
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

/** What a watch state watches, by the scenario watched. */
export const WATCHES: Readonly<Record<WatchedPreset, string>> = {
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
 *  where the story names one. Step draws wave `wave` while one is held or some
 *  node is still behind, and otherwise runs to the window's edge. */
export function pressClause(v: RunView, control: Control, wave: number): Blurb {
  const s = v.story;
  switch (control) {
    case 'play':
      if (s.kind === 'rate-limiting' && (s.act === 'refused' || s.act === 'sliding-refused')) return [' and the window starts to slide'];
      if (s.kind === 'partition' && s.act === 'counted') return blurb` and every node counts ${v.oracle} until the window empties`;
      return [];
    case 'step':
      if (v.clock !== 'before-wave' && v.nodes.every((n) => n.lag.kind !== 'Behind')) return [" to reach the window's edge"];
      return wave <= 1 ? blurb` to send wave ${1}` : wave === 2 ? blurb` for wave ${2}, the relays` : blurb` for wave ${wave}`;
    case 'send':
      return s.kind === 'rate-limiting' && s.act === 'drained' && v.subjectCount > 0
        ? blurb` · node ${v.selected} admits while it still counts ${v.subjectCount}`
        : [];
  }
}
