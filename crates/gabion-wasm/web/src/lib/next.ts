// The one next action in every state of every scenario (design language,
// "The next action"): a pure function of the run, which the caption's action
// line names verbatim and whose control alone is filled. Each scenario's story
// is a value of its own type, moved through its acts in `observe`, the one
// place a story changes; `nextAction` reads it and the run's state; `goal` is
// the act's declarative sentence above the line; `progress` is a wait's meter.
// Each story's acts are decided by a switch of their own with no default, so
// an act cannot go without an action, a goal or a move.

import { blurb, fmtSeconds, zoneAt, type Blurb } from './format';
import type { Admission, Burst, Lag } from './sim/types';

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
export type WaitReason = 'starting' | 'run' | 'window' | 'bucket' | 'oldest-bucket' | 'failover';

/** The fault a gateway story offers: the gateway silenced (it hears nothing
 *  and says nothing), or cut off from its own zone (it still hears the zone
 *  and answers the sender, but the zone cannot hear it). */
export type GatewayFault = 'silenced' | 'unheard';

/** A story-only action: a button in the action line. */
type InlineAction =
  | { readonly kind: 'select'; readonly node: number; readonly zone: number }
  | { readonly kind: 'use-requests'; readonly requests: number }
  | { readonly kind: 'fault'; readonly fault: GatewayFault; readonly node: number; readonly zone: number; readonly origin: number };

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
type WatchedPreset = 'overload' | 'loss' | 'steady' | 'no-zone';

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

/** Rate limiting: `open`, `filling` (a Send admitted whole), `refused` (a
 *  Send the limit split or rejected whole),
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

/** Gateway failure and Gateway cut, told from `origin`'s gateway into
 *  `zone`: `open` (healthy, no fault offered), `sent` (a Send's relays have
 *  left the gateway and landed: the fault is offered), `faulted` (the reader
 *  broke `gateway`; `failovers` is `origin`'s failover count then) and
 *  `done` (`origin` moved to another gateway and every reachable node has
 *  the hits). */
export type Gateway =
  | {
      readonly kind: 'gateway';
      readonly fault: GatewayFault;
      readonly next: PresetId;
      readonly origin: number;
      readonly zone: number;
      readonly act: 'open' | 'sent';
    }
  | {
      readonly kind: 'gateway';
      readonly fault: GatewayFault;
      readonly next: PresetId;
      readonly origin: number;
      readonly zone: number;
      readonly act: 'faulted' | 'done';
      readonly gateway: number;
      readonly failovers: number;
    };

/** A scenario told by watching it. */
interface Watched {
  readonly kind: 'watch';
  readonly watched: WatchedPreset;
  readonly next: PresetId;
}

/** Each scenario's story, as it stands: a preset declares its opening. */
export type Story = RateLimiting | Propagation | Partition | Gateway | Watched | { readonly kind: 'sandbox' };

/** Whether story `s` has reached its end, or has no scripted end (a watched
 *  scenario, Sandbox): then every fault and repair is offered, and before it
 *  only the story's own offer is. */
export function storyDone(s: Story): boolean {
  switch (s.kind) {
    case 'rate-limiting':
    case 'propagation':
    case 'partition':
    case 'gateway':
      return s.act === 'done';
    case 'watch':
    case 'sandbox':
      return true;
  }
}

/** A decided Send as a story reads it: admitted whole, split by the limit
 *  (some admitted, the rest refused), or refused whole, in the zone of the
 *  node that decided it. */
export interface Verdict {
  readonly kind: 'admitted' | 'split' | 'refused';
  readonly zone: number;
}

/** What Send `burst`, decided in `zone`, means to a story. */
export function verdictOf(burst: Burst, zone: number): Verdict {
  if (burst.kind === 'Admitted') return { kind: 'admitted', zone };
  return { kind: burst.admitted > 0 ? 'split' : 'refused', zone };
}

/** What one applied snapshot tells a story. */
export interface Observation {
  /** The verdict on the Send this command decided, if any. */
  readonly decided: Verdict | null;
  /** The selected node's decision on one more request, after. */
  readonly brim: Admission | undefined;
  readonly oracle: number;
  readonly bucket: number;
  readonly cuts: number;
  /** The zone the scenario cut off, if it cut one. */
  readonly cutZone: number | null;
  /** The gateway this command broke, if it was a gateway story's fault. */
  readonly faulted: number | null;
  /** The gateway story's origin's gateway failovers so far. */
  readonly failovers: number;
  /** Whether no node that waiting can reach is behind. */
  readonly agreed: boolean;
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
    case 'gateway':
      return observeGateway(s, o);
    case 'watch':
    case 'sandbox':
      return s;
  }
}

function observeGateway(s: Gateway, o: Observation): Gateway {
  const { kind, fault, next, origin, zone } = s;
  switch (s.act) {
    case 'open':
      return o.decided === null || o.decided.kind === 'refused' ? s : { ...s, act: 'sent' };
    case 'sent':
      return o.faulted === null ? s : { kind, fault, next, origin, zone, act: 'faulted', gateway: o.faulted, failovers: o.failovers };
    case 'faulted':
      return o.failovers > s.failovers && o.agreed ? { ...s, act: 'done' } : s;
    case 'done':
      return s;
  }
}

function observeRate(s: RateLimiting, o: Observation): RateLimiting {
  const d = o.decided;
  const verdict = d?.kind;
  const opens = o.brim?.kind === 'Admitted';
  switch (s.act) {
    case 'open':
    case 'filling':
      if (d === null) return s;
      if (d.kind === 'refused') return { ...s, act: 'refused' };
      return {
        ...s,
        act: d.kind === 'admitted' ? 'filling' : 'refused',
        zones: s.zones | (1 << d.zone),
        sendBucket: o.bucket,
      };
    case 'refused':
      return opens ? { ...s, act: 'reopened' } : s;
    case 'reopened':
      if (verdict === 'admitted') return { ...s, sendBucket: o.bucket };
      return verdict === undefined ? s : { ...s, act: 'sliding-refused' };
    case 'sliding-refused':
      return opens ? { ...s, act: 'drained' } : s;
    case 'drained':
      return verdict === 'admitted' || verdict === 'split' ? { ...s, act: 'done' } : s;
    case 'done':
      return s;
  }
}

function observePropagation(s: Propagation, o: Observation): Propagation {
  switch (s.act) {
    case 'open':
      return o.decided === null || o.decided.kind === 'refused' ? s : { ...s, act: 'sent' };
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
      if (d === null || d.kind === 'admitted') return s;
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
  /** The selected node's decision on one more request, its count and its
   *  non-empty buckets. */
  readonly brim: Admission;
  readonly subjectCount: number;
  readonly subjectStrata: number;
  readonly cutZone: number | null;
  /** The repair the heading offers, by its label, or null. */
  readonly repair: Blurb | null;
  readonly clock: Clock;
  readonly busy: boolean;
  /** Requests per Send: committed and valid, committed and invalid, or being
   *  typed. */
  readonly requests: 'valid' | 'invalid' | 'editing';
  /** The scenario's own Requests per Send. */
  readonly defaultRequests: number;
  readonly oracle: number;
  /** Simulated time, the window bucket it falls in and the bucket's width. */
  readonly virtualMs: number;
  readonly bucketNow: number;
  readonly bucketMs: number;
  readonly limit: number;
  readonly windowMs: number;
  readonly zoneCount: number;
  readonly loss: number;
  /** Every zone, in index order, by name. */
  readonly zones: readonly { readonly name: string }[];
  /** A gateway story's gateway: the one it broke, or before the fault the
   *  origin's gateway into the story's zone; null elsewhere. */
  readonly gateway: number | null;
  /** How long the selected node has waited on that gateway's answer, if it
   *  waits. */
  readonly unanswered: number | null;
  /** `gossip.suspect_after`, from the engine. */
  readonly suspectAfterMs: number;
  /** The gateway story's origin's gateway failovers so far. */
  readonly failovers: number;
}

/** The one next action in the state `v` describes. */
export function nextAction(v: RunView): NextAction {
  if (v.requests === 'editing') return { kind: 'edit' };
  if (v.busy) return wait('starting');
  if (v.requests === 'invalid') return { kind: 'inline', action: { kind: 'use-requests', requests: v.defaultRequests } };
  // Gateway cut's trickle keeps every run going to its cap: there a stopped
  // run offers the story's next step, but the fault's own run plays on.
  if (v.story.kind === 'gateway' && v.clock === 'capped' && v.story.act !== 'faulted') return gatewayAction(v, v.story);
  if (v.clock === 'capped') return press('play');
  if (v.clock === 'before-wave') return press('step');
  // From the fault's press to the ending nothing is filled: the run plays
  // through the consequence, the detection and the failover.
  if (v.story.kind === 'gateway' && v.story.act === 'faulted') return v.clock === 'paused' ? press('play') : wait('failover');
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
    case 'gateway':
      return gatewayAction(v, s);
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

function gatewayAction(v: RunView, s: Gateway): NextAction {
  switch (s.act) {
    case 'open':
      return press('send');
    case 'faulted':
      return wait('failover');
    case 'sent':
      return v.gateway === null
        ? press('send')
        : { kind: 'inline', action: { kind: 'fault', fault: s.fault, node: v.gateway, zone: s.zone, origin: s.origin } };
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
    case 'failover':
      // The origin's wait on the silent gateway, then the run.
      if (v.story.kind === 'gateway' && v.story.act === 'faulted' && v.failovers === v.story.failovers && v.unanswered !== null)
        return v.suspectAfterMs <= 0 ? 1 : clamp01(v.unanswered / v.suspectAfterMs);
      return progress(v, 'run', from);
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
      const left = v.brim.kind === 'Rejected' ? v.brim.reopens_ms : 0;
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
    case 'gateway':
      return gatewayGoal(v, s);
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
      return v.brim.kind === 'Rejected'
        ? blurb`Every node now sees the limit, so node ${v.selected} decides on its own whether the next request fits.`
        : blurb`The limit of ${v.limit} hits per ${seconds} s is shared by every node in all ${v.zoneCount} zones, and each request is one hit.`;
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

function gatewayGoal(v: RunView, s: Gateway): Blurb {
  const g = v.gateway ?? 0;
  const zone = zoneAt(v.zones, s.zone);
  const silenced = s.fault === 'silenced';
  switch (s.act) {
    case 'open':
      return blurb`Node ${g} is node ${s.origin}'s gateway into ${zone}: it passes node ${s.origin}'s hits on to the rest of ${zone}.`;
    case 'sent':
      return silenced
        ? blurb`Node ${g} just passed node ${s.origin}'s hits on to the rest of ${zone}. What happens to them when it falls silent?`
        : blurb`Node ${g} just passed node ${s.origin}'s hits on to ${zone}. What happens when it can still hear ${zone}, but ${zone} cannot hear it?`;
    case 'faulted':
      return silenced
        ? blurb`Node ${g} is silenced. What will node ${s.origin} do with hits it cannot pass on?`
        : blurb`Node ${g} hears ${zone}, but ${zone} cannot hear it. How will node ${s.origin} find out?`;
    case 'done':
      return silenced
        ? blurb`Node ${s.origin} found another way into ${zone}; node ${g} is still silenced.`
        : blurb`Node ${g} told node ${s.origin} it cannot relay, and node ${s.origin} found another way into ${zone}.`;
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
};

/** What a wait waits for, as its clause. */
export function waitClause(reason: WaitReason, v: RunView): Blurb {
  const subject = v.selected;
  switch (reason) {
    case 'starting':
      return ['starting the simulator'];
    case 'run':
      return ['the hits are spreading · the run pauses itself when every node has them'];
    case 'window':
      return blurb`the heading counts down to node ${subject}'s reopening`;
    case 'bucket':
      return ["the next one-second bucket opens, so the next Send's hits form their own layer"];
    case 'oldest-bucket':
      return blurb`the bottom layer leaves the window first, and node ${subject} admits again`;
    case 'failover':
      return failoverClause(v);
  }
}

/** A gateway story's wait from its fault to its ending: the detection, with
 *  how long the origin has waited, then the failover's run. */
function failoverClause(v: RunView): Blurb {
  const s = v.story;
  if (s.kind !== 'gateway' || s.act !== 'faulted') return waitClause('run', v);
  const zone = zoneAt(v.zones, s.zone);
  if (v.failovers > s.failovers) return blurb`node ${s.origin} now reaches ${zone} through another gateway · the hits are spreading`;
  if (s.fault === 'unheard')
    return blurb`node ${s.gateway} hears nothing back from ${zone}; it will tell node ${s.origin} it cannot relay`;
  const after = fmtSeconds(v.suspectAfterMs);
  return v.unanswered === null
    ? blurb`node ${s.origin} gives up on node ${s.gateway} after ${after} without an answer, then picks another gateway into ${zone}`
    : blurb`node ${s.origin} has waited ${fmtSeconds(v.unanswered)} for node ${s.gateway} to answer · it gives up at ${after}, then picks another gateway into ${zone}`;
}

/** What follows a fault's button on the action line: what the fault does,
 *  and the request the press also brings. */
export function faultClause(fault: GatewayFault, origin: number, zone: string, requests: number): Blurb {
  const does = fault === 'silenced' ? ['it will hear nothing and say nothing'] : blurb`it will still hear ${zone}, but ${zone} will not hear it`;
  return [' · ', ...does, ...blurb`, and node ${origin} then gets its next ${requests} requests`];
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
