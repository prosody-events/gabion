// The next-action table: each state a story can stand in, as the run it reads,
// and the one action the page must name there. `next.spec.ts` runs every row
// through `nextAction`.

import type { Control, NextAction, Propagation, RateLimiting, RunView, WaitReason } from '../src/lib/next';
import type { Admission, Lag } from '../src/lib/sim/types';

const caught: Lag = { kind: 'CaughtUp' };
const behind: Lag = { kind: 'Behind', missing_for_ms: 5 };
const stranded: Lag = { kind: 'Stranded', missing_for_ms: 600 };
const admits: Admission = { kind: 'Admitted', total_before: 0 };
const refuses: Admission = { kind: 'Rejected', total: 100, reopens: { kind: 'In', ms: 9900 } };
const never: Admission = { kind: 'Rejected', total: 0, reopens: { kind: 'Never' } };

/** Twelve nodes in three zones, node `i` in zone `i mod 3`, each `lag`. */
const nodes = (lag: (id: number) => Lag = () => caught) =>
  Array.from({ length: 12 }, (_, id) => ({ id, zone: id % 3, lag: lag(id) }));

/** Each story in act `act`, the rest of its state from `more`. */
const rl = (act: RateLimiting['act'], more: Partial<RateLimiting> = {}): RateLimiting => ({ kind: 'rate-limiting', next: 'partition', act, zones: 0, sendBucket: null, ...more });
const prop = (act: Propagation['act']): Propagation => ({ kind: 'propagation', next: 'rate-limiting', act });
/** Zone partition's constant half, for its rows' stories. */
const P = { kind: 'partition', next: 'gateway-failure' } as const;

/** A paused Rate limiting run at load, node 0 selected. */
export const BASE: RunView = {
  story: rl('open'),
  selected: 0,
  selectedZone: 0,
  nodes: nodes(),
  nextSend: admits,
  subjectCount: 0,
  subjectStrata: 0,
  subjectAtLimit: false,
  cutZone: null,
  repair: null,
  clock: 'paused',
  flight: 'none',
  busy: false,
  hits: 'valid',
  lastDecision: null,
  burstHits: 25,
  sendHits: 25,
  oracle: 0,
  virtualMs: 3000,
  bucketNow: 3,
  bucketMs: 1000,
  limit: 100,
  windowMs: 10_000,
  zoneCount: 3,
  loss: 0,
};


// The expected actions, written out here rather than built by next.ts's own
// constructors, so a wrong constructor cannot change what is expected too.
const press = (control: Control): NextAction => ({ kind: 'press', control });
const wait = (reason: WaitReason): NextAction => ({ kind: 'wait', reason });

/** One state and its action. */
export interface Row {
  readonly name: string;
  readonly view: Partial<RunView>;
  readonly action: NextAction;
}


export const TABLE: readonly Row[] = [
  // The precedence every story shares.
  { name: 'typing Hits per Send', view: { hits: 'editing', busy: true }, action: { kind: 'edit' } },
  { name: 'the engine starting', view: { busy: true }, action: wait('starting') },
  { name: 'an invalid Hits per Send', view: { hits: 'invalid' }, action: { kind: 'inline', action: { kind: 'use-hits', hits: 25 } } },
  { name: 'invalid hits during a run', view: { hits: 'invalid', clock: 'settled' }, action: { kind: 'inline', action: { kind: 'use-hits', hits: 25 } } },
  { name: 'a request heavier than the limit', view: { lastDecision: never }, action: { kind: 'inline', action: { kind: 'use-hits', hits: 25 } } },
  { name: 'paused mid-wave', view: { flight: 'frozen' }, action: press('play') },
  { name: 'a run stopped at its cap', view: { clock: 'capped' }, action: press('play') },
  { name: 'a wave held for Step', view: { flight: 'waiting', clock: 'per-wave' }, action: press('step') },
  { name: "a Send's run", view: { clock: 'settled', flight: 'flying' }, action: wait('run') },
  // Rate limiting, state by state (RL-0 to RL-13).
  { name: 'RL-0 load', view: {}, action: press('send') },
  { name: 'RL-2 zone a sent, select zone b', view: { story: rl('filling', { zones: 1, sendBucket: 3 }), oracle: 25 }, action: { kind: 'inline', action: { kind: 'select', node: 1, zone: 1 } } },
  { name: 'RL-3 node 1 selected', view: { story: rl('filling', { zones: 1 }), selected: 1, selectedZone: 1, oracle: 25 }, action: press('send') },
  { name: 'RL-4 zone b sent, select zone c', view: { story: rl('filling', { zones: 3 }), selected: 1, selectedZone: 1, oracle: 50 }, action: { kind: 'inline', action: { kind: 'select', node: 2, zone: 2 } } },
  { name: 'RL-4 zone b emptied: zone c is still next', view: { story: rl('filling', { zones: 1 }), nodes: nodes().filter((n) => n.zone !== 1), zoneCount: 2, oracle: 25 }, action: { kind: 'inline', action: { kind: 'select', node: 2, zone: 2 } } },
  { name: 'RL-5 node 2 sends again', view: { story: rl('filling', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 75 }, action: press('send') },
  { name: 'RL-6 at the limit', view: { story: rl('filling', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, subjectAtLimit: true, nextSend: refuses }, action: press('send') },
  { name: 'RL-7 refused', view: { story: rl('refused', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, nextSend: refuses, lastDecision: refuses }, action: press('play') },
  { name: 'RL-8 the window', view: { story: rl('refused', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, nextSend: refuses, clock: 'playing' }, action: wait('window') },
  { name: 'RL-9 reopened', view: { story: rl('reopened', { zones: 7, sendBucket: 2 }), selected: 2, selectedZone: 2, clock: 'playing' }, action: press('send') },
  { name: 'RL-10 the bucket', view: { story: rl('reopened', { zones: 7, sendBucket: 3 }), selected: 2, selectedZone: 2, clock: 'playing', oracle: 25 }, action: wait('bucket') },
  { name: 'RL-10 the bucket, paused', view: { story: rl('reopened', { zones: 7, sendBucket: 3 }), selected: 2, selectedZone: 2, oracle: 25 }, action: press('play') },
  { name: 'RL-11 the oldest bucket', view: { story: rl('sliding-refused', { zones: 7 }), selected: 2, selectedZone: 2, clock: 'playing', oracle: 100, subjectStrata: 4, nextSend: refuses }, action: wait('oldest-bucket') },
  { name: 'RL-11 one stratum', view: { story: rl('sliding-refused', { zones: 7 }), selected: 2, selectedZone: 2, clock: 'playing', oracle: 100, subjectStrata: 1, nextSend: refuses }, action: wait('window') },
  { name: 'RL-12 the partial fall-off', view: { story: rl('drained', { zones: 7 }), selected: 2, selectedZone: 2, clock: 'playing', oracle: 75 }, action: press('send') },
  { name: 'RL-13 the end', view: { story: rl('done', { zones: 7 }), selected: 2, selectedZone: 2, clock: 'playing', oracle: 100 }, action: { kind: 'go', to: 'partition' } },
  // Propagation, Send stopping before each wave.
  { name: 'Propagation at load', view: { story: prop('open'), limit: 1_000_000, oracle: 50 }, action: press('send') },
  { name: 'Propagation waiting for its heartbeat', view: { story: prop('sent'), clock: 'per-wave', nodes: nodes((id) => (id === 0 ? caught : behind)) }, action: wait('run') },
  { name: 'Propagation wave 1 held', view: { story: prop('sent'), clock: 'per-wave', flight: 'waiting' }, action: press('step') },
  { name: 'Propagation settled', view: { story: prop('sent'), oracle: 75 }, action: press('step') },
  { name: 'Propagation at the end', view: { story: prop('done') }, action: { kind: 'go', to: 'rate-limiting' } },
  // Zone partition.
  { name: 'Partition at load', view: { story: { ...P, act: 'open' }, cutZone: 2 }, action: press('send') },
  { name: 'Partition near side refused', view: { story: { ...P, act: 'far-side', refused: 'near' }, cutZone: 2 }, action: { kind: 'inline', action: { kind: 'select', node: 2, zone: 2 } } },
  { name: 'Partition in the cut zone', view: { story: { ...P, act: 'far-side', refused: 'near' }, cutZone: 2, selected: 2, selectedZone: 2 }, action: press('send') },
  { name: 'Partition both refused', view: { story: { ...P, act: 'repair' }, cutZone: 2, repair: ['Reconnect zone c'], nodes: nodes((id) => (id % 3 === 2 ? stranded : caught)) }, action: { kind: 'repair', label: ['Reconnect zone c'] } },
  { name: 'Partition both refused, relays still landing', view: { story: { ...P, act: 'repair' }, cutZone: 2, clock: 'playing', nodes: nodes((id) => (id % 3 === 2 ? stranded : id === 1 ? behind : caught)) }, action: wait('run') },
  { name: 'Partition reconnected', view: { story: { ...P, act: 'counted', both: true }, oracle: 200 }, action: press('play') },
  { name: 'Partition counting down', view: { story: { ...P, act: 'counted', both: true }, oracle: 200, clock: 'playing', nextSend: refuses }, action: wait('window') },
  { name: 'Partition at the end', view: { story: { ...P, act: 'done', both: true } }, action: { kind: 'go', to: 'gateway-failure' } },
  // The watched scenarios and Sandbox.
  { name: 'a watched scenario, paused', view: { story: { kind: 'watch', watched: 'loss', next: 'rate-limiting' } }, action: press('play') },
  { name: 'a watched scenario, playing', view: { story: { kind: 'watch', watched: 'loss', next: 'rate-limiting' }, clock: 'playing' }, action: { kind: 'watch', what: 'loss', next: 'rate-limiting' } },
  { name: 'Sandbox', view: { story: { kind: 'sandbox' }, clock: 'playing' }, action: press('send') },
];

/** The words no goal clause may begin with: verbs a control could carry. */
export const COMMANDS = [
  'send', 'press', 'play', 'step', 'select', 'reconnect', 'silence', 'cut', 'fill', 'watch', 'wait', 'click', 'type', 'go',
  'use', 'break', 'try', 'heal', 'restart', 'add', 'remove',
] as const;

/** A goal's clauses: split at sentence ends, ";", ":", ",", "·", " and " and
 *  " then ". */
export const clauses = (goal: string): string[] =>
  goal
    .split(/[.;:,·]| and | then /)
    .map((c) => c.trim())
    .filter((c) => c !== '');
