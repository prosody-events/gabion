// The next-action table: each state a story can stand in, as the run it reads,
// and the one action the page must name there. `next.spec.ts` runs every row
// through `nextAction`.

import { START, type Control, type NextAction, type Progress, type RunView, type WaitReason } from '../src/lib/next';
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

/** A paused Rate limiting run at load, node 0 selected. */
export const BASE: RunView = {
  story: 'rate-limiting',
  next: 'partition',
  progress: START,
  selected: 0,
  selectedZone: 0,
  nodes: nodes(),
  nextSend: admits,
  subjectStrata: 0,
  subjectAtLimit: false,
  cuts: 0,
  cutZone: null,
  untilSettled: 'off',
  flight: 'none',
  playing: false,
  capped: false,
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

const at = (act: Progress['act'], more: Partial<Progress> = {}): Progress => ({ ...START, act, ...more });

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
  { name: 'invalid hits during a run', view: { hits: 'invalid', untilSettled: 'settled', playing: true }, action: { kind: 'inline', action: { kind: 'use-hits', hits: 25 } } },
  { name: 'a request heavier than the limit', view: { lastDecision: never }, action: { kind: 'inline', action: { kind: 'use-hits', hits: 25 } } },
  { name: 'paused mid-wave', view: { flight: 'frozen' }, action: press('play') },
  { name: 'a run stopped at its cap', view: { capped: true }, action: press('play') },
  { name: 'a wave held for Step', view: { flight: 'waiting', untilSettled: 'per-wave' }, action: press('step') },
  { name: "a Send's run", view: { untilSettled: 'settled', playing: true, flight: 'flying' }, action: wait('run') },
  // Rate limiting, state by state (RL-0 to RL-13).
  { name: 'RL-0 load', view: {}, action: press('send') },
  { name: 'RL-2 zone a sent, select zone b', view: { progress: at('filling', { zones: 1, sendBucket: 3 }), oracle: 25 }, action: { kind: 'inline', action: { kind: 'select', node: 1, zone: 1 } } },
  { name: 'RL-3 node 1 selected', view: { progress: at('filling', { zones: 1 }), selected: 1, selectedZone: 1, oracle: 25 }, action: press('send') },
  { name: 'RL-4 zone b sent, select zone c', view: { progress: at('filling', { zones: 3 }), selected: 1, selectedZone: 1, oracle: 50 }, action: { kind: 'inline', action: { kind: 'select', node: 2, zone: 2 } } },
  { name: 'RL-5 node 2 sends again', view: { progress: at('filling', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 75 }, action: press('send') },
  { name: 'RL-6 at the limit', view: { progress: at('filling', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, subjectAtLimit: true, nextSend: refuses }, action: press('send') },
  { name: 'RL-7 refused', view: { progress: at('refused', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, nextSend: refuses, lastDecision: refuses }, action: press('play') },
  { name: 'RL-8 the window', view: { progress: at('refused', { zones: 7 }), selected: 2, selectedZone: 2, oracle: 100, nextSend: refuses, playing: true }, action: wait('window') },
  { name: 'RL-9 reopened', view: { progress: at('reopened', { zones: 7, sendBucket: 2 }), selected: 2, selectedZone: 2, playing: true }, action: press('send') },
  { name: 'RL-10 the bucket', view: { progress: at('reopened', { zones: 7, sendBucket: 3 }), selected: 2, selectedZone: 2, playing: true, oracle: 25 }, action: wait('bucket') },
  { name: 'RL-10 the bucket, paused', view: { progress: at('reopened', { zones: 7, sendBucket: 3 }), selected: 2, selectedZone: 2, oracle: 25 }, action: press('play') },
  { name: 'RL-11 the oldest bucket', view: { progress: at('sliding-refused', { zones: 7 }), selected: 2, selectedZone: 2, playing: true, oracle: 100, subjectStrata: 4, nextSend: refuses }, action: wait('oldest-bucket') },
  { name: 'RL-11 one stratum', view: { progress: at('sliding-refused', { zones: 7 }), selected: 2, selectedZone: 2, playing: true, oracle: 100, subjectStrata: 1, nextSend: refuses }, action: wait('window') },
  { name: 'RL-12 the partial fall-off', view: { progress: at('drained', { zones: 7 }), selected: 2, selectedZone: 2, playing: true, oracle: 75 }, action: press('send') },
  { name: 'RL-13 the end', view: { progress: at('done', { zones: 7 }), selected: 2, selectedZone: 2, playing: true, oracle: 100 }, action: { kind: 'go', to: 'partition' } },
  // Propagation, Send stopping before each wave.
  { name: 'Propagation at load', view: { story: 'propagation', next: 'rate-limiting', limit: 1_000_000, oracle: 50 }, action: press('send') },
  { name: 'Propagation waiting for its heartbeat', view: { story: 'propagation', next: 'rate-limiting', untilSettled: 'per-wave', playing: true, progress: at('sent'), nodes: nodes((id) => (id === 0 ? caught : behind)) }, action: wait('run') },
  { name: 'Propagation wave 1 held', view: { story: 'propagation', next: 'rate-limiting', untilSettled: 'per-wave', flight: 'waiting', progress: at('sent') }, action: press('step') },
  { name: 'Propagation settled', view: { story: 'propagation', next: 'rate-limiting', progress: at('sent'), oracle: 75 }, action: press('step') },
  { name: 'Propagation at the end', view: { story: 'propagation', next: 'rate-limiting', progress: at('done') }, action: { kind: 'go', to: 'rate-limiting' } },
  // Zone partition.
  { name: 'Partition at load', view: { story: 'partition', next: 'gateway-failure', cuts: 1, cutZone: 2 }, action: press('send') },
  { name: 'Partition near side refused', view: { story: 'partition', next: 'gateway-failure', cuts: 1, cutZone: 2, progress: at('far-side', { zones: 1 }) }, action: { kind: 'inline', action: { kind: 'select', node: 2, zone: 2 } } },
  { name: 'Partition in the cut zone', view: { story: 'partition', next: 'gateway-failure', cuts: 1, cutZone: 2, progress: at('far-side', { zones: 1 }), selected: 2, selectedZone: 2 }, action: press('send') },
  { name: 'Partition both refused', view: { story: 'partition', next: 'gateway-failure', cuts: 1, cutZone: 2, progress: at('repair', { zones: 3 }), nodes: nodes((id) => (id % 3 === 2 ? stranded : caught)) }, action: { kind: 'repair' } },
  { name: 'Partition reconnected', view: { story: 'partition', next: 'gateway-failure', progress: at('counted', { zones: 3 }), oracle: 200 }, action: press('play') },
  { name: 'Partition counting down', view: { story: 'partition', next: 'gateway-failure', progress: at('counted', { zones: 3 }), oracle: 200, playing: true, nextSend: refuses }, action: wait('window') },
  { name: 'Partition at the end', view: { story: 'partition', next: 'gateway-failure', progress: at('done', { zones: 3 }) }, action: { kind: 'go', to: 'gateway-failure' } },
  // The watched scenarios and Sandbox.
  { name: 'a watched scenario, paused', view: { story: 'watch', next: 'rate-limiting' }, action: press('play') },
  { name: 'a watched scenario, playing', view: { story: 'watch', next: 'rate-limiting', playing: true }, action: { kind: 'watch', next: 'rate-limiting' } },
  { name: 'Sandbox', view: { story: 'sandbox', next: null, playing: true }, action: press('send') },
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
