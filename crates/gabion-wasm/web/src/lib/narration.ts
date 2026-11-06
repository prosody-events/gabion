// The narration's sentences: the last event and its reason, one job, in the
// page's only live region. Pure templates over the numbers the page read from
// the engine's batch and snapshot; the page decides when each is said.

import { blurb, joined, noun, type Blurb } from './format';

/** A gateway as a sentence names it: its node and the zone it carries hits
 *  into. */
export interface GatewayName {
  readonly node: number;
  readonly zone: string;
}

/** Idle: what the selection means, and what its ports mark. */
export function idle(subject: number, gateways: readonly GatewayName[], onScale: boolean): Blurb {
  const head = blurb`node ${subject} is selected, so Send goes to it.`;
  if (!onScale || gateways.length === 0) return [...head, ' Send hits to watch them spread.'];
  const [first, ...rest] = gateways;
  const more = rest.flatMap((g) => blurb`, node ${g.node} into ${g.zone}`);
  return [...head, ...blurb` Its ports mark its gateways: node ${first.node} carries its hits into ${first.zone}`, ...more, '.'];
}

/** An admitted request whose hits left at once: an eager flush. */
export function admittedEager(node: number, hits: number, before: number, limit: number): Blurb {
  return blurb`node ${node} admitted ${hits} ${noun(hits, 'hit')}: ${before} + ${hits} is within the limit of ${limit}. It decided alone, then told its zone and one node in each other zone at once.`;
}

/** An admitted request too small to send at once: it waits for the node's
 *  heartbeat, counted down beside the sentence. Under the error budget of
 *  `bps` basis points of the limit, the budget is why. */
export function admittedWaiting(node: number, hits: number, limit: number, bps: number): Blurb {
  const small: Blurb =
    hits * 10_000 < limit * bps ? blurb` · that is under ${bps / 100} % of the limit, so it waits for its next heartbeat` : [' · it waits for its next heartbeat'];
  return [...blurb`node ${node} admitted ${hits} ${noun(hits, 'hit')}`, ...small];
}

/** The one rejection: a whole request refused, with the nodes whose hits fill
 *  the window when the node sees them all and more than one sent them. */
export function rejected(node: number, hits: number, view: number, limit: number, senders: readonly number[] | null): Blurb {
  const by: Blurb = senders === null ? [] : [', admitted at nodes ', ...joined(senders.map((s) => [s])), ','];
  return [
    ...blurb`node ${node} rejected all ${hits}: it sees ${view}`,
    ...by,
    ...blurb` and ${view} + ${hits} would pass the limit of ${limit} · nothing was counted or sent`,
  ];
}

/** A request heavier than the limit: no window can ever hold it. */
export function never(node: number, hits: number, limit: number): Blurb {
  return blurb`node ${node} can never admit a request of ${hits}: the limit is ${limit} · lower Hits per Send`;
}

/** The first wave of the selected node's own packets, on Step: to its zone
 *  peers and one gateway in each other zone. */
export function waveOne(subject: number, zone: string, peers: number, gateways: readonly GatewayName[]): Blurb {
  const via = joined(gateways.map((g) => blurb`node ${g.node} (${g.zone})`));
  const to: Blurb = gateways.length === 0 ? [] : [' and to gateways ', ...via];
  return [...blurb`node ${subject} sent them to ${peers} ${noun(peers, 'node')} of ${zone}`, ...to];
}

/** The first wave of an eager flush, on Step: the zone peers and the gateways
 *  that counted the hits. */
export function waveOneCounted(peers: readonly number[], gateways: readonly number[]): Blurb {
  const via: Blurb = gateways.length === 0 ? [] : [` and ${noun(gateways.length, 'gateway')} `, ...joined(gateways.map((g) => blurb`node ${g}`))];
  return [`${noun(peers.length, 'node')} `, ...joined(peers.map((p) => [p])), ...via, ' counted them'];
}

/** The gateways' relays, on Step. */
export function waveTwo(gateways: readonly number[], reached: number, crossed: number, sent: number, lost: number): Blurb {
  const lostPart: Blurb = lost > 0 ? blurb` · ${lost} lost` : [];
  return [
    'gateways ',
    ...joined(gateways.map((g) => blurb`node ${g}`)),
    ...blurb` passed them on to ${reached} ${noun(reached, 'node')} · ${crossed} of the ${sent} packets crossed a zone`,
    ...lostPart,
  ];
}

/** Appended when a run ends with every node holding the hits. */
export const EVERY_NODE: Blurb = [' · every node has it'];

/** A run that stopped with nodes it can never reach: why. */
export function strandedStop(states: readonly Blurb[]): Blurb {
  const why = states.length === 1 ? ', so its hits cannot cross until it is reconnected' : ', so their hits cannot cross until they are reconnected';
  return ['paused: ', ...joined(states), why];
}

/** A run that stopped at its cap with a node still behind. */
export function cappedStop(periods: number, node: number): Blurb {
  return blurb`paused after ${periods} heartbeats: node ${node} is still behind`;
}

/** Hits leaving the window: all of them, or the oldest bucket's, and whether
 *  the selected node admits its next Send again. */
export function expiry(left: number, emptied: boolean, windowS: number, subject: number, admits: number | null): Blurb {
  const again: Blurb = admits === null ? [] : emptied ? blurb` · node ${subject} admits again` : blurb` · node ${subject} admits ${admits} again`;
  return emptied
    ? [...blurb`the ${left} ${noun(left, 'hit')} left the ${windowS} s window · every node stopped counting them at the same instant, without a packet`, ...again]
    : [...blurb`the oldest ${left} ${noun(left, 'hit')} left the window`, ...again];
}

/** A Step that found nothing to fly or expire. */
export const NOTHING_WAITING: Blurb = ['nothing is waiting: no hits to send and none in the window'];

/** A cut made over a wave in flight: the wave landed first. */
export const ARRIVED_FIRST: Blurb = [' · the packets already on their way arrived first'];

/** A fresh start, and what it discarded. */
export function rebuilt(scenario: string, discarded: Blurb): Blurb {
  return discarded.length === 0 ? [`Paused at the start of ${scenario}`] : [`Paused at the start of ${scenario} · discarded `, ...discarded];
}

/** A node's gateway into a zone changed. */
export function gatewayChanged(node: number, zone: string, to: number | undefined, from: number | undefined): Blurb {
  if (to === undefined) return blurb`node ${node} has no gateway into ${zone}`;
  return from === undefined
    ? blurb`node ${node} now reaches ${zone} through node ${to}`
    : blurb`node ${node} now reaches ${zone} through node ${to}, after node ${from}`;
}
