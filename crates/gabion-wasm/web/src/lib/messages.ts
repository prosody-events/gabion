// The Messages view's model and words: the filter the engine's capture takes,
// a row as the rows buffer packs it, a selected message's detail, and every
// word the inspector prints about a message. The capture counts and filters;
// the page only reads (crates/gabion-wasm/src/capture.rs).

import { fmtCount } from './format';
import { RIDERS, ROLES, type ListedRepeat, type MessageRole, type PacketKind, type Tier } from './sim/types';

/** The rows buffer's layout: `HEAD` head slots, then up to `MAX_ROWS` rows of
 *  `ROW` slots (`capture::{HEAD_SLOTS, MAX_ROWS, ROW_SLOTS}`). */
export const HEAD = 40;
export const ROW = 11;
export const MAX_ROWS = 64;
export const ROWS_LEN = HEAD + MAX_ROWS * ROW;

/** Mirror of `crate::event::MessageType`, in By type's fixed order: the type
 *  filter's bits are these indices. */
export const TYPES = [
  'OwnData',
  'RelayData',
  'Repeat',
  'Skip',
  'Ack',
  'GapReport',
  'Probe',
  'ProbeAnswer',
  'CannotRelay',
  'SweepRequest',
  'SweepPage',
] as const;
export type MessageType = (typeof TYPES)[number];

/** Each type's word. */
export const TYPE_WORD: Readonly<Record<MessageType, string>> = {
  OwnData: 'own data',
  RelayData: 'relay data',
  Repeat: 'repeat',
  Skip: 'skip',
  Ack: 'ack',
  GapReport: 'gap report',
  Probe: 'probe',
  ProbeAnswer: 'probe answer',
  CannotRelay: 'cannot relay',
  SweepRequest: 'sweep request',
  SweepPage: 'sweep page',
};

/** The types By type shows even at zero: they answer whether the page
 *  records acknowledgements and NAKs. */
export const ALWAYS_SHOWN: ReadonlySet<MessageType> = new Set(['Ack', 'GapReport']);

/** Mirror of `capture::Filter`. */
export type Way = 'Both' | 'Sent' | 'Received';
export type PairWay = 'Both' | 'AToB' | 'BToA';
export type TierFilter = 'Any' | 'InZone' | 'CrossZone';
export type MessageScope =
  | { kind: 'All'; tier: TierFilter }
  | { kind: 'Node'; node: number; way: Way; tier: TierFilter }
  | { kind: 'Pair'; pair: [number, number]; way: PairWay };
export interface MessageFilter {
  scope: MessageScope;
  /** One bit per `TYPES` index; 0 lists every type. */
  types: number;
  lost_only: boolean;
}

/** The view a reader chose, kept on the run: whose messages, which way and
 *  tier, which types, lost only, and the message selected. The node scope
 *  follows the selection. */
export interface MessagesView {
  scope: 'node' | 'pair' | 'all';
  /** The pair's other node, with the selected one. */
  other: number;
  way: Way;
  pairWay: PairWay;
  tier: TierFilter;
  types: number;
  lostOnly: boolean;
}

export const FRESH_VIEW: MessagesView = { scope: 'node', other: -1, way: 'Both', pairWay: 'Both', tier: 'Any', types: 0, lostOnly: false };

/** The engine's filter for `view` with `selected` the selected node. */
export function filterOf(view: MessagesView, selected: number): MessageFilter {
  const scope: MessageScope =
    view.scope === 'all'
      ? { kind: 'All', tier: view.tier }
      : view.scope === 'pair' && view.other !== selected
        ? { kind: 'Pair', pair: [selected, view.other], way: view.pairWay }
        : { kind: 'Node', node: selected, way: view.way, tier: view.tier };
  return { scope, types: view.types, lost_only: view.lostOnly };
}

/** What a message's sending came to. */
export type Fate = 'delivered' | 'in flight' | 'lost';
/** Why a lost message was lost (`capture::LostBy`). */
export type LostBy = 'Blocked' | 'Loss' | 'QueueFull';

const FATES: readonly { fate: Fate; by?: LostBy }[] = [
  { fate: 'delivered' },
  { fate: 'in flight' },
  { fate: 'lost', by: 'Blocked' },
  { fate: 'lost', by: 'Loss' },
  { fate: 'lost', by: 'QueueFull' },
];

/** One row of the view, decoded from the rows buffer. */
export type Row =
  | {
      kind: 'message';
      ordinal: number;
      seq: number;
      at: number;
      src: number;
      dst: number;
      bytes: number;
      cells: number;
      role: MessageRole;
      riders: number;
      fate: Fate;
      by: LostBy | undefined;
      tier: Tier;
      repeat: ListedRepeat | undefined;
      /** 2 selected, 1 related to the selection, else 0. */
      relation: number;
      flags: number;
      /** A data message's range `(a, b]`, or another's own and relay marks. */
      a: number;
      b: number;
    }
  | { kind: 'marker'; ordinal: number; at: number; marker: Marker }
  | { kind: 'interval'; ordinal: number; ms: number }
  | { kind: 'gone'; ordinal: number };

/** Mirror of `capture::MarkerKind`, as a row carries it. */
export type Marker =
  | { kind: 'request'; node: number; admitted: number; rejected: number }
  | { kind: 'gateway'; origin: number; zone: number; from: number | undefined; to: number | undefined }
  | { kind: 'added'; node: number }
  | { kind: 'removed'; node: number }
  | { kind: 'heal' };

const id = (v: number): number | undefined => (v < 0 ? undefined : v);

function markerOf(r: Float64Array, o: number): Marker {
  const [a, b, c, d] = [r[o + 2], r[o + 3], r[o + 4], r[o + 5]];
  switch (r[o]) {
    case 0:
      return { kind: 'request', node: a, admitted: b, rejected: c };
    case 1:
      return { kind: 'gateway', origin: a, zone: b, from: id(c), to: id(d) };
    case 2:
      return { kind: 'added', node: a };
    case 3:
      return { kind: 'removed', node: a };
    default:
      return { kind: 'heal' };
  }
}

/** Row `i` of a read that started at ordinal `first`. */
export function rowAt(r: Float64Array, first: number, i: number): Row {
  const o = HEAD + i * ROW;
  const ordinal = first + i;
  const code = r[o + 6];
  switch (code >>> 24) {
    case 0: {
      const fate = FATES[(code >>> 16) & 0xf] ?? FATES[0];
      const repeat = (code >>> 22) & 3;
      return {
        kind: 'message',
        ordinal,
        seq: r[o],
        at: r[o + 1],
        src: r[o + 2],
        dst: r[o + 3],
        bytes: r[o + 4],
        cells: r[o + 5],
        role: ROLES[code & 0xff],
        riders: (code >>> 8) & 0xff,
        fate: fate.fate,
        by: fate.by,
        tier: ((code >>> 20) & 3) === 1 ? 'CrossZone' : 'InZone',
        repeat: repeat === 1 ? 'AfterTimeout' : repeat === 2 ? 'AfterGapReport' : undefined,
        relation: r[o + 7],
        flags: r[o + 8],
        a: r[o + 9],
        b: r[o + 10],
      };
    }
    case 1:
      return { kind: 'marker', ordinal, at: r[o + 1], marker: markerOf(r, o) };
    case 2:
      return { kind: 'interval', ordinal, ms: r[o + 1] };
    default:
      return { kind: 'gone', ordinal };
  }
}

/** The head of the rows buffer. */
export interface Head {
  total: number;
  kept: number;
  viewLen: number;
  firstKept: number;
  inFlight: number;
  inFlightAll: number;
  lostShown: number;
  bytesShown: number;
  shown: number;
  scopeCount: number;
  scopeLost: number;
  counts: readonly number[];
  lost: readonly number[];
}

export function headOf(r: Float64Array): Head {
  const n = TYPES.length;
  return {
    total: r[0],
    kept: r[1],
    viewLen: r[2],
    firstKept: r[3],
    inFlight: r[4],
    inFlightAll: r[5],
    lostShown: r[6],
    bytesShown: r[7],
    shown: r[8],
    scopeCount: r[9],
    scopeLost: r[10],
    counts: Array.from(r.subarray(11, 11 + n)),
    lost: Array.from(r.subarray(11 + n, 11 + 2 * n)),
  };
}

/** Mirror of `capture::Detail`: one message, read, owned by the page. */
export interface MessageDetail {
  seq: number;
  at_ms: number;
  src: number;
  dst: number;
  wire_len: number;
  role: MessageRole;
  riders: number;
  repeat: 'AfterTimeout' | 'AfterGapReport' | 'AfterAnswer' | undefined;
  tier: Tier;
  fate: { kind: 'InFlight' } | { kind: 'Delivered' } | { kind: 'Lost'; by: LostBy };
  bytes: number[];
  body_kept: boolean;
  header: {
    kind: PacketKind;
    flags: number;
    cells: number;
    sender: number | undefined;
    sender_incarnation: number;
    sender_zone: number;
    ack_incarnation: number;
    own_ack: number;
    own_ack_as_gateway: boolean;
    relay_ack: number;
    range: [number, number] | undefined;
  };
  cells: { origin: number | undefined; rule: string; key: string; bucket: number; count: number; incarnation: number; updated_ms: number }[];
  links: { relation: Relation; seq: number; role: MessageRole; offset_ms: number }[];
  asks_answer: boolean;
}

/** Mirror of `capture::Relation`. */
export type Relation =
  | 'AnsweredBy'
  | 'Answers'
  | 'AcknowledgedBy'
  | 'Acknowledges'
  | 'ReportedMissingBy'
  | 'ReportsMissing'
  | 'RevealedBy'
  | 'Reveals'
  | 'RepeatedAs'
  | 'Repeats';

export const RELATION_WORD: Readonly<Record<Relation, string>> = {
  AnsweredBy: 'answered by',
  Answers: 'answers',
  AcknowledgedBy: 'acknowledged by',
  Acknowledges: 'acknowledges',
  ReportedMissingBy: 'reported missing by',
  ReportsMissing: 'reports missing',
  RevealedBy: 'revealed by',
  Reveals: 'reveals',
  RepeatedAs: 'repeated as',
  Repeats: 'repeats',
};

/** Each role's word. "Ack" is the page's one abbreviation. */
export const ROLE_WORD: Readonly<Record<MessageRole, string>> = {
  OwnData: 'own data',
  RelayData: 'relay data',
  SweepPage: 'sweep page',
  OwnSkip: 'skip',
  RelaySkip: 'skip',
  GapReport: 'gap report',
  Probe: 'probe',
  SweepRequest: 'sweep request',
  ProbeAnswer: 'probe answer',
  Ack: 'ack',
};

/** A node as a row names it: removed ones say so. */
export const nodeName = (id: number, live: ReadonlySet<number>): string => (live.has(id) ? `node ${id}` : `node ${id} (removed)`);

/** Positions `(lower, upper]` written inclusive: "position 7", "positions 7–8". */
export function positions(lower: number, upper: number): string {
  return upper - lower === 1 ? `position ${upper}` : `positions ${lower + 1}–${upper}`;
}

/** The gist of a message: what it says, in a few words. */
export function gist(m: { role: MessageRole; flags: number; cells: number; a: number; b: number }): string {
  switch (m.role) {
    case 'OwnData':
    case 'RelayData':
    case 'OwnSkip':
    case 'RelaySkip':
      return positions(m.a, m.b);
    case 'SweepPage':
      return `${fmtCount(m.cells)} ${m.cells === 1 ? 'cell' : 'cells'}`;
    case 'GapReport':
      return (m.flags & GAP_OWN) !== 0 ? `missing from position ${m.a + 1}` : `missing relays from position ${m.b + 1}`;
    case 'Probe':
      return 'asks for an answer';
    case 'SweepRequest':
      return 'asks for every live count';
    case 'Ack':
    case 'ProbeAnswer':
      return m.b > 0 ? `has through position ${m.a} · relays through ${m.b}` : `has through position ${m.a}`;
  }
}

/** The header's flag bits (`gabion::wire::header`). */
export const GAP_OWN = 0x02;
export const GAP_RELAY = 0x04;

/** The riders' words, in their fixed order. */
export function riderWords(riders: number, repeat: ListedRepeat | undefined, zone: string | undefined): string[] {
  const words: string[] = [];
  if ((riders & RIDERS.GAP_REPORT) !== 0) words.push('gap report');
  if (repeat !== undefined) words.push('repeat');
  if ((riders & RIDERS.PROBE) !== 0) words.push('probe');
  if ((riders & RIDERS.PROBE_ANSWER) !== 0) words.push('probe answer');
  if ((riders & RIDERS.CANNOT_RELAY) !== 0) words.push('cannot relay');
  if ((riders & RIDERS.FOR_ZONE) !== 0) words.push(zone === undefined ? 'for its zone' : `for ${zone}`);
  return words;
}

/** An instant in seconds with three decimals: "3.005". */
export const fmtInstant = (ms: number): string => (ms / 1000).toFixed(3);

/** A signed offset: "+312 ms", "−5 ms". */
export const fmtOffset = (ms: number): string => `${ms < 0 ? '−' : '+'}${Math.abs(Math.round(ms))} ms`;

/** Whether a message asks its receiver for an answer: data, a probe, or a
 *  probe riding another message. */
export const asksAnswer = (m: { role: MessageRole; riders: number }): boolean =>
  m.role === 'OwnData' || m.role === 'RelayData' || m.role === 'Probe' || (m.riders & RIDERS.PROBE) !== 0;

/** A marker's words. */
export function markerWords(m: Marker, zone: (z: number) => string): string {
  switch (m.kind) {
    case 'request':
      if (m.rejected === 0) return `node ${m.node} admitted ${fmtCount(m.admitted)} ${m.admitted === 1 ? 'request' : 'requests'}`;
      if (m.admitted === 0) return `node ${m.node} refused ${fmtCount(m.rejected)} ${m.rejected === 1 ? 'request' : 'requests'}`;
      return `node ${m.node} admitted ${fmtCount(m.admitted)} of ${fmtCount(m.admitted + m.rejected)} requests`;
    case 'gateway':
      if (m.to === undefined) return `node ${m.origin} lost its gateway into ${zone(m.zone)}`;
      if (m.from === undefined) return `node ${m.origin} chose node ${m.to} as its gateway into ${zone(m.zone)}`;
      return `node ${m.origin} switched its gateway into ${zone(m.zone)} from node ${m.from} to node ${m.to}`;
    case 'added':
      return `node ${m.node} joined`;
    case 'removed':
      return `node ${m.node} was removed`;
    case 'heal':
      return 'every link healed';
  }
}

/** What the detail's sentence needs beyond the message: node names, a
 *  node's zone, and the words for the cut that blocked a lost one. */
export interface Says {
  name: (id: number) => string;
  zoneOf: (id: number) => string | undefined;
  cut: string | undefined;
}

const capital = (s: string): string => s.charAt(0).toUpperCase() + s.slice(1);

/** One or two sentences: what the message said, then what came of it. */
export function sentenceOf(d: MessageDetail, says: Says): string {
  const from = says.name(d.src);
  const to = says.name(d.dst);
  const link = (r: Relation) => d.links.find((l) => l.relation === r);
  const range = d.header.range;
  const span = range === undefined ? '' : positions(range[0], range[1]);
  let first: string;
  switch (d.role) {
    case 'OwnData':
    case 'RelayData': {
      const what = d.role === 'OwnData' ? `${from}'s own changes` : 'changes it relays';
      const zone = (d.riders & RIDERS.FOR_ZONE) !== 0 ? `, for ${to} to pass on into ${says.zoneOf(d.dst) ?? 'its zone'}` : '';
      if (d.repeat === undefined) first = `${capital(from)} sent ${to} ${what}, ${span}${zone}.`;
      else {
        const report = link('Repeats');
        const why =
          d.repeat === 'AfterGapReport'
            ? `because ${to} reported a gap`
            : d.repeat === 'AfterTimeout'
              ? 'because no ack came within one heartbeat'
              : `because ${to}'s answer showed them still owed`;
        first = `${capital(from)} sent ${span} again, ${why}${report === undefined ? '' : ` (#${report.seq})`}.`;
      }
      break;
    }
    case 'OwnSkip':
    case 'RelaySkip':
      first = `${capital(from)} told ${to} that ${span} expired before they were sent, so ${to} can count them as received.`;
      break;
    case 'GapReport': {
      const own = (d.header.flags & GAP_OWN) !== 0;
      first = own
        ? `${capital(from)} told ${to} it is missing ${to}'s changes from position ${d.header.own_ack + 1} on: a gap report, gabion's NAK.`
        : `${capital(from)} told ${to} it is missing ${to}'s relays from position ${d.header.relay_ack + 1} on: a gap report, gabion's NAK.`;
      break;
    }
    case 'Probe':
      first = `${capital(from)} asked ${to} to answer: a probe, or a greeting, which is the same bytes.`;
      break;
    case 'ProbeAnswer':
      first = `${capital(from)} answered ${to}'s probe: it has ${to}'s changes through position ${d.header.own_ack}.`;
      break;
    case 'Ack':
      first = `${capital(from)} told ${to} it has ${to}'s changes through position ${d.header.own_ack}.`;
      break;
    case 'SweepRequest':
      first = `${capital(from)} asked ${to} for every live count: a sweep request, sent when a node joins.`;
      break;
    case 'SweepPage':
      first = `${capital(from)} sent ${to} ${fmtCount(d.header.cells)} ${d.header.cells === 1 ? 'count' : 'counts'} of a sweep: every live count it holds.`;
      break;
  }
  let second = '';
  const repeated = link('RepeatedAs');
  if (d.fate.kind === 'Lost') {
    const reported = link('ReportedMissingBy');
    const recovered =
      reported !== undefined && repeated !== undefined
        ? ` ${capital(to)} reported the gap (#${reported.seq}), and ${from} repeated it (#${repeated.seq}).`
        : repeated !== undefined
          ? ` ${capital(from)} repeated it (#${repeated.seq}).`
          : '';
    second = `Lost: ${lostWords(d.fate.by, d.dst, d.src, says.cut)}.${recovered}`;
  } else if ((d.riders & RIDERS.CANNOT_RELAY) !== 0) {
    second = `It also says ${from} cannot pass hits on into its zone, so ${to} picks another gateway there.`;
  } else if ((d.header.flags & 0x08) !== 0) {
    second = `It carries cannot relay, which ${to} ignores here: ${from} is in its own zone.`;
  } else if (d.role === 'GapReport' && repeated !== undefined) {
    second = `${fmtOffset(repeated.offset_ms).replace('+', '')} later, ${to} repeated them (#${repeated.seq}).`;
  } else if (d.fate.kind === 'InFlight') {
    second = 'It is still on its way.';
  } else {
    const answer = link('AcknowledgedBy') ?? link('AnsweredBy');
    if (answer !== undefined) second = `${capital(to)} answered ${fmtOffset(answer.offset_ms).replace('+', '')} later (#${answer.seq}).`;
    else if (d.asks_answer) second = `${capital(to)} has not answered so far.`;
  }
  return second === '' ? first : `${first} ${second}`;
}

/** A lost message's reason, said plainly. */
export function lostWords(by: LostBy, dst: number, src: number, cut: string | undefined): string {
  switch (by) {
    case 'Blocked':
      return cut ?? `the link from node ${src} to node ${dst} is cut`;
    case 'Loss':
      return 'the link drops packets';
    case 'QueueFull':
      return `node ${dst}'s queue was full`;
  }
}
