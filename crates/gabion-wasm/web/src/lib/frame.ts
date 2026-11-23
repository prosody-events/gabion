// The frame as gabion encodes it: the 86-byte header's fields, mirroring
// `write_header` (crates/gabion/src/wire/header.rs), and the body's sections
// (crates/gabion/src/wire/body.rs). One table, because every kind shares the
// header. The frame diagram and the hex view both draw from it, and a test
// checks that every byte of a real packet belongs to exactly one field, so
// the diagram cannot drift from the codec.

/** One field: its first byte, its length, its plain name, its wire name, and
 *  how to read its value from the frame's bytes. */
export interface Field {
  readonly id: string;
  readonly at: number;
  readonly len: number;
  readonly plain: string;
  readonly wire: string;
  /** One sentence, at most 50 characters, for the field line. */
  readonly says: string;
}

export const HEADER_LEN = 86;

/** The header's fields, in byte order. */
export const HEADER_FIELDS: readonly Field[] = [
  { id: 'magic', at: 0, len: 4, plain: 'magic', wire: 'MAGIC', says: 'Marks the bytes as a gabion packet.' },
  { id: 'version', at: 4, len: 2, plain: 'version', wire: 'VERSION', says: 'The wire format’s version.' },
  { id: 'kind', at: 6, len: 1, plain: 'kind', wire: 'kind', says: 'What the packet carries.' },
  { id: 'flags', at: 7, len: 1, plain: 'flags', wire: 'flags', says: 'Eight signals, one bit each.' },
  { id: 'cluster', at: 8, len: 16, plain: 'cluster', wire: 'cluster_id_hash', says: 'Which cluster sent it.' },
  { id: 'sender', at: 24, len: 16, plain: 'from node', wire: 'sender.id', says: 'The node that sent it.' },
  { id: 'start', at: 40, len: 4, plain: 'from node’s start', wire: 'sender.incarnation', says: 'Which start of that node sent it.' },
  { id: 'zone', at: 44, len: 4, plain: 'from node’s zone', wire: 'sender_zone', says: 'The zone the node says it is in.' },
  { id: 'cells', at: 48, len: 2, plain: 'cells', wire: 'cell_count', says: 'How many counts the body carries.' },
  { id: 'ack-start', at: 50, len: 4, plain: 'ack for start', wire: 'acks.incarnation', says: 'The receiver’s start the acks are for.' },
  { id: 'own-ack', at: 54, len: 8, plain: 'has its own changes through', wire: 'acks.own', says: 'How far it holds the receiver’s changes.' },
  { id: 'relay-ack', at: 62, len: 8, plain: 'has its relays through', wire: 'acks.relay', says: 'How far it holds the receiver’s relays.' },
  { id: 'lower', at: 70, len: 8, plain: 'after position', wire: 'range.lower', says: 'The data starts after this position.' },
  { id: 'upper', at: 78, len: 8, plain: 'through position', wire: 'range.upper', says: 'The data runs through this position.' },
];

/** The rows the diagram breaks the header into: at field boundaries, so no
 *  field wraps, each row at most 16 bytes on one shared scale. */
export const FRAME_ROWS: readonly (readonly string[])[] = [
  ['magic', 'version', 'kind', 'flags'],
  ['cluster'],
  ['sender'],
  ['start', 'zone'],
  ['cells', 'ack-start'],
  ['own-ack'],
  ['relay-ack'],
  ['lower', 'upper'],
];

/** The flag bits, bit 7 first, as RFC diagrams order them. */
export const FLAG_BITS: readonly { bit: number; name: string; says: string }[] = [
  { bit: 7, name: 'reserved, always 0', says: 'Unused: always 0.' },
  { bit: 6, name: 'probe', says: 'Asks the receiver to answer.' },
  { bit: 5, name: 'probe answer', says: 'Answers the receiver’s probe.' },
  { bit: 4, name: 'ack as its gateway', says: 'Its own-changes ack is as a gateway.' },
  { bit: 3, name: 'cannot relay', says: 'It cannot pass hits on into its zone.' },
  { bit: 2, name: 'gap in relays', says: 'It is missing some of the relays.' },
  { bit: 1, name: 'gap in its own changes', says: 'It is missing some own changes.' },
  { bit: 0, name: 'for zone (pass on)', says: 'The gateway passes these on.' },
];

/** The kinds, by kind byte. */
export const KIND_WORDS = ['ack', 'own', 'relay', 'sweep', 'sweep request'] as const;

/** Little-endian unsigned integer of `len` bytes at `at`, as a number (exact
 *  below 2⁵³) or, past that, a bigint. */
export function readLe(bytes: readonly number[], at: number, len: number): bigint {
  let v = 0n;
  for (let i = len - 1; i >= 0; i--) v = (v << 8n) | BigInt(bytes[at + i] ?? 0);
  return v;
}

/** The body's sections for a packet of `n` cells: the two dictionaries' sizes
 *  are read from the body itself. */
export function bodyFields(bytes: readonly number[], n: number): Field[] {
  if (n === 0 || bytes.length <= HEADER_LEN) return [];
  let at = HEADER_LEN;
  const rules = bytes[at] ?? 0;
  const out: Field[] = [];
  const push = (id: string, len: number, plain: string, wire: string, says: string): void => {
    out.push({ id, at, len, plain, wire, says });
    at += len;
  };
  push('rule-count', 1, 'rules', 'rule_dict_len', 'How many rules the cells name.');
  push('rules', 16 * rules, 'rule ids', 'rule_dict', 'The rules’ ids, once each.');
  const nodes = bytes[at] ?? 0;
  push('node-count', 1, 'nodes', 'node_dict_len', 'How many origins the cells name.');
  push('nodes', 16 * nodes, 'node ids', 'node_dict', 'The origins’ ids, once each.');
  push('rule-slot', n, 'rule of each cell', 'rule_slot', 'Each cell’s rule, by its place.');
  push('key', 16 * n, 'key of each cell', 'key_hash', 'Each cell’s key.');
  push('bucket', 4 * n, 'second of each cell', 'bucket', 'Each cell’s window second.');
  push('node-slot', n, 'origin of each cell', 'node_slot', 'Each cell’s origin, by its place.');
  push('origin-start', 4 * n, 'origin’s start', 'origin_incarnation', 'Which start of the origin counted it.');
  push('updated', 8 * n, 'last updated', 'last_update_millis', 'When the origin last changed it.');
  push('count', 4 * n, 'count of each cell', 'count', 'Each cell’s total: the larger is kept.');
  return out;
}

/** The field that owns byte `i`, header and body alike. */
export function fieldAt(fields: readonly Field[], i: number): Field | undefined {
  return fields.find((f) => i >= f.at && i < f.at + f.len);
}
