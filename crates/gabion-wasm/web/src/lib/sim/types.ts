// Hand-written TypeScript mirrors of the serde shapes in
// `crates/gabion-wasm/src/{event,config}.rs`. The generated `gabion_wasm.d.ts`
// types every payload as `any` — serde-wasm-bindgen crosses the boundary as
// plain objects — so these interfaces are the real contract.
//
// Numeric boundary convention (asserted at runtime in `sim.ts`, checked
// statically by `pnpm run check`):
//
//   • u128 ids — `rule`, `key`, `node_id` — cross as
//     **hex strings** ("0x…"), serialized by `crate::hex`.
//   • `Option::None` crosses as `undefined` (serde-wasm-bindgen's default), so
//     every Option field is typed `T | undefined`, never `T | null`.
//   • u64 counts and timestamps — `virtual_ms`, `tick`, `count`,
//     `aggregate_total`, `oracle_total`, … — cross as JS **numbers**.
//     serde-wasm-bindgen's `serialize_large_number_types_as_bigints` defaults
//     to `false`, so any u64 within 2^53 is a plain number. The visualizer's
//     hit volumes stay far below that, so `number` is exact here. If a future
//     scenario can exceed 2^53, flip that serializer flag and widen these to
//     `bigint`.

/** A u128 identifier rendered as a "0x"-prefixed hex string across the boundary. */
export type Hex = string;

// -- Configuration ----------------------------------------------------------

/** Mirror of `crate::config::SimConfig`. Every field is `#[serde(default)]`, so
 *  a partial object is valid — missing fields take the production-aligned
 *  defaults baked into the Rust side. */
export interface SimConfig {
  nodes: number;
  tick_interval_ms: number;
  target_err_bps: number;
  min_emit_interval_ms: number;
  rule_limit: number;
  rule_window_ms: number;
  rule_bucket_ms: number;
  uniform_loss: number;
  /** Zones nodes are placed in, 1 to 6: node `i` at build is in zone `i mod zones`. */
  zones: number;
  zone_mode: ZoneMode;
}

/** Mirror of `crate::config::ZoneMode`: whether nodes are told their zone. */
export type ZoneMode = 'Configured' | 'Unset';

/** Mirror of `crate::engine::LinkPolicyKind` (serde-tagged by `kind`). */
export type LinkPolicyKind =
  | { kind: 'Pass' }
  | { kind: 'Block' }
  | { kind: 'DropFirst'; count: number }
  | { kind: 'DropProb'; p: number };

// -- Events -----------------------------------------------------------------

/** Mirror of `crate::event::PacketKind`, read from the packet's header. */
export type PacketKind = 'Ack' | 'Own' | 'Relay' | 'Sweep' | 'SweepRequest';

/** Mirror of `crate::event::Tier`: whether a packet stayed inside its sender's
 *  zone, by placement. */
export type Tier = 'InZone' | 'CrossZone';

/** Mirror of `crate::event::EventKind` (serde-tagged by `type`). */
export type EventKind =
  | { type: 'Tick'; node: number }
  | { type: 'ThresholdFire'; node: number }
  | { type: 'PacketSent'; src: number; dst: number; bytes: number; kind: PacketKind; tier: Tier }
  | { type: 'PacketDelivered'; src: number; dst: number; bytes: number }
  | { type: 'PacketDropped'; src: number; dst: number; bytes: number; kind: PacketKind; tier: Tier }
  | { type: 'GatewayChanged'; origin: number; zone: number; from: number | undefined; to: number | undefined }
  | { type: 'CellCreated'; node: number; rule: Hex; key: Hex; bucket: number; count: number }
  | { type: 'CellUpdated'; node: number; rule: Hex; key: Hex; bucket: number; count: number }
  | { type: 'CellExpired'; node: number; rule: Hex; key: Hex; bucket: number };

/** Mirror of `crate::event::Event`. */
export interface SimEvent {
  tick: number;
  virtual_ms: number;
  kind: EventKind;
}

/** Mirror of `crate::event::EventBatch` — the result of one step / submit. */
export interface EventBatch {
  events: SimEvent[];
  virtual_ms: number;
  tick: number;
}

// -- Snapshot ---------------------------------------------------------------

/** Mirror of `crate::event::CellView`. */
export interface CellView {
  rule: Hex;
  key: Hex;
  bucket: number;
  count: number;
  age_ms: number;
  origin: number | undefined;
  is_local: boolean;
}

/** What a peer is to a node (`gabion::gossip::Role`, snake_case). */
export type Role = 'unheard' | 'zone' | 'gateway' | 'remote' | 'incompatible';

/** Whether a node waits for a peer's answer (`gabion::gossip::AnswerState`). */
export type AnswerState = 'settled' | 'owed' | 'suspect' | 'cannot_relay';

/** Each answer state as the page shows it: whether the gateway is suspected
 *  (a hollow ◇ rather than a filled ◆) and the word the targets table prints. */
export const ANSWER: Record<AnswerState, { readonly suspect: boolean; readonly label: string }> = {
  settled: { suspect: false, label: 'ok' },
  owed: { suspect: false, label: 'ok' },
  suspect: { suspect: true, label: 'suspected' },
  cannot_relay: { suspect: true, label: 'cannot relay' },
};

/** Mirror of `crate::event::PeerView`. */
export interface PeerView {
  /** The peer's stable id, if the engine can resolve its address. */
  id: number | undefined;
  /** The zone the peer reports, by index; absent while unknown or with no zone. */
  zone: number | undefined;
  role: Role;
  answer: AnswerState;
  /** How long the oldest packet waiting for the peer's answer has waited. */
  unanswered_ms: number | undefined;
  /** Own-stream positions sent to the peer and not yet acknowledged. */
  own_unacked: number;
  /** The same for the relay stream. */
  relay_unacked: number;
  /** The peer's gossip node id once an inbound packet has revealed it. */
  node_id: Hex | undefined;
}

/** Mirror of `crate::event::StoreStats` — the CRDT store's occupancy and
 *  saturation counters (itself a mirror of `gabion::crdt::CellStoreStats`). */
export interface StoreStats {
  active_cells: number;
  cell_capacity: number;
  rule_slots_used: number;
  rule_slots_capacity: number;
  node_slots_used: number;
  node_slots_capacity: number;
  cell_store_full_rejects: number;
  rule_dictionary_full_rejects: number;
  node_dictionary_full_rejects: number;
}

/** Mirror of `crate::event::NodeState`. `id` is the node's stable identity (see
 *  the Rust module note): assigned once, never reused, so it survives other
 *  nodes joining and leaving and has gaps once a member has been removed. The
 *  counters mirror the runtime's `AdminSnapshot`; `gateway_for`,
 *  `own_data_bytes` and `missing_for_ms` the engine counts; and `store_stats` mirrors its
 *  `CellStoreStats`. */
export interface NodeState {
  id: number;
  /** The zone the node is placed in. */
  zone: number;
  /** The on-the-wire gossip identity (`NodeIdentity.node_id`), distinct from the
   *  display `id`. */
  node_id: Hex;
  /** Incarnation — bumped on restart; always 1 in the sim. */
  incarnation: number;
  aggregate_total: number;
  /** Heartbeat rounds the runtime has run. */
  rounds_heartbeat: number;
  /** Gateways replaced because they went unanswered or said they cannot relay. */
  gateway_failovers: number;
  /** Gap reports sent: a peer's stream skipped positions. */
  gap_reports_sent: number;
  /** Own-stream timeouts that resent from the last ack. */
  rto_rewinds_own: number;
  /** The same for the relay stream. */
  rto_rewinds_relay: number;
  /** Join sweeps this node started answering. */
  sweeps_served: number;
  /** Origins whose runtime sends this node's zone their changes through this
   *  node: the gateway rows of every node, inverted. */
  gateway_for: number;
  /** Bytes of the node's own-stream packets it has sent since it joined,
   *  delivered or lost. */
  own_data_bytes: number;
  /** How long ago the origin of the oldest change the node lacks last updated
   *  that change's cell: a lower bound on how long it has been behind.
   *  Absent while it holds every change. */
  missing_for_ms: number | undefined;
  /** Per-rule error budget ε at the most recent request; pending crossing it
   *  triggers an eager (threshold) flush. 0 until a request is seen. */
  error_budget: number;
  store_stats: StoreStats;
}

/** Mirror of `crate::event::Inspected`: the inspected node's cells and peer
 *  rows, read at the same instant as the rest of the snapshot. */
export interface Inspected {
  id: number;
  cells: CellView[];
  peers: PeerView[];
}

/** Mirror of `crate::event::ZoneView`: a zone and its live members. */
export interface ZoneView {
  name: string;
  live: number;
}

/** Mirror of `crate::event::ClassBytes`. */
export interface ClassBytes {
  data: number;
  control: number;
}

/** Mirror of `crate::event::BytesByTier`: bytes sent since the session began,
 *  delivered or lost. */
export interface BytesByTier {
  in_zone: ClassBytes;
  cross_zone: ClassBytes;
}

/** Mirror of `crate::event::ClusterState` — every node's state, pulled each
 *  frame, with one inspected node's rows. `oracle_total` is the simulator-only
 *  ground truth. */
export interface ClusterState {
  virtual_ms: number;
  tick: number;
  nodes: NodeState[];
  /** The rows of the node the snapshot was taken inspecting; absent when none
   *  was, or it is not live. */
  inspected?: Inspected;
  /** Every zone in index order, with its live members. */
  zones: ZoneView[];
  bytes_by_tier: BytesByTier;
  oracle_total: number;
  /** The bucket epoch the watched rule sits in at `virtual_ms`, from the CRDT's
   *  `RuleDescriptor::current_epoch`. The Strata right-anchors its fixed-width
   *  grid on this epoch rather than recomputing the boundary in TS. */
  bucket_epoch_now: number;
}
