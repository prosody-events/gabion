// Hand-written TypeScript mirrors of the serde shapes in
// `crates/gabion-wasm/src/{event,config}.rs`. The generated `gabion_wasm.d.ts`
// types every payload as `any` — serde-wasm-bindgen crosses the boundary as
// plain objects — so these interfaces are the real contract.
//
// Numeric boundary convention (asserted at runtime in `sim.ts`, checked
// statically by `pnpm run check`):
//
//   • u128 ids — `rule`, `key`, `node_id`, `rule_fingerprint` — cross as
//     **hex strings** ("0x…"), serialized by `crate::hex`.
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
  fanout: number;
  tick_interval_ms: number;
  target_err_bps: number;
  min_emit_interval_ms: number;
  rule_fingerprint: Hex;
  rule_limit: number;
  rule_window_ms: number;
  rule_bucket_ms: number;
  rng_seed: number;
  uniform_loss: number;
  cell_capacity: number;
}

/** Mirror of `crate::engine::LinkPolicyKind` (serde-tagged by `kind`). */
export type LinkPolicyKind =
  | { kind: 'Pass' }
  | { kind: 'Block' }
  | { kind: 'DropFirst'; count: number }
  | { kind: 'DropProb'; p: number };

// -- Events -----------------------------------------------------------------

/** Mirror of `crate::event::EventKind` (serde-tagged by `type`). */
export type EventKind =
  | { type: 'Tick'; node: number }
  | { type: 'ThresholdFire'; node: number }
  | { type: 'PacketSent'; src: number; dst: number; bytes: number }
  | { type: 'PacketDelivered'; src: number; dst: number; bytes: number }
  | { type: 'PacketDropped'; src: number; dst: number; bytes: number }
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
  origin: number | null;
  is_local: boolean;
}

/** Mirror of `crate::event::PeerView`. */
export interface PeerView {
  /** The peer's stable id, if the engine can resolve its address. */
  id: number | null;
  /** The peer's gossip node id once an inbound packet has revealed it. */
  node_id: Hex | null;
}

/** Mirror of `crate::event::NodeState`. `id` is the node's stable identity (see
 *  the Rust module note): assigned once, never reused, so it survives other
 *  nodes joining and leaving and has gaps once a member has been removed. */
export interface NodeState {
  id: number;
  aggregate_total: number;
  ticks_total: number;
  threshold_fires: number;
  cells: CellView[];
  peers: PeerView[];
}

/** Mirror of `crate::event::ClusterState` — the full per-node dump pulled on
 *  seek / re-render. `oracle_total` is the simulator-only ground truth. */
export interface ClusterState {
  virtual_ms: number;
  tick: number;
  nodes: NodeState[];
  oracle_total: number;
}
