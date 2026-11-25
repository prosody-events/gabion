// A thin, typed wrapper over the generated wasm `Sim`. It owns the one-time
// module init, converts the JS `number` arguments the UI works in into the
// `bigint`s wasm-bindgen expects for u64 parameters, and stamps the untyped
// boundary results with the contracts declared in `types.ts`.

import init, { Sim as WasmSim, default_config, max_zones } from '../../wasm/gabion_wasm.js';
import type { Cut } from '../cuts';
import { ROWS_LEN, type MessageDetail, type MessageFilter } from '../messages';
import type { ClusterState, EventBatch, LinkPolicyKind, SimConfig, StepUntil } from './types';

/** A live node as a fault reads it: its id and zone. */
export interface Member {
  readonly id: number;
  readonly zone: number;
}

let initPromise: Promise<unknown> | null = null;
// The module's memory, whose buffer the capture's rows are read through.
let memory: WebAssembly.Memory | null = null;

/** Initialize the wasm module exactly once, even under concurrent callers. */
function ensureInit(): Promise<unknown> {
  if (initPromise === null) {
    initPromise = init().then((out) => {
      memory = out.memory;
    });
  }
  return initPromise;
}

/** Confirm the numeric boundary convention `types.ts` documents actually holds
 *  at runtime: u64 fields arrive as JS numbers. Runs
 *  once, on the first batch crossed, and throws loudly if the contract drifts
 *  (e.g. a serde-wasm-bindgen default change) rather than silently corrupting
 *  every count downstream. */
let boundaryChecked = false;
function assertBoundary(batch: EventBatch): void {
  if (boundaryChecked) return;
  boundaryChecked = true;
  if (typeof batch.virtual_ms !== 'number') {
    throw new TypeError(
      `wasm boundary contract broken: expected u64 fields as JS numbers, got ` +
        `virtual_ms=${typeof batch.virtual_ms}. ` +
        `Update web/src/lib/sim/types.ts (and the serializer in the Rust crate).`,
    );
  }
}

/** Whether faults `a` and `b` are the same fault. */
const sameCut = (a: Cut, b: Cut): boolean =>
  a.kind === b.kind && ('zone' in a ? a.zone : -1) === ('zone' in b ? b.zone : -1) && ('node' in a ? a.node : -1) === ('node' in b ? b.node : -1);

/** A live simulation. Mirrors the wasm `Sim` surface with idiomatic JS types,
 *  and keeps the faults the page made as a list of what each means. */
export class Sim {
  #inner: WasmSim;
  // The capture's rows buffer, as one view, rebuilt only when a growth of
  // the module's memory detaches it.
  #rows: Float64Array | null = null;
  // Every fault made since the last heal, in the order made, each once: the
  // engine keeps link policies, not what they were for. Replaced, never
  // mutated, so a reader may keep the list it was given.
  #cuts: readonly Cut[] = [];

  private constructor(inner: WasmSim) {
    this.#inner = inner;
  }

  /** Build a cluster and spawn its engine. A partial config is fine; omitted
   *  fields take the Rust-side production defaults. Throws on an invalid
   *  config, with the engine's message. */
  static async create(config: Partial<SimConfig> = {}): Promise<Sim> {
    await ensureInit();
    return new Sim(new WasmSim(config));
  }

  /** The Rust-side production-aligned `SimConfig` defaults — the single source
   *  the control sliders open from (no hand-maintained TS mirror, so changing
   *  `gabion::defaults` is the only edit). */
  static async defaultConfig(): Promise<SimConfig> {
    await ensureInit();
    return default_config() as SimConfig;
  }

  /** The most zones a session places nodes in: the zone stepper's ceiling. */
  static async maxZones(): Promise<number> {
    await ensureInit();
    return max_zones();
  }

  /** A burst of `requests` one-hit requests for `key` at `node`, at the
   *  current virtual time: the node decides them in order first (the batch's
   *  `Decided` event), and the admitted requests' eager flush is delivered at
   *  the same instant before it resolves: its packets and the receivers' cell
   *  events are in the batch. With `until` an `Event`, a flush's wave is
   *  held, as a step by event holds one, until the next call. Throws for zero
   *  requests. */
  async submitRequest(node: number, key: number, requests: number, until: StepUntil = 'Time'): Promise<EventBatch> {
    const batch = (await this.#inner.submit_request(node, BigInt(key), BigInt(requests), until)) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Advance virtual time by at most `deltaMs`, stopping where `until` says,
   *  resolving to the events produced. */
  async step(deltaMs: number, until: StepUntil = 'Time'): Promise<EventBatch> {
    const batch = (await this.#inner.step(BigInt(deltaMs), until)) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Pull every node's state of `key` for a re-render, with the identity,
   *  store and peer rows of node `inspect` alone. */
  async snapshot(inspect: number | null, key: number): Promise<ClusterState> {
    return (await this.#inner.snapshot(inspect, BigInt(key))) as ClusterState;
  }

  /** The faults made since the last heal. */
  cuts(): readonly Cut[] {
    return this.#cuts;
  }

  /** Make fault `cut` over the live `nodes`, and list it, unless it is
   *  already made. */
  async cut(cut: Cut, nodes: readonly Member[]): Promise<void> {
    if (this.#cuts.some((c) => sameCut(c, cut))) return;
    this.#cuts = [...this.#cuts, cut];
    await this.#apply(cut, nodes);
  }

  /** Apply every listed fault again over the live `nodes`, so a node that
   *  joined since is cut as its zone or peers are. Blocking a blocked link is
   *  a no-op. */
  async reapply(nodes: readonly Member[]): Promise<void> {
    for (const cut of this.#cuts) await this.#apply(cut, nodes);
  }

  /** Undo fault `cut` alone: the scenario's own network on every link, its
   *  loss included, then every other listed fault again over the live
   *  `nodes`. */
  async reconnect(cut: Cut, nodes: readonly Member[]): Promise<void> {
    const rest = this.#cuts.filter((c) => !sameCut(c, cut));
    await this.heal();
    this.#cuts = rest;
    await this.reapply(nodes);
  }

  /** Restore the scenario's own network on every link, its loss included,
   *  and clear the list. */
  async heal(): Promise<void> {
    await this.#inner.heal();
    this.#cuts = [];
  }

  /** Block the links fault `cut` names among `nodes`: a zone's every link to
   *  the rest, a silenced node's to every other, or a node's sends to its own
   *  zone. */
  async #apply(cut: Cut, nodes: readonly Member[]): Promise<void> {
    switch (cut.kind) {
      case 'zone': {
        const inside = nodes.filter((n) => n.zone === cut.zone).map((n) => n.id);
        const rest = nodes.filter((n) => n.zone !== cut.zone).map((n) => n.id);
        if (inside.length > 0 && rest.length > 0) await this.#inner.partition(inside, rest);
        return;
      }
      case 'silenced': {
        const others = nodes.filter((n) => n.id !== cut.node).map((n) => n.id);
        if (others.length > 0 && nodes.some((n) => n.id === cut.node)) await this.#inner.partition([cut.node], others);
        return;
      }
      case 'unheard-in-zone':
        if (!nodes.some((n) => n.id === cut.node)) return;
        for (const peer of nodes) {
          if (peer.zone === cut.zone && peer.id !== cut.node) {
            await this.#inner.set_link_policy(cut.node, peer.id, { kind: 'Block' } satisfies LinkPolicyKind);
          }
        }
    }
  }

  /** Add a fresh cold-start node to the live cluster (no rebuild). Resolves to
   *  the events the join produced; the new node's id appears in the next
   *  snapshot. It joins `zone`, or the zone with the fewest live members,
   *  the lowest index on a tie. */
  async addNode(zone?: number): Promise<EventBatch> {
    const batch = (await this.#inner.add_node(zone)) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Remove the live node with stable `id`. Throws if no live node has it. */
  async removeNode(id: number): Promise<EventBatch> {
    const batch = (await this.#inner.remove_node(id)) as EventBatch;
    assertBoundary(batch);
    this.#cuts = this.#cuts.filter((c) => c.kind === 'zone' || c.node !== id);
    return batch;
  }

  /** Start the run's message log: the next message is number 1. */
  markRunStart(): void {
    this.#inner.mark_run_start();
  }

  /** Set the Messages view's filter; the view is rebuilt at once. */
  setMessageFilter(filter: MessageFilter): void {
    this.#inner.set_message_filter(filter);
  }

  /** Write the view's head and up to `count` rows from ordinal `first` into
   *  the rows buffer, and return the buffer and the rows written. The buffer
   *  is shared: read it before the next call. */
  messageRows(first: number, count: number): { rows: Float64Array; written: number } {
    const written = this.#inner.message_rows(first, count);
    if (memory === null) throw new Error('the wasm module is not initialized');
    if (this.#rows === null || this.#rows.buffer !== memory.buffer) {
      this.#rows = new Float64Array(memory.buffer, this.#inner.message_rows_ptr(), ROWS_LEN);
    }
    return { rows: this.#rows, written };
  }

  /** Select message `seq`, or clear the selection, and return its detail,
   *  decoded now; undefined once it is no longer kept. */
  selectMessage(seq: number | null): MessageDetail | undefined {
    return (this.#inner.select_message(seq ?? undefined) ?? undefined) as MessageDetail | undefined;
  }

  /** End the session and tear every runtime down. */
  async shutdown(): Promise<void> {
    await this.#inner.shutdown();
  }
}
