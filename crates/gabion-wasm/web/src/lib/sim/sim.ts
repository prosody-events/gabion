// A thin, typed wrapper over the generated wasm `Sim`. It owns the one-time
// module init, converts the JS `number` arguments the UI works in into the
// `bigint`s wasm-bindgen expects for u64 parameters, and stamps the untyped
// boundary results with the contracts declared in `types.ts`.

import init, { Sim as WasmSim, default_config } from '../../wasm/gabion_wasm.js';
import type { ClusterState, EventBatch, LinkPolicyKind, SimConfig } from './types';

let initPromise: Promise<unknown> | null = null;

/** Initialize the wasm module exactly once, even under concurrent callers. */
function ensureInit(): Promise<unknown> {
  if (initPromise === null) {
    initPromise = init();
  }
  return initPromise;
}

/** Confirm the numeric boundary convention `types.ts` documents actually holds
 *  at runtime: u64 fields arrive as JS numbers, u128 ids as hex strings. Runs
 *  once, on the first batch crossed, and throws loudly if the contract drifts
 *  (e.g. a serde-wasm-bindgen default change) rather than silently corrupting
 *  every count downstream. */
let boundaryChecked = false;
function assertBoundary(batch: EventBatch): void {
  if (boundaryChecked) return;
  boundaryChecked = true;
  if (typeof batch.virtual_ms !== 'number' || typeof batch.tick !== 'number') {
    throw new TypeError(
      `wasm boundary contract broken: expected u64 fields as JS numbers, got ` +
        `virtual_ms=${typeof batch.virtual_ms}, tick=${typeof batch.tick}. ` +
        `Update web/src/lib/sim/types.ts (and the serializer in the Rust crate).`,
    );
  }
}

/** A live simulation. Mirrors the wasm `Sim` surface with idiomatic JS types. */
export class Sim {
  #inner: WasmSim;

  private constructor(inner: WasmSim) {
    this.#inner = inner;
  }

  /** Build a cluster and spawn its engine. A partial config is fine; omitted
   *  fields take the Rust-side production defaults. Throws synchronously on an
   *  invalid config (so a bad shared URL surfaces immediately). */
  static async create(config: Partial<SimConfig> = {}): Promise<Sim> {
    await ensureInit();
    return new Sim(new WasmSim(config));
  }

  /** The Rust-side production-aligned `SimConfig` defaults — the single source
   *  the control sliders open from (no hand-maintained TS mirror, so changing
   *  `gabion::defaults` is the only edit). u128 fields arrive as hex strings,
   *  matching the `SimConfig` contract in `types.ts`. */
  static async defaultConfig(): Promise<SimConfig> {
    await ensureInit();
    return default_config() as SimConfig;
  }

  /** Inject `hits` requests for `key` at `node`, at the current virtual time. */
  async submitRequest(node: number, key: number, hits: number): Promise<EventBatch> {
    const batch = (await this.#inner.submit_request(node, BigInt(key), BigInt(hits))) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Advance virtual time by `deltaMs`, resolving to the events produced. */
  async step(deltaMs: number): Promise<EventBatch> {
    const batch = (await this.#inner.step(BigInt(deltaMs))) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Advance virtual time to absolute `virtualMs` (a no-op if already past). */
  async stepTo(virtualMs: number): Promise<EventBatch> {
    const batch = (await this.#inner.step_to(BigInt(virtualMs))) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Pull the full per-node cluster state for a re-render. */
  async snapshot(): Promise<ClusterState> {
    return (await this.#inner.snapshot()) as ClusterState;
  }

  /** Install a directed link policy between two nodes. */
  async setLinkPolicy(src: number, dst: number, policy: LinkPolicyKind): Promise<void> {
    await this.#inner.set_link_policy(src, dst, policy);
  }

  /** Cut every link between two node groups, both directions. */
  async partition(groupA: number[], groupB: number[]): Promise<void> {
    await this.#inner.partition(groupA, groupB);
  }

  /** Restore every directed link to lossless `Pass`. */
  async heal(): Promise<void> {
    await this.#inner.heal();
  }

  /** Add a fresh cold-start node to the live cluster (no rebuild). Resolves to
   *  the events the join produced; the new node's id appears in the next
   *  snapshot. */
  async addNode(): Promise<EventBatch> {
    const batch = (await this.#inner.add_node()) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** Remove the live node with stable `id`. Throws if no live node has it. */
  async removeNode(id: number): Promise<EventBatch> {
    const batch = (await this.#inner.remove_node(id)) as EventBatch;
    assertBoundary(batch);
    return batch;
  }

  /** End the session and tear every runtime down. */
  async shutdown(): Promise<void> {
    await this.#inner.shutdown();
  }
}
