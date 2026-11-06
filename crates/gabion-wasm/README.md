# gabion-wasm

The visualizer's bridge: the **real** gabion gossip + CRDT core compiled to
WebAssembly and driven from a TypeScript frontend. There is no new simulator
here — [`engine::run_engine`](src/engine.rs) stands up N
[`GossipRuntime`](../gabion/src/gossip/runtime.rs)s on one shared
`gossip::sim::SimRouter`, the same deterministic router the library's own
tests run on, and exposes it over a command channel (`submit_request`,
`step`, `set_link_policy`, `add_node`, `snapshot`, …). The session's config
places the nodes in zones (`zones`, `zone_mode`).

## Why a hand-driven clock

`wasm32-unknown-unknown` has no OS clock, so `std::time::Instant::now()` — which
tokio's time driver calls — panics. The production `TokioClock` therefore can't
run in the browser. The fix is the injectable tick source on
[`gabion::gossip::Clock`](../gabion/src/gossip/clock.rs): the trait gained an
associated `Ticker`, so production keeps its `tokio::time::interval` ticker
(behaviour unchanged) while this crate injects [`ManualClock`](src/clock.rs),
which advances virtual time and fires gossip ticks from the engine — touching no
`tokio::time`. The runtime asks for two tickers, the heartbeat and the
prompt-round pacer; each rides its own bounded `mpsc` (not a `watch`, which
coalesces) so every fired tick is delivered exactly once and replay stays
deterministic. Each node heartbeats at its own fixed phase, a pure function of
its id spread across the period, as production's unrelated process starts
spread theirs. The engine steps to the next due instant: a node's heartbeat, or
one relay floor after a node armed its prompt ticker (it has relays, resends,
gap reports or sweeps waiting). A quiet cluster costs one sub-step per distinct
phase, and each sub-step reads the counters of, and drains for, only the nodes
it touched.

## Admission and held waves

Each node decides every request from its own count table: gabion's
`window_total` on its `Aggregate` and `RuleSpec::admits`, the comparison the
server and nginx make. A rejected request is counted, per node and for the
cluster, and never recorded. An admitted one is recorded, and the packets of
its eager flush stay held, as a sub-step's do, until the next command, so no
count changes before its packet lands. `step` with `{ Event: { key } }` stops
before a wave of packets that carry cells lands, or at the instant some node's
window of `key` drops a bucket that holds hits. The truth counts a departed
node's hits as long as a survivor still holds them. The snapshot reports each node's windowed view, per-bucket counts,
lag (caught up, behind, or stranded where waiting cannot help), its one-hit
decision and next heartbeat; heal and every joiner use the scenario's own
network, its loss included.

## Zones

- Node `i` is placed in zone `i mod zones` (`zone-a`, `zone-b`, …), and a joiner
  goes to the zone it asks for or the smallest one.
- `zone_mode` `Configured` passes each node its zone through the library's zone
  setting; `Unset` tells nodes nothing, so they send to every peer directly.
- Every node binds 127.0.0.1, so zones come from configuration, never from an
  address.
- Each sent packet is tagged with its kind, read with `gabion::wire::read_header`,
  and its tier, by placement, which is what keeps the no-zone view honest.
- `bytes_by_tier` is cumulative by tier and class, classed by packet kind: data
  is own, relay and sweep packets; control is acks, probes and their answers,
  gap reports and sweep requests.
- `GatewayChanged` comes from each runtime's own gateway rows: the engine reads
  the counters of every node a sub-step touched and fetches a snapshot only for
  a node whose role count moved. A removal moves no role, so the removal itself
  re-reads every survivor whose gateway the departed node was, and reports the
  gateway gone, or replaced, as that survivor's runtime now has it.
- `gateway_for` counts, per node, the origins whose runtime routes into its zone
  through it, inverted from the gateway rows the engine already keeps;
  `own_data_bytes` counts each node's own-stream bytes, the weights of the share
  chart's no-zone prediction. `max_zones()` exports the zone ceiling the page's
  stepper reads.

## Node ceiling

The page caps live nodes at `MAX_LIVE_NODES` = 100 in `web/src/App.svelte`. At
100 nodes in 3 zones a warm virtual second plays in about 215 ms, inside the
250 ms budget, and its first second, which includes cold start, in about
255 ms. A 183-node cluster (the `prod-peak` pod count) takes about 405 ms a
warm second, over the budget, so the cap stays. Per-node heartbeat phases cost
the steps little (about 9 ms a second at 100 nodes, 17 ms at 183): a quiet
sub-step drains and reads only the nodes it touched. The snapshot carries more
per node than it did (the windowed view and its buckets, the one-hit decision,
the lag and the hit counters), about 12 % more a frame at 100 nodes; most of
every second is snapshots.

To reproduce, build the release wasm (`cd web && pnpm run build:wasm`), serve
`web/src` (`python3 -m http.server 8000 --directory web/src`), open it in
Chromium, and run this in the console. It seeds the steady workload (10 hits on
one key at each of nodes 0, 3, 6 and 9, then 3 hits a second round-robin) and
plays each virtual second as the page does with node 0 inspected, 60 frames of
`step_to` plus `snapshot`:

```js
const m = await import('/wasm/gabion_wasm.js'); await m.default();
const N = 100;
const sim = new m.Sim({ nodes: N, zones: 3, rule_limit: 1000000 });
for (const n of [0, 3, 6, 9]) await sim.submit_request(n, 1n, 10n);
const out = []; let hit = 0;
for (let s = 0; s < 5; s++) {
  let step = 0, snap = 0;
  for (let f = 1; f <= 60; f++) {
    const v = s * 1000 + Math.round((f * 1000) / 60);
    while (hit < Math.floor((v * 3) / 1000)) await sim.submit_request(hit++ % N, 1n, 1n);
    let t = performance.now(); await sim.step_to(BigInt(v)); step += performance.now() - t;
    t = performance.now(); await sim.snapshot(0, 1n, 25n); snap += performance.now() - t;
  }
  out.push({ second: s, step, snap, total: step + snap });
}
await sim.shutdown(); out;
```

Measured on 2026-11-05: Apple M2 Pro, headless Chromium 156.0.8078.4
(Playwright), release wasm. Wall time in ms per virtual second, two runs; the
first second includes cold start. The previous engine (lockstep heartbeats, no
admission), built from the parent commit and measured in the same session, gave
185 to 194 ms a warm second at 100 nodes and 352 to 370 ms at 183.

| nodes | second | steps (run 1) | snapshots (run 1) | total (run 1) | total (run 2) |
|---|---|---|---|---|---|
| 100 | 0 | 56 | 205 | 261 | 253 |
| 100 | 1 | 9 | 204 | 213 | 209 |
| 100 | 2 | 9 | 208 | 217 | 211 |
| 100 | 3 | 9 | 209 | 218 | 211 |
| 100 | 4 | 10 | 208 | 218 | 212 |
| 183 | 0 | 196 | 404 | 600 | 540 |
| 183 | 1 | 18 | 397 | 415 | 396 |
| 183 | 2 | 18 | 389 | 407 | 398 |
| 183 | 3 | 16 | 392 | 408 | 409 |
| 183 | 4 | 19 | 402 | 421 | 405 |

In the same session, a quiet 60 s `step` took 25 to 33 ms at 12 nodes and 109
to 135 ms at 48 (3 zones, 500 ms heartbeat), and a 48-node Steady state played
at 4× (3 one-hit requests a second, 67 ms of virtual time a frame) spent a
median 1.9 ms of engine time a frame (95th percentile 2.3 ms): submits, step
and snapshot.

## Layout

- `engine.rs` — the single long-lived engine task: builds the cluster, owns the
  runtimes, and turns commands into virtual-time advances + heartbeat and
  prompt fires.
- `clock.rs` — `ManualClock` / `ManualTicker` (the wasm time source).
- `shims.rs` — `EventEmittingAggregateStore` + `LoggingSimTransport` turn the
  runtime's existing push surfaces into a typed event log; the transport tags
  each sent packet with its kind and tier.
- `event.rs` / `config.rs` — the serde shapes crossing the JS boundary (`u128`
  ids render as hex strings); `event.rs` holds the snapshot shapes.
- `wasm.rs` — the `#[wasm_bindgen]` `Sim` surface, compiled only for `wasm32`.

The engine and shims are target-agnostic and tested natively with
`cargo nextest` (no wasm toolchain needed). `wasm.rs` is the only `wasm32`-gated
module.

## Build & test

```sh
make wasm-check          # cross-compile for wasm32 + run the native tests
```

Native tests cover the engine logic; to confirm the artifact actually *runs*
under a real wasm executor (regression guard for the `Instant::now` panic):

```sh
cd crates/gabion-wasm
wasm-pack build --target nodejs --dev
node smoke.cjs           # submit → gossip → converge → snapshot, prints DONE OK
```

`wasm-pack` downloads the `wasm-bindgen` that matches `Cargo.lock`.

## Frontend

The browser app that imports this crate's wasm package lives in
[`web/`](web/README.md) — Svelte 5 + Vite + TypeScript. `make wasm-check`
builds it (wasm-pack release + `svelte-check` + Vite build) alongside the
native engine tests. See [`web/README.md`](web/README.md) for the dev server,
the Playwright screenshot/in-browser smoke, and why the build uses
`wasm-pack --target web` instead of `vite-plugin-wasm`.
