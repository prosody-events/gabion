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
deterministic. The engine steps to the next heartbeat, or one relay floor when
a node's prompt ticker is armed (it has resends, gap reports or sweeps
waiting), so a quiet cluster costs one step per heartbeat.

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
  every node's counters each sub-step and fetches a snapshot only for a node
  whose role count moved. A removal moves no role, so the removal itself
  re-reads every survivor whose gateway the departed node was, and reports the
  gateway gone, or replaced, as that survivor's runtime now has it.
- `gateway_for` counts, per node, the origins whose runtime routes into its zone
  through it, inverted from the gateway rows the engine already keeps;
  `own_data_bytes` counts each node's own-stream bytes, the weights of the share
  chart's no-zone prediction. `max_zones()` exports the zone ceiling the page's
  stepper reads.

## Node ceiling

The page caps live nodes at `MAX_LIVE_NODES` = 100 in `web/src/App.svelte`. A
183-node cluster (the `prod-peak` pod count, in 3 zones) plays a warm virtual
second in about 205 ms, inside the 250 ms budget, but its first second, which
includes cold start, takes about 380 ms, and the warm margin of about 45 ms on
the machine below is too thin to assume for slower ones, so the cap stays.

To reproduce, build the release wasm (`cd web && pnpm run build:wasm`), serve
`web/src` (`python3 -m http.server 8000 --directory web/src`), open it in
Chromium, and run this in the console. It seeds the steady workload (10 hits on
one key at each of nodes 0, 3, 6 and 9, then 3 hits a second round-robin) and
plays each virtual second as the page does with node 0 inspected, 60 frames of
`step_to` plus `snapshot`:

```js
const m = await import('/wasm/gabion_wasm.js'); await m.default();
const sim = new m.Sim({ nodes: 183, zones: 3, rule_limit: 1000000 });
for (const n of [0, 3, 6, 9]) await sim.submit_request(n, 1n, 10n);
const out = []; let hit = 0;
for (let s = 0; s < 5; s++) {
  let step = 0, snap = 0;
  for (let f = 1; f <= 60; f++) {
    const v = s * 1000 + Math.round((f * 1000) / 60);
    while (hit < Math.floor((v * 3) / 1000)) await sim.submit_request(hit++ % 183, 1n, 1n);
    let t = performance.now(); await sim.step_to(BigInt(v)); step += performance.now() - t;
    t = performance.now(); await sim.snapshot(0); snap += performance.now() - t;
  }
  out.push({ second: s, step, snap, total: step + snap });
}
await sim.shutdown(); out;
```

Measured on 2026-11-03: Apple M2 Pro, macOS 26.7.1, headless Chromium 145.0.7632.6
(Playwright), release wasm. Wall time in ms per virtual second, two runs; the
first second includes cold start.

| second | steps (run 1) | snapshots (run 1) | total (run 1) | total (run 2) |
|---|---|---|---|---|
| 0 | 183 | 211 | 394 | 382 |
| 1 | 13 | 199 | 212 | 208 |
| 2 | 14 | 199 | 212 | 206 |
| 3 | 13 | 191 | 204 | 204 |
| 4 | 12 | 190 | 204 | 204 |

A snapshot takes about 3 ms at 183 nodes, because it carries every node's
scalar state but the cells and peer rows of the inspected node alone; with no
node inspected (`snapshot()`) a warm second totals about 193 ms. Carrying every
node's cells and all 183 × 182 peer rows instead took about 35 ms a snapshot,
2,000 ms a second.

## Layout

- `engine.rs` — the single long-lived engine task: builds the cluster, owns the
  runtimes, and turns commands into virtual-time advances + tick fires.
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
