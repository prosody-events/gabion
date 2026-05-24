# gabion-wasm

The visualizer's bridge: the **real** gabion gossip + CRDT core compiled to
WebAssembly and driven from a TypeScript frontend. There is no new simulator
here — [`engine::run_engine`](src/engine.rs) stands up N
[`GossipRuntime`](../gabion/src/gossip/runtime.rs)s on one shared
`gossip::sim::SimRouter`, exactly the machinery `gossip-bench` exercises, and
exposes it over a command channel (`submit_request`, `step`, `set_link_policy`,
`snapshot`, …).

## Why a hand-driven clock

`wasm32-unknown-unknown` has no OS clock, so `std::time::Instant::now()` — which
tokio's time driver calls — panics. The production `TokioClock` therefore can't
run in the browser. The fix is the injectable tick source on
[`gabion::gossip::Clock`](../gabion/src/gossip/clock.rs): the trait gained an
associated `Ticker`, so production keeps its `tokio::time::interval` ticker
(behaviour unchanged) while this crate injects [`ManualClock`](src/clock.rs),
which advances virtual time and fires gossip ticks from the engine — touching no
`tokio::time`. Ticks ride a per-node bounded `mpsc` (not a `watch`, which
coalesces) so every fired tick is delivered exactly once and replay stays
deterministic.

## Layout

- `engine.rs` — the single long-lived engine task: builds the cluster, owns the
  runtimes, and turns commands into virtual-time advances + tick fires.
- `clock.rs` — `ManualClock` / `ManualTicker` (the wasm time source).
- `shims.rs` — `EventEmittingAggregateStore` + `LoggingSimTransport` turn the
  runtime's existing push surfaces into a typed event log.
- `event.rs` / `config.rs` — the serde shapes crossing the JS boundary (`u128`
  ids render as hex strings).
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

`getrandom` 0.4 selects its browser backend from the `wasm_js` feature in
`Cargo.toml` (no `RUSTFLAGS` cfg needed); `wasm-pack` downloads the
`wasm-bindgen` that matches `Cargo.lock`.

## Frontend

The browser app that imports this crate's wasm package lives in
[`web/`](web/README.md) — Svelte 5 + Vite + TypeScript. `make wasm-check`
builds it (wasm-pack release + `svelte-check` + Vite build) alongside the
native engine tests. See [`web/README.md`](web/README.md) for the dev server,
the Playwright screenshot/in-browser smoke, and why the build uses
`wasm-pack --target web` instead of `vite-plugin-wasm`.
