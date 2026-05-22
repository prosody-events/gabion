# Gabion contributor notes

## What Gabion is

Gabion is a distributed rate limiter. Cluster members maintain per-origin
counters in a CRDT, exchange them over an anti-entropy UDP gossip protocol,
and admit or reject incoming requests against the cluster-wide aggregate.
Two adapters consume the same core: `gabiond`, an Envoy-compatible gRPC
service, and an in-process NGINX module. Both share the same admission
hot path, the same CRDT, the same wire codec, the same rule machinery.

Cluster-wide counts are eventually consistent. Admission is local and
allocation-free; an operator under heavy load pays for one atomic read of
SHM (nginx) or one `DashMap` lookup (server), no syscalls, no allocations.

## Crate layout

The workspace lives under `crates/`:

- **`gabion`** — the library. Pure Rust, no transport bindings. Modules:
  - `crdt` — per-origin counter store, dirty rings, peer frontier,
    delta/expiration sinks. Bounded; zero allocation after construction.
  - `gossip` — anti-entropy runtime (`GossipRuntime`), `GossipClient`,
    `Clock`, `GossipTransport`, the deterministic `sim::SimTransport`
    used in tests, and the admin/peer interface.
  - `wire` — the on-the-wire codec for gossip frames (header, body,
    HMAC auth). Each UDP packet is self-describing and independently
    decodable.
  - `rules` — `Rule` / `RuleTable` / `Descriptor` / stable fingerprint
    hashing. Shared by both adapters so two nodes with identical rules
    emit identical identifiers.
  - `discovery` — `PeerDiscovery` trait and the Kubernetes EndpointSlice
    implementation.
  - `defaults` — the production tunables both adapters consume.

- **`server`** (`gabion-server`) — the `gabiond` binary. Tonic gRPC
  rate-limit service speaking Envoy's
  `envoy.service.ratelimit.v3` protocol, plus a small admin HTTP
  endpoint. `SharedLimiter<C>` is the admission entry point;
  `DashMapStore<C>` is the cluster-aggregate read surface.

- **`nginx`** (`gabion-nginx`) — the NGINX module. Two execution
  contexts share one `mmap`'d SHM zone: every worker runs the access
  phase (read aggregate, decide, push to SHM queue), and one elected
  worker spawns a thread that drains the queue, drives `GossipRuntime`,
  and writes deltas back into the SHM aggregate. The library half
  (`access`, `headers`, `identity`, `leader`, `rules`, `shm`) builds and
  tests without the `ngx-module` feature; FFI glue is gated behind it.

- **`loader`** — a load generator. Drives the gRPC service (or an HTTP
  endpoint sitting in front of nginx) with a configurable tenant /
  hit-rate mix; used in `make kubernetes-*` smoke tests.

- **`gossip-bench`** — gossip propagation simulator. Runs scenario JSON
  specs through `gossip::sim::SimTransport` and emits result JSON;
  `bench/plot.py` produces the convergence plots referenced from
  `docs/Gossip Propagation Benchmarks.md`.

Deployment manifests, NGINX docker-compose, Kubernetes smoke harnesses,
and the cross-version NGINX/OpenResty build matrices live under
`deploy/`. Background documents — CRDT design, gossip gap analysis,
distributed rate-limit semantics — live under `docs/`.

## How a request flows

1. Client traffic hits either nginx (with `gabion_limit_rule` set on the
   location) or an Envoy fleet pointed at `gabiond`.
2. The adapter walks `RuleTable::matching` and for each matched rule
   reads the cluster-aggregate window total via `window_total(...)`.
3. If `total + hits > rule.limit`, reject. Otherwise, push a
   `(rule_fingerprint, key_hash, bucket, hits, now_millis)` record into
   the local SHM queue (nginx) or hand it to `GossipClient::record`
   (server).
4. The gossip runtime folds local records into its `CellStore`, gossips
   dirty rows to peers every tick, applies inbound deltas to the local
   aggregate, and ages out expired buckets.

The library knows nothing about YAML, gRPC, or nginx. Adapters bridge
their own config to library types and own their own transport.

## How to read this file

These notes are the rules of the road. They override defaults; read them
before making changes that touch admission, the CRDT, gossip, or any SHM
boundary.

## Allow by default

When an internal limit, lookup, or defensive guard is hit during request
admission — a matched-rules cap exceeded, a config table missing an entry,
a transient error reading a variable — the request **must be allowed
through** rather than rejected. Rate limiting is best-effort: failing open
is the safer mode because a buggy or saturated limiter that rejects
real traffic is far more harmful than briefly under-counting.

Concretely:

- Don't introduce new `Reject` branches for conditions that represent
  *gabion's own* limits or errors (rule-table inconsistencies, internal
  buffer overflows, etc.). Decline (nginx) or Allow (server) instead, and
  record a stat / log so operators can see the bypass.
- Cardinality limits enforced against *client-supplied* input
  (descriptor counts, byte budgets) still reject — those are user-facing
  guardrails, documented in operator-facing config, not silent fallbacks.
- UTF-8 decode failures on nginx variable values decline (allow), and
  bump `declines_invalid_descriptor` so the bypass is observable.

If you find an existing reject path that violates this principle, fix it
in the same commit that touches the surrounding code.

## No allocation on the hot path

The per-request decision path and the CRDT mutation path must not allocate.
Pre-size everything up front; carry stack-resident bounded collections
(`arrayvec::ArrayVec`) through the request; iterate `RuleTable::matching`
without `collect`ing; read SHM aggregates through atomic loads only.

- A `Vec` that occasionally grows is acceptable for caller-owned buffers
  that the hot path reuses across requests (e.g. the gRPC `mapped`
  descriptor scratch) — but pre-allocate with `with_capacity` from the
  call site so steady state is allocation-free.
- Hard caps on per-request work live in `gabion::defaults` and feed both
  adapters' `ArrayVec` sizings. Touching one without the other is a bug.
- The CRDT `CellStore` allocates only at construction. Every later
  mutation reuses pre-sized columns, dictionaries, dirty rings, and the
  freelist; if you find yourself adding a `push` that may grow, redesign
  around a bounded structure.

## Minimize copies

Borrow through admission. `Descriptor<'a>` is `&str`/`&str`; `LimitRequest`
and `AccessCtx` are borrowed views; nginx variable values are read
directly out of nginx-owned buffers. Don't introduce `String` or `Vec<u8>`
in the request path to satisfy a lifetime — fix the lifetime.

Identity is hashed once and threaded as `u128` / `KeyHash` from there. A
second hash of the same bytes is a bug, not an optimisation.

For owned string data that won't be mutated after construction (config
entries, rule names, descriptor keys held in long-lived tables), prefer
`Box<str>` over `String`. `String` carries a capacity field for amortised
growth that owned-immutable data never uses — `Box<str>` is one word
narrower per field, signals intent ("this is frozen"), and rules out
accidental in-place mutation. Same logic for `Box<[T]>` over `Vec<T>`
when the length is fixed at construction. Reach for `String`/`Vec<T>`
only when you actually need `push`, `extend`, or capacity reuse.

## Strive for performance; benchmark the hot code

Hot paths get realistic benchmarks (see `crates/gabion/benches/crdt.rs`)
modelled after the actual production states the code sits in — steady
state, traffic burst, cold start, repair, expiry. New hot-path code
either lands a bench scenario or extends an existing one. "Realistic"
beats "synthetic": measure 95%-hit/5%-insert mixes, not pure inserts.

`make bench-check` ensures benches compile; reference runs are taken on
demand. Don't add benches that only measure microscopic ops without a
production analogue.

## Make invalid states unrepresentable

Use the type system instead of runtime asserts. Examples already in the
tree:

- `EnforcementMode` is an enum, not a `bool`.
- `RuleSpec` is a `Copy` summary distinct from `Rule`; the hot path
  cannot accidentally hold the heavyweight type.
- `KeyHash`, `NodeId`, `RuleSlot`, `NodeSlot`, `BucketEpoch` are
  newtype/typedef wrappers so a `u128` from one domain cannot stand in
  for another.
- `Decision`, `AccessOutcome`, `RejectReason` are enums with one variant
  per terminal state; matchers exhaust them rather than carrying
  sentinel codes.

When you add a new piece of state, ask whether two values that shouldn't
both be true (or false) can be expressed as variants of one enum. If
yes, do that instead of adding a flag.

## Results signal errors

Public functions that can fail return `Result<_, E>` with a `thiserror`
enum tailored to the call site (`ServeError`, `ConfigError`,
`GossipError`, `EncodeError`, etc.). Errors carry context, not a
stringified original. Internal infallible code returns plain values —
don't wrap success-only paths in `Result` for symmetry.

Panicking is never allowed in production code when idiomatic error
handling is possible. If a failure can be expressed as a `Result`,
express it that way and let the caller decide. `panic!`/`expect`/`unwrap`
are not a shortcut for "I don't want to plumb an error type."

`debug_assert!` (and friends) are the right tool for enforcing
invariants the type system can't capture: they document the
precondition, catch violations in tests and debug builds, and compile
out of release binaries so they cannot take down a production node.
Reach for them instead of a runtime `panic!` whenever the condition is
a programmer-error invariant rather than a recoverable failure.

Runtime `panic!`/`expect`/`unwrap` survive in release builds and are
reserved for genuine invariants that cannot be expressed any other way
(e.g. `stable_hasher`'s constant secret length, where the input is a
compile-time constant). Anything that depends on inputs, config, or
I/O must surface a typed error.

## Simplicity, beauty, elegance

The current shape of admission — a single 80-line function in
`crates/nginx/src/access.rs` that builds descriptors, walks matching
rules once, plans events into a stack buffer, and commits in one pass —
is the bar. Prefer one straight-line pass over two cute ones. Prefer a
named local with a clear purpose over a clever fold. Prefer fewer
modules with sharper boundaries over more modules with shared utility
soup.

When reviewing your own change before posting it, take a second pass
purely to remove: dead branches, redundant clones, intermediate
collections, indirection that doesn't earn its keep, traits with one
impl, comments that restate the code. The smallest correct diff wins.

## Refactor all the way through

When you change a shape, change every caller. Don't leave a
deprecated alias, a `from_old` shim, a "back-compat" reexport, or a
parallel code path "until callers migrate" — migrate them in the same
commit. Vestigial code rots faster than it's worth.

The Makefile's `hygiene` target enforces a couple of these
mechanically: no `dyn Trait` / `Box<dyn>` (we monomorphize), no version
pins inside per-crate `Cargo.toml` (workspace deps only). Match the
spirit when you add anything new: if it's the only caller of something,
inline it; if it's a leftover module from a previous shape, delete it.

## No `#[allow]`. `#[expect(...)]` only with permission

Lint suppressions hide regressions. Don't introduce `#[allow(...)]`.

`#[expect(...)]` is acceptable only when the user has explicitly
sanctioned it for that specific site (e.g. `#[expect(static_mut_refs)]`
on the nginx FFI module table). When you use one, leave a comment
explaining the invariant that makes the lint a false positive, not just
"the lint is wrong".

## Unsafe must justify itself and be tested under Miri

Every `unsafe` block carries a `SAFETY:` comment that enumerates the
preconditions and the local reason each one holds. Public `unsafe fn`s
carry a `# Safety` doc section listing the same preconditions for
callers.

The cross-process boundary (SHM init, the MPSC queue, the
single-writer/multi-reader aggregate, the leader lease) is exercised by
`crates/nginx/tests/safety.rs`, which runs under Stacked Borrows
*and* Tree Borrows Miri:

- `make miri-safety`     — Stacked Borrows
- `make miri-safety-tb`  — Tree Borrows
- `make miri-all`        — both modes across the whole `gabion-nginx` crate

New unsafe code lands a safety test that covers its preconditions, and
either passes under both Miri modes or has a written, narrow exemption
documented in the test.

## Errors and warnings are written for the operator

The person reading a log line at 2am, or staring at a `gabiond` exit
code, is the audience. Write to them. Aim for Rust-compiler /
Elm-compiler quality: empathetic, specific, and ending with a next
step.

Every operator-facing error and warning answers three questions:

1. **What happened**, in plain language. Not "EINVAL on descriptor
   ingest", but "rejecting requests that attach too many rate-limit
   descriptors".
2. **Why it's likely happening.** The probable cause, including the
   benign and the adversarial reading where both are real. "Usually a
   misbehaving client or an attack trying to exhaust gabion's tracking
   memory."
3. **What to do next.** A concrete config key, env var, doc anchor, or
   command. "Raise the relevant key under `cardinality_limits` in your
   gabion config." "Look for an earlier error log entry." Never end
   with the bare fact.

Other rules:

- **Be honest about what happened.** If allow-by-default kicked in,
  the message must say the request was *allowed* and the count
  *under-counted*, not that it was "rejected conservatively". The
  message is the operator's only window into the bypass.
- **Attach structured fields** (`domain = ..., descriptor_count = ...,
  failed_total = ...`) so logs are filterable. Don't bake values into
  the prose if a field will do.
- **Rate-limit chatty warnings** with the power-of-two pattern in
  `crates/server/src/lib.rs`. A bad client at 50k rps should produce
  ~log₂(N) lines, not 50k.
- **Name the knob.** If raising a limit fixes it, name the constant
  (`STORAGE_MAX_MATCHED_RULES`) or the config key
  (`cardinality_limits`). Operators shouldn't have to grep the source
  to find it.
- **Low-level `#[error("...")]` strings may stay terse** when the
  caller wraps them with context (`ConfigError`, `GossipError` variants
  that already name the failing stage). But any error that ultimately
  reaches a log line or stderr must include the three-question shape
  by the time it gets there.
- **No blame, no hedging, no jargon-for-its-own-sake.** "The gossip
  background task has stopped" beats "GossipRuntime exited
  non-deterministically". Avoid "unexpected" — if it's worth logging,
  describe it.

The exemplars in tree are `note_cardinality_reject` and
`note_gossip_record_failure` in `crates/server/src/lib.rs`. Copy their
shape.

## Documentation lives in README.md

Project-level documentation belongs in `README.md` (or the appropriate
`README.md` under a crate or `docs/` subdirectory). Don't scatter
explanatory prose into new top-level `*.md` files, into long comment
blocks at the top of source files, or into `CLAUDE.md` itself —
`CLAUDE.md` is for contributor rules, not narrative docs. When you
need to write something a human will read outside the code, put it in
README.

## Workflow conventions

- **`cargo nextest` is the only sanctioned test runner.** Do not
  invoke `cargo test`. Nextest is faster, supports per-test timeouts,
  surfaces failures earlier, and is what CI runs. If you write a new
  script or CI step, it calls `cargo nextest run`, not `cargo test`.
  Install with `cargo install cargo-nextest --locked`. The Makefile's
  `require-nextest` target prints the install command if it's missing.
- **`cargo +nightly fmt` is the only sanctioned formatter.** Stable
  `rustfmt` does not understand all the unstable knobs in
  `rustfmt.toml`; running it produces a diff CI rejects. Use
  `make format` (writes) or `make fmt` (checks). Install nightly with
  `rustup toolchain install nightly --component rustfmt`. The
  Makefile's `require-nightly-fmt` target prints the install command
  if it's missing.
- `make test` runs `fmt-check`, `clippy -D warnings`, workspace
  `nextest`, the safety integration test, and `hygiene`. Run it before
  declaring a change done. `make ci` adds Miri (Stacked Borrows),
  `bench-check`, and the nginx smoke tests.
- Tests live in their own file in their own module: each tested
  module `foo.rs` gets a `tests.rs` file inside the module's own
  subdirectory (`foo/tests.rs`), declared from `foo.rs` as
  `#[cfg(test)] mod tests;`. Don't bury a
  `#[cfg(test)] mod tests { ... }` block at the bottom of the
  production file, and don't drop a sibling `foo_tests.rs` next to
  `foo.rs`. Cross-process integration lives in
  `crates/nginx/tests/safety.rs`. Run individual tests with
  `cargo nextest run -p <crate> <filter>`.
- Shared production tunables live in `gabion::defaults`. Both adapters
  consume them; introduce new tunables there, not as duplicated `const`s
  in each adapter.
- The library crate (`gabion`) knows nothing about YAML, gRPC, or
  nginx. Adapters bridge their own config to library types.
- High-volume operator warnings (cardinality rejects, gossip record
  failures, matched-rule overflows) use the power-of-two rate-limited
  `tracing::warn!` pattern in `crates/server/src/lib.rs` — copy it
  rather than inventing another throttle.
