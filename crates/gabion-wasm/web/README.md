# Gabion Gossip Visualizer — frontend

The browser app: an explorable explanation of gabion's gossip-based distributed
rate limiter, driven by the **real** gossip + CRDT core compiled to WebAssembly
(see [`../README.md`](../README.md)). Svelte 5 + Vite + TypeScript.

The current build is the three-pane "Atelier Stage" chassis — a quiet **control
rail** on the left (scenario presets, a keyboard-accessible "send a burst"
control, and the network Heal action), the dominant **PixiJS stage** in the
center (kept ≥ 55% of the width so it stays the single focal point), and the
**charts dashboard** (Phase 5) on the right. On the stage, a deterministic ring
of N nodes is drawn
on a deep WebGL canvas, each node showing its view of the cluster total as a
cell arc that fills toward the simulator's ground-truth oracle; gossip packets
fly between nodes as eased light-beams (thickness ∝ bytes, dropped packets fade
mid-flight); the cluster pulses once, in sync, the instant every node agrees. A
transport bar steps the engine; a burst of hits seeded on one node at `t = 0`
gossips outward until the ring converges — the end-to-end proof the gabion core
runs in a browser. Clicking any node injects a fresh burst there (a pure inject
at the current virtual time), so you can poke the cluster and watch it
re-converge — the first of the Phase 6 interactions.

The right rail is the **convergence dashboard**: a pinned headline metric
("disagreement → 0", the live spread between the best- and worst-informed node,
latching "converged in N rounds" when it hits zero), a **Convergence Fan**
(every node's view racing a heavy dashed ground-truth oracle), and a
**Disagreement decay** area — the textbook anti-entropy curve, self-caused by
the burst. The charts are uPlot canvases sharing one cursor; they read a
bounded, column-major rolling history that the engine appends to on each step.

Under the **Sustained overload** preset the dashboard adds a third panel, the
**Aggregate vs. limit** chart (B1): the cluster's true total climbing across a
dashed limit line into a shaded **REJECTING** band — *why gabion exists*. It is
shown only for that scenario because it is only legible there. A single burst
can't tell the story: to keep each node's local delta under gabion's threshold
budget (ε = L·bps/(10 000·N)) while the cluster sum exceeds the limit you'd need
an absurd error budget, and anything larger trips the eager flush and converges
in one round, erasing the multi-hop spread the other scenarios exist to show.
So overload instead *feeds* the cluster at a steady rate against a low limit;
the aggregate ramps across it within seconds, and the per-node views track the
climb tightly (eager flushing as the healthy behavior, on display). The feed is
a carry-save rate folded into the play loop (`App.svelte`), so the total
injected by virtual time T is exactly ⌊rate·T⌋ regardless of how play chunked
the steps — deterministic, without the scrubber's timed-event driver.

## Layout

- `src/App.svelte` — orchestrates the session: builds the `Sim`, runs the
  gated play loop, owns transport state, and routes each step's events to the
  stage.
- `src/tokens.css` — the design tokens, the one source of design values. Each
  colour holds its light and dark value in `light-dark()`: the OS preference
  picks one and `<html data-theme="light|dark">` overrides it. The study
  dashboard keeps a byte-identical copy. `src/app.css` holds only the base
  element rules.
- `src/lib/theme.ts` — `readTheme` resolves the tokens PixiJS and uPlot need as
  numbers by painting each one and reading the pixel back; `watchTheme` reports
  an OS scheme or `data-theme` change; `cssColor` formats a colour for uPlot.
- `src/lib/sim/` — the wasm boundary. `types.ts` hand-mirrors the serde shapes
  (the generated `.d.ts` types every payload as `any`); `sim.ts` is the typed
  wrapper that converts JS numbers to the `bigint`s wasm-bindgen wants for u64
  arguments and asserts the numeric boundary contract at runtime.
- `src/lib/stage/` — the renderer. `layout.ts` is the shared coordinate system
  (ring positions, fit transform, and the inverse `nodeAt` hit-test) consumed by
  the canvas, the DOM labels, and the click-a-node handler alike; `renderer.ts`
  is the framework-agnostic `StageRenderer` that owns the PixiJS canvas, draws
  the layers, and animates beams + the pulse with GSAP.
- `src/lib/charts/` — the dashboard. `history.ts` is the bounded, column-major
  rolling time-series (a plain class, not reactive — the owner bumps a single
  counter to drive redraws); `options.ts` builds each uPlot chart's options
  (shared cursor sync, direct labels, legend off); `Chart.svelte` is the generic
  imperative uPlot wrapper.
- `src/lib/components/` — `Stage.svelte` (the thin Svelte wrapper around the
  renderer plus the DOM label overlay), `Dashboard.svelte` (the headline metric
  + charts), `ControlRail.svelte` (the left rail: scenario presets, a
  collapsible "Tune the cluster" disclosure with rebuild sliders for fanout /
  error budget / packet loss, the keyboard-accessible "send a burst to node N"
  control, and a Heal action shown only for the partition / isolation
  scenarios), and `TransportBar.svelte`.
- `src/lib/presets.ts` — the scenario presets. A preset is a config plus an
  `async seed(sim)` that fires its opening commands (partitions, seed bursts)
  against a fresh cluster, and optionally a `traffic` rate the play loop feeds
  in continuously (the overload scenario; surfaces the Aggregate-vs-Limit
  chart). Deliberately *not* a timed script — the partition→heal narrative is a
  user gesture during play, and the sustained feed is a carry-save rate, not a
  scheduled event list; the timed-event driver only becomes load-bearing for
  the scrubber's replay, so it waits for that.
- `src/wasm/` — generated by `pnpm run build:wasm`; never edited or committed.
- `tests/screenshot.spec.ts` — the Playwright visual + in-browser smoke.
- `tests/audit.spec.ts` — the automated design audit (see "Design rules").

## Why a DOM label overlay over the canvas

The PixiJS `<canvas>` is opaque to assistive tech and to test tooling — there is
no DOM text to read. So each node's index and total are rendered as a small
absolutely-positioned DOM overlay (in tabular lining figures) on top of the
canvas, sharing `layout.ts`'s coordinates so a label always sits on its disc.
That overlay is the canvas's text alternative, the sighted-reader's numerals,
*and* what the Playwright assertions query — which keeps the functional test
independent of whether headless WebGL produced exact pixels. The overlay is
`pointer-events: none` so it never intercepts the click-a-node hit-test, which
maps a pointer back through the fit transform to the nearest disc (`nodeAt` in
`layout.ts`). Clicking a disc is a pointer-only power gesture; the
keyboard/AT-accessible "send to node N" equivalent lands with the control rail.

## Develop

```sh
pnpm install
pnpm run dev        # builds the wasm (debug) and starts Vite with HMR
```

`pnpm run build` rebuilds the wasm in release, type-checks with `svelte-check`,
and runs the production Vite build — this is what `make wasm-check` runs.

## Screenshots & the in-browser smoke

```sh
pnpm exec playwright install chromium   # one-time
pnpm run build
pnpm run screenshot                      # writes screenshots/ring-{initial,gossip,converged}.png
pnpm run audit                           # ten projects; writes audit-shots/
```

`pnpm run screenshot` is the functional smoke, run once at 1440 px in the light
theme. `pnpm run audit` is the design audit below.

The spec asserts the cluster boots and gossips the seeded burst out to every
node — the in-browser regression check the R2 note in `../src/wasm.rs` flagged.
It downloads a browser, so (like the node `smoke.cjs`) it is an on-demand run,
not part of the `make wasm-check` gate.

## Design rules

The audit checklist for this page and, through `docs/study/README.md`, for the
study dashboard. A rule that assumes the study's data model says "N/A" and why.

1. Bayesian wording only: the UI prints no statistics; run-specific timings say "in this run" (A8).
2. Relative time only: virtual time is elapsed seconds, never a wall-clock time (A8).
3. Fixed order, never re-sorted: nodes by stable id everywhere, a fixed preset list (visual audit).
4. Tufte-grade: data-ink, direct labels, no chartjunk, no legends (visual audit).
5. Light and dark from tokens, and the phone works (A1, A7, visual audit).
6. Screenshot-audited and Playwright-tested (CI).
7. No literal numbers in markup (not yet automated).
8. Every estimate followed by its interval: N/A, the UI shows exact values of one deterministic run.
9. One notation per context, fixed significant figures, singular units (A2).
10. Honest scale, axes fitted to the data (visual audit).
11. Zero gets its own treatment, never symlog (visual audit).
12. Censored values marked "≥" (not yet automated).
13. Tie band: N/A, the UI makes no comparisons.
14. Status sentence and specific empty states (not yet automated).
15. The reference is drawn first, muted and labelled (visual audit).
16. One encoding everywhere (visual audit).
17. Shape carries meaning and colour only repeats it (visual audit).
18. Seed ticks: N/A, one run, no seeds.
19. Paint order and halos; labels stay off the series (A3, visual audit).
20. Colour tokens only, type tokens, named geometry constants, measured labels (grep gates).
21. Page order, titles, notes, axis units, line length of about 80 characters (visual audit).
22. Breakpoints that keep everything usable: 1100 px and 640 px, audited at 1440, 1280, 1100, 768 and 400 px (A1, A3).
23. A "Show as table" fallback and a method section (not yet automated).
24. Scorecard rules: N/A, no scorecard.
25. Detail-pane rules: fitted axes, no duplicated sentences (visual audit).
26. One rAF-coalesced draw, a debounced ResizeObserver, focus restored (not yet automated).
27. Accessibility: focusable, tooltips on hover, focus and tap, targets of at least 24 px, contrast (A3, A6, A7).
28. Automated checks carried over (CI).

The grep gates, which print nothing: no colour literal outside `tokens.css`
(`rg -n '#[0-9a-fA-F]{3,8}\b|0x[0-9a-fA-F]{6}|rgba?\(|hsla?\(' src --glob '!tokens.css'`),
no pixel `font-size` (`rg -n 'font-size:\s*[0-9.]+px' src`), and no pre-token
name (`rg -n 'var\(--(ink|signal|chrome|stage|space|text)-' src`).

Automated checks, run on every preset with the transport paused:

- A1: no page or console errors, no failed requests, no page-level horizontal overflow.
- A2: no "NaN", "undefined", "null", "Infinity", "[object" or "e+" in page text.
- A3: no two visible text boxes overlap by 1 px or more, and no text is clipped by an `overflow: hidden` ancestor; canvas labels, chart marks, tooltips and beam geometry are not yet covered.
- A4: data correctness, recomputed independently from the engine snapshot (not yet automated).
- A5: fixed order across the grid, table, chart series and inspector rows (not yet automated).
- A6: interaction, including keyboard navigation and tooltips (not yet automated).
- A7: contrast from computed styles, text at 4.5:1 and graphics at 3:1 on the surface they sit on, in both themes, with targets of at least 24 px.
- A8: the wording scan (rule 1) and the clock-time scan (rule 2).
- A9: a digit scan outside data-bound elements (not yet automated).

`tests/audit.spec.ts` automates A1, A2, A3 for DOM text, A7 and A8; the rest are
reviewed by hand. It runs in ten projects (five widths, each in light and dark,
with motion reduced) and checks each preset a second time with `data-theme`
forcing the other theme. It saves a full-page PNG per project, preset and theme
to `audit-shots/`, which is gitignored; CI uploads it as a workflow artifact.

The visual audit reads those PNGs for: alignment, spacing rhythm, type
hierarchy, nothing clipped, a dark theme as considered as the light one, and a
fully usable phone layout.

## Why no `vite-plugin-wasm`

The plan named `vite-plugin-wasm`, but the wasm package is built with
`wasm-pack --target web` (see `build:wasm` in `package.json`), whose glue loads
the module via `new URL('…_bg.wasm', import.meta.url)`. Vite resolves and emits
that natively, so no plugin is needed — one fewer dependency and no
top-level-await shim. `--target web` also keeps its output isolated in
`src/wasm/`, leaving the node-target `../pkg/` (used by `smoke.cjs`) untouched.

## Numeric boundary

`u128` ids (rule, key, node id, fingerprint) cross as `"0x…"` hex strings;
`u64` counts and timestamps cross as JS numbers (serde-wasm-bindgen's default,
exact below 2^53 — far above the visualizer's volumes). `sim.ts` asserts this
once at runtime and `types.ts` documents it; if a scenario could exceed 2^53,
flip the serializer to bigints and widen the types together.
