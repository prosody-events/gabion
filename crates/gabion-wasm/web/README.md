# Gabion Gossip Visualizer — frontend

The browser app: an explorable explanation of gabion's two-tier zone gossip,
driven by the **real** gossip + CRDT core compiled to WebAssembly (see
[`../README.md`](../README.md)). Svelte 5 + Vite + TypeScript. It shows one
deterministic simulated run on one machine, not a measured estimate; measured
results, with credible intervals, are in the gossip study (`docs/study/`), which
the page links to from its header.

The page has a header (title, one status sentence, the one-run statement and a
"How to read this" glossary), a workspace of three panes (the control rail, the
stage, and the charts or the node inspector) and a transport bar.

## Tiering view

**Stage.** Each zone is a ring of nodes, and the rings sit evenly on one larger
circle around an empty centre, zone a at the top and then clockwise
(`src/lib/stage/layout.ts`). With `w` and `h` the stage's size, `m` the margin a
node needs outside its centre (the largest disc, the gap to its label and the
width of a label, measured) and `Z` the zone count, the rings sit at distance
`R = (min(w, h)/2 − m) / (1 + K_Z·sin(π/Z))` from the centre and their nodes at
`r_z = R·K_Z·sin(π/Z)` from the ring's centre, with `K_Z = 0.75`; with one zone
`R = 0` and `r_z = min(w, h)/2 − m`. A node's angle in its ring depends only on
its rank by id within the zone, so order never depends on a value. A disc's
radius is the least of `R_MAX`, the room on its ring (`2π·r_z / (S_D·n_z)`) and
`BEAM_CLEARANCE·r_z − CROSS_WIDTH/2`. The last term is what keeps a cross-zone
beam clear of a third ring's discs: the curves come within 0.386·`r_z` of a
third ring at six zones, so a disc and half a stroke must fit in 0.35·`r_z`.
Everything is in CSS pixels of the stage box, so labels are screen-size at every
width and nothing is letterboxed.

**Beams.** Every packet is drawn along `beamPath` (the one function that gives a
beam its route): inside a zone a straight segment, across zones a quadratic
curve bent through the stage's empty centre. Position carries the tier and two
data strokes repeat it: thin `--chart-muted` inside a zone, thicker `--color-fg`
with an arrowhead across zones. Control packets (header-only acks and probes,
gap reports, sweep requests) are small dots with no tail. A dropped packet greys
and stops half-way. Dashed strokes are not used: a beam is often shorter than a
dash. With a node selected, the beams that are not its own (and not relays sent
by its gateways) are drawn at a quarter of their alpha, the one mark the
contrast audit exempts. The renderer keeps every beam in a pre-sized pool and
draws a frame into one `Graphics` per mark style, so a frame allocates nothing
of its own.

**Roles.** Selecting a node marks it with a thick ring (the origin) and the
gateway the runtime reports for each remote zone with a diamond: filled ◆, or
hollow ◇ once the runtime suspects it. A key under the stage names every mark
and a one-line annotation reports the latest gateway change or join-sweep
request.

**Labels.** Zone titles sit at the ring centres and node ids outside the disc.
Labels are placed greedily (zone titles first, then ids in (zone, id) order) and
a label is drawn only if its measured box, halo included, lies inside the stage
and overlaps no label and no disc. The renderer publishes the stage centre
(`data-centre`), the rings (`data-rings`), the stroke width (`data-cross-width`)
and every label's box (`data-label-boxes`) on its canvas, and every chart
publishes its label boxes the same way, which is how the audit checks canvas
text.

**Why a node grid over the canvas.** The PixiJS canvas is opaque to assistive
tech and to test tooling. `NodeGrid` lays every node over its disc as a
focusable cell of a `role="grid"` (one row per zone, a roving tabindex), named by
a sentence ("Node 7, zone b, sees 412 of 430, 18 behind, gateway into zone b for
9 origins"; only the focused cell carries the live sentence). Left and Right
move within a zone, Up and Down to the same rank in the adjacent zone, Home and
End to the zone's ends, Enter or Space selects and Escape deselects. The cells
take no pointer events: the stage's hit test (`nodeAt`, the nearest disc within
its radius or a 24 px target) picks the node, so a mouse, a touch and a test all
reach the same code. Hover, focus and tap show one tooltip ("Node 7 · zone b ·
sees 412 of 430 (18 behind) · gateway into zone b for 9 origins"), and "Show as
table" lists the same rows as a table; both are in (zone, id) order, never
re-sorted. Every printed number is a `[data-field]` element, which is the DOM
source the functional tests read.

**Charts** (while no node is selected). Chart 1 plots σ, the share of data bytes
(`Own`, `Relay` and `Sweep` packets) that crossed zones over the rule window, on
a fixed 0–100 % axis, over two dashed predictions: with zones, `(Z−1)/(N−1)`, and
with no zone, the mean of `(N − n_z)/(N − 1)` over origins weighted by each
origin's own data bytes in the window (the engine counts them per node as
`own_data_bytes`). A window with no data leaves a gap. Charts 2 and 3 are one
panel per zone with members, in zone order: the band from the least to the most
informed node over the true total, and the spread (max − min) over a zero
reference; chart 3 ends with a "whole cluster" panel. A hovered or focused node
draws its own line in its zone's panel. Chart 4, aggregate against limit, shows
only under Sustained overload.

**Inspector.** Selecting a node replaces the charts with its hero total, the
bucket window, **Gossip targets** and its identity. Gossip targets is one table:
the node's own zone, its zone peers, one row per remote zone naming the gateway
its runtime chose (with the changes not yet acknowledged and how long an answer
has been owed), the failover, gap report, retransmission and sweep counters, and
the error budget. Every figure is the runtime's own admin row; the page chooses
nothing.

**Controls.** The rail has the scenario presets, a "Tune the cluster" disclosure
(zones up to the engine's ceiling, zone mode, error budget, loss, gossip interval,
window, limit; edits stage until Rebuild), the network actions (cut a zone off;
with a node selected, silence its gateway into a zone or cut that gateway from
its zone, both named by the selection; heal), live membership (add into a zone,
remove) and a burst. The workspace stays mounted across a rebuild, so an open
disclosure and keyboard focus survive it.

**Presets** build their blurbs from the config they run with, so no number in
them is typed. Traffic burst, Steady state, Sustained overload, Zone partition,
Packet loss, Gateway failure (the origin's gateway silenced), Gateway cut from
its zone (one direction blocked, which the `CANNOT_RELAY` backstop repairs), No
zone configured and Sandbox. Traffic burst and the gateway presets open with
their origin selected.

`window.sim` is the live engine, the handle the audit and the console use
(`sim.snapshot(id)`); each chart's plot element carries its uPlot instance as
`uplot`, which the audit reads to check every mark lies inside its plot.

## Layout

- `src/App.svelte` — orchestrates the session: builds the `Sim`, runs the gated
  play loop, owns transport state and the selection, and routes each step's events
  to the stage.
- `src/tokens.css` — the design tokens, the one source of design values. Each
  colour holds its light and dark value in `light-dark()`: the OS preference
  picks one and `<html data-theme="light|dark">` overrides it, which sets the
  browser floor at Chrome 123, Safari 17.5 and Firefox 120. The study dashboard
  keeps a byte-identical copy. `src/app.css` holds only the base element rules.
- `src/lib/theme.ts` — `readTheme` resolves the colours and the canvas type
  PixiJS and uPlot need as numbers, painting each colour and reading the pixel
  back; `watchTheme` reports an OS scheme or `data-theme` change; `cssColor`
  formats a colour for uPlot.
- `src/lib/format.ts` — the one place a number or zone name becomes text.
- `src/lib/sim/` — the wasm boundary. `types.ts` hand-mirrors the serde shapes
  (the generated `.d.ts` types every payload as `any`); `sim.ts` is the typed
  wrapper that converts JS numbers to the `bigint`s wasm-bindgen wants for u64
  arguments, asserts the numeric boundary contract at runtime, keeps the set of
  blocked links the status sentence counts, and wraps the network actions.
- `src/lib/stage/` — `layout.ts` is the geometry (`stageLayout`, `beamPath`,
  `pointOn`, `nodeAt`); `renderer.ts` is the framework-agnostic `StageRenderer`
  that owns the PixiJS canvas, the beam pool, the labels and the glyphs.
- `src/lib/charts/` — `history.ts` is the bounded, column-major rolling
  time-series with the windowed share; `options.ts` builds each uPlot chart's
  options (shared cursor sync, references drawn first, labels through one
  halo-and-record helper); `Chart.svelte` is the generic imperative uPlot
  wrapper.
- `src/lib/components/` — `Stage` (the renderer, key and annotation),
  `NodeGrid` (grid, tooltip, table), `Dashboard`, `NodeInspector` with
  `GossipTargets` and `BucketStrata`, `HeadlineMetric`, `ControlRail`,
  `TransportBar` and `InfoTip`.
- `src/lib/presets.ts` — the scenario presets: a config, a blurb built from it,
  an optional selected origin and an `async seed(sim, config)` that fires the
  opening commands against a fresh cluster. Deliberately *not* a timed script:
  the cut-then-heal story is a user gesture, and a sustained feed is a
  carry-save rate the play loop folds into each advance.
- `src/wasm/` — generated by `pnpm run build:wasm`; never edited or committed.
- `tests/screenshot.spec.ts` — the functional smoke.
- `tests/audit.spec.ts` — the automated design audit (see "Design rules").

## Develop

```sh
pnpm install
pnpm run dev        # builds the wasm (debug) and starts Vite with HMR
```

`pnpm run build` rebuilds the wasm in release, type-checks with `svelte-check`,
and runs the production Vite build — this is what `make wasm-check` runs.

## Running the browser tests

```sh
pnpm exec playwright install chromium   # one-time
pnpm run build
pnpm run screenshot                      # writes screenshots/ring-{initial,gossip,converged}.png
pnpm run audit                           # ten projects; writes audit-shots/
```

See "Tests" below for what each suite checks.

## Design rules

The audit checklist for this page and, through `docs/study/README.md`, for the
study dashboard. A rule that assumes the study's data model says "N/A" and why.

1. Bayesian wording only: the UI prints no statistics; run-specific timings say "in this run" (A8).
2. Relative time only: virtual time is elapsed seconds, never a wall-clock time (A8).
3. Fixed order, never re-sorted: zones by index, nodes by (zone, stable id), a fixed preset list (A5).
4. Tufte-grade: data-ink, direct labels, no chartjunk, no legends (visual audit). Zone ring guides repeat what position and the ring title say, so they are exempt from the 3:1 graphics rule.
5. Light and dark from tokens, and the phone works (A1, A7, visual audit).
6. Screenshot-audited and Playwright-tested (CI).
7. No literal numbers in markup: preset blurbs come from config and every printed number is a `data-field` element (A9).
8. Every estimate followed by its interval: N/A, the UI shows exact values of one deterministic run.
9. One notation per context, fixed significant figures, singular units (A2).
10. Honest scale, axes fitted to the data (visual audit).
11. Zero gets its own treatment, never symlog (visual audit).
12. Censored values marked "≥": not yet met. A node that has not caught up needs the age of the oldest change it is missing, which the snapshot does not carry.
13. Tie band: N/A, the UI makes no comparisons.
14. Status sentence and specific empty states (A4).
15. The reference is drawn first, muted and labelled (visual audit).
16. One encoding everywhere (visual audit).
17. Shape carries meaning and colour only repeats it (visual audit).
18. Seed ticks: N/A, one run, no seeds.
19. Paint order and halos; labels stay off the series (A3, visual audit).
20. Colour tokens only, type tokens, named geometry constants, measured labels (grep gates).
21. Page order, titles, notes, axis units, line length of about 80 characters (visual audit).
22. Breakpoints that keep everything usable: 1100 px and 640 px, audited at 1440, 1280, 1100, 768 and 400 px (A1, A3).
23. A "Show as table" fallback and a method section (A4).
24. Scorecard rules: N/A, no scorecard.
25. Detail-pane rules: fitted axes, no duplicated sentences (visual audit).
26. One rAF-coalesced draw, a debounced ResizeObserver, focus kept across a rebuild (A6).
27. Accessibility: a focusable roving-tabindex grid, sentence labels, tooltips on hover, focus and tap, targets of at least 24 px, contrast (A3, A6, A7).
28. Automated checks carried over (CI).

The grep gates, which `make hygiene` runs: no colour literal outside
`tokens.css`, no pixel `font-size`, and no token a component names that
`tokens.css` does not define (so no pre-token name such as `--ink` survives).

Automated checks, run on every preset with the transport paused:

- A1: no page or console errors, no failed requests, no page-level horizontal overflow.
- A2: no "NaN", "undefined", "null", "Infinity", "[object" or "e+" in page text.
- A3: no two visible text boxes, DOM or canvas, overlap by 1 px or more, also in a converged state; no text is below the smallest type token (`--font-size-caption-sm`) and none is clipped by an `overflow: hidden` ancestor; every chart value lies inside its plot; tooltips stay inside the stage; and for every zone count the stage's rings (grown by the largest disc) and labels lie inside it and no cross-zone `beamPath` comes within a disc and half a stroke of a third ring.
- A4: data correctness, recomputed independently from the engine snapshot in the test's own code: the status counts, each node's behind-by and gateway-for count, each chart-3 spread, the windowed share, both chart-1 predictions (with unequal zones, and absent with one node) and, for a selected origin, its Gossip targets.
- A5: fixed order across the grid, table, chart panels and inspector rows.
- A6: interaction: the zone stepper and Rebuild, each network action and heal, keyboard navigation with focus kept across a rebuild, a tooltip on hover, focus and tap, and the study link's form (`study/` joins the site only in the Pages build, so its href is checked, not followed).
- A7: contrast from computed styles, text at 4.5:1 and graphics at 3:1 on the surface they sit on, translucent marks composited over it first, in both themes, with targets of at least 24 px.
- A8: the wording scan (rule 1) and the clock-time scan (rule 2).
- A9: a digit scan outside `[data-field]` elements (rule 7).

`tests/audit.spec.ts` automates all nine, in ten projects, and checks each preset a second time with `data-theme` forcing the other theme. It saves a full-page PNG per project, preset and theme to `audit-shots/`.

The visual audit reads those PNGs for: alignment, spacing rhythm, type
hierarchy, nothing clipped, a dark theme as considered as the light one, and a
fully usable phone layout.

## Tests

`pnpm run screenshot` is the functional smoke (twenty tests, one project at
1440 px in the light theme): it boots the real core, gossips a burst to every
node, selects, adds and removes nodes, drives every control and checks the
inspector, reading numbers from the node table. `pnpm run audit` is the design
audit: ten projects (widths 1440, 1280, 1100, 768 and 400, each in light and dark,
with motion reduced). Its main test walks every preset to a fixed virtual time
with the transport paused, runs A1 to A9, checks the stage tooltips, and recomputes
A4 and A5 from the engine's snapshot, then repeats the page checks with
`data-theme` forcing the other theme. Further tests check chart 1 across the
rule window and with unequal zones and one node, the stage geometry for every
zone count (at the widest and the narrowest stage) and the interactions, with
touch. Full-page PNGs land in `audit-shots/`, which is gitignored; CI uploads it
as a workflow artifact. All of it downloads a browser, so, like the node
`smoke.cjs`, it is an on-demand and CI run, not part of `make wasm-check`. On the
machine that wrote this the whole audit takes about four and a half minutes with one worker and under two with six.

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
