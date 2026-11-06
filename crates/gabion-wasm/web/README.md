# Gabion · a distributed rate limiter — the visualizer's frontend

The browser app: an explorable explanation of how gabion rate-limits, and of
the two-tier zone gossip that spreads its counts, driven by the **real** gossip
+ CRDT core compiled to WebAssembly (see
[`../README.md`](../README.md)). Svelte 5 + Vite + TypeScript. It shows one
deterministic simulated run on one machine, not a measured estimate; measured
results, with credible intervals, are in the gossip study (`docs/study/`), which
the page links to from its header.

The page has a one-row header (the title, the lede that states the cluster and
its limit, the one-run statement and a "How to read this" glossary), a
workspace of three panes (the control rail, the stage, and the detail pane) and
a transport bar. It opens on Rate limiting, paused, with node 0 selected.

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
radius, one for every ring, is the least of `R_MAX`, the room on the fullest
ring (`2π·r_z / (S_D·n_z)`), `BEAM_CLEARANCE·r_z − CROSS_WIDTH/2`, and what the
nearest node of another ring leaves: that distance less `NEIGHBOUR_GAP`, a
selection outline's reach and a ring's, halved, so a selected node's outline
never touches a facing ring. A stage too small for `R_MAX` is laid out again
with the margin its own discs need. The
third term is what keeps a cross-zone beam clear of a third ring's discs: the
curves come within 0.386·`r_z` of a third ring at six zones, so a disc and half
a stroke must fit in 0.35·`r_z`.
Everything is in CSS pixels of the stage box, so labels are screen-size at every
width and nothing is letterboxed.

**Beams.** Every packet is drawn along `beamPath` (the one function that gives a
beam its route): inside a zone a straight segment, across zones a quadratic
curve bent through the stage's empty centre. Position carries the tier and two
data strokes repeat it: thin `--chart-muted` inside a zone, thicker `--color-fg`
across zones. A data packet in flight is a short tail with a head mark, a dot
inside a zone and an arrowhead across zones. Control packets (header-only acks
and probes, gap reports, sweep requests) are small dots with no tail. A lost
packet is a grey half-line ending in a stop bar, never a head, so it cannot
read as a packet in flight. Dashed strokes are not used: a beam is often
shorter than a dash.

Beams fly on a presentation clock (`canvas.beams.clock`): a flight is 520 ms of
it, which Speed scales and Pause freezes. In every preset but Sustained
overload the clock holds the engine: a wave of data packets is drawn, and the
engine delivers it, and each count changes, only when the renderer reports it
landed (rule 29). A Send's decision stands alone for 250 ms (`ARRIVAL_MS`)
before its eager flush flies. A landed data route stays drawn, whole, 1 px at
`LANDED_ALPHA` with no head: history, not the event in focus. It stays until
the next command where no feed runs, so a Send's waves read as a diagram, and
leaves at landing where a feed runs; a new layout drops it. With a node
selected, the beams that are not its own (and not relays sent by its gateways)
are drawn at a quarter of their alpha. `FADED_ALPHA` and `LANDED_ALPHA` are the
marks the contrast audit exempts, by name. The renderer keeps every beam in a
pre-sized pool and draws into one `Graphics` per mark style, and only when
something changed or a beam is in flight, so a frame allocates nothing of its
own. Each beam in the pool packs its flags (`layout.ts`): `CROSS` 1, `CONTROL`
2, `RELAY` 4, `DROPPED` 8 and `LANDED` 16.

**Nodes.** Each node is a vessel inside a ring (`renderer.ts`, `glyphs.ts`).
The disc's inside, `--color-panel` with no outline, fills from the bottom to the
node's own windowed count over 1.25 limits (`VESSEL_SCALE`, `lib/limit.ts`), in
`--color-level`, one stratum per non-empty window bucket, oldest lowest, where
the inside is at least `STRATA_MIN_INNER` px and a request's stratum at least
`STRATUM_MIN`; the part over the brim is `--color-bad`. The brim, 1.5 px of ink
at 0.8 of the inside, is the limit; it thickens into a red cap while the
engine's `brim` decision is `Rejected`. A limit more than `LIMIT_ON_SCALE`
requests away is off the scale: no fill, brim or cap, and the key says so. A
refused Send drops a red band into the vessel's headroom, the hits that would
have overflowed, clipped to the disc, rebounding and fading over 600 ms (at
rest and hatched under reduced motion). The ring is the share of the cluster's
windowed hits the node has heard: a closed neutral ring while its `lag` is
`CaughtUp`, an amber arc over a thin amber track while it is behind, held a
twelfth short of closing (`ARC_NOTCH`), sweeping up to its new share as a
packet lands. The selected node wears a 1.5 px outline, which sweeps closed
toward its heartbeat while it holds hits it has not sent; a hovered disc, or a
control that names a node, previews it dashed. The selected node's gateways
wear a port: a diamond outside the ring facing the zone each relays into,
hollow while suspected; their relays leave from it. Every drawn value goes into
the canvas's `glyphs` table first and is drawn from there.

**Labels.** Zone titles sit at the ring centres and each node's name, "node 4"
("node 4 · silenced", "node 4 · can't reach zone b" where a cut names it),
above the disc in the upper half of its ring and below it otherwise, 7 px
outside the ring, flipped or nudged when it would come within 12 px of another
name or touch a node's marks. The stage paints, back to front, the ring
guides, the landed routes, the labels with their halo, the beams in flight and
the nodes: a halo knocks out the ground, while a packet in flight, the figure,
crosses a zone title instead of vanishing under it. The renderer publishes the
stage centre (`data-centre`), the rings (`data-rings`), the stroke width
(`data-cross-width`), the marks drawn (`data-marks`) and every label's box
(`data-label-boxes`) on its canvas, and every chart publishes its label boxes
the same way, which is how the audit checks canvas text.

**Why a node grid over the canvas.** The PixiJS canvas is opaque to assistive
tech and to test tooling. `NodeGrid` lays every node over its disc as a
focusable cell of a `role="grid"` (one row per zone, a roving tabindex) holding
the node's count, centred in its disc (all counts shown or all hidden, as they
fit), and a Send's decision beside it ("+25 admitted", "25 rejected"), named by
a sentence ("Select node 7, zone b, sees 412 of 430, 18 behind"; only the
focused cell carries the live sentence, and a node that
lacks a change adds how long, at least). Left and Right
move within a zone, Up and Down to the same rank in the adjacent zone, Home and
End to the zone's ends, Enter or Space selects and Escape returns the selection
to the scenario's node. The cells
take no pointer events: the stage's hit test (`nodeAt`, the nearest disc within
its radius or a 24 px target) picks the node, so a mouse, a touch and a test all
reach the same code. Hover, focus and tap show one tooltip ("select node 7 ·
zone b · sees 412 of 430 (18 behind) · not caught up (≥ 1.5 s)"), and "Show as
table" lists the same rows as a table; both are in (zone, id) order, never
re-sorted. Every printed number is a `[data-field]` element, which is the DOM
source the functional tests read.

**Charts**, under the selected node's title, in the order the scenario lists
them. Chart 1 plots σ, the share of data bytes
(`Own`, `Relay` and `Sweep` packets) that crossed zones over the rule window, on
a fixed 0–100 % axis, over two dashed predictions: with zones, `(Z−1)/(N−1)`, and
with no zone, the mean of `(N − n_z)/(N − 1)` over the nodes weighted by each
node's own data bytes in the window (the engine counts them per node as
`own_data_bytes`). A window with no data leaves a gap. Charts 2 and 3 are one
panel per zone with members, in zone order: the band from the least to the most
informed node over the true total, and the spread (max − min) over a zero
reference; chart 3 ends with a "whole cluster" panel. A hovered or focused node
draws its own line in its zone's panel. Chart 4, the true total against the
limit, shows first where the limit is on the scale (Rate limiting, Zone
partition, Sustained overload).

**Detail pane.** One column that never swaps: the selected node's title, the
scenario's charts, its bucket window, **Gossip targets** and its identity, and
"Remove node N" at the foot. Gossip targets opens with the route sentence
("node 0 sends to its own zone directly, to zone b through node 4, and to zone
c through node 8") and is one table: the node's own zone, its zone peers, one
row per remote zone naming the gateway its runtime chose (with the changes not
yet acknowledged and how long an answer has been owed), the failover, gap
report, retransmission and sweep counters, and the error budget. Every figure
is the runtime's own admin row; the page chooses nothing.

**Controls** (one model: time, traffic, network, membership, configuration).
The transport holds Play/Pause, Step, Send and Speed. Send sends a request of
Hits per Send to the selected node, its label saying so ("Send a request of 25
hits to node 0", shorter on narrower screens, `lib/controls.ts`). A command
that creates something to spread (an admitted Send, Heal, Add) pressed while
paused plays until no node that waiting can reach is behind and one heartbeat
period has passed since the last landing, so the acknowledgements have flown,
or 16 periods, then pauses itself; the readout says "playing until every node
has it" while it does, and "packets in flight" while a wave holds the clock.
In Propagation the Send also stops before each wave, which Step flies. A
refused Send runs no time; pressed while playing, a command leaves time alone.
Step goes to the next event (the next wave of packets that carry hits, the
next instant hits leave the window, or one heartbeat period when neither is
coming) and is never disabled for playing. The rail has the scenarios in four
groups (the current one marked by a rule, never filled, and inert), Hits per
Send (checked when committed: Enter or leaving the field), Restart, a "Tune the
cluster" disclosure (edits stage, counted on its summary, until Rebuild;
"Back to the scenario's settings" restages the scenario's own), the network
actions (cut a zone off; under a heading naming the selected node, silence one
of its gateways or cut it off from its zone; heal) and Add. A scenario button,
Restart and Rebuild build the new run before they tear the old one down, and
Undo brings the old run back exactly until the next command or 30 s. A disabled
control says why beside it. The workspace stays mounted across a rebuild, so an
open disclosure and keyboard focus survive it. The transport says how long the
last heartbeat period took to watch at the chosen speed.

**Presets** (`presets.ts`): Rate limiting, Propagation and Sustained overload;
Zone partition, Packet loss, Gateway failure and Gateway cut; Steady state and
No zone configured; Sandbox. Each declares its config, the node it selects,
Hits per Send, its charts, its story and the scenario its story hands over to.
Rate limiting (limit 100, the default) and Zone partition (limit 100, zone c cut
at load) put the limit on the scale. Every preset opens on a warmed, settled
cluster: the page steps whole heartbeat periods until every node has a gateway
into every other zone and a period passes with no packet, then seeds it.
Propagation settles its seed before the first paint. Only the presets whose
subject is traffic have a feed (Steady state, No zone configured, Sustained
overload), plus Gateway cut's trickle of 2 hits/s at node 0, which keeps node 0
sending data its cut gateway must answer with "cannot relay". Every preset
declares its `pacing`: `'live'` for Sustained overload, `'held'` for the rest.

**The next action** (`lib/next.ts`). In every state the page decides one next
action from the run alone: press a transport control, press a button in the
caption's action line (Select node N, Use 25 hits), press the heading's repair,
go to the next scenario at a story's end, finish typing Hits per Send, wait, or
watch. Its control is the only filled one on the page (`data-primary`); the
action line names it by the label the control renders; a wait fills nothing and
shows a meter of its progress; the goal above the line explains the act and
never commands. Each story moves through acts in `observe`, the one place a
run's act changes. "Skip to the next step", the first Tab stop, focuses the
filled control or the line.

**The heading and the narration.** The heading (`lib/heading.ts`) states the
cluster: how many nodes see how many hits of the limit, the gap, the selected
node's reopening countdown while it refuses, the over-admission while the truth
passes the limit, the names of the cuts that strand nodes and the one repair
that undoes them. Each form has a template id (`data-template`). The narration
(`lib/narration.ts`), the page's one live region, states the last event and its
reason: a decision, a run's end, a refusal with the nodes whose hits fill the
window, an expiry, a wave on Step, a gateway change, a fresh start.

`window.sim` is the live engine, the handle the audit and the console use
(`sim.snapshot(inspect, watch)`); each chart's plot element carries its uPlot
instance as `uplot`, which the audit reads to check every mark lies inside its
plot; the stage canvas carries `beams`, the renderer's beam pool (`clock`,
`redraws`, `inFlight`, `due`, and the `push` and `land` the audit wraps to see
each beam enter and land) and `beamGfx`, the Graphics the beams are drawn into
(read to check how a landed route is drawn) and `glyphs`, the `GlyphTable` of
every node mark as drawn (vessel heights, strata, cap, ring role and arc, port
centres, the refusal's band, the selection and the preview); and the stage pane
publishes `data-held` (the sequence number of the wave held, "0" when none is),
`data-flight` (`none`, `waiting`, `flying` or `frozen`), `data-run` (`off`,
`settled` or `per-wave`) and `data-playing`.

## Fonts

Source Sans 3 (the interface, counts and numerals), Source Serif 4 (the goal
and the glossary) and Source Code Pro (hex ids), self-hosted from the
google/fonts registry at one pinned commit (`FONTS_COMMIT` in the Makefile).
`make fonts` downloads the three variable faces and their `OFL.txt`, instances
them to the weights the page uses (400 and 600; Source Serif 4's optical size
8 to 20), subsets them to the characters the pages print, and writes the
woff2 files and licences to `src/fonts/` and, byte-identical (a hygiene check),
to `docs/study/dashboard/fonts/`. It needs the network and fonttools, so it is
not part of `make test`; its outputs are committed. `font-display: block` holds
text until a face loads; the canvas and the charts wait for the faces
(`fontsReady`, `lib/theme.ts`), failing open after `FONT_GATE_MS`.

## Layout

- `src/App.svelte` — orchestrates the session: builds and warms the `Sim`,
  runs the play loop, owns transport state and the selection, and runs every
  engine call through one queue, holding each wave until the stage reports it
  landed. The stage takes state as props (the clock's running and speed, the
  trail) and the packets as commands (`show`, `finish`, `clearTrail`, `reset`)
  in the queue's order.
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
  arguments, asserts the numeric boundary contract at runtime, and keeps the
  list of cuts (`lib/cuts.ts`) by what each means, applying them to a node
  that joins.
- `src/lib/run.svelte.ts` — `Run`, one run of one scenario: its engine and what
  the page keeps about it, which Undo swaps back whole.
- `src/lib/next.ts`, `heading.ts`, `narration.ts`, `controls.ts`, `limit.ts`,
  `glyphs.ts`, `requests.ts` — the next action, the heading, the narration's
  sentences, the transport's labels, the limit's scale, the one glyph geometry
  and the admitted requests' ring.
- `src/lib/stage/` — `layout.ts` is the geometry (`stageLayout`, `beamPath`,
  `pointOn`, `nodeAt`); `renderer.ts` is the framework-agnostic `StageRenderer`
  that owns the PixiJS canvas, the beam pool, the labels and the glyphs.
- `src/lib/charts/` — `history.ts` is the bounded, column-major rolling
  time-series with the windowed share; `options.ts` builds each uPlot chart's
  options (shared cursor sync, references drawn first, labels through one
  halo-and-record helper); `Chart.svelte` is the generic imperative uPlot
  wrapper.
- `src/lib/components/` — `Stage` (the renderer and the key), `NodeGrid`
  (grid, counts, decisions, tooltip, table), `StageCaption` (goal and action
  line), `StageHeading`, `Dashboard`, `NodeInspector` with `GossipTargets` and
  `BucketStrata`, `ControlRail`, `TransportBar`, `RoleGlyph` and `Icon` (the
  drawn marks), `Sentence` and `InfoTip`.
- `src/lib/presets.ts` — the scenario presets: a config, a `pacing`, the node
  it selects, Hits per Send, its charts, its story and the scenario it hands
  over to, an optional feed and an `async seed(sim, config, gateways)` that
  fires the opening commands against a fresh, warmed cluster (`warm`). A feed
  injects ⌊rate·T⌋ hits by virtual time T since the build was handed to the
  reader.
- `src/wasm/` — generated by `pnpm run build:wasm`; never edited or committed.
- `tests/screenshot.spec.ts` — the functional smoke.
- `tests/audit.spec.ts` — the automated design audit (see "Design rules").
- `tests/motion.spec.ts` — the waves in motion (see "Tests").
- `tests/story.spec.ts` — each story walked by its next actions;
  `tests/next.spec.ts` and `tests/next-actions.ts` — the next-action table.
- `tests/drive.ts` — Step, a heartbeat period of Steps, Steps until a
  condition, a selection and a Send that returns once its run has ended,
  shared by the specs; `tests/sourcemap.ts` maps a bundle position
  to its source through the hidden source maps the build writes.

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
pnpm run audit                           # eleven projects; writes audit-shots/
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
12. Censored values marked "≥": a node that lacks a change reads "not caught up (≥ t)" in its tooltip, and the stage heading gives the largest t, from one heartbeat period up, where t is the time since the oldest missing change's cell was last updated at its origin (the engine's `lag`, `Behind` or `Stranded`, with its `missing_for_ms`). That update is no earlier than the change itself, so t never overstates how long the node has been behind (A4).
13. Tie band: N/A, the UI makes no comparisons.
14. The lede and the stage heading state the cluster; specific empty states (A4, A18).
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
29. Cause before effect: in every preset but Sustained overload, and on every Step, the clock waits while a wave of data packets is drawn; the engine delivers the wave, and each count changes, when it lands. A command first fast-forwards any run in flight past its relays, then shows its own effect; a rebuild discards it. Sustained overload shows counts as the engine computes them. Step stops at the next wave, or at the next instant hits leave the window (motion suite).
30. One next action: in every state exactly one control is filled, the caption's action line quotes its label, a wait fills nothing and shows its progress, and the goal never commands (A49, A54, A56; story suite).

The grep gates, which `make hygiene` runs: no colour literal outside
`tokens.css`, no pixel `font-size`, no token a component names that
`tokens.css` does not define (so no pre-token name such as `--ink` survives),
and no `new Graphics` in `renderer.ts` outside its constructor and
`#createNode`.

Automated checks, run on every preset after a walk by Step:

- A1: no page or console errors, no failed requests, no page-level horizontal overflow.
- A2: no "NaN", "undefined", "null", "Infinity", "[object" or "e+" in page text.
- A3: no two visible text boxes, DOM or canvas, overlap by 1 px or more, also in a converged state; no text is below the smallest type token (`--font-size-caption-sm`) and none is clipped by an `overflow: hidden` ancestor; every chart value lies inside its plot and every y axis labels at least two values; no glyph that carries meaning is set from a font; the stage names every node; tooltips stay inside the stage; and for every zone count the stage's rings (grown by the largest disc) and labels lie inside it, no two nodes of different rings touch, and no cross-zone `beamPath` comes within a disc and half a stroke of a third ring, and every beam in flight bends through the centre exactly when its nodes' zones differ and wears its packet's kind, tier and fate (a dot for an acknowledgement or a sweep request, a relay marked as one), one beam per packet.
- A4: data correctness, recomputed independently from the engine snapshot in the test's own code: the lede's counts, each node's behind-by and gateway-for count, the heading (A18: its form, truth, gap, countdown and censored "not caught up (≥ t)"), each chart-3 spread, the windowed share, both chart-1 predictions (with unequal zones, and absent with one node) and, for the selected node, its Gossip targets.
- A5: fixed order across the grid, table, chart panels and Gossip-targets rows; the key lists exactly the marks drawn.
- A42: every node mark against the layout and the snapshot: each port on its circle facing its zone and clear of its disc, every ring and count; the selection clear of facing rings; each name a node's name, smaller than the counts, inside the canvas and clear of every mark; the fill's top and brim on the vessel's scale, the cap with the brim decision, the ring's role with the lag.
- A6: interaction: the zone stepper and Rebuild, each network action and heal, keyboard navigation with focus kept across a rebuild, a tooltip on hover, focus and tap, and the study link's form (`study/` joins the site only in the Pages build, so its href is checked, not followed).
- A7: contrast from computed styles, text at 4.5:1 and graphics at 3:1 on the surface they sit on, translucent marks composited over it first, in both themes, with targets of at least 24 px.
- A8: the wording scan (rule 1) and the clock-time scan (rule 2).
- A9: a digit scan outside `[data-field]` elements (rule 7).

`tests/audit.spec.ts` automates them, in ten projects, and checks each preset a second time with `data-theme` forcing the other theme. It saves a full-page PNG per project, preset and theme to `audit-shots/`. `tests/motion.spec.ts` checks rule 29 with motion on and saves Propagation's two waves, paused mid-flight, as `propagation-wave1.png` and `propagation-wave2.png`.

The visual audit reads those PNGs for: alignment, spacing rhythm, type
hierarchy, nothing clipped, a dark theme as considered as the light one, and a
fully usable phone layout.

## Tests

`pnpm run screenshot` is the functional smoke (one project at 1440 px in the
light theme): it boots the real core on a settled cluster, spreads a Send to
every node and watches the hits leave the window, sends from the transport to
the selection, selects, adds and removes nodes (and checks a joiner is cut as
its zone), drives every control and checks the detail pane, reading
numbers from the node table, and checks that every data packet Sustained
overload sends, by its feed's submits or by its steps, is drawn, one beam each.

`pnpm run audit` runs four suites. `tests/next.spec.ts` checks every row of
the next-action table, every goal against the clause rule and the heading's
forms, with no page. `tests/story.spec.ts` walks Rate limiting state by state
in every audit project and the motion project, pressing only the filled
control, and checks in each state that one control is filled and the line
quotes it, that a wait shows a meter that never falls, that the goal never
commands and that one live region speaks, with the heading's templates and the
refusal's sentence; Propagation's Send stopping before each wave that Step
flies; Zone partition's 200 admitted against 100; Hits per Send checked on
commit; the skip link and keys; buttons and boundaries and the dashed preview
of every control that names a node; the title; the one-column pane; and a
story's hand-over and Undo. The design audit is ten projects (widths
1440, 1280, 1100, 768 and 400, each in light and dark, with motion reduced).
Its main test walks every preset a few heartbeat periods by Step at 4×, runs A1
to A9, checks the stage tooltips, and recomputes A4 and A5 from the engine's
snapshot, then repeats the page checks with `data-theme` forcing the other
theme. Further tests check chart 1 across the rule window, with unequal zones
over a window that opens at a kept mark, and with one node (at the widest and
the narrowest stage); the share window at any snapshot rate; the stage geometry
for every zone count (at the widest and the narrowest stage, with facing discs
of neighbouring rings kept apart); that the beams a Step draws are its packets,
one each with its packet's kind, tier and fate, over packets of every kind (a
node added to the live cluster brings the join sweep); that a paused stage's
labels are not rebuilt per snapshot; that every preset opens warmed and settled
(each seeded node's first wave reaches one gateway per other zone, and
Propagation opens with its seed everywhere and no beam); that runs go quiet
after agreement; that both gateway presets fail over within the rule window;
that the transport bar keeps its height and its readouts their place when the
heartbeat reading appears, and is one row from 1100 px up; the node glyph
through a Rate limiting walk (levels, strata, caps, the rings of a held
flight, counts and their contrast, and the refusal's band inside its disc,
above its brim and clear of its count); the faces (every visible text in the
three families, every request to the page itself) and the stage drawing though
a face fails to load; the key against the marks drawn; and the interactions,
with touch.

The motion suite (one project, 1440 px, light, motion on) checks rule 29: in
every held preset, played at 4× after a Send, no node's count rises before a
beam into it lands or its own request is admitted, no wave leaves while another
flies, simulated time does not move while a wave is held, and the counts shown
during a hold are the engine's (also for Gateway cut at limit 100 and a
one-second heartbeat, whose trickle flushes at once, held too); back-to-back
Sends each flush at once, past the emit floor; Step flies Propagation's two
waves one at a time, at 1× and at the slowest speed, and then reaches the
window's edge; a flight lasts the flight over the speed, at 1× and at a fifth,
and Pause freezes it, pool, clock and pixels, with the beams in flight painted
over the labels; the heartbeat readout; a ring sweeping up as its packet
lands; the allocations of the stage's per-frame and per-snapshot functions,
every one tracked by the heap profiler in a browser that only interprets and
mapped to its source through the hidden source maps: after warm stretches, a
stretch of 300 played frames of Packet loss (node 0's gateways' relays drawn),
Rate limiting (vessels redrawn, rings sweeping, refusals played) and
Propagation (the heartbeat sweep), in which they allocate nothing but boxed
doubles, the engine's own; and the landed routes, with motion
on and reduced, painted under the labels. The checks of how long a flight or a
heartbeat takes to watch, of the back-to-back Sends and of the allocations run
the page on Playwright's fake clock, so they hold on any machine.

Full-page PNGs land in `audit-shots/`, which is gitignored; CI uploads it as a
workflow artifact. All of it downloads a browser, so, like the node
`smoke.cjs`, it is an on-demand and CI run, not part of `make wasm-check`.
On the machine that wrote this, `pnpm run audit` takes three to five minutes
with Playwright's default workers, depending on what else is running.

## Why no `vite-plugin-wasm`

The app needs no `vite-plugin-wasm`: the wasm package is built with
`wasm-pack --target web` (see `build:wasm` in `package.json`), whose glue loads
the module via `new URL('…_bg.wasm', import.meta.url)`, which Vite resolves and
emits natively: one fewer dependency and no top-level-await shim. `--target web` also keeps its output isolated in
`src/wasm/`, leaving the node-target `../pkg/` (used by `smoke.cjs`) untouched.

## Numeric boundary

`u128` ids (rule, key, node id, fingerprint) cross as `"0x…"` hex strings;
`u64` counts and timestamps cross as JS numbers (serde-wasm-bindgen's default,
exact below 2^53 — far above the visualizer's volumes). `sim.ts` asserts this
once at runtime and `types.ts` documents it; if a scenario could exceed 2^53,
flip the serializer to bigints and widen the types together.
