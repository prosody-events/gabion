# gossip-bench report — typography & plot audit (AUDIT2)

Audited artefacts:

- PDF: `target/gossip-bench/report.pdf` — 11 pages, US-letter
- Typst: `crates/gossip-bench/bench/report.typ` (833 lines)
- Plot code: `crates/gossip-bench/bench/render.py`, `crates/gossip-bench/bench/tufte.py`
- SVGs: `target/gossip-bench/figures/{convergence,fanout_sweep,scale_n,loss,partition,staleness}.svg`

## 1. Executive verdict

A careful, restrained document with the bones of an elegant report; weakened by a handful of mechanical defects rather than taste failures. **Bringhurst: 6.5 / 10.** **Tufte: 6 / 10.** The single biggest weakness is that every plot is set in DejaVu Serif while the body is set in New Computer Modern; this jars on every figure-bearing page. Second-biggest: the *Tufte* range-frame helper exists in `tufte.py` but is never called, and the captions hide the figure numbers Bringhurst explicitly asks for.

---

## 2. Page-by-page audit

### Page 1 — Title page

- Title at 26pt regular in NCM Roman is the right call (Bringhurst 7.2 — body face, larger size, no display face). It sits at `v(38%)` which is close to the golden-ratio optical centre. Good.
- Subtitle (lines `report.typ:146–149`) is set italic in `sub-color`. Italic over three centred lines reads as a quote, not a subtitle; Bringhurst 4.4 says ragged centred blocks of italic should be three lines at most and want extra leading. It's fine here but the manual `\` line breaks are brittle — replace with `set par(justify: false)` plus natural wrap.
- Footer slug (`report.typ:152–156`) mixes a date, `raw` (typewriter) tokens, and `· `-style middle-dot separators. The middle dot is wrapped in `#h(0.4em)` on both sides, producing visible loose dots. Replace with `sym.bullet.small` or `sym.dot.c` and remove the manual spacing (`#h(0.4em) · #h(0.4em)` → ` · `).
- Title page has no figure number, no plate, no epigraph: nothing else competes for the eye. Restful. Keep.
- No page number; the running head is suppressed via `if here().page() > 1`. Correct.

### Page 2 — *What this document is* + glossary

- **Measure is too wide.** Body runs ~85 characters per line (5.9-inch text block at 10 pt NCM). Bringhurst 2.2 caps the comfortable measure at 75 characters; this report is comfortably above. Either narrow the measure (e.g. `outside: 1.95in` to drop to ~78 chars) or bump body to 11 pt. The fix touches `report.typ:34`.
- The H1 "What this document is" is set in letter-spaced small caps at 16 pt. Two issues: (a) small-cap H1 plus small-cap H2 means the hierarchy is differentiated only by size, not by style — Bringhurst's "stately progression" calls for at least one axis of contrast. Make H1 NCM Roman caps at 16 pt and reserve small caps for H2; or make H1 italic at 18 pt. (b) The 16-pt small caps wrap across two lines on pages 4 ("Convergence: how many rounds to inform / everyone"), 6, 10 — the wrap is at an arbitrary point. Add `set par(leading: 0.7em)` inside the H1 show-rule so a two-line heading doesn't crowd vertically.
- Em dashes inconsistent. Body uses unspaced em dash (e.g. "small handful of peers—its *fanout*—and sends them"). Caption on page 4 has "production's default—six—would lie" with the same convention. But the prelude on page 4 has "a small constant penalty—" with a *trailing* space-em-dash that orphans the dash at line end. Sweep `report.typ` for `—\n` and `—\s\n` and normalize.
- Definition list "round  one tick_interval…" uses `sc(term) #h(0.5em) body` (line 114). Small caps "round" + horizontal space + body is unusual: a Bringhurst hanging indent expects the term flush left and the body indented further (or a tab). Right now `hanging-indent: 1.5em` is set, but the term and body share a single justified paragraph and the spacing reads as a gap, not a separator. Use `block(grid(columns: (5em, 1fr), …))` or a `terms` list.
- River risk: line 5 of the second body paragraph (page 2, "the receiving peer's view differs, the two sides merge…") has a thin vertical river running from the space after "view" through "merge". Caused by full justification on an 85-char measure with `hyphenate: true` flipping word breaks. Mitigates if measure narrows.

### Page 3 — Methodology

- **Headings collide with the previous paragraph.** The H2 *Per-tick driving* runs flush against the trailing line of *Per-node setup*. The H2 show-rule has `above: 1.1em` but the prior paragraph ends mid-measure so the extra leading isn't visible enough. Bump `above` to 1.6em.
- The paragraph beginning "Each simulated node owns one `GossipRuntime`..." has a *missing word*. The text reads "...; its outbound transport is a `CountingTransport` wrapping the in-process `SimTransport` its downstream aggregate store is the `BenchAggregateStore<u32>`...". Should be `SimTransport`. **Its downstream aggregate store...` — a missing period or semicolon. Inspect `report.typ:289–292`.
- The raw inline `BenchAggregateStore<u32>` containing the `<` `>` characters renders as code, fine, but the surrounding sentence justifies around it leaving large word-spaces. Consider wrapping such inline `raw` blocks in `box[#raw(…)]` so Typst's justifier treats them as atomic.
- The text in the definition list on page 3 ("Membership / failure detection.") uses the small-caps idiom from `defn()` but the term contains a slash — the small-caps version renders the slash mid-cap height. Replace with an en dash (`Membership – failure detection`).
- The first paragraph after the H1 "Methodology" is set without first-line indent (correct per Bringhurst 5.2.4) but the *next* H2 "Per-node setup" is immediately followed by another `#set par(first-line-indent: 0em)`. The pattern is repeated 7 times. Move it into a `show heading: it => { it; set par(first-line-indent: 0em) }` rule so this isn't hand-managed on every section.

### Page 4 — Convergence section + first plot

- **The figure has no number.** `report.typ:85–89` defines a caption show-rule that emits only `it.body`, dropping `it.supplement` (set on line 95) and `it.counter`. Bringhurst 8.5: figure numbers belong in figure captions. The cross-reference "see Figure 3" idiom is impossible right now. Fix at `report.typ:88`:
  ```typst
  #it.supplement #context it.counter.display(it.numbering).
  #h(0.4em) #it.body
  ```
- **Caption is centred.** The show rule sets `justify: false` but never sets alignment; Typst defaults to centred for figure captions. Bringhurst 8.5 wants ragged-right (left-aligned). Add `set align(left)` on line 87.
- **Plot uses DejaVu Serif.** Every numeric label, every axis title, every direct-label is rendered as a DejaVu glyph-path. Body is New Computer Modern. The visual mismatch is severe on the "f=1, f=2..." right-edge labels because they sit next to the running head. The root cause is mathtext: `set_xscale("log", base=2)` makes matplotlib auto-format ticks as `$2^3$`, and mathtext is *always* rasterized as paths regardless of `svg.fonttype: 'none'`. The fix is to drop mathtext and supply explicit string ticks:
  ```python
  left.set_xticks([4, 8, 16, 32, 64, 128, 256])
  left.set_xticklabels(["4", "8", "16", "32", "64", "128", "256"])
  ```
  at `render.py:127` and equivalent everywhere `set_xscale("log", base=2)` appears (lines 127, 145, 266, 275).
- The convergence plot has **two y-axes ranges that lie about the data**. Left pane shows `f=1` peaking at 14 rounds, jagging up-down between N=64 and N=128 (12→10→14). That non-monotonic jag for the *push-only chain* is the result that needs flagging in the caption — Tufte's "captions explain what to look at and why" — but the current caption just narrates "rounds collapse". Annotate the f=1 jag explicitly.
- "What good looks like / What would be bad" blocks (`prelude(...)`) read as italic-cap labels followed by body, which is good Bringhurstian voice. But the colon (".") after each sc-label is rendered post-small-caps; the period stays at full body weight. Convention is to put the period *inside* the small caps so the spacing is uniform. Move `]` boundary in `report.typ:127–135`.
- Page break splits the convergence section header from its body across pages 3→4. Add `pagebreak(weak: true)` or `block(breakable: false)` around H1 + first paragraph.

### Page 5 — Convergence table + Fanout sweep plot

- **The table has vertical rules.** `report.typ:391` sets `stroke: 0.3pt + sub-color` which renders a full grid: top, bottom, all internals, every vertical separator. Bringhurst rule 6: vertical rules are noise. Replace with explicit horizontal rules only:
  ```typst
  stroke: none,
  table.hline(),
  table.header(...),
  table.hline(),
  // rows...
  table.hline(),
  ```
- Table column headers are set `text(style: "italic")[N]`, `text(style: "italic")[f = 1]`. Italic for English column names is wrong; italic is reserved for variables and titles. `N` and `f` are variables, italic is fine; "median rounds", "loss", "final divergence", "runs converged" should be small caps. Update lines 392–393, 512–516, 597–601.
- **Twin-axis plot violates Tufte explicitly.** The fanout-sweep plot at `render.py:184` uses `ax.twinx()` to overlay rounds-vs-fanout (left axis, monochrome) and bytes/s (right axis, red). Tufte: avoid dual axes; they let arbitrary scaling lie. Consider splitting into two stacked panels with a shared x-axis (a small multiple).
- Right-axis y-label "bytes per node, per second" gets clipped by the figure border; the label runs off the right edge of the canvas. Either rotate to 270° or shorten to "bytes / node / s".
- Direct labels `"rounds (left axis)"` and `"bytes / s (right axis)"` carry "(left axis)" and "(right axis)" in parens — the labels are talking to the reader about the *chart structure*. Tufte's direct-label idiom labels the *line itself*. Rename to "rounds" and "bytes / s" and let position speak.

### Page 6 — Scale plot + headline number prose

- Same DejaVu Serif font mismatch (axis tick labels `2^3`, `2^5`, `2^7`, `2^9`).
- **Both panels have a full bottom and left spine extending past the data**. `range_frame()` is *defined* in `tufte.py:84` but **never called** in `render.py`. The advertised Tufte range-frame is unrealized. Fix: in each plot, after plotting, call `range_frame(ax, df["nodes"], df["rounds"])` etc.
- Right panel y-axis is zero-anchored (`render.py:284`) — correct for the SWIM constant-load claim and well-justified in prose. Keep, but the y-tick at 0 is the bottom spine — the range-frame call would *make* the spine stop at 3300 (the minimum) which would contradict the zero-anchoring intent. Compromise: keep the full left spine on the right pane *because* zero-anchoring is the point, but apply range-frame to the left pane.
- Plot title "scaling: rounds-to-converge (f = 3)" uses lowercase + parenthetical. The matching title on the right pane "per-node bandwidth (the SWIM constant-load claim)" is parenthetical-explanatory and reads more like a caption. Drop both titles and merge into the Typst caption — Tufte typically captions outside the plot in surrounding prose.
- "At $N = 1024$ the simulator reports **#n1024_bytes_per_s B / node / s** of gossip bandwidth..." (`report.typ:471`). Using `*…*` produces bold, which Bringhurst 7 forbids in serif body text. Replace with `sc[#n1024_bytes_per_s B/node/s]` or italic.

### Page 7 — Loss plot + loss table + Partition heading

- Loss plot: vertical jitter is a Tufte-correct strip plot. Good.
- The **annotation arrow "median over 3 trials"** uses `arrowstyle="->"` (`render.py:343`) while the partition plot uses `arrowstyle="-"` (`render.py:398`). Choose one; Tufte's *Beautiful Evidence* uses bare lines (`"-"`) for callouts because the arrowhead is chartjunk.
- Loss x-labels are categorical (`0%`, `10%`, ...). But the spacing is uniform whereas the underlying p values are uniform too — fine. However the label "per-link drop probability (i.i.d.)" parenthesises the statistical model; consider moving "(i.i.d.)" into the caption since the caption already says "i.i.d.".
- Loss table is full-grid same as convergence table. Same Bringhurst 6 fix.
- Page-break: the partition section heading "Partition + heal: surviving a split brain" appears at the bottom of page 7 with only the prelude blocks; the figure jumps to page 8. Add `block(breakable: false)` around the heading + prelude so the heading travels with its plot.
- Loss table column "final divergence" is a single repeated `0`. Tufte: "if the table is mostly one value, drop the column and put it in the caption." Move to caption ("final divergence is 0 in every trial").

### Page 8 — Partition plot + Staleness heading

- **Partition plot has y-axis below zero.** `ax.set_ylim(-0.5, ...)` (`render.py:439`) pulls the spine below the data. Range-frame would fix it; without that, the lower extension is wasted ink.
- The **dashed ground-truth line** is rendered at `linewidth=1.6` while individual node lines are `0.8`. The hierarchy is correct: ground truth is the reference, individual traces are samples. Tufte-correct.
- "nodes 4..7 (cut side, pre-heal)" label is placed at `t[len(t) // 4]` — about 5 s in. The label sits *on* the x-axis at y=0 because the cut-side total is zero. Fine, but the alpha-0.45 line behind it is barely visible; bump to 0.6 or use a distinct color. Note `right_side = PALETTE[3]` which is `#274060` (navy) — already distinguishable; the alpha is the only thing dimming it.
- "ground truth" right-edge label has no marker on the line, so it floats in space. Add a small tick or arrowhead so the eye lands on the line — or move the label inline with the trace.
- **Staleness plot connects k=4 and k=8 with a diagonal line.** No data exists at k=5,6,7. Lines between known sample points imply continuous interpolation — Tufte's caution against visual extrapolation. Switch the plot type to a markers-only scatter, or join with a dotted segment:
  ```python
  ax.plot(df["sources"], df["p50"], marker="o", linestyle=":", color=INK)
  ```
  (`render.py:472–473`).
- Staleness y-axis 0 to 100 ms with ticks at every 20 ms — Tufte "few, useful" — but the labels read 0, 20, 40, 60, 80, 100 which is six. Three (0, 50, 100) would suffice given the data is 0 or 100.

### Page 9 — Bandwidth scaling table + Synthesis opening

- Table same full-grid problem. Fix once globally with a `show table: ...` rule.
- The bandwidth-scaling table column "wall-clock (ms)" rounds to integers; the underlying number is `n * 100 ms` for `n rounds`. Three significant figures of precision when the data only has 1.5 sig figs is misleading. Either omit (the reader can multiply) or annotate as "≈ #N × 100 ms".
- Synthesis opens with `sc[Headline.]` followed by inline body. The `#h(0.4em)` between sc-label and body produces a visible gap. Convention: small-cap lead, comma, normal-cap body, separator is sentence-spacing, not extra horizontal space. Replace `sc[Headline.] #h(0.4em) On the four...` with `sc[Headline] — On the four...` (em dash separator).
- **Page number is on the wrong side.** Two-sided document (asymmetric margins `inside: 1.05in, outside: 1.55in`) but the header alignment `(left, right)` is hard-coded (`report.typ:39–42`). On a *verso* page (page 2, 4, 6, ...), the right edge is the *inside* (gutter) edge. Bringhurst 3.3: page numbers go on the outside corner. Branch:
  ```typst
  let p = here().page()
  let is-recto = calc.odd(p)
  grid(
    columns: (1fr, auto),
    align: if is-recto { (left, right) } else { (left, right) }, // need to swap which side has fr
    if is-recto { sc[gabion gossip evaluation] } else { str(p) },
    if is-recto { str(p) } else { sc[gabion gossip evaluation] },
  )
  ```
- The Headline paragraph ends "...Astrolabe." but "Demers, Karp, SWIM, Bimodal Multicast, and Astrolabe" is a series of paper-name nouns set roman; the surrounding paragraph implies these are *citations* (compare with the reference list, where each is italic). Make them italic to signal title-ness.

### Page 10 — Honest caveats + literature comparison

- Bulleted list uses `*We have not yet measured under churn.*` (bold) at the start of each bullet. Bringhurst 7: no bold in serif body. Replace with `_…_` (italic) or `sc[…]`. (Lines 654, 661, 668 of `report.typ`.)
- Section heading "How gabion compares to the literature" wraps at the bottom of page 10; the first claim block is on the same page. Good.
- `claim-block` (`report.typ:704`) uses `stroke: (left: 1pt + sub-color)` giving each citation a left rule. This is a *change bar* idiom and reads as "this is quoted material" — pleasant. Keep, but the 1pt rule is heavy; drop to 0.5pt.
- "Their measurement." and "Ours." inside `claim-block` are italic body labels. Convention: small caps. Update `report.typ:712–713`.
- The bibliographic `paper` field uses `_Epidemic Algorithms_` (italic) inside `strong[#paper]` — bold italic. Bold + italic is double emphasis; per Bringhurst, choose one. Drop the `strong[…]` wrapper.

### Page 11 — Reference list

- Reference list uses `hanging-indent: 1.2em` with `justify: false` (line 794). Good — exactly the Bringhurst convention for bibliographies. Ragged right correct on a list of short entries.
- Author names "Demers, A., D. Greene, C. Hauser, et al. 1987." — comma after surname, initial of first name. Inconsistent with "Van Renesse, R., K. Birman..." which puts surname "Van Renesse" *and* doesn't comma-separate. Pick a style (APA / Chicago) and normalize.
- Italic title slugs (`_Epidemic algorithms for replicated database maintenance._`) good. The trailing period is inside the italic; convention is outside. Move it.
- Year "1987." has period — fine. "PODC '87" uses curly quote — but the source uses straight apostrophe in the typst markup; Typst's smart-quote rule should convert. Verify by checking the rendered glyph (should be ' not ').
- The references run to the bottom of page 11 with no widow risk. Last entry is DeCandia 2007 (Dynamo) — present in the reference list but **never cited** in the body. Either cite Dynamo or remove from references.

---

## 3. Plot-by-plot audit

### `convergence.svg`

- All tick labels (`2^3`, `2^5`, `2^7`, `f=1...f=8`, numeric y-axis) are DejaVu Serif paths. Body is NCM. (`render.py:127, 145`)
- Range-frame not applied: bottom spine extends from x≈4 to x≈260, not just to the actual data minimum. (`tufte.py:84` — function defined, never called.)
- Two-panel layout is correct small-multiples-style. (`render.py:90`.)
- Direct labels at right edge work, but `f=2` and `f=8` lines on the left pane converge near y=4 — labels stack on top of each other at N=256. Either jitter vertically or use `direct_label(..., xytext=(4, +6))` for upper and `(4, -6)` for lower.
- Dotted `log₂ N` reference is good and direct-labelled. Keep.
- Y-axis label "rounds to converge" reads — but right pane "bytes per node, per second" is overlong. Shorten to "bytes / s / node".
- Two panels use slightly different y-axis baselines (left starts at ~2, right at ~600) — fine because they're different units, but visual baseline alignment would be courteous.

### `fanout_sweep.svg`

- DejaVu Serif paths everywhere.
- Twin-axis plot. Two scales, one panel. Tufte: avoid. Restructure as two stacked panels.
- Right-edge labels "rounds (left axis)" and "bytes / s (right axis)" annotate the chart structure rather than the data. Replace.
- Right y-axis label clipped on page 5 — extends past the figure box.
- Tick at x = 12 is on the edge; the right-edge label at last["fanout"] + 1.5 padding works, but the data point itself lands on the spine. Increase pad.

### `scale_n.svg`

- DejaVu Serif paths.
- No range-frame.
- The left-pane log₂ N reference line and the observed line: at N=1024 the observed sits at 7 and reference at 10. The vertical gap (3 rounds) is visually small because the y-axis is 0 to ~10. Bigger story would emerge if the y-axis started at 1.
- Right pane is zero-anchored — but the spine extends from 0 to ~4500 even though all data is between 3300 and 4500. Zero-anchoring is well-defended in the prose, so don't strip the lower extension entirely; but a *secondary* y-tick label at the data minimum would honour Tufte's range-frame intent.

### `loss.svg`

- DejaVu Serif paths.
- Strip plot is correct Tufte form.
- Red crossbar median is good; arrow annotation "median over 3 trials" with `arrowstyle="->"` is inconsistent with partition plot. Pick one arrow style.
- Y-axis ticks at 2, 3, 4, 5, 6 — but data range is 2 to 5. Drop "6" and lower the upper spine to 5.5.
- X-axis tick labels are categorical strings (`0%`, `10%`...) which is fine since the data is discrete. No font issue here.

### `partition.svg`

- DejaVu Serif paths.
- The `set_ylim(-0.5, ...)` extends spine below the data — wastes vertical space and violates range-frame.
- Eight semi-transparent node traces clump into two visible bands; the alpha=0.45 makes any individual line hard to read but the *band* is the right unit. Keep.
- "heal at t = 10 s" callout uses `arrowstyle="-"` — different from loss plot. Normalize.
- "ground truth" right-edge label floats free of the dashed line. Add `arrowstyle="-"` callout connecting label to line, or simply place a small text at the line's end (current `xytext=(6, 6)` puts the label 6pt above and 6pt right of the last data point — that's correct as a direct label; the issue is the line is *dashed* and the dash visually trails off, leaving a gap between label and line. Switch to solid line for ground truth or move label closer.

### `staleness.svg`

- DejaVu Serif paths.
- Lines connect k=1, 2, 4, 8 — three actual segments and a wide diagonal between 4 and 8. The diagonal visually interpolates through k=5, 6, 7 where no data exists. Either add the missing k values, switch to markers-only, or dot the unsupported segments.
- Y-axis 0 to 100 with 5 intermediate gridlines. The data only takes two values (0 and 100). Three ticks (0, 50, 100) would suffice.
- p50 line at 0 ms for k=1,2,4 reads as "no lag at all" — but the underlying data was reported by `r["headline"]["p50_staleness_millis"] or 0`. The `or 0` substitutes 0 for `null` (`render.py:464–465`). That's deceitful: a `null` p50 (no measurement) silently becomes a 0 (perfect performance). Show null values as missing markers, not as zeros.
- Direct labels `"p50"` and `"p95"` good; offsets `(14, -12)` and `(14, 12)` handle the coincidence at k=8 reasonably.

---

## 4. Ranked punch list

### BLOCKER

1. **Plot text in DejaVu Serif, body in New Computer Modern.** `crates/gossip-bench/bench/render.py:127, 145, 266, 275` (every `set_xscale("log", base=2)`). Mathtext tick labels are always raster paths; fix by replacing log-scale auto-labels with explicit string ticks:
   ```python
   left.set_xticks([4, 8, 16, 32, 64, 128, 256])
   left.set_xticklabels(["4", "8", "16", "32", "64", "128", "256"])
   ```
   Then `svg.fonttype: "none"` (`tufte.py:78`) will actually emit `<text>` elements that pick up NCM at PDF compile.
   Matters: same-page font mismatch between figure and surrounding prose violates Bringhurst 1.1 (one face) and Tufte (typography of plot labels should match the surrounding text).

2. **Range-frame defined but never called.** `crates/gossip-bench/bench/tufte.py:84` (def) — zero call sites in `render.py`. Add `range_frame(ax, xs, ys)` after each plot in `render.py:130–133, 150–152, 271–272, 280, 347–348, 437–440, 501–502`.
   Matters: this is the headline Tufte technique the module advertises; its absence is felt in every plot as wasted ink at the spine ends.

3. **Measure too wide.** Body runs ~85 cpl. `crates/gossip-bench/bench/report.typ:34`. Either:
   ```typst
   margin: (top: 0.95in, bottom: 0.95in, inside: 1.3in, outside: 1.95in),
   ```
   (drops measure to ~5.25 in / ~76 cpl), or set body to 11 pt at line 52.
   Matters: Bringhurst 2.2 — comfortable measure is 45–75 cpl; current 85 measurably slows reading and produces rivers under full justification.

4. **Figure numbers suppressed.** `report.typ:88`. Replace `it.body` with `[#it.supplement #context it.counter.display(it.numbering). #h(0.4em) #it.body]`.
   Matters: Bringhurst 8.5 — figure numbers belong in captions; cross-references are impossible without them.

5. **Figure captions centred.** `report.typ:87`. Add `set align(left)` inside the show-rule.
   Matters: Bringhurst 8.5 — captions ragged-right (left-aligned) so they read as prose, not as titles.

6. **All tables full-grid (vertical rules everywhere).** `report.typ:389, 506, 591`. Change `stroke: 0.3pt + sub-color` to `stroke: none` and add explicit `table.hline(stroke: 0.4pt + sub-color)` rows after header and at top/bottom.
   Matters: Bringhurst 6 — vertical rules are noise; horizontals at top, bottom, and below header.

7. **Page number on gutter side of verso pages.** `report.typ:39–42`. Branch the grid layout on `calc.odd(here().page())` so the page number sits on the outside corner regardless of recto/verso.
   Matters: Bringhurst 3.3 — folios at the outside, never the gutter.

8. **Twin-axis fanout plot.** `render.py:184–225`. Restructure as two vertically stacked panels sharing the x-axis (small multiples):
   ```python
   fig, (top, bot) = plt.subplots(2, 1, figsize=(6.6, 4.0), sharex=True)
   top.plot(df["fanout"], df["rounds"], ...)
   bot.plot(df["fanout"], df["bytes_per_s"], ...)
   ```
   Matters: Tufte explicitly warns against dual y-axes; arbitrary scaling lies about correlation.

9. **Bold for emphasis in serif body.** `report.typ:471` (`*#n1024_bytes_per_s B / node / s*`), `report.typ:654, 661, 668` (`*We have not yet measured under churn.*` etc). Replace `*…*` with `_…_` (italic) or `sc[…]`.
   Matters: Bringhurst 7 — emphasis in serif body is italic or small caps, never bold.

10. **Staleness plot interpolates through missing data.** `render.py:472–473`. Switch to `linestyle=":"` between known points or markers-only. Also fix the `or 0` data-handling at lines 464–465 — null values should be missing markers, not zeroes.
    Matters: Tufte — don't visually extrapolate; the diagonal between k=4 and k=8 implies measurements at k=5,6,7 that don't exist.

### POLISH

11. **H1 and H2 differentiated only by size.** Both are letter-spaced small caps. `report.typ:63–74`. Make H1 NCM Roman caps at 16 pt (no small-caps wrap), keep H2 as small caps.
    Matters: Bringhurst — stately progression needs at least one axis of contrast.

12. **Italic table headers.** `report.typ:392–393, 512–516, 597–601`. Replace `text(style: "italic")[median rounds]` etc with `sc[median rounds]`. (Leave `N` and `f = 1` italic — those are variables.)
    Matters: italic = variable / title-of-work; small caps = section label.

13. **Caption parentheticals are chart-structure annotations, not data.** `render.py:201, 208`. Drop "(left axis)" / "(right axis)" — direct labels label the *line*, not the chart.

14. **Inconsistent arrow styles in plots.** `render.py:343` uses `arrowstyle="->"`, `render.py:398` uses `arrowstyle="-"`. Pick `"-"` (no arrowhead).
    Matters: Tufte — bare lines are quieter; arrowheads are chartjunk.

15. **Plot titles compete with Typst captions.** `render.py:130, 148, 218, 269, 278, 331, 436, 497` (every `title_only(ax, …)`). Drop in-plot titles; let the Typst figure caption own the title.

16. **Caveat list bold.** `report.typ:654, 661, 668`. Replace `*…*` with `_…_`. (Same as item 9; listed again for visibility — it's the same root issue across two regions.)

17. **Loss table "final divergence" column is constant.** `report.typ:506–518`. Drop the column; mention "final divergence is 0 across all 18 trials" in the caption.

18. **Bandwidth-scaling table over-precise wall-clock.** `report.typ:591–603`. The wall-clock column is just `rounds × 100 ms`; remove it or annotate as "≈ N × 100 ms" in the caption.

19. **Em-dash inconsistency at line ends.** Sweep `report.typ` for `— ` (em + space) and `—\n` patterns. Make all em dashes unspaced (Bringhurst preference).

20. **Header alignment of the running head.** `report.typ:38–46`. After the page-number fix, the slug "GABION GOSSIP EVALUATION" is currently always *left*; with the swap, it should also flip to inside on verso pages so the page number is always outside.

21. **`#h(0.4em)` separators after sc-labels.** `report.typ:127, 130, 134, 612, 628, 651, 678`. Replace the manual horizontal space with em dash or comma, e.g. `sc[Headline] — `.

22. **Twin colour palette mismatch.** `tufte.py:31–40` defines an 8-colour palette but plots only consistently use entries 0, 2, 3. Either prune the palette or document the role of each colour. The "accent red" (`#8a1a1f`) is well-chosen and used purposefully; the other six are visual fallow.

23. **`claim-block` left rule too heavy.** `report.typ:709`. `1pt + sub-color` → `0.5pt + sub-color`.

24. **Bold-italic in claim-block paper field.** `report.typ:711`. Drop the `strong[#paper]` wrapper — paper title is already italic via the `_…_` in the call site.

25. **References — "Demers, A., D. Greene…" vs "Van Renesse, R., K. Birman…"**. Normalize author-list style (Chicago or APA, pick one). `report.typ:796–832`.

26. **Reference italic includes trailing period.** `report.typ:796–832` — `_Epidemic algorithms… maintenance._` ends with period inside italic; move outside.

27. **Dynamo reference uncited.** `report.typ:830–832`. Either cite Dynamo in the literature-comparison section or remove the entry.

28. **Subtitle on title page uses manual `\` line breaks.** `report.typ:146–149`. Replace with `set par(justify: false, leading: 1.0em)`.

29. **Footer slug dots inflated by `#h(0.4em)`.** `report.typ:152–156`. Replace `#h(0.4em) · #h(0.4em)` with a single space + bullet + space, or use `sym.space.med`.

30. **Heading `set par(first-line-indent: 0em)` hand-rolled at every section.** `report.typ:165, 192, 270, 285, 308, 331, 340, 400, 470, 580, 610, 697`. Encode once:
   ```typst
   #show heading: it => { it; set par(first-line-indent: 0em) }
   ```
   Matters: Bringhurst 5.2.4 lives in one place, not twelve.

31. **Inline `<u32>` etc inside `raw` force big word-spaces.** `report.typ:292` (`BenchAggregateStore<u32>`). Wrap raw inline blocks in `box(raw(...))` to make them atomic to the justifier.

32. **Definition list term/body separation visually weak.** `report.typ:108–115`. Use a `grid` or `terms` element for proper hanging-indent term layout.

33. **River on page 2.** Caused by 85-cpl justification with hyphenation. Will resolve with item 3 (narrower measure) or item 9 reform.

34. **Loss plot y-axis maximum at 6 with no data above 5.** `render.py:348`. Drop the `+1`; range-frame will then end at 5.

35. **Partition plot ylim below zero.** `render.py:439`. Change `-0.5` to `0`; range-frame to data extent.

36. **Staleness y-axis has six ticks, two unique data values.** `render.py:495` or via `ax.set_yticks([0, 50, 100])`.

37. **Partition heal-callout y-coordinate `max(gt) * 0.55`.** `render.py:393`. With ground-truth = 7 the callout sits at y≈3.85 — near the middle, fine; but if max(gt) changes the label drifts. Anchor to the heal-line + tick frame instead.

38. **The "definition list" `defn` term has slash in "Membership / failure detection".** `report.typ:251`. Small-caps slash renders mid-cap-height. Use en dash.

39. **Page-break protection on figures.** `report.typ:91–97`. Wrap `figure-svg` body in `block(breakable: false, …)` so figure + caption don't split.

40. **Pre/heading-paragraph spacing on H1 wraps.** When the H1 wraps to two lines, the `above: 1.5em` / `below: 0.65em` blocks the line spacing inside the heading itself. Add `set par(leading: 0.7em)` in the H1 show rule (`report.typ:64`).

---

## 5. Editor judgment calls

Bringhurst and Tufte do not always agree, and a few decisions belong to the editor:

1. **Justified vs ragged-right body.** Bringhurst (rule 4.1) allows justified body if the measure is wide enough (>38 ems — true here even at the *desired* 76 cpl). Tufte's own books (*Visual Display*, *Beautiful Evidence*) are set ragged-right because they're closer to lecture notes than print monographs. This is a *technical report*, but the literature it compares against (Demers, Karp, SWIM papers) is all justified. **My recommendation: keep justified, narrow the measure.** Confirm.

2. **Plot titles inside the image vs in the Typst caption.** Tufte usually puts titles in surrounding prose; Bringhurst doesn't speak to it. With Typst owning numbering (after fix #4), in-plot titles become redundant — but a printed-out plot loses context. **My recommendation: drop in-plot titles, since the report is consumed as a PDF, not as detached figures.** Confirm.

3. **Letter-spaced small caps for H1 vs Roman caps.** Bringhurst 3.2.2 says letter-space all-caps strings 5–10 %. The H1 currently does (0.06 em). But Bringhurst 7 also says heading hierarchy needs a visible step from H1 to H2 — and right now they're the same style at different sizes. **Either H1 in NCM Roman caps (16 pt) with H2 in small caps (10.5 pt), or H1 italic (18 pt) and H2 small-caps.** Confirm.

4. **Two-sided vs one-sided.** Margins are currently asymmetric (`inside: 1.05in, outside: 1.55in`) indicating duplex print intent, but PDFs are read on screen, where asymmetric margins read as off-centre on every odd page. **Make the document symmetric (`margin: (x: 1.3in, y: 0.95in)`) unless a printed copy is the canonical artefact.** Confirm.

5. **f=1 row in convergence table — keep or drop?** f=1 is a degenerate linear-chain case where the O(log N) bound *doesn't apply*; including it draws the eye toward the rows that *do* honour the bound (Tufte: comparison) but also pollutes the table with rows where the result is unsurprising. **Recommend: keep f=1 but italicize the row to mark it as the chain-degenerate baseline.** Confirm.

6. **Reference list flush left ragged vs justified.** Bringhurst 4.4 says ragged for narrow blocks and short paragraphs. Reference list is currently ragged (`justify: false`, line 794). Some technical-report houses prefer justified for citation lists. **Recommend: keep ragged.** Confirm.

7. **DropFirst, DropProb etc. names in body.** Currently rendered as `raw` (typewriter). Bringhurst would render technical identifiers in italic; modern technical-writing convention is typewriter. **Recommend: keep `raw` for *code identifiers*, but render *concepts* (push-pull, anti-entropy, peer-frontier dedup) in italic, never `raw`.** Sweep `report.typ`. Confirm.
