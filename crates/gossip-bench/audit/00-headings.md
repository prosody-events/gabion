# Heading hierarchy — audit

## 1. Verdict

The heading system has **no enforced graphic distance between H2 and the
body's small-caps lead** (`sc()` / `lead()` / `defn()` / `prelude()`): both
sit at 10pt small caps letterspaced 0.06em, so the H2 line bleeds into the
definitions and prelude leads below it and stops reading as a heading at
all. Compounding this, the level steps are uniform-ratio (title 26pt → H1
16pt → H2/body 10pt ≈ 1.6× at every step), so what should be three distinct
typographic tiers reads as a single tier with mildly different sizes.

A second, easily-overlooked detail: the source comment on line 14 advertises
"four sizes (16/10.5/10/8.5 pt)" and the H2 rule comment on line 62 says
"H2 = 10.5 pt small caps" — but line 71 actually sets `size: 10pt`. The
intended 0.5pt size lift that would have separated H2 from body small caps
never made it into the code. The "clunky" feel is partly an unfixed bug.

## 2. What "clunky" means concretely

- **p. 2** — "Glossary you can skip if you know the field" (H2) is followed
  one line later by `ROUND` (a `defn` term). Both are 10pt small caps,
  tracked 0.06em. They read as the same level.
- **p. 2** — "What is measured here" (H2) is immediately followed by three
  stacked `defn` leads: `Convergence.`, `Per-node bandwidth.`,
  `Loss tolerance.` Four nominally-different ranks (page-header / H2 /
  defn-term) all rendered as letterspaced small caps at the same body
  size. The reader cannot scan the page by heading shape.
- **p. 3** — "What is not measured here" (H2) collides with three more
  `defn` leads (`Membership / failure detection.`, `Real-network UDP
  characteristics.`, `Churn.`). Same problem, more acute because the
  H2 sits at the top of the page where it should anchor the eye.
- **p. 4** — "Convergence: how many rounds to inform / everyone" (H1)
  wraps with a single orphaned word on line 2. Long letterspaced small
  caps make the orphan particularly visible.
- **p. 6** — "Loss tolerance: convergence with dropped / packets" (H1) —
  same orphan-word break.
- **p. 6** — "Scale: holding shape from $N = 4$ to $N = 1024$" (H1)
  occupies almost the full measure on one line; the math is set inline
  at the heading's letterspaced small-caps size, which renders the math
  italics oddly large and off-character against the rest of the title.
- **p. 8** — "Steady-state staleness: how far behind is the / slowest
  reader?" — break after "the", a textbook ugly line break.
- **p. 9** — "Synthesis: how good is this gossip system?" (H1) is
  followed by `HEADLINE.` (a `sc()` lead) one body line down. The H1's
  size step is doing all the hierarchical work; the small-caps register
  is identical to the lead immediately under it.
- **Title page → H1 ratio collapse** — the title is 26pt regular
  lowercase, the H1 is 16pt small caps. The case direction is *backwards*
  (more caps at the lower level than the higher), and the 26:16 ratio
  is the same 1.6× as 16:10 — so every step is the same size jump.
- **Above/below spacing is too uniform** — H1 uses `above: 1.5em, below:
  0.65em`; H2 uses `above: 1.1em, below: 0.25em`. The H1 block is barely
  more open than the H2 block, so the eye does not register them as
  different ranks of break.
- **Page header collision** — the running header (line 41) is `sc()`
  letterspaced. So every page reads: small-caps header, small-caps H1
  (where present), small-caps H2 (where present), small-caps `defn`
  leads. Four registers, one shape.

## 3. Proposed redesign

Replace the existing show-rules (lines 63–74) with:

```typst
// Heading hierarchy. Three distinct graphic registers:
//   H1 — 19 pt regular, letterspaced small caps, hairline rule below.
//   H2 — 11 pt regular, letterspaced small caps, no rule.
//   H3 — 10 pt italic title case, body size, tight spacing.
// Steps are non-uniform: 26 -> 19 (1.37x), 19 -> 11 (1.73x), 11 -> 10
// (1.10x via italic-vs-roman). The italic break at H3 deliberately
// avoids competing with the small-caps body leads.
#show heading.where(level: 1): it => {
  set text(size: 19pt, weight: "regular")
  set par(first-line-indent: 0em, leading: 0.5em)
  block(above: 2.2em, below: 0.4em)[
    #smallcaps(text(tracking: 0.08em)[#it.body])
    #v(-0.35em)
    #line(length: 100%, stroke: 0.4pt + sub-color)
  ]
}
#show heading.where(level: 2): it => {
  set text(size: 11pt, weight: "regular")
  set par(first-line-indent: 0em)
  block(above: 1.6em, below: 0.45em)[
    #smallcaps(text(tracking: 0.08em)[#it.body])
  ]
}
#show heading.where(level: 3): it => {
  set text(size: 10pt, weight: "regular", style: "italic")
  set par(first-line-indent: 0em)
  block(above: 1.0em, below: 0.2em)[#it.body]
}
```

Three companion changes are required for the H2 fix to actually take:

1. **Drop small caps from the body leads.** `sc()` (line 29), `lead()`
   (line 103), the term half of `defn()` (line 114), and the three
   labels in `prelude()` (lines 126, 130, 134) currently use the same
   letterspaced small caps as headings. Replace them with body-size
   italic title case (`text(style: "italic")[#name]`) — Bringhurst rule 7
   allows italic *or* small caps for body emphasis; reserving small caps
   exclusively for headings restores the contrast.
2. **Tighten H1 line breaks.** For the three H1 titles that wrap on a
   widow ("Convergence: how many rounds to inform everyone", "Loss
   tolerance: convergence with dropped packets", "Steady-state
   staleness: how far behind is the slowest reader?"), insert a manual
   linebreak with `\` before the colon's second clause, or shorten the
   second clause. At 19pt these will wrap regardless — the question is
   only where.
3. **Consider numbered chapters.** Rendering H1 as "I · Convergence: how
   many rounds to inform everyone" gives the eye a non-textual anchor
   distinct from the running header, and `numbering: "I."` on
   `#set heading` plus a tiny tweak in the show-rule is all it takes.
   Bringhurst-style technical reports almost always do this.

The hairline rule below H1 is the single highest-value visual cue here:
it converts "slightly larger small caps" into "a chapter break you cannot
miss" without any added weight.

## 4. Trade-offs

The new H1 is bigger (19 vs 16pt), spaced more openly (above 2.2em vs
1.5em), and carries a hairline rule — so each chapter eats roughly one
extra body line of vertical space, and the document will likely grow by
about one page. The italic body leads lose the slight gravitas small caps
gave them; in exchange the page gains a clearly scannable hierarchy.
Numbered chapters (if adopted) commit the document to a sequential
narrative reading order, which this report already follows — there is no
real cost, only the small editorial work of confirming the numbering
matches the table of contents (currently none). The biggest unrecoverable
loss is on dense pages like p. 2 and p. 3, where four `defn` blocks
currently pack tightly under an H2 — with the new spacing they will run
slightly into the next page. That is the right trade: a heading that
fails to read as a heading is worse than a page break.
