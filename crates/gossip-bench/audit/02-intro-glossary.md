# Intro and glossary (pages 2–3) — Bringhurst audit

## 1. Verdict

The prose is good and the small-caps / em-dash / curly-quote discipline is real — but the page is set on a measure roughly 25 % too wide and the H2s are typographically indistinguishable from the small-caps defn terms beneath them, so the second half of page 2 reads as one undifferentiated wall of small caps. Fix the measure and give the H2s a register of their own and this section becomes genuinely Bringhurst-grade.

## 2. Specific issues

### BLOCKER — Measure is ~90–100 characters, well over Bringhurst's 75
Line 1 of the second paragraph ("A gossip protocol is a way for a set of machines to agree on shared") counts ~67 chars, but most lines run to 90+ — count "nginx pod and each gabiond gRPC pod maintains a local view of "how many requests has the cluster" → 96 chars including spaces. Bringhurst (§2.1.2) names 66 as the ideal and 45–75 as the comfortable range; over 75 the eye fatigues and the return-sweep starts missing the next line. The cause is in `report.typ` line 34:
```typst
margin: (top: 0.95in, bottom: 0.95in, inside: 1.05in, outside: 1.55in),
```
On US-Letter (8.5 in) this leaves 5.9 in of measure at 10 pt — roughly 90 chars. Either widen the outside margin to ~2.5 in (giving a ~4.95 in / 75-char measure and a Tschichold-like asymmetric page), or bump the body to 11 pt. The first is cheaper and recovers the right rhythm with no other knock-ons.

### BLOCKER — H2 has no register distinct from the defn terms below it
Lines 70–74 set H2 as 10 pt regular letterspaced small caps, identical in size and treatment to `#sc(term)` inside `defn()` (lines 113 and 29). On page 2 the eye cannot tell "GLOSSARY YOU CAN SKIP IF YOU KNOW THE FIELD" from "ROUND" or "FANOUT" — they're the same face, same size, same tracking, on consecutive lines. Bringhurst (§4.2) is explicit: headings must read as headings on a glance, not on a re-read. Either step the H2 up (e.g. 11 pt, or italic small caps), or add a thin rule above it, or set defn terms in roman small caps and reserve letterspaced small caps for headings. The minimum fix at lines 70–74:
```typst
#show heading.where(level: 2): it => {
  set text(size: 10.5pt, weight: "regular")
  set par(first-line-indent: 0em)
  block(above: 1.6em, below: 0.45em)[#sc(it.body)]
}
```
A half-point of size and ~50 % more air above is the smallest change that gives the heading a beat of silence.

### BLOCKER — Defn term does not hang into the margin
Lines 108–115: the `defn` block sets `hanging-indent: 1.5em` but the term is rendered inline (`#sc(term) #h(0.5em) #body`) flush with the rest of the body block. Bringhurst's classical hanging-indent glossary (§5.4) puts the *term* in the margin and the body inboard, so the page edge presents an alphabetical index the eye can scan vertically. The current setting reads as "small-caps lead-in," not as a glossary. If you want a true hanging glossary, give the defn its own left-shifted block (e.g. `block(inset: (left: 4em), …)` with the term placed at `place(left, dx: -4em)`). If you prefer to keep lead-in style, drop the `hanging-indent` because the term sits inboard anyway — currently the indent does work but the term doesn't.

### POLISH — Inconsistent punctuation on defn terms
Glossary terms (lines 194, 198, 203, 209, 216, 221) carry no trailing period: `round`, `fanout`, `peer-frontier dedup`. The "What is measured here" terms (lines 227, 231, 235, 240, 245) end with a period: `Convergence.`, `Per-node bandwidth.`, `Loss tolerance.`. Same for "What is not measured" (lines 251, 257, 262). Pick one. The Bringhurst-aligned choice is *no period* — the visual gap (`#h(0.5em)` at line 113) does the separating work that a period would otherwise do, and a period after letterspaced small caps reads as a stray dot.

### POLISH — Loose word spacing on lines containing unbreakable code spans
Justified setting + `#raw("tick_interval")`, `#raw("EndpointSlice")`, `#raw("CountingTransport")` etc. produces visibly loose lines (e.g. the second line of the "Membership / failure detection" defn on page 3, around the word "the"). Two cheap mitigations: (a) set the body to allow looser hyphenation (`#set par(linebreaks: "optimized")` is already the default in Typst, but a slightly higher `cost.hyphenation` tolerance helps), or (b) for code identifiers that overrun a line, allow breaks with `#raw(block: false, …)` plus an explicit zero-width break point on internal underscores. The simplest single fix is widening the measure (see BLOCKER #1) — at 75 chars the looseness mostly disappears.

### POLISH — Orphan single word on last line of `Loss tolerance` defn
Page 2, last defn ends "…so re-runs are / reproducible." A one-word last line is what Bringhurst (§2.4.3) calls a *runt*; classic remedy is to tighten the preceding line, prefer "PRNG so reruns are reproducible" (drop the hyphen) at line 237–238, or push a different word break with a `\u{00AD}` soft hyphen. Small but the eye catches it.

### POLISH — H2 "What is measured here" / "What is not measured here" insufficiently distinguished from each other
Same face, same size, same tracking, same vertical air. The semantic flip is the whole point of those two sections — measured vs. not measured — and the typography does nothing to signal it. Consider italic small caps for the negative ("WHAT IS *NOT* MEASURED HERE") or reversing the air (more above, none below) so the second section reads as a deliberate counter-section, not a continuation.

## 3. Judgment call for the editor

**Widen the outside margin, or step the body to 11 pt?** Both fix the measure. A wider outside margin (Tschichold's asymmetric 2:3:4:6 or similar) keeps the body small and gives marginal room for shoulder-notes — which this document does not currently use but could (e.g. a marginal "see §4.2" instead of inline parentheticals). A 11 pt body stays symmetric and is the simpler change but eats the page count. The Bringhurstian answer is the asymmetric margin; the pragmatic answer is 11 pt. Pick the asymmetric margin if you ever expect to add marginalia; pick 11 pt if you never will.
