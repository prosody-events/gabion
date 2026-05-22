# Title page — Bringhurst audit

## 1. Verdict

The page is quiet and almost there: optical centre is honoured, the family is consistent, there is no chartjunk, and the foot slug correctly recedes. The single largest lift is **demoting the wordy second-and-third lines of the subtitle to a true subtitle** (one short clause, not three) and giving the title a touch of letterspacing — right now the centred subtitle reads as a paragraph of body copy that wandered onto the cover, and the title sits without the breathing room Bringhurst reserves for display sizes.

## 2. Specific issues

### BLOCKER — Subtitle is a paragraph, not a subtitle
The subtitle is set in justified-by-default centred lines with a manual `\` break, producing three near-equal lines that read as body prose. Bringhurst (Ch. 2, §2.1.4 and the chapter on titling) wants the subtitle to be *visibly subordinate in length as well as size*, and warns explicitly against centred multi-line settings that crowd the title.

Line 145–149, replace:
```typst
  #text(size: 11.5pt, style: "italic", fill: sub-color)[
    Convergence, bandwidth, loss tolerance, and partition recovery,\
    measured on the in-process simulator and read against the\
    published gossip literature.
  ]
```
with:
```typst
  #text(size: 11.5pt, style: "italic", fill: sub-color)[
    convergence, bandwidth, loss, partition — measured against the literature
  ]
```
One italic line, set lowercase, with an unspaced em-dash separator. The full prose belongs in the opening paragraph of the body, not the cover.

### BLOCKER — Title has no letterspacing and no air below
At 26 pt the title is dense; Bringhurst (§3.2.1) calls for *negative* tracking at display sizes but only when the face was cut for text — NCM at 26 pt opens up a touch of positive tracking nicely, and a hairline of space before the subtitle improves the read.

Line 143–144, replace:
```typst
  #text(size: 26pt, weight: "regular")[gabion gossip evaluation]
  #v(0.7em)
```
with:
```typst
  #text(size: 26pt, weight: "regular", tracking: 0.01em)[gabion gossip evaluation]
  #v(1.1em)
```

### POLISH — Foot slug uses middle-dot-as-bullet with hard-spaced separators
The footer reads `2026-05-21  ·  gossip-bench  ·  rendered by …`. The middle dot is correct (Bringhurst prefers it to the bullet), but the `#h(0.4em) · #h(0.4em)` on both sides produces visibly loose gaps. A thin space on either side is the convention.

Line 152–156, replace:
```typst
#align(center, text(size: 8.5pt, fill: sub-color)[
  #datetime.today().display("[year]-[month]-[day]") #h(0.4em) · #h(0.4em)
  #raw("gossip-bench") #h(0.4em) · #h(0.4em)
  rendered by #raw("bench/render.py") and #raw("typst compile")
])
```
with:
```typst
#align(center, text(size: 8.5pt, fill: sub-color)[
  #datetime.today().display("[year]-[month]-[day]")#h(0.25em)·#h(0.25em)#raw("gossip-bench")#h(0.25em)·#h(0.25em)rendered by #raw("bench/render.py")
])
```
Also drops the redundant `and typst compile` — every Typst PDF is rendered by typst; saying so is noise.

### POLISH — ISO 8601 date on a typographic title page
ISO is correct for logs and filenames; on a printed title page Bringhurst's spirit (and centuries of practice) is a written month. Either keep ISO and commit to it as a deliberate "this is a technical artefact" gesture, or switch.

Line 153, alternative:
```typst
  #datetime.today().display("[month repr:long] [day padding:none], [year]")
```
yields `May 21, 2026`. Recommended if the document is meant to be read by humans before machines.

### POLISH — `gossip-bench` as raw (monospace) at the foot
The monospace slug at 8.5 pt sits well against the serif title — it reads as a code identifier, which it is. Keep. No change needed; this is the one place chartjunk would have been tempting and you avoided it.

### POLISH — Title block at 78 % width
Line 142: `block(width: 78%, …)`. With a one-line title and (after the fix above) a one-line subtitle, the 78 % cage no longer earns its keep — the subtitle will no longer wrap. Consider `width: auto` so the centring is purely glyph-driven; or, if you want the golden-section feel, `width: 61.8%` for the *subtitle only* and let the title sit free.

## 3. Judgment call for the editor

**ISO date or written date?** The whole document is a measurement artefact whose audience is engineers reading PDFs alongside JSONL, so `2026-05-21` is defensible as a deliberate register choice. But a cover page is the one place a document gets to address the reader as a human first. Pick one — and if you pick ISO, commit to it everywhere (headers, captions) so it reads as policy rather than oversight.
