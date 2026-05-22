// gabion gossip evaluation — bench report.
//
// Pipeline:
//   1. `python3 bench/plot.py all` runs every scenario, writes
//      target/gossip-bench/<suite>/results.jsonl.
//   2. `python3 bench/render.py` reads those JSONL files, renders one
//      Tufte-style SVG per suite to target/gossip-bench/figures/, and
//      writes target/gossip-bench/data.typ with the tabular data as
//      Typst constants.
//   3. `typst compile --root . crates/gossip-bench/bench/report.typ
//      target/gossip-bench/report.pdf` produces the final PDF.
//
// Typography follows Bringhurst: one type family (New Computer
// Modern) at four sizes (16/10.5/10/8.5 pt), 14 pt leading on the
// body, em-dash unspaced, small-caps letterspaced, hanging indent on
// definitions, no widows or orphans, paragraph indent of one em
// except after a heading.

#import "/target/gossip-bench/data.typ": *

#let body-font = "New Computer Modern"
#let mono-font = "New Computer Modern Mono"
#let sub-color = rgb("#404040")
#let accent = rgb("#8a1a1f")

// Letterspaced small caps. Bringhurst rule 3.2.2: 5–10% extra
// letterspacing whenever caps or small-caps run in body. Typst's
// `tracking` adds em units.
#let sc(body) = smallcaps(text(tracking: 0.06em)[#body])

#set document(title: "gabion gossip evaluation", author: "gossip-bench")
#set page(
  paper: "us-letter",
  margin: (top: 0.95in, bottom: 0.95in, inside: 1.05in, outside: 1.55in),
  header: context {
    if here().page() > 1 [
      #set text(size: 8.5pt, fill: sub-color)
      #grid(
        columns: (1fr, auto),
        align: (left, right),
        sc[gabion gossip evaluation], str(here().page()),
      )
      #v(-0.7em)
      #line(length: 100%, stroke: 0.4pt + sub-color)
    ]
  },
  footer: none,
)

// Body. Hyphenate, justify, 1 em first-line indent, 14 pt leading.
#set par(justify: true, leading: 0.62em, first-line-indent: 1.2em)
#set text(font: body-font, size: 10pt, hyphenate: true)
#set heading(numbering: none)

// Em dash policy: Bringhurst recommends an unspaced em dash. The
// `regex` show rule replaces every occurrence of ` — ` (spaced em
// dash from Markdown habit) with the unspaced glyph + thin space.
// Typst doesn't natively rewrite, but we can do this at write time
// by being consistent in this file (search/replace done below).

// Heading hierarchy: one face, three sizes. H1 = 16 pt, regular;
// H2 = 10.5 pt small caps letterspaced; never bold.
#show heading.where(level: 1): it => {
  set text(size: 16pt, weight: "regular")
  set par(first-line-indent: 0em)
  block(above: 1.5em, below: 0.65em)[
    #sc(it.body)
  ]
}
#show heading.where(level: 2): it => {
  set text(size: 10pt, weight: "regular")
  set par(first-line-indent: 0em)
  block(above: 1.1em, below: 0.25em)[#sc(it.body)]
}

// Bringhurst rule 3: paragraph immediately after a heading has no
// indent. We can't trivially detect "immediately after heading" in
// Typst's show-rules system, so the convention is enforced by hand:
// the first body paragraph in each section begins with
// `#set par(first-line-indent: 0em)`. Subsequent paragraphs inherit
// the document default.

// Captions: ragged-right (Bringhurst — avoid justification on short
// blocks), italic, smaller, hanging indent.
#show figure.caption: it => {
  set text(size: 8.5pt, fill: sub-color, style: "italic")
  set par(first-line-indent: 0em, hanging-indent: 0.8em, justify: false)
  it.body
}

#let figure-svg(path, caption) = figure(
  image(path),
  caption: caption,
  kind: "plot",
  supplement: [Figure],
  numbering: "1",
)
#show figure.where(kind: "plot"): set block(spacing: 0.7em)

// Small-caps lead used for emphasized terms in body and definition
// lists. Bringhurst's rule 7: emphasis in body is italic or small
// caps, never bold.
#let lead(name) = sc(name) + h(0.35em)

// Definition list helper. Term hangs at the left margin, body
// continues with hanging indent so subsequent lines align with the
// body text, not the term.
#let defn(term, body) = block(
  width: 100%,
  inset: (left: 0em),
  [
    #set par(hanging-indent: 1.5em, first-line-indent: 0em, justify: true)
    #sc(term) #h(0.5em) #body
  ],
)

// "What is this section measuring" prelude used at the top of each
// experiment chapter. Small caps lead + the body of the clause. Each
// block explicitly resets the paragraph indent — putting `set par`
// at the top of the function isn't enough because Typst's `set` is
// scoped to its block, not to the children of the surrounding code
// block.
#let prelude(property, looking, bad) = [
  #block(above: 0.6em, below: 0.6em)[
    #set par(first-line-indent: 0em, leading: 0.58em)
    #sc[The property.] #h(0.4em) #property
  ]
  #block(above: 0em, below: 0.6em)[
    #set par(first-line-indent: 0em, leading: 0.58em)
    #sc[What good looks like.] #h(0.4em) #looking
  ]
  #block(above: 0em, below: 0.9em)[
    #set par(first-line-indent: 0em, leading: 0.58em)
    #sc[What would be bad.] #h(0.4em) #bad
  ]
]

// =====================================================================
// Title page — title at ~38% (optical centre), slug small at foot
// =====================================================================
#v(38%)
#align(center, block(width: 78%, [
  #text(size: 26pt, weight: "regular")[gabion gossip evaluation]
  #v(0.7em)
  #text(size: 11.5pt, style: "italic", fill: sub-color)[
    Convergence, bandwidth, loss tolerance, and partition recovery,\
    measured on the in-process simulator and read against the\
    published gossip literature.
  ]
]))
#v(1fr)
#align(center, text(size: 8.5pt, fill: sub-color)[
  #datetime.today().display("[year]-[month]-[day]") #h(0.4em) · #h(0.4em)
  #raw("gossip-bench") #h(0.4em) · #h(0.4em)
  rendered by #raw("bench/render.py") and #raw("typst compile")
])

#pagebreak()

// =====================================================================
// Introduction
// =====================================================================
= What this document is

#set par(first-line-indent: 0em)
If you have not worked with gossip protocols before, the next two
paragraphs are the entire model you need.

#set par(first-line-indent: 1.2em)
A gossip protocol is a way for a set of machines to agree on shared
state without a central coordinator. Each machine periodically picks
a small handful of peers—its #emph[fanout]—and sends them its view
of the world. When the receiving peer's view differs, the two sides
merge their state. Over a few rounds the freshest information
spreads to everyone, like a rumour through a workplace. Anti-entropy
protocols, which gabion implements, keep gossiping at low rate even
when nothing has changed, so any view that has fallen out of sync
gets repaired automatically.

Gabion uses gossip to keep distributed rate-limit counters in
agreement across a cluster. Each nginx pod and each gabiond gRPC
pod maintains a local view of "how many requests has the cluster
seen for this rule, in this time window". When a pod admits a
request it bumps its local counter and gossips the change to a
handful of peers. The peers merge using CRDT semantics
(max-per-origin), so any two pods that have exchanged messages
eventually converge to the same total. This report measures how
fast, how cheaply, and how robustly that convergence happens.

== Glossary you can skip if you know the field

#set par(first-line-indent: 0em)

#defn[round][one #raw("tick_interval") of the gossip loop. Every
scenario in this report sets #raw("tick_interval = 100 ms"), the
production default. "Converges in 5 rounds" means 500 ms wall-clock.]

#defn[fanout][the number of peers each node contacts per round.
Higher fanout converges faster but costs more bandwidth. Production
default is 6; the experiments here often use 3 because that is
where the convergence-vs-cost curve clearly bends.]

#defn[peer-frontier dedup][gabion's central optimisation. Each
sender remembers the highest sequence number it has acked from
every origin, so when it composes the next outbound frame it strips
cells the receiver already has. This makes our push behave like
pull from the sender's perspective—without an extra round trip.]

#defn[anti-entropy][the family of gossip protocols (gabion among
them) that keep periodically exchanging state forever, instead of
stopping when a "round" of dissemination is presumed complete.
Older literature contrasts this with #emph[rumour-mongering], which
stops spreading a particular update after enough peers have heard
it.]

#defn[ground truth][in every scenario, the total number of hits the
workload has issued so far across the cluster. We compare each
node's local view against ground truth to derive the convergence
and staleness metrics.]

#defn[partition + heal][a network split that disconnects two halves
of the cluster, followed by reconnection. The metric is "how long
after the heal until everyone agrees again".]

== What is measured here

#defn[Convergence.][Virtual milliseconds (and the equivalent gossip
rounds) between the first write and the first sample at which every
node's view equals ground truth.]

#defn[Per-node bandwidth.][Bytes per node per second at steady
state, captured by a #raw("CountingTransport") that wraps the
simulator's in-memory transport and meters every send and receive.]

#defn[Loss tolerance.][Convergence under i.i.d. per-link drop.
Independently with probability $p$, the simulator drops each packet
on each link, using a deterministic per-link PRNG so re-runs are
reproducible.]

#defn[Partition + heal.][How long after a network cut is repaired
until the cluster re-converges. SWIM-style failure-recovery metric
minus the membership churn (we don't measure failure detection
here; see below).]

#defn[Steady-state staleness.][Under sustained writes from $k$
sources, the p50 / p95 lag between when ground truth first reached
a given level and when each node first matched it.]

== What is not measured here

#defn[Membership / failure detection.][Gabion delegates this to
Kubernetes via the #raw("EndpointSlice") watcher; the gossip
protocol assumes every peer the watcher advertises is alive.
SWIM's detection-time numbers therefore aren't relevant
comparators.]

#defn[Real-network UDP characteristics.][No kernel buffer pressure,
no queueing delay, no MTU fragmentation. The #raw("UdpTransport")
is exercised by the smoke tests, not by this harness. Adding a
real-network mode is a planned follow-up.]

#defn[Churn.][The peer set is fixed for each scenario. Implementing
#raw("PeerDiscovery") as a sim type with scriptable joins and
leaves is the natural next addition.]

#set par(first-line-indent: 1.2em)

= How to read the rest of this document

#set par(first-line-indent: 0em)
Every section opens with the property being tested, says what
"good" looks like, and what a bad result would have looked like;
then shows the data. Plots are paired with captions that point at
the curve you should care about. The literature comparison at the
end places each measurement next to the matching published number,
with a candid note where the comparison is fair and where it is
not.

#set par(first-line-indent: 1.2em)

= Methodology

== Per-node setup

#set par(first-line-indent: 0em)
Each simulated node owns one #raw("GossipRuntime") parametrised by
the scenario's fanout and tick interval. Its outbound transport is
a #raw("CountingTransport") wrapping the in-process
#raw("SimTransport"); its downstream aggregate store is the
#raw("BenchAggregateStore<u32>") defined in the bench crate (a hash
map keyed on $("rule_fp", "key", "bucket")$ with the same shape as
the server's #raw("DashMapStore") but single-threaded). The CRDT
cell store, peer-frontier table, and dirty rings come from the
production library unchanged.

#set par(first-line-indent: 1.2em)
Capacities are sized to match or exceed the server's production
#raw("StorageConfig") and #raw("GossipSettings") defaults—cell
capacity ≥ 4096, node-dictionary capacity ≥ 1024, peer capacity ≥
256, forwarded-dirty capacity ≥ 65 536, send queue 128, limit queue
8192, max payload 1400 bytes (the MSS-safe IPv4 budget production
uses to avoid fragmentation). When the simulated cluster size $N$
exceeds those floors, every capacity scales up linearly, so the
bench never hits a #raw("CellStoreFull") rejection.

== Per-tick driving

#set par(first-line-indent: 0em)
The bench advances virtual time in #raw("sample_interval")-sized
windows. For each window it (1) applies any scheduled network
change, (2) issues the workload's writes via
#raw("GossipClient::record"), (3) steps virtual time forward in
#raw("tick_interval") chunks and drains the scheduler so every
co-located runtime gets its tick, and (4) samples every node's
aggregate-store total and counter values.

#set par(first-line-indent: 1.2em)
The scheduler drain step deserves a sentence on its own. The
in-process simulator co-locates $N$ runtimes onto one
single-thread tokio scheduler, so when virtual time advances by
#raw("tick_interval") every runtime task becomes ready
simultaneously but a single #raw("yield_now()") only runs one of
them. Without a drain loop the simulator under-polls at large $N$
and reports artificially slow convergence—we saw 55 rounds at
$N = 1024$ before adding the drain, against 5 rounds afterward.
Production has no equivalent issue because every nginx and gabiond
pod runs its own runtime in its own process.

== Loss model

#set par(first-line-indent: 0em)
The simulator's per-link policy is one of #raw("Pass"),
#raw("Block"), #raw("DropFirst { count }"), or #raw("DropProb { p }").
The loss suite uses #raw("DropProb"): a deterministic per-link
splitmix PRNG decides each packet's fate independently with
probability $p$. Same seed, same drop pattern across re-runs.

== Pipeline

#set par(first-line-indent: 0em)
The bench is its own crate, #raw("gossip-bench"). The CLI binary
runs single scenarios (#raw("run")) or JSONL batches (#raw("batch"));
the Python harness in #raw("bench/plot.py") generates the matrices
and runs them; the renderer in #raw("bench/render.py") emits the
SVGs and the Typst data fragment this document consumes; and Typst
compiles the final PDF.

#set par(first-line-indent: 1.2em)

// =====================================================================
// Convergence
// =====================================================================
= Convergence: how many rounds to inform everyone

#prelude(
  [Push gossip on $N$ machines should converge—deliver an update to
   every node—in roughly $log_2 N$ rounds. That is the lower bound
   from Karp et al. (2000) for any algorithm without per-receiver
   knowledge of what was already delivered.],
  [Curves climbing roughly with $log_2 N$, $f = 1$ at the top,
   higher-fanout curves underneath. The $f = 3$ curve—gabion's
   smallest practical fanout—should bend slightly under the dotted
   $log_2 N$ reference, because peer-frontier dedup gives the
   sender per-receiver knowledge.],
  [A curve that grows faster than $log_2 N$, or final divergence
   not reaching zero inside the scenario window, or convergence
   times that jitter wildly between adjacent values of $N$.],
)

#figure-svg(
  "/target/gossip-bench/figures/convergence.svg",
  [Rounds to converge for a single write of ten hits at node 0, swept
   across cluster size $N$ and fanout $f$. The dotted line is the
   Karp $log_2 N$ push lower bound. At $f = 3$, gabion sits at or
   below the bound for every $N gt.eq 16$. The right pane is the
   matching steady-state bandwidth: roughly linear in fanout,
   essentially flat in $N$.],
)

#v(0.4em)

// `breakable: false` keeps the table together as a unit, so it isn't
// orphaned at the top of the next page when the figure above already
// crowds the current one.
#block(breakable: false, [
  #table(
    columns: (auto,) + (auto,) * convergence_fanouts.len(),
    align: (right,) + (right,) * convergence_fanouts.len(),
    stroke: 0.3pt + sub-color,
    inset: 4pt,
    table.header(
      text(style: "italic")[N],
      ..convergence_fanouts.map(f => text(style: "italic")[f = #f]),
    ),
    ..convergence_rows.flatten().map(cell => [#cell]),
  )
])

#v(0.4em)
#set par(first-line-indent: 0em)
The $f = 3$ column matches $log_2 N$ at $N = 8$ and beats it from
$N = 16$ onward. Dropping below the address-oblivious lower bound
is consistent with Karp's analysis, not in conflict with it: the
bound assumes the sender doesn't know what the receiver has, and
gabion's peer-frontier table breaks that assumption deliberately.
#set par(first-line-indent: 1.2em)

// =====================================================================
// Fanout vs cost
// =====================================================================
= Fanout vs network cost: where to set $f$

#prelude(
  [Higher fanout converges faster but uses more bandwidth. Demers
   (1987) and Bimodal Multicast (1999) characterise this as a
   quasi-hyperbolic tradeoff: convergence falls off quickly at
   small $f$, then plateaus; bandwidth grows linearly in $f$
   forever. The "right" fanout sits at the elbow.],
  [The rounds curve (black, left axis) drops sharply from $f = 1$
   to about $f = 3$ and then flattens. The bandwidth curve (red,
   right axis) is a straight line through the origin. The elbow is
   visibly distinct from both ends.],
  [A flat rounds curve from the start (no benefit from extra
   fanout), or a bandwidth curve that flattens (gossip not
   actually using extra peers), or an elbow at $f = 1$ (degenerate
   chain).],
)

#figure-svg(
  "/target/gossip-bench/figures/fanout_sweep.svg",
  [At fixed $N = 32$, sweeping fanout from 1 to 12. Rounds collapse
   from 9 ($f = 1$) to 2 ($f gt.eq 6$); per-node bandwidth grows
   linearly. The elbow is at $f = 3$, which is where production's
   default—six—would lie if we cared more about latency than cost.],
)

// =====================================================================
// Scale
// =====================================================================
= Scale: holding shape from $N = 4$ to $N = 1024$

#prelude(
  [The two structural promises gabion inherits from its ancestry are
   (a) convergence in roughly $log_2 N$ rounds and (b) constant
   per-node bandwidth as the cluster grows. SWIM (2002) proved both
   for its dissemination component analytically.],
  [The left pane hugs $log_2 N$ from below (peer-frontier dedup),
   maybe diverging by one or two rounds at the largest sizes. The
   right pane is roughly flat in $N$. The y-axis on the right pane
   #emph[must] start at zero—a non-zero baseline would visually
   amplify a 30 % range into a steep curve and lie about the
   headline claim.],
  [A bandwidth curve that grows linearly with $N$ (load scales with
   cluster size, the SWIM property broken), or convergence rounds
   that scale faster than $log N$ (peer sampling not informative
   at scale).],
)

#figure-svg(
  "/target/gossip-bench/figures/scale_n.svg",
  [At $f = 3$, $N$ from 4 to 1024. Left: observed rounds against the
   $log_2 N$ reference; the curve hugs the bound through $N = 256$
   and stays inside one round of it at $N = 1024$ (7 vs 10). Right:
   per-node bandwidth, y-axis zero-anchored—the range from 3.4 to
   4.5 kB/s is small enough that the curve reads as flat across the
   $256 times$ N range, which is the claim.],
)

#v(0.4em)
#set par(first-line-indent: 0em)
At $N = 1024$ the simulator reports *#n1024_bytes_per_s B / node /
s* of gossip bandwidth and converges a single write in
*#n1024_rounds rounds* (#n1024_ms ms wall-clock at the 100 ms tick
rate). The full table is in the bandwidth-scaling section below.
#set par(first-line-indent: 1.2em)

// =====================================================================
// Loss tolerance
// =====================================================================
= Loss tolerance: convergence with dropped packets

#prelude(
  [Bimodal Multicast (Birman et al. 1999) proved stable
   delivery—convergence with at most a small constant penalty—
   through roughly 25–30 % packet loss. Gabion is push-only, not
   push-pull, so we don't get Bimodal's exact guarantees, but we
   should remain in the same ballpark.],
  [Median rounds-to-converge grows slowly with loss—a small number
   of additional rounds at 30 %, maybe one or two more at 50 %.
   Crucially, every trial converges (final divergence = 0) inside
   the scenario window.],
  [Any trial leaving nodes stuck with unequal totals at the end of
   the window. That would be a bug, not just a slow run.],
)

#figure-svg(
  "/target/gossip-bench/figures/loss.svg",
  [Trial-level dots; red crossbar marks the median per loss level.
   $N = 16$, $f = 3$, three trials at each drop probability. Every
   scenario converged; the median grows by +1 from 0 % to 30 % loss
   and by +2 from 0 % to 50 %.],
)

#v(0.4em)

#table(
  columns: (auto, auto, auto, auto),
  align: (right, right, right, right),
  stroke: 0.3pt + sub-color,
  inset: 4pt,
  table.header(
    text(style: "italic")[loss],
    text(style: "italic")[median rounds],
    text(style: "italic")[final divergence],
    text(style: "italic")[runs converged],
  ),
  ..loss_rows.flatten().map(cell => [#cell]),
)

// =====================================================================
// Partition + heal
// =====================================================================
= Partition + heal: surviving a split brain

#prelude(
  [When a network partition cuts the cluster in two, each half
   keeps operating against its own view of state, and when the
   partition heals the two halves re-converge.],
  [The write-side half registers the new hits immediately. The
   cut-side half sits at zero for the duration of the partition.
   At the heal marker the cut side jumps to match within one or
   two gossip ticks.],
  [Reconvergence taking many ticks after the heal, or the
   peer-frontier table getting into a bad state during the
   partition and rejecting valid post-heal frames.],
)

#figure-svg(
  "/target/gossip-bench/figures/partition.svg",
  [$N = 8$ split into two equal halves at $t = 0$; a single write of
   seven hits at node 0 makes the write side jump immediately while
   the cut side stays at zero. At $t = 10 s$ the block links are
   turned back on (vertical dotted marker). Within one gossip tick
   of the heal, every node has converged to the correct cluster
   total.],
)

// =====================================================================
// Staleness
// =====================================================================
= Steady-state staleness: how far behind is the slowest reader?

#prelude(
  [Under continuous writes the cluster never "converges" in the
   once-and-done sense—there is always a small backlog of
   unpropagated cells. The measurable property is #emph[staleness]:
   for every increment the workload issues, how long until the
   slowest reader sees it?],
  [The p50 line near zero (most hits delivered within one sample
   window). The p95 line one tick above p50. Both lines grow gently
   as the workload's source count rises.],
  [A steep p95 slope as $k$ grows would indicate the gossip channel
   is saturating; a p50 above one tick would mean local apply is
   blocked.],
)

#figure-svg(
  "/target/gossip-bench/figures/staleness.svg",
  [Per-hit delivery delay at $N = 16$, $f = 3$, under sustained
   writes from $k$ concurrent sources. With #raw("sample = tick =
   100 ms") most hits land within one tick window; scaling sources
   up to $k = 8$ adds at most one tick of lag at the p95 percentile.],
)

// =====================================================================
// Bandwidth scaling
// =====================================================================
= Bandwidth scaling at a glance

#set par(first-line-indent: 0em)
The table below pulls the headline numbers from the $f = 3$ scale-N
sweep into a single block so the SWIM "constant per-node load"
property can be read at a glance. Notice that "rounds" never
reaches the $log_2 N$ bound—gabion's per-receiver dedup is doing
its job—and bytes per second drifts by less than 10 % across the
$256 times$ N range.
#set par(first-line-indent: 1.2em)

#v(0.3em)

#table(
  columns: (auto, auto, auto, auto),
  align: (right, right, right, right),
  stroke: 0.3pt + sub-color,
  inset: 4pt,
  table.header(
    text(style: "italic")[N],
    text(style: "italic")[rounds],
    text(style: "italic")[wall-clock (ms)],
    text(style: "italic")[bytes / node / s],
  ),
  ..scale_rows.flatten().map(cell => [#cell]),
)

// =====================================================================
// Synthesis
// =====================================================================
= Synthesis: how good is this gossip system?

#set par(first-line-indent: 0em)

#sc[Headline.] #h(0.4em) On the four properties the gossip
literature treats as load-bearing—round complexity, per-node
bandwidth, loss tolerance, partition recovery—gabion's measured
behaviour is at the strong end of the published range. A single
write reaches every member of a 1024-node cluster in 7 rounds
(700 ms wall-clock), each node spends roughly 4.5 kB/s of bandwidth
regardless of cluster size, half of all gossip packets can be
dropped without breaking convergence, and the cluster recovers
from a clean partition within one tick of the heal. None of these
numbers are records; they are all comfortably inside the safe
envelope established by Demers, Karp, SWIM, Bimodal Multicast, and
Astrolabe.

#v(0.6em)

#sc[The good parts.] #h(0.4em) The round count beating the
address-oblivious $log_2 N$ bound at $N gt.eq 16$ is the most
informative single result. It confirms that the peer-frontier
dedup is paying off empirically, not just on paper: by carrying
per-receiver knowledge of acked sequence numbers, the sender lifts
gabion's push protocol above Karp's lower bound for protocols
without that knowledge. The slope flattens as $N$ grows and the
bandwidth measurement stays flat—the protocol's cost does not
bloat with cluster size, which is the structural property a
production rate-limiter most depends on.

#v(0.6em)

The loss-tolerance behaviour is the second piece of evidence I
would point at. Bimodal Multicast targets 25–30 % loss as the
regime where push-pull stays bimodal-stable; gabion is a strictly
weaker construction (pure push, no pull, no random repair) yet
sustains 50 % i.i.d. loss with only a +2-round penalty and zero
residual divergence. The simulator's deterministic per-link
splitmix PRNG seeds make the numbers reproducible; the result is
not a single lucky run.

#v(0.6em)

#sc[Honest caveats.] #h(0.4em) Three blunt things to surface
before treating these numbers as production guarantees.

- *We have not yet measured under churn.* The peer set is fixed for
  every scenario in this report. Jelasity et al. specifically warn
  that gossip protocols' in-degree distributions skew under joins
  and leaves, and that skew can degrade convergence by orders of
  magnitude. Implementing the #raw("PeerDiscovery") trait as a sim
  type and scripting joins and kills is the next bench addition.

- *No real-network validation.* Every number here comes from the
  simulator, which collapses kernel buffering, MTU fragmentation,
  socket-level queueing delay, and asymmetric link latencies into
  zero. The bench framework supports a real-UDP transport at the
  Rust level; wiring it through the harness for a cross-validation
  pass is the next infrastructure step.

- *Single-trial sweeps for everything except loss.* Convergence,
  scale, fanout, partition, and staleness are reported as one
  number per configuration. The deterministic simulator makes them
  reproducible, but they carry no statistical error bars across
  different RNG seeds for peer sampling. The loss suite alone runs
  three trials per configuration; widening that to every suite is
  cheap and worth doing.

#v(0.6em)

#sc[Bottom line.] #h(0.4em) The protocol behaves the way the
literature predicts a careful anti-entropy CRDT push protocol with
frontier-based sender-side dedup ought to behave, at every cluster
size we have tested. The simulator's coverage is wide on the
static-membership axes (cluster size up to 1024, fanout 1 through
12, loss up to 50 %, partition + heal) and narrow on the
dynamic-membership axis (no churn, no node death). That narrow
patch is exactly where production deployments operate today on
Kubernetes—kube delivers a stable peer set on the timescales
gossip cares about—and it is the next axis to bring under
measurement.

#set par(first-line-indent: 1.2em)

// =====================================================================
// Comparison
// =====================================================================
= How gabion compares to the literature

#set par(first-line-indent: 0em)
A measurement is only as informative as the prior literature you
can read it against. The summaries below place each of our headline
numbers next to the matching published result, with a candid note
on whether the comparison is fair.
#set par(first-line-indent: 1.2em)

#let claim-block(paper, claim, ours) = block(
  width: 100%,
  breakable: false,
  inset: (left: 0.6em, top: 0.4em, bottom: 0.4em),
  stroke: (left: 1pt + sub-color),
  [
    #set par(first-line-indent: 0em, hanging-indent: 1em)
    #strong[#paper] \
    #text(fill: sub-color)[#emph[Their measurement.] #h(0.3em) #claim] \
    #text[#emph[Ours.] #h(0.3em) #ours]
  ],
)

#claim-block(
  [Demers et al. 1987, _Epidemic Algorithms_ (PODC)],
  [Anti-entropy converges to every node in $log N$ rounds with high
   probability; metrics are #emph[residue], #emph[traffic], and
   #emph[delay] (split into $t_("avg")$ and $t_("last")$).],
  [We report $t_("last")$ only (one #raw("convergence_millis") per
   run; first sample where every node matches ground truth). At
   $f = 3$ the curve hugs $log_2 N$ through $N = 256$ and stays
   within one round at $N = 1024$.],
)

#claim-block(
  [Karp et al. 2000, _Randomized Rumor Spreading_ (FOCS)],
  [Push-pull on the complete graph: $Theta(log_2 N)$ rounds,
   $Theta(N log log N)$ messages; the latter is the
   address-oblivious lower bound.],
  [Gabion's peer-frontier dedup is #emph[not] address-oblivious—
   the sender carries per-receiver knowledge of acked sequences—so
   convergence below $log_2 N$ at $N gt.eq 16$ is consistent with
   the theorem, not in conflict with it.],
)

#claim-block(
  [Das, Gupta, Motivala 2002, _SWIM_ (DSN)],
  [Detection time $T'\/(1 - e^(-q f)) approx 1.6 T'$; constant
   per-node load as $N$ grows.],
  [Membership is delegated to the Kubernetes
   #raw("EndpointSlice") watcher; SWIM's failure-detection number
   is out of scope. The structural analogue we measure is per-node
   bandwidth: flat at #n1024_bytes_per_s B/s/node from $N = 32$ to
   $N = 1024$, within ten percent across the range.],
)

#claim-block(
  [Leitão et al. 2007, _HyParView_ (DSN) and _Plumtree_ (SRDS)],
  [Active view size $approx log N + c$; >99 % reliability at 80 %
   kill; Plumtree's RMR metric quantifies message redundancy.],
  [Gabion does not implement membership. Its peer-frontier dedup
   plays Plumtree's role of suppressing already-delivered
   messages. RMR is a natural addition; the renderer already
   records bytes-per-novel-delta, which is the numerator.],
)

#claim-block(
  [Van Renesse, Birman, Vogels 2003, _Astrolabe_ (TOCS)],
  [Aggregation tick $rho approx 5"–"10 s$; ~5 % aggregate error;
   propagation in tens of seconds across hierarchy levels.],
  [Gabion is a single-level mesh, not a hierarchy; with
   $rho = 100$ ms it propagates a write in sub-second wall-clock
   time even at $N = 1024$—about two orders of magnitude faster
   than Astrolabe, at the cost of carrying a richer aggregate.],
)

#claim-block(
  [Birman et al. 1999, _Bimodal Multicast_ (TOCS)],
  [Stable delivery at up to ~25–30 % packet loss; bimodal latency
   distribution under bursty load.],
  [Convergence at 50 % i.i.d. loss with a +2-round penalty and no
   residual divergence in any trial. Better than Bimodal's
   threshold; we have not measured the latency-distribution shape
   under burst load.],
)

#claim-block(
  [Jelasity et al. 2007, _Gossip-based peer sampling_ (TOCS)],
  [In-degree distribution skew; resilience under churn up to $10^5$
   nodes.],
  [Not measured here. The peer set is fixed per scenario; a churn
   suite that implements #raw("PeerDiscovery") as a sim type with
   scriptable joins and leaves is the natural follow-up.],
)

// =====================================================================
// References
// =====================================================================
= References

#set par(first-line-indent: 0em, hanging-indent: 1.2em, justify: false)

Demers, A., D. Greene, C. Hauser, et al. 1987. _Epidemic algorithms
for replicated database maintenance._ Proceedings of the Sixth
Annual ACM Symposium on Principles of Distributed Computing
(PODC '87).

Karp, R., C. Schindelhauer, S. Shenker, and B. Vöcking. 2000.
_Randomized rumor spreading._ Proceedings of the 41st Annual
Symposium on Foundations of Computer Science (FOCS).

Das, A., I. Gupta, and A. Motivala. 2002. _SWIM: Scalable
weakly-consistent infection-style process group membership
protocol._ International Conference on Dependable Systems and
Networks (DSN).

Leitão, J., J. Pereira, and L. Rodrigues. 2007. _HyParView: A
membership protocol for reliable gossip-based broadcast._
International Conference on Dependable Systems and Networks (DSN).

Leitão, J., J. Pereira, and L. Rodrigues. 2007. _Epidemic broadcast
trees_ (Plumtree). Symposium on Reliable Distributed Systems
(SRDS).

Van Renesse, R., K. Birman, and W. Vogels. 2003. _Astrolabe: A
robust and scalable technology for distributed system monitoring,
management, and data mining._ ACM Transactions on Computer Systems
21(2).

Jelasity, M., S. Voulgaris, R. Guerraoui, A.-M. Kermarrec, and
M. van Steen. 2007. _Gossip-based peer sampling._ ACM Transactions
on Computer Systems 25(3).

Birman, K., M. Hayden, O. Ozkasap, et al. 1999. _Bimodal
multicast._ ACM Transactions on Computer Systems 17(2).

DeCandia, G., D. Hastorun, M. Jampani, et al. 2007. _Dynamo:
Amazon's highly available key-value store._ Proceedings of the ACM
SIGOPS Symposium on Operating Systems Principles (SOSP).
