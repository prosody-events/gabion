# Disseminating windowed counter deltas in a gossiped rate limiter: related work and candidate designs

This section surveys the literature and industry practice on spreading
CRDT counter state among a cluster of rate limiters. It then fixes the
candidate set, the parameters and the faithfulness invariants for the
simulation study. The harness, the regimes and the results are reported
separately. Every candidate runs on one shared harness and is scored by
one shared judge.

## 1. Problem statement

A cluster of `N` nodes, with `3 <= N <= 300`, runs in one region across
three availability zones (AZs). Each node admits requests against its own
estimate of a cluster-wide, per-key, windowed request count. The count is
a state-based CRDT. A *cell* `(origin, rule, key, bucket)` is a
grow-only counter written only by its origin node. The cluster value of a
key is the sum over origins of the max-merged cells in the live buckets
of its window. Rule windows are mostly 10 s with 1 s buckets, and some
are 1 h. Membership is known in full from service discovery, which may
lag. Transport is UDP: packets can be lost and nothing is retransmitted
unless the protocol does it.

The reference production deployment sees the following load:

- 60 to 183 pods typical-to-peak (median about 80), with an autoscaler ceiling of 300.
- About 650 counted req/s typical and about 1,800 at peak.
- About 2,100 active keys, each hit about every 3.2 s, so about 630 distinct keys/s.
- Up to about 6,400 live cells.
- About 1,000 to 1,200 brand-new pods per day, from rollout waves, autoscaler steps and evictions.

The objective is the **best rate-limit accuracy per byte**. Accuracy
means low over-admission and low under-count against the true aggregate.
Bytes means total bytes on the wire across the whole cluster over a fixed
stretch of virtual time, IP and UDP headers included. Two further
requirements apply: staleness must stay under a second, and a new node
must rebuild its window quickly.

Three properties of this problem separate it from the settings most
gossip papers study:

1. **Single-writer cells.** Every update has exactly one origin, and
   per-origin sequence numbers order it totally. Origin-scoped digests are
   therefore exact, with no need for vector clocks over replicas
   ([9], [1]).
2. **Superseding state.** A later value of a cell subsumes earlier ones.
   An update that is lost is repaired by the next update to the same cell,
   or by any repair that ships the current value. Lossless delivery of
   every message is not needed, only convergence of the current state
   ([1], [9], [13]).
3. **Tiny, frequent, mostly-unique updates.** About 630 keys/s, each
   usually hit on one node, gives one or two cells per origin per round.
   Per-packet headers and per-peer control traffic, not payload, dominate
   bytes at large `N`. Batching across origins and across rounds is
   therefore a first-order effect.

## 2. Related work

### 2.1 Delta-state CRDT synchronisation

Almeida, Shoker and Baquero define delta-state CRDTs [1]. Replicas ship
join-irreducible *deltas* in place of full state. The paper gives two
anti-entropy algorithms. The first converges over lossy, duplicating and
reordering channels by re-shipping delta-groups. The second keeps one
delta buffer per neighbour, garbage-collected by *acknowledgement*, and
falls back to full state when a neighbour lags beyond the buffer.

Enes et al. [2] identify two inefficiencies of classic delta propagation
on non-mesh topologies:

- **Back-propagation (BP):** returning a delta to the node it came from.
- **Redundant reception (RR):** forwarding parts of a delta group the
  receiver already has.

Removing both brings transmission close to optimal in their measured
deployments. Gabion's per-peer frontier is an RR filter. The
origin/source-skip ablation on `exp/frontier-ablation` is BP removal.

Akka Distributed Data [25] is the closest production analogue. It sends
deltas on change and gossips state periodically, every 2 s by default.
It caps entries per message, at 500 by default, falls back to full state
when a delta is too large or a node joins, and prunes state left by
departed nodes. Its default cadence is an order of magnitude slower than
our staleness target.

A master's thesis [30] studies a CRDT-counter rate limiter directly and
trades precision for performance. We have not verified its numbers and
cite it only as related work.

### 2.2 Anti-entropy and digest reconciliation

Demers et al. [13] separate *anti-entropy*, which reconciles whole state
with a random peer, from *rumor mongering*, which pushes a hot update
until it is seen too often. Their analysis of push, pull and push-pull is
still the basis of gossip cost models. Kermarrec, Massoulié and Ganesh
[14] show that infect-and-forward-once with mean fanout `ln N + c`
reaches every node with probability `e^{-e^{-c}}`. That threshold drives
both `main` and the flat forward-once candidate.

Scuttlebutt [9] specialises anti-entropy to single-writer, versioned
key-value state:

- Peers exchange per-origin max versions as a digest.
- Each side returns the deltas the other lacks, in version order, so the
  receiver's per-origin version always stays a valid prefix.
- When the MTU binds, deltas are ordered "scuttle-depth" or
  "scuttle-breadth".
- An AIMD flow-control scheme bounds the update rate.

Cassandra's three-way SYN/ACK/ACK2 gossip [31] is a production
descendant of this design.

Bimodal Multicast [10] pairs an unreliable best-effort multicast with
periodic digest-based anti-entropy rounds. Delivery is bimodal: almost
all nodes get a message or almost none do, with exponentially small
failure probability, and no per-receiver acknowledgement state is kept.

### 2.3 Structured, tree and mesh broadcast

Plumtree [3] builds a spanning tree for each broadcast source on a
HyParView [4] overlay:

- The full message is pushed *eagerly* along tree links.
- Message identifiers (`IHAVE`) are announced *lazily* along the other
  links.
- A lazy announcement with no matching eager delivery within a timeout
  triggers `GRAFT`, which pulls the message and repairs the tree.
- A duplicate eager delivery triggers `PRUNE`.

`riak_core_broadcast` runs Plumtree with one eager/lazy set per root.

Thicket [5] builds several trees in which each node is interior in at
most one. This balances forwarding load and bounds the damage one failure
does.

GossipSub v1.1 [6, 7] keeps a bounded-degree mesh with parameters
`D = 6`, `D_lo = 4`, `D_hi = 12` and `D_lazy = 6`, and a 1 s heartbeat.
Full messages are pushed to mesh peers, and lazy `IHAVE`/`IWANT` gossip
goes to non-mesh peers. It is validated in large deployed networks. Its
peer-scoring defences target adversaries that a trusted cluster does not
have.

Snow [8] uses no overlay at all. Each node sorts its full membership into
a ring and places the root at the midpoint. It splits the ring into `k`
sub-ranges, each led by its midpoint node, and each child recurses within
its range, giving a balanced `k`-ary tree. The paper's default is
`k = 4`, and its height is `⌈log_k((k-1)N) + 1⌉`. Reliability comes from
leaf-to-root acknowledgement aggregation with root rebroadcast on timeout.
At `N = 500` it reports lower redundancy than gossip or Plumtree, at
similar or better latency, under churn. Snow is a single-author preprint
and has not been peer reviewed. Of all the designs, it fits gabion's
setting of full membership with no overlay most directly.

### 2.4 Hierarchical aggregation and estimation

Astrolabe [11] arranges nodes into a hierarchy of zones. Each zone
aggregates its children's attributes, so per-node load grows with the
depth of the hierarchy rather than with `N`. Three AZs give gabion a
natural two-level hierarchy. Running Consul gossip at large scale also
shows that segmenting gossip pools along AZ lines cuts queueing and
speeds convergence [29].

Push-sum [12] estimates a cluster-wide sum with `O(1)` state per key and
`O(log N + log 1/ε)` rounds. However, it conserves mass only over
lossless links, and it estimates a global total, not windowed per-origin
buckets that expire. We do not take it forward as a candidate (§3.3).

### 2.5 Set reconciliation for repair and catch-up

Set reconciliation computes the difference between two sets in
communication that scales with the size of the difference, not the size
of the sets. Four schemes are relevant:

- **Erlay** [16] (minisketch/BCH) pairs low-fanout flooding with periodic
  reconciliation. It saves about 40% of the bandwidth of Bitcoin
  transaction relay, but a sketch's capacity must bound the difference
  size.
- **Rateless IBLTs** [15] remove that bound. The receiver keeps decoding
  coded symbols until the difference resolves, at about 1.35–1.7 times
  the optimal size and in linear time.
- **Range-based reconciliation** (Negentropy) [17] recursively
  fingerprints sorted ranges, costing `O(d log n)` over several round
  trips.
- **ConflictSync** [18] applies these techniques to the join
  decompositions of state-based CRDTs. It has been evaluated on sets but
  not on nested maps of counters.

None of these meets a sub-second deadline for steady-state
dissemination. They are candidates for the *repair* and *catch-up* lanes,
and appear in this study as bolt-on components (§4).

### 2.6 Rate limiters in industry

No production rate limiter we could find publishes the byte cost of
spreading counters between peers.

**Central coordinators.** Most practice relies on one of three designs:

- **A central store.** Envoy's RLS [21] calls an external service backed
  by Redis, and the stores used by Kong-style and Redis-based limiters
  follow the same pattern. We did not verify those per-vendor designs
  beyond their documentation.
- **Quota leases from a coordinator hierarchy.** Doorman [22] and Envoy's
  RLQS [21] work this way.
- **Owner aggregation.** Gubernator's `GLOBAL` mode [20] consistent-hashes
  each key to an owner. Peers answer from a local cache and forward
  batched hits to the owner asynchronously. The owner sums them and
  broadcasts the current status to every peer.

**Peer-to-peer coordination.** Raghavan et al.'s distributed rate limiting
[19] is the classic academic precedent for gossip-style demand
estimation. It splits a global limit across a handful of limiters.

**Window approximation.** Cloudflare [23] approximates a sliding window
from two fixed windows. Over 400 M requests they report 0.003% wrong
decisions but a 6% mean gap in rate. Their counters stay scoped to each
PoP and are not gossiped.

**Sync interval versus accuracy.** API7 [24] measured how accuracy depends
on the sync interval `s` relative to the window. For `N` instances with
per-instance rate `r`, overshoot is about `(N-1)·r·s` per window. At
`s / W = 2%` the error is about 1%. This gives a testable prediction: at
fixed staleness, error grows with `N`.

**Coverage gap.** We found no primary engineering write-ups from the
large web operators on peer counter synchronisation. A 2026 comparison of
Redis-backed limiter algorithms [34] involves no gossip. The measurements
in this study are therefore the primary evidence. We do not claim that
industry favours any candidate.

### 2.7 Lessons carried into the study

- **Two lanes.** The literature converges on a fast, best-effort lane
  for freshness ([10], [16], [25], [26]), paired with a slow repair lane
  for loss and churn. The open question for gabion is where the
  byte-optimal split lies at `N = 3…300`.
- **Batching.** Per-packet overheads dominate tiny updates. Designs that
  batch many origins' updates into one packet (trees, relays, owners)
  trade latency and hop count for fewer headers.
- **Liveness.** Liveness must come from service discovery, not from
  short silence timeouts, because a CPU-starved node triggers false
  suspicion [27].
- **Accuracy depends on `N`.** It must be measured against staleness
  and `N` jointly [24], and only regimes where keys approach their limits
  exercise over-admission at all.

## 3. Candidate set

Every candidate shares the following, so that only the dissemination
design differs:

- the `CellStore` CRDT join (max per cell, sum across origins);
- the wire encoding of a cell;
- the gossip round scheduler from `main`, which decides **when** an
  origin's own changes leave it: a 500 ms heartbeat, plus a synthetic
  round when a rule's error budget is exceeded, with a 5 ms floor.

Each design decides **to whom** data goes, what is relayed, and how loss
and churn are repaired. Design-intrinsic timers such as `GRAFT` timeouts,
mesh heartbeats and repair rounds keep the values given in the sources.

### 3.1 Required designs (already examined)

| id | design | primary sources | starting point |
|----|--------|-----------------|----------------|
| `main` | Coverage-fanout anti-entropy with peer frontier and repair lane (baseline, no spike) | [13], [14], [33] | `origin/main` (cfcf26c), which is the harness branch itself |
| `flat-once` | Infect-and-forward-once, newest first, fanout `⌈ln N + c⌉` | [14], [13] | `fix/gossip-novelty-first` (PR #108) |
| `direct-ack` | Direct mail to every peer plus per-peer cumulative acks (delta-state anti-entropy on a full mesh) | [1], [2] | `gabion-design-research/research/scratch-variants.patch` (`Variant::DirectAck`) |
| `plumtree` | Per-origin epidemic broadcast trees (eager push, lazy `IHAVE`, `GRAFT`/`PRUNE`); hybrid `+1` random peer as a parameter | [3], [4] | `exp/plumtree-sim` (`Kind::Plumtree`) |
| `snow` | Membership-view `k`-ary tree per root | [8] | `exp/plumtree-sim` (`Kind::Snow`) |
| `thicket` | `T` Plumtree-style trees, each node interior in at most one | [5] | `exp/plumtree-sim` (`Kind::Thicket`) |
| `gossipsub` | Bounded-degree mesh, eager to mesh, lazy `IHAVE`/`IWANT` | [6], [7] | `exp/plumtree-sim` (`Kind::GossipSub`) |
| `scuttlebutt` | Push-pull per-origin version-digest reconciliation | [9], [31] | none in code (cost estimate only in `research/model.py`) |
| `owner-agg` | Key-owner aggregation and broadcast (Gubernator `GLOBAL`-like) | [20] | none in code (cost estimate only in `research/model.py`) |

### 3.2 New candidates from the survey

At most three new candidates were allowed. Two pass the bar: each is
distinct from the required set, is credible for this problem, and tests a
question the required set leaves open.

- **`bimodal` — Bimodal Multicast** [10]. This is direct mail with **no**
  acknowledgement state, plus one digest anti-entropy exchange per round
  with a single random peer. It tests whether the `O(N)` ack state of
  `direct-ack` pays for itself under constant churn. `direct-ack` must
  keep that state per peer and rebuild it for about 1,000 new pods a day,
  while `bimodal`'s repair cost stays `O(1)` peers per round. Bimodal
  Multicast is peer-reviewed, analysed, and the canonical source of the
  "best-effort push plus digest repair" pattern that the survey points
  to.
- **`az-hier` — AZ-hierarchical two-level dissemination**, inspired by
  Astrolabe zones [11] and AZ-aligned gossip segments [29]. It is the
  only candidate that uses the deployment's topology: traffic crosses an
  AZ boundary about twice per delta instead of about `2N/3` times. It
  also batches all origins' cross-AZ relays into one packet per local
  peer per round. Cross-AZ bytes cost money in cloud deployments and are
  reported separately.

### 3.3 Considered and rejected

- **Push-sum [12].** A lost message loses mass, so the estimator needs
  lossless links or extra mass-recovery machinery. It estimates one
  global sum, not per-origin windowed buckets with expiry. It would
  replace the CRDT rather than disseminate it.
- **SWIM/memberlist infection-style dissemination [26], [28].** Push
  rumor mongering with a retransmit limit of `λ·⌈log₁₀(N+1)⌉` is the
  same family as `flat-once`, differing only in its fanout and transmit
  count parameters. The retransmit limit is offered to improvers as
  component `retransmit-limit`.
- **Erlay [16] and the other set-reconciliation schemes [15], [17],
  [18].** These are repair and catch-up lanes, not steady-state
  dissemination, and are listed as components.
- **DRL share allocation [19] and Doorman leases [22].** These change the
  admission semantics to enforcing a local share, rather than
  disseminating the aggregate. They are out of scope for a
  dissemination study.
- **The sliding-window approximation [23].** This changes how state is
  represented, not how it is disseminated.

### 3.4 Mechanisms, parameters and faithfulness invariants

The following invariants apply to **every** candidate. The auditor checks
them before any design-specific invariant.

- **I-1.** A node learns another node's state only from packets the
  simulated transport delivered. Protocol logic never reads another
  node's memory, the oracle, global membership, or the true aggregate.
- **I-2.** Membership comes only from discovery events, with the
  discovery lag the harness models. There is no instant global view.
- **I-3.** Cells use the shared codec and the shared `CellStore` join.
  Any extra control field (ack, digest, `IHAVE`, edge sequence) is encoded
  explicitly on the wire and counted by the judge.
- **I-4.** Per-node protocol state is bounded and sized at construction.
  It does not grow without bound with time, rounds or departed peers.
- **I-5.** There is no special-casing on scenario, regime name or node
  index, and no tuning against the judge's metrics inside protocol code.
- **I-6.** Randomness comes only from the node's seeded RNG.
  Determinism comes from iteration in a stable order: no `HashMap`
  iteration may drive behaviour.

#### `main`

The mechanism is as on `origin/main` (`crates/gabion/README.md`, "How
gossip works"):

1. Each round sends to `⌈ln(n) + 3⌉` random peers.
2. Each pick's frame is filled from the `local_dirty` lane, then the
   `forwarded_dirty` lane, then the repair lane.
3. Candidate cells are pruned against `peer_frontier`.

Parameters are the shipped defaults: tick 500 ms, `c = 3`, error budget
100 bps, 5 ms minimum emit interval, 1,400 B payload. The `main` arm is
not modified by the study.

#### `flat-once` (PR #108)

The mechanism follows Kermarrec, Massoulié and Ganesh [14]:

1. Each round sends the changes since the last round, newest first, to
   `⌈ln(n) + c⌉` distinct random peers.
2. A node that merges a cell whose count rose forwards it **once**, in
   its next round.
3. Dirty rings are cleared at the end of each round.
4. A heartbeat's first pick also carries a 256-cell repair slice, which
   rotates through the live cells.

The peer frontier is a skip hint only. Parameters: `c = 3`, as in
PR #108.

Invariants:

- A merged change is forwarded at most once per node per change.
- Picks are sampled without replacement within a round.
- Fanout depends on `n` only, never on the volume of the dirty set.
- Convergence never depends on the frontier hint. Disabling it changes
  bytes, not correctness.

#### `direct-ack`

The mechanism follows the delta-state anti-entropy algorithm with acks
[1], on a full mesh:

1. Each round, an origin sends its own changes directly to **every**
   peer.
2. **Nothing is forwarded.**
3. Every packet a node sends to peer `p` piggybacks a cumulative ack:
   the highest contiguous origin sequence it holds for each origin `p`
   has been sending it. In practice that is `p`'s own sequence, since only
   origins send.
4. The sender retransmits to `p` its cells above `p`'s last ack, once
   per retransmission timeout. The timeout is `max(2·RTT_est, 1 tick)`.
5. If a peer's ack lags the retained delta range, the sender falls back
   to shipping that peer the origin's full live window.

Parameters: the shared round scheduler, and acks piggybacked on data with
a standalone ack only when there is no reverse traffic within one tick.

Invariants:

- Only an origin sends that origin's cells. No third-party forwarding.
- Acks are cumulative per origin and monotone. An ack never covers a
  sequence the acker has not merged.
- Retransmission is driven only by the acks received, not by the oracle
  or a timer guess about delivery.
- Per-peer ack state is created on a discovery join and freed on a
  discovery leave.

#### `plumtree`

The mechanism is Plumtree [3], with eager and lazy sets kept per root
(origin), as in `riak_core_broadcast`. The active view is `⌈ln n + 3⌉`
random peers, which stands in for HyParView [4] because membership is
full.

1. A message is an origin's change, named by `(origin, incarnation,
   origin sequence)`.
2. Eager peers receive the full cells. Lazy peers receive batched
   `IHAVE(origin, seq)` once per heartbeat.
3. A lazy `IHAVE` for a sequence not received within
   `missing_timeout = 500 ms` triggers `GRAFT` to the announcer, which
   moves it to eager and pulls the missing cells.
4. A duplicate eager delivery triggers `PRUNE`, which moves the sender to
   lazy for that root.
5. Per-edge sequence numbers plus a NACK repair UDP loss on eager edges.
   This adapts the paper's assumption of reliable links to UDP.
6. Hybrid parameter: `extra_random ∈ {0, 1}` random non-tree peers per
   round.

Starting parameters are from `exp/plumtree-sim`: `missing_timeout_ms =
500`, `lazy_targets = 2`, `full_ihave_every = 8`, `relay_floor_ms = 5`,
`nack_retry_ms = 100`.

Invariants:

- `PRUNE` is sent only on a duplicate *eager* delivery for that root.
- `GRAFT` is sent only after a lazy announcement is followed by the
  timeout expiring without delivery.
- Eager and lazy sets are kept per root. They partition the active view
  for each root.
- Relayed cells keep the origin's sequence, so `IHAVE` and `GRAFT` name
  the same message cluster-wide.
- The hybrid random peer, if enabled, is counted as data traffic.

#### `snow`

The mechanism is Snow [8]:

1. Each node sorts its membership view into a ring.
2. For a root `r`, the ring is rotated so that `r` sits at the midpoint.
3. Each node splits its responsibility range into left and right halves,
   and each half into `k/2` sub-ranges. Each sub-range's midpoint node
   becomes a child.
4. The packet carries the child's range bounds `[lo, hi)`. A receiver
   recomputes its children inside the carried range from its own view. If
   a bound node is missing from its view, it inserts it.
5. A range of at most `k` nodes is delivered as leaves.
6. Reliability follows the paper: leaves ack their parent, and interior
   nodes ack only after all their children have acked. A root that does
   not collect all `k` acks within its timeout rebroadcasts. The
   `exp/plumtree-sim` variant replaced this with lazy `IHAVE`. That
   variant is allowed only as a labelled parameter (`snow_lazy`), not as
   the default.

Parameters: `k = 4` (the paper's default), ack timeout of 2× the expected
tree RTT, and one rebroadcast attempt before the repair lane takes over.

Invariants:

- Children are a pure function of the local view, the root and the
  carried range.
- There are no random peers on the tree path.
- No node sends the same message twice to the same child unless a
  rebroadcast timeout has expired.
- Acks are aggregated bottom-up. A parent never acks before its children
  do.
- View disagreement is handled only by the range-carrying rule, not by
  consulting global membership.

#### `thicket`

The mechanism is Thicket [5]:

1. `T` Plumtree-style trees are built over the active view.
2. Each node is interior (forwards) in at most one tree. Its interior
   tree is chosen by `hash(node) mod T`, or by the paper's
   load-balancing rule.
3. Each origin's change is sent down one tree, chosen round-robin by
   sequence, so forwarding load spreads across trees.
4. Missing-message detection and `GRAFT`/`PRUNE` follow Plumtree,
   restricted so that a `GRAFT` never makes a node interior in a second
   tree.

Parameter: `T = 3`, the default on `exp/plumtree-sim` and matched to
3 AZs. The paper explores small `T`.

Invariants:

- A node forwards in at most one tree. A leaf in a tree never relays
  that tree's messages.
- Tree repair never violates the at-most-one-interior rule.
- Tree selection per message is deterministic from `(origin, seq)`.

#### `gossipsub`

The mechanism is GossipSub v1.1 [7], using one topic for the whole
cluster:

1. Each node keeps a mesh of degree `D` within `[D_lo, D_hi]`, maintained
   each heartbeat with `GRAFT` and `PRUNE`.
2. Full messages are pushed to mesh peers.
3. Each heartbeat, `IHAVE` for the message ids in the recent message
   cache goes to `max(D_lazy, 0.25 × non-mesh)` non-mesh peers.
4. A peer requests ids it is missing with `IWANT`.
5. Peer scoring and anti-Sybil machinery are omitted, because the
   cluster is trusted.

Parameters are the spec defaults: `D = 6`, `D_lo = 4`, `D_hi = 12`,
`D_lazy = 6`, gossip factor 0.25, a message cache of 5 heartbeats of
which 3 are gossiped, and a 1 s mesh heartbeat. The data round scheduler
is shared.

Invariants:

- Full payloads go only to mesh peers, or in answer to an `IWANT`.
- `IHAVE` carries ids only.
- Mesh size is kept within `[D_lo, D_hi]` only by heartbeat maintenance.
- The message cache window bounds what `IWANT` can serve.

#### `scuttlebutt`

The mechanism is Scuttlebutt [9]:

1. Each round, a node sends one random peer a digest: the max version per
   origin it holds.
2. The peer replies with every cell whose origin version exceeds the
   digest, in per-origin version order, plus its own digest.
3. The initiator answers with the cells the peer lacks.
4. When the reply would exceed the MTU budget, cells are chosen by the
   paper's "scuttle-depth" ordering, so each origin's delivered sequence
   stays a prefix and the receiver's digest stays exact.
5. Gossip rounds run at the shared round cadence. Because the round
   cadence drives freshness, an optional fanout `f > 1` is a parameter.

Parameters: one exchange per round (`f = 1`), with `f ∈ {1, 2, 3}` as a
sensitivity sweep. The paper's AIMD flow control is disabled at the
start and offered to improvers.

Invariants:

- Data moves only in response to a digest. There is no unsolicited push
  of cells.
- Per-origin delivery is in version order, so a receiver's digest never
  claims a version it lacks.
- Digests are per origin, not per cell.
- Exchanges are pairwise between the initiator and one random peer per
  fanout slot.

#### `owner-agg`

The mechanism is Gubernator's `GLOBAL` mode [20]:

1. Each `(rule, key)` has one owner, chosen by rendezvous hash over the
   discovered membership.
2. A non-owner admits from its local cache of the owner's last broadcast
   total, plus its own unreported hits.
3. Each non-owner batches its hits per owner and sends them to the owner
   every `sync_wait`.
4. The owner folds the hits into its cells, which it holds as the
   origin-tagged cells of the contributing nodes, so the CRDT join stays
   exact.
5. Every `broadcast_interval`, the owner broadcasts the changed totals
   for its keys to every peer.
6. When membership changes, ownership moves to the new rendezvous winner.
   The new owner rebuilds the key from the hits re-sent by contributors
   that have not seen an owner acknowledgement. Contributors re-send to
   the new owner any window hits the old owner did not acknowledge.

Parameters: `sync_wait = broadcast_interval = 100 ms`. This is chosen to
meet the sub-second staleness target with margin, since Gubernator's own
defaults batch on sub-millisecond windows for request coalescing and are
not cadences. One owner per key, and a batch limit of 1,000 keys per
update (Gubernator's `GlobalBatchLimit`).

Invariants:

- Only the owner broadcasts a key's total.
- A non-owner never forwards another node's hits.
- Ownership is a pure function of `(key, discovered membership)`.
- Hits are acknowledged by the owner and re-sent until acknowledged or
  out of window, so ownership moves cannot silently drop them.
- A non-owner never relays a broadcast total to other peers.

#### `bimodal` (new)

The mechanism is Bimodal Multicast [10], adapted to state CRDTs:

1. Each round, an origin sends its own changes directly to every peer.
   This is the best-effort first phase: there are no acks and no
   retransmission.
2. Every `repair_period`, each node sends one random peer a digest: the
   max version per origin over the live window.
3. The peer answers with the cells the sender lacks (push-pull
   solicitation) and may request what it lacks itself.
4. Because cells are state, any node holding a newer cell may serve it.
   This matches the paper, where retransmissions come from any member's
   buffer.

Parameters: `repair_period = 1 s` with a fanout of 1. The paper uses one
repair round per gossip period with fanout 1. A sweep covers
`{0.5, 1, 2} s`.

Invariants:

- The first phase carries no ack and keeps no per-receiver delivery
  state.
- Repair is digest-driven and pairwise with random peers.
- A node never retransmits first-phase data except in answer to a
  digest.
- Repair traffic is bounded per round, as the paper's buffer-limit rule
  requires.

#### `az-hier` (new)

The mechanism is two-level dissemination, with Astrolabe-style zones
[11] set to AZs:

1. **Level 0, within an AZ.** An origin sends its own changes to every
   peer in its AZ.
2. **Level 1, across AZs.** For each other AZ `Z`, the origin sends the
   same changes to one gateway `g(origin, Z)`, the rendezvous-hash winner
   of `(origin, m)` over `Z`'s members `m`. The gateway re-sends them,
   within its own AZ, to every member except itself. It batches every
   origin it relays into one packet per local peer per round.
3. Every hop uses per-peer cumulative acks (as in `direct-ack`), so loss
   is repaired hop by hop.
4. If a gateway does not ack within the retransmission timeout, the
   origin moves to the next rendezvous rank in `Z`.
5. In Astrolabe's aggregation mode (`aggregate = true`), the gateway
   instead forwards per-`(key, bucket)` sums of the remote AZ's cells,
   tagged with an epoch for each gateway. This loses per-origin
   exactness, and the judge measures the cost. The default is
   `aggregate = false`, which keeps per-origin cells and an exact CRDT.

The AZ label comes from discovery (pod topology), which the harness
supplies.

Invariants:

- Cells cross an AZ boundary only from an origin to a gateway.
- A gateway relays only into its own AZ.
- A gateway is a pure function of `(origin, Z's discovered members)`.
- Failover is triggered only by missing acks, never by oracle liveness.
- With `aggregate = false`, the per-origin CRDT join is unchanged.

## 4. Components available to improvers

These mechanisms can be bolted onto any candidate in the improvement
phase. Each must be enabled through an explicit, labelled parameter, so
that the baseline and improved arms stay separable.

| id | mechanism | sources |
|----|-----------|---------|
| `pull-sweep-join` | A new or restarted node requests the live window (all origins' live cells) from `k = 2` rendezvous-chosen peers, paced to the MTU budget, before relying on push. | [25], `gabion-catchup-sim/sim-runs` |
| `digest-repair` | A periodic per-origin version-digest exchange with one random peer (Scuttlebutt-style) as a repair lane behind any push design. | [9], [10], [31] |
| `riblt-repair` | A rateless IBLT stream over hashes of `(origin, rule, key, bucket, count)` for catch-up and large-difference repair. One stream serves any peer. | [15], [18] |
| `range-recon` | Negentropy range-fingerprint reconciliation for catch-up, compared against a plain bulk pull of the window. | [17] |
| `bp-skip` | Never send a cell back to its origin or to the peer it came from. | [2], `exp/frontier-ablation` |
| `rr-filter` | Strip from an outgoing delta group what the peer already holds, using a peer frontier built from acks or digests rather than hints. | [2] |
| `error-budget-emit` | The adaptive threshold round: emit early when a rule's unreplicated hits exceed its error budget. This is the shared scheduler on `main`. | [33] |
| `near-limit-urgency` | Send keys near their limit at once and batch keys far below it on a slower cadence. | [32], [19] |
| `full-state-fallback` | Ship full window state to a peer whose ack or digest lags the retained delta range. | [1], [25] |
| `departed-pruning` | Garbage-collect per-peer and per-origin protocol state on a discovery leave, after a grace period. | [25] |
| `retransmit-limit` | Push-gossip transmit count `λ·⌈log₁₀(N+1)⌉` (memberlist `RetransmitMult = 4`) in place of forward-once. | [26], [28] |
| `edge-nack` | Per-edge sequence numbers, NACK and selective resend for tree or mesh edges over UDP. | `exp/plumtree-sim` |
| `aimd-flow` | Scuttlebutt's AIMD flow control on the update rate under overload. | [9] |

## 5. Regimes

The base matrix fixed for the study is:

- prod-typical: 80 nodes, 650 req/s;
- prod-peak: 183 nodes, 1,800 req/s;
- prod-4x: 300 nodes, 7,200 req/s;
- 5% and 20% packet loss at prod-peak;
- low scale: 3 and 5 nodes;
- idle;
- churn: a rollout wave plus a silent crash;
- long window: 1 h rules at 60 nodes.

The evidence supports two additions, and no more:

1. **`hot-key`, the over-admission regime.** The background workload
   seldom drives a key to its limit, so over-admission, the primary
   accuracy metric, is barely exercised. Overshoot of about
   `(N-1)·r·s` [24] and the burst-to-every-instance worst case [24], [19]
   only show up when keys exceed their limits. This is the common abusive
   client or retry-storm case for any rate limiter.
2. **`az-partition`.** Correlated failures in cloud fleets are scoped to
   an AZ [29]. Isolating one AZ for a while and then healing it exercises
   repair-lane healing, tree and mesh repair, owner failover and gateway
   failover in a way that iid loss does not.

## References

1. P. S. Almeida, A. Shoker, C. Baquero. *Delta State Replicated Data Types.* JPDC 111, 2018. https://arxiv.org/abs/1603.01529
2. V. Enes, P. S. Almeida, C. Baquero, J. Leitão. *Efficient Synchronization of State-based CRDTs.* ICDE 2019. https://arxiv.org/abs/1803.02750
3. J. Leitão, J. Pereira, L. Rodrigues. *Epidemic Broadcast Trees.* SRDS 2007. https://www.dpss.inesc-id.pt/~jleitao/pdf/srds07-leitao.pdf
4. J. Leitão, J. Pereira, L. Rodrigues. *HyParView: A Membership Protocol for Reliable Gossip-Based Broadcast.* DSN 2007. https://www.dpss.inesc-id.pt/~jleitao/pdf/dsn07-leitao.pdf
5. M. Ferreira, J. Leitão, L. Rodrigues. *Thicket: A Protocol for Building and Maintaining Multiple Trees in a P2P Overlay.* SRDS 2010. https://www.dpss.inesc-id.pt/~jleitao/pdf/srds10-thicket.pdf
6. D. Vyzovitis, Y. Napora, D. McCormick, D. Dias, Y. Psaras. *GossipSub: Attack-Resilient Message Propagation in the Filecoin and ETH2.0 Networks.* 2020. https://arxiv.org/abs/2007.02754
7. libp2p. *gossipsub v1.1 specification.* https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md
8. C. Tong. *Snow: Self-organizing Broadcast Protocol for Cloud.* arXiv preprint, 2025. https://arxiv.org/abs/2504.02676
9. R. van Renesse, D. Dumitriu, V. Gough, C. Thomas. *Efficient Reconciliation and Flow Control for Anti-Entropy Protocols.* LADIS 2008. https://doi.org/10.1145/1529974.1529983
10. K. P. Birman, M. Hayden, O. Ozkasap, Z. Xiao, M. Budiu, Y. Minsky. *Bimodal Multicast.* ACM TOCS 17(2), 1999. https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/Bimodal.pdf
11. R. van Renesse, K. P. Birman, W. Vogels. *Astrolabe: A Robust and Scalable Technology for Distributed System Monitoring, Management, and Data Mining.* ACM TOCS 21(2), 2003. https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/astrolabe.pdf
12. D. Kempe, A. Dobra, J. Gehrke. *Gossip-Based Computation of Aggregate Information.* FOCS 2003. https://www.cs.cornell.edu/johannes/papers/2003/focs2003-gossip.pdf
13. A. Demers, D. Greene, C. Hauser, W. Irish, J. Larson, S. Shenker, H. Sturgis, D. Swinehart, D. Terry. *Epidemic Algorithms for Replicated Database Maintenance.* PODC 1987. https://doi.org/10.1145/41840.41841
14. A.-M. Kermarrec, L. Massoulié, A. J. Ganesh. *Probabilistic Reliable Dissemination in Large-Scale Systems.* IEEE TPDS 14(3), 2003. https://doi.org/10.1109/TPDS.2003.1189583
15. L. Yang, Y. Gilad, M. Alizadeh. *Practical Rateless Set Reconciliation.* SIGCOMM 2024. https://arxiv.org/abs/2402.02668
16. G. Naumenko, G. Maxwell, P. Wuille, A. Fedorova, I. Beschastnikh. *Erlay: Efficient Transaction Relay for Bitcoin.* CCS 2019. https://arxiv.org/abs/1905.10518
17. A. Meyer. *Range-Based Set Reconciliation.* 2022. https://arxiv.org/abs/2212.13567 ; implementation: https://github.com/hoytech/negentropy
18. *ConflictSync: Bandwidth Efficient Synchronization of Divergent State.* arXiv preprint, 2025. https://arxiv.org/abs/2505.01144
19. B. Raghavan, K. Vishwanath, S. Ramabhadran, K. Yocum, A. C. Snoeren. *Cloud Control with Distributed Rate Limiting.* SIGCOMM 2007. https://cseweb.ucsd.edu/~snoeren/papers/drl-sigcomm07.pdf
20. Mailgun. *Gubernator architecture* (v2.4.0). https://raw.githubusercontent.com/mailgun/gubernator/v2.4.0/docs/architecture.md
21. Envoy. *Global rate limiting.* https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting
22. YouTube. *Doorman: global distributed client-side rate limiting* (archived). https://github.com/youtube/doorman
23. Cloudflare. *How we built rate limiting capable of scaling to millions of domains.* https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
24. API7. *Rate limiting accuracy.* https://docs.api7.ai/api7-gateway/how-to-guides/traffic-management/rate-limiting-accuracy
25. Akka. *Distributed Data.* https://doc.akka.io/libraries/akka-core/current/typed/distributed-data.html
26. HashiCorp. *memberlist.* https://github.com/hashicorp/memberlist
27. A. Dadgar, J. Phillips, J. Currey. *Lifeguard: Local Health Awareness for More Accurate Failure Detection.* 2017. https://arxiv.org/abs/1707.00788
28. A. Das, I. Gupta, A. Motivala. *SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol.* DSN 2002. https://doi.org/10.1109/DSN.2002.1028914
29. HashiCorp. *Consul scale test report to observe gossip stability.* https://www.hashicorp.com/en/blog/consul-scale-test-report-to-observe-gossip-stability
30. *Trading performance for precision in a CRDT-based rate-limiting system.* Master's thesis, Chalmers University of Technology. https://odr.chalmers.se/handle/20.500.12380/304211
31. Apache Cassandra. *Gossip.* https://cassandra.apache.org/doc/latest/cassandra/architecture/gossip.html
32. Unkey. *Rate limiting consistency model.* https://engineering.unkey.com/architecture/ratelimiting/consistency-model
33. C. Olston, J. Jiang, J. Widom. *Adaptive Filters for Continuous Queries over Distributed Data Streams.* SIGMOD 2003. https://doi.org/10.1145/872757.872825 ; I. Sharfman, A. Schuster, D. Keren. *A Geometric Approach to Monitoring Threshold Functions over Distributed Data Streams.* SIGMOD 2006. https://doi.org/10.1145/1142473.1142508
34. arXiv:2602.11741 (2026), a comparison of Redis-backed rate-limiter algorithms. https://arxiv.org/abs/2602.11741
