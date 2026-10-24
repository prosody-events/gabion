# gabion

The pure-Rust library at the heart of every gabion deployment. It owns
the CRDT, the gossip runtime, the wire codec, the rule machinery, and the
discovery interface, while knowing nothing about YAML, gRPC, or nginx.
Both adapters in the workspace (`gabion-server` and `gabion-nginx`) build
their configuration on top of the same library types and call the same
hot paths.

This README is the canonical guide to the library. If you want to
understand how the cluster stays consistent, how a packet is shaped, or
which knob to turn when an operator asks, read it end-to-end. For the
CRDT data structures behind the gossip protocol, see [`CRDT.md`](CRDT.md).
For cross-adapter vocabulary (rule, descriptor, bucket, fail-open), see
the [repo root README](../../README.md#glossary).

## Contents

- [Module map](#module-map)
- [How gossip works](#how-gossip-works)
  - [The wire frame](#the-wire-frame)
  - [The anti-entropy loop](#the-anti-entropy-loop)
  - [Coverage fanout](#coverage-fanout)
  - [Adaptive emit rate](#adaptive-emit-rate)
  - [Dirty rings and the peer frontier](#dirty-rings-and-the-peer-frontier)
- [Design notes: Thicket dissemination](#design-notes-thicket-dissemination)
- [Operator knobs](#operator-knobs)
- [What we measured](#what-we-measured)
- [References](#references)

## Module map

The library is divided into six modules, and every type that crosses an
adapter boundary lives in exactly one of them. The dependency graph is
a DAG, so no module imports from a module that imports it.

| Module       | Path                          | What it owns                                                                                                                              |
|--------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `crdt`       | `src/crdt/*`                  | The per-origin counter store. Robin Hood hash index, structure-of-arrays columns, bounded dirty rings, per-peer frontier. Allocates only at construction. See [`CRDT.md`](CRDT.md). |
| `rules`      | `src/rules/*`                 | `Rule`, `RuleTable`, `Descriptor`, fingerprint hashing. Two nodes with the same rules produce identical `rule_fingerprint` values, which is what makes the CRDT addressable across the cluster. |
| `gossip`     | `src/gossip/*`                | `GossipRuntime`, `GossipClient`, `GossipTransport`, `Clock`, the `sim::SimTransport` used in tests, and the `AdminCommand` / `AdminSnapshot` observability surface. |
| `wire`       | `src/wire/*`                  | The on-the-wire codec. One UDP packet = header + rule dictionary + node dictionary + cell rows + optional HMAC tag. Each packet decodes independently. |
| `discovery`  | `src/discovery/*`             | `PeerDiscovery` trait and the Kubernetes EndpointSlice implementation. Adapters consume `PeerEvent` streams; the gossip runtime treats peer add/remove as a normal `select!` arm. |
| `defaults`   | `src/defaults.rs`             | Constants shared by both adapters: tick interval, fanout floor, payload caps, the per-rule error budget, and the min-emit floor. |

`gossip` sits at the top of that graph and depends on every other
module, whereas `wire` and `crdt` are independent of each other.
`defaults` is a leaf: `rules` and `gossip` import it, and it imports
nothing else in this crate. Adapters consume the public surface of
`gossip`, `rules`, and `discovery`, together with the constants in
`defaults`.

## How gossip works

> **This branch disseminates along Thicket trees.** Sections "The
> anti-entropy loop", "Coverage fanout" and "Dirty rings and the peer
> frontier" below describe the baseline, whose random per-round fanout and
> peer-frontier pruning the trees replace. The adaptive emit rate is unchanged;
> the wire frame gains an edge number and control frames, and the CRDT keeps
> the origin's sequence on merge. What replaces the baseline, and every
> way it departs from the papers, is in
> [Design notes: Thicket dissemination](#design-notes-thicket-dissemination).

Gossip is the protocol that turns N independent counter stores into one
*eventually consistent* counter store. Every node admits requests
against its local view of the cluster aggregate, and gossip is what
keeps that view fresh. From the adapter's side it shows up as a single
call: admission writes a row through `GossipClient::record`, and a
later runtime tick turns those rows into outbound frames.

The protocol has two adaptive aspects, and they are the reason gabion
holds up under load that a textbook anti-entropy loop would not:

1. **Coverage fanout** — the per-tick peer count is sized to the
   *coverage threshold* `⌈ln(n)+c⌉` (`n` = peer count), the fanout that
   provably reaches every node. It scales with cluster size, not the
   dirty set: a burst rides one fat frame instead of widening the pick.
   `fanout` is the floor; small clusters go full-mesh.
2. **Adaptive emit rate** — the gossip cadence adapts to per-rule
   pressure. Heartbeats fire every `tick_interval` unconditionally, but
   the runtime also fires a synthetic *threshold* tick between
   heartbeats whenever a hot rule's locally unreplicated hits would
   exceed its per-site error budget. The budget bounds cluster-wide
   unreplicated error per rule by 1 % of the rule's limit (default), no
   matter the request rate. A floor `min_emit_interval` caps the worst
   case so an adversarial workload cannot pin the gossip socket.

The rest of this section walks through each in turn, then closes with
the dirty rings and peer frontier — the data structures that decide
which cells go to which peer.

### The wire frame

Every gossip frame is one self-describing UDP packet, with no session
state on either side: each frame decodes independently, even if its
predecessor was dropped. Loss of a packet costs you a few cells until
the next tick refills them.

The header is fixed at 80 bytes. Every multi-byte field is encoded
little-endian; the first four bytes on the wire are `0x32 0x47 0x42
0x47` so a packet capture reads `2GBG`. (The numeric `MAGIC` literal
`0x4742_4732` spells "GBG2" when written most-significant byte first;
the LE encoding then writes those bytes in reverse.)

```
byte  field                  width
----  ---------------------  -----
0     MAGIC                  4   (0x4742_4732)
4     VERSION                2   (= 1)
6     flags                  2   (FLAG_MORE | FLAG_AUTHENTICATED |
                                  FLAG_EDGE | FLAG_CONTROL)
8     cluster_id_hash       16
24    sender_node_id        16
40    sender_incarnation     4
44    count_width            4   (2, 4, or 8 — width of the count column;
                                  matches u16/u32/u64 `impl Count`)
48    cell_count             4
52    body_len               4
56    min_origin_sequence    8
64    max_origin_sequence    8
72    edge_sequence          8   (number on the eager edge; 0 off it)
----                       ----
                            80
```

The body follows immediately. When `cell_count > 0` it carries a
*per-frame* rule dictionary, a per-frame node dictionary, and the cell
rows. Each cell row references rule and node slots by `u8` index into
those dictionaries (so the same fingerprint isn't repeated 4096 times),
plus its `key_hash`, `bucket`, origin incarnation, `last_update_millis`,
`origin_sequence`, and `count`:

```
[rule_dict_len    : u8     ]    R rules in this frame (1–255)
[rule_dict        : u128×R ]    rule fingerprints
[node_dict_len    : u8     ]    D origins in this frame (1–255)
[node_dict        : u128×D ]    origin node ids
[rule_slot        : u8 ×N  ]    each < R
[key_hash         : u128×N ]
[bucket           : u32 ×N ]
[node_slot        : u8 ×N  ]    each < D
[origin_incarn.   : u32 ×N ]
[last_update_ms   : u64 ×N ]
[origin_sequence  : u64 ×N ]
[count            : C  ×N  ]    count_width × N bytes
```

When the frame carries no cells (a frontier ack, say), the body is
empty.

If the cluster is configured with a shared HMAC key, an additional
**32-byte HMAC-SHA256 tag** is appended over the header and body, and
`FLAG_AUTHENTICATED` is set. Inbound packets whose tag fails verify are
dropped on the floor, and the whole cluster must agree on the key.

The codec lives in `src/wire/{header,body,auth}.rs`. The library's own
`FrameLimits::default()` is 256 KB — useful for tests and the simulator
— but production adapters plumb `gabion::defaults::GOSSIP_MAX_PAYLOAD_BYTES`
(1400 bytes, the safe IPv4 MSS floor) into `FrameLimits` so frames stay
well below the path MTU and avoid IP fragmentation. Frames larger than
the cap split into multiple packets at the sender, and each packet is
independently decodable.

### The anti-entropy loop

The runtime (`GossipRuntime`, in `src/gossip/runtime.rs`) is a
single-threaded event loop driven by one `tokio::select!` with six
arms: limit requests from the adapter, inbound packets, outbound
writability, peer membership churn, admin commands, and the heartbeat
tick. After every iteration that produced delta or expiration rows, the
runtime calls `aggregates.apply(...)` exactly once — that is the
contract with the downstream rate-limit decision path. Empty iterations
(an idle tick, a peer event with no CRDT effect) short-circuit the
apply so the read side of the aggregate is never disturbed for no
reason.

```mermaid
sequenceDiagram
    autonumber
    participant Adapter as Adapter (nginx / gabiond)
    participant Runtime as GossipRuntime
    participant Store as CellStore
    participant Tx as UDP transport
    participant Peer as Remote peer

    Adapter->>Runtime: record(rule, key, hits, limit, now)
    Runtime->>Runtime: handle_limit_request
    Runtime->>Store: ingest_local(observation)
    Store-->>Runtime: delta row + dirty mark
    Note over Runtime: set want_immediate_flush<br/>if pending > ε_R
    Runtime->>Adapter: aggregates.apply(deltas, expirations)

    loop every tick (heartbeat OR synthetic threshold)
        Runtime->>Runtime: handle_gossip_tick
        Runtime->>Store: expire_at(now)
        Runtime->>Store: fill_gossip_frame_for_peer
        Store-->>Runtime: pruned cell list
        Runtime->>Tx: encode + try_send_to(peer)
        Tx->>Peer: UDP frame
        Runtime->>Adapter: aggregates.apply(expirations)
    end

    Peer->>Tx: UDP frame
    Tx->>Runtime: recv_from
    Runtime->>Store: merge_remote(observations)
    Store-->>Runtime: delta rows + frontier acks
    Runtime->>Adapter: aggregates.apply(deltas, expirations)
```

`CellStore::merge_remote` performs the CRDT join: max-merge per
`(origin, key, bucket)`, sum across origins. Every dirty row also goes
into a peer-frontier ring so the next outbound frame can prune anything
that peer has already acknowledged.

### Coverage fanout

Three separate concerns pull on the gossip plane, and it is tempting to
fold them all into the fanout knob. They are governed by three different
variables, and gabion keeps them apart:

| Concern | Governed by | Mechanism |
|---|---|---|
| **Coverage** — reach every node reliably | cluster size `n` | **fanout** = `⌈ln(n)+c⌉` |
| **Volume** — a big burst of dirty cells | dirty-set size | frame fill: `max_cells_per_tick` + packet-splitting |
| **Urgency** — a fresh delta matters *now* | per-rule error budget | threshold-fire emissions (`target_err_bps`) |

Only the first is a fanout question. Kermarrec, Massoulié & Ganesh (IEEE
TPDS 2003, *Probabilistic Reliable Dissemination in Large-Scale
Systems*, Theorem 1) prove that a directed gossip round with mean fanout
`ln(n)+c` reaches **every** node with probability → `e^(−e^(−c))`:
*"there is a sharp threshold in the required fanout at log n."* The
fanout needs to grow by only 1 each time `n` rises by a factor of e, and
a single round *"can easily be modified … to send several notifications
per gossip message"* — which is exactly what gabion does: the whole
dirty set rides one fat frame, so burst **volume** never widens the peer
pick — many cells share one packet, so a burst does not translate into
proportionally more rounds.

The pick lives in `GossipRuntime::handle_gossip_tick`:

```rust
let n = peers.len();
let coverage = (n as f64).ln() + GOSSIP_COVERAGE_MARGIN;   // c = 3.0
let pick_count = config.fanout.max(coverage.ceil() as usize).min(n);
```

A partial Fisher-Yates shuffle then picks `pick_count` distinct peers
from `self.peers`. The peers must be distinct rather than sampled with
replacement, because sampling the same peer twice in one tick would burn
a send-pool slot encoding the same frame twice. `config.fanout` is a
hard floor; `.min(n)` makes small clusters full-mesh (coverage demands
it there and the bandwidth is trivial). Some example pick counts at
`fanout = 3`, `c = 3`:

| peers `n` | `⌈ln(n)+c⌉` | `pick_count`               |
|-----------|-------------|----------------------------|
| 1         | 3           | 1 (capped at peers)        |
| 4         | 5           | 4 (capped at peers)        |
| 15        | 6           | 6                          |
| 31        | 7           | 7                          |
| 63        | 8           | 8                          |
| 255       | 9           | 9                          |
| 9 999     | 13          | 13                         |

Fanout trades **bandwidth** (linear in the pick: the cluster ships
`n·pick·frame` per tick) against **coverage-failure probability**
(`≈ e^(−e^(−c))` per round) and **latency** (`≈ log_{1+pick} n` rounds,
with diminishing return above the threshold). `⌈ln(n)+c⌉` is the
provably-minimal fanout for reliable coverage of a *single* round, so it
is the natural operating point. We size to `c = 3` — a 95.1 % per-round
reach — rather than a higher margin, because gabion is not a one-shot
disseminator: anti-entropy re-gossips every dirty cell each tick until
the peer frontier acks it, so a node missed in one round is caught by
the next and the per-round miss compounds away (4.9 % → ≈ 0.24 % after
two rounds). KMG's validated single-shot threshold sits near `c ≈ 4`
(fanout 13 @ 10 000, 15 @ 50 000); a re-gossiping system does not need to
match it, and `c = 3` saves ≈ 1 peer per tick at every scale. The
bandwidth floor on quiet ticks is unchanged from a fixed fanout — the
pick is stable for a given cluster size, not pulsing with load. The
`coverage_fanout` bench
[below](#what-we-measured) shows the pick tracking `⌈ln(n)+c⌉` and
staying flat as burst volume changes.

### Adaptive emit rate

The heartbeat tick is bounded below by `tick_interval` (default
500 ms). That cadence works for cold rules but is pessimistic for hot
ones, because a saturating burst between two heartbeats could leak many
admissions past the limit before the next tick. Lowering
`tick_interval` for everyone is not an answer either: it costs
bandwidth on every quiet rule in the cluster.

The runtime crosses this trade with a per-rule error budget.
`GossipRuntime::handle_limit_request` keeps a small `rule_pending`
column indexed by rule slot; each admission adds its hits. The runtime
also computes a per-site safe zone:

```
ε_R = max(1, L_R · target_err_bps / (10_000 · N))
```

where `L_R` is the rule's limit and `N` is the peer count. When
`rule_pending` for that rule crosses `ε_R`, the runtime sets
`want_immediate_flush = true`. On the next loop iteration the top of
the run loop reads that flag (instead of waiting on `tick.tick()`) and
dispatches a synthetic gossip tick straight away. The post-emit sweep
inside `handle_gossip_tick` zeroes the rule's pending column and stamps
its `last_emit_ms`.

The constant `target_err_bps` is in basis points of each rule's own
limit, default 100 = 1 %. The cluster-wide unreplicated error per rule
is then bounded by `N · ε_R` (Sharfman, Schuster & Keren, SIGMOD 2006)
— roughly 1 % of the rule's limit, regardless of the request rate.

A floor `min_emit_interval` (default 5 ms) sits in front of the flush:
the runtime won't set `want_immediate_flush` if the gap since the last
emit is below the floor. Under adversarial traffic ε saturates to 1 and
every hit would otherwise emit; the floor caps the worst-case emit rate
so a bad client cannot pin the gossip socket.

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Accumulating: limit request<br/>adds hits to rule_pending
    Accumulating --> Accumulating: pending ≤ ε_R
    Accumulating --> ReadyToFire: pending > ε_R
    ReadyToFire --> Idle: now − last_emit < min_emit_interval<br/>(floor blocks the emit)
    ReadyToFire --> Emit: now − last_emit ≥ min_emit_interval
    Emit --> Idle: synthetic tick dispatched,<br/>rule_pending reset,<br/>last_emit_ms stamped
    Idle --> Emit: heartbeat tick<br/>(unconditional; drains cold rules)
```

Three operational properties fall out of this design. The cluster-wide
unreplicated error per rule is bounded: at the default
`target_err_bps = 100`, the global error never exceeds 1 % of the
rule's limit no matter how fast requests come in. Cold rules still
propagate, because the heartbeat is independent of the threshold path
and any rule whose pending count never reaches ε_R still replicates
every `tick_interval`. Finally, under an adversarial workload where ε
saturates and every hit would otherwise emit, `min_emit_interval` caps
the worst-case emit rate; convergence still happens, but the cluster
absorbs the burst at a different bandwidth budget.

The `error_budget` and `min_emit_clamp` bench suites measure both
sides of this trade.

### Dirty rings and the peer frontier

Three data structures decide which cells the current tick sends to
which peer:

1. **`local_dirty`** — every locally-observed hit pushes onto this
   bounded ring. Drained by every tick (heartbeat and threshold).
2. **`forwarded_dirty`** — every cell merged from a peer pushes onto a
   separate ring, so the runtime can forward a remote delta even if
   its local origin column is quiet. Larger than `local_dirty` because
   forwarded cells fan out N-to-many.
3. **`peer_frontier`** — for each peer × each origin slot, the highest
   sequence number that peer has already acked. The sender uses this
   to skip cells the recipient already has.

Together they keep `fill_gossip_frame_for_peer` allocation-free and
strictly bounded: the runtime walks at most `max_cells_per_tick` cells
through the three lanes (local dirty → forwarded dirty → repair),
prunes each candidate against the peer's frontier, and stops when the
budget is exhausted. The rings are pre-sized at construction; pushes
that hit the cap bump an overflow counter and silently drop, and the
next repair-lane sweep picks the dropped cell back up.

`CRDT.md` documents these structures end-to-end with traces, slot
lifecycles, and invariant proofs. Read it once if you ever need to
debug the CRDT.

## Design notes: Thicket dissemination

Changes travel down `T = 3` trees instead of to `⌈ln n + c⌉` random peers.
The design is Thicket (Ferreira, Leitão & Rodrigues, *Thicket: A Protocol
for Building and Maintaining Multiple Trees in a P2P Overlay*, SRDS 2010)
over Plumtree (Leitão, Pereira & Rodrigues, *Epidemic Broadcast Trees*, SRDS
2007). The code is `src/gossip/runtime/{thicket,forward,repair,edge}.rs`.

### Mechanism

1. **Classes.** A node is *interior* in exactly one tree, `hash(address) mod
   T`, and a *leaf* in the other two. Every node computes every peer's class
   from discovery alone.
2. **Trees.** In its own tree a node keeps a backbone view of `⌈ln m + c⌉`
   class-mates (`m` of them), uniform over the class-mates it knows, and, per
   root (the origin of a change), an eager subset of it, which starts as the
   whole view and shrinks on PRUNE. It also pushes to the leaves that attached
   to it, at most twice the even share of leaves per interior node and never
   fewer than `THICKET_LEAF_CHILDREN_MIN`. In each other tree it has one
   parent, an interior node of that tree chosen uniformly among those it
   knows, that it attached to. A parent that has taken all the leaves it will
   refuses the attach with a PRUNE and the leaf picks another.
3. **Which tree.** A change is a cell version named `(origin, incarnation,
   origin sequence)`. Its tree is `(hash(origin) + sequence) mod T`, so an
   origin's consecutive changes rotate through the trees.
4. **Forwarding.** An interior node forwards a change of its own tree to its
   eager neighbours for that root and its leaf children, except the sender,
   in a relay round at most 5 ms after merging it. A leaf never relays: it
   sends its own changes of that tree to its parent, and receives the rest from
   it. A tree with no interior node at all is served straight from the origin.
5. **PRUNE.** A node that receives a second eager copy of a change moves the
   sender to lazy for that root, here and at the sender (PRUNE), and the first
   copy of a change makes its sender eager for that root, as in Plumtree's
   Algorithm 1. A leaf that is pushed to by a node that is not its parent
   detaches from it.
6. **IHAVE and GRAFT.** Each heartbeat a node sends an IHAVE to 2 random peers
   naming each change it made, or that a tree pushed to it, since the last
   one. A receiver that holds neither that version of the cell nor a newer one
   starts a 500 ms timer for the change, and when it runs out GRAFTs the
   announcer, preferring among the peers that announced it one interior in
   the change's tree: it asks for the cell and repairs that tree through the
   announcer, which evidently receives it. In its own tree the node asks to
   become the announcer's backbone neighbour; in a tree it is a leaf of whose
   parent it has not heard from for a missing timeout, an announcer interior
   there becomes its parent, and the old parent is detached. A GRAFT never
   makes the receiver interior in a second tree: for any other tree the
   grafting node is at most a leaf child, and only of an interior node of
   that tree.
7. **Scheduling.** The shared scheduler is unchanged: a node's own changes
   leave on the 500 ms heartbeat or a threshold round (error budget, 5 ms
   floor). A change merged from a peer is forwarded in a relay round.

### Parameters

All are constants in `gabion::defaults` (`THICKET_*`). They are not
`GossipConfig` fields: the legacy scenario suite in `gossip-bench` builds
`GossipConfig` field by field and cannot change here.

| Constant | Value | Meaning |
|---|---|---|
| `THICKET_TREES` | 3 | `T`, matched to three availability zones |
| `THICKET_MISSING_TIMEOUT_MS` | 500 | an announced change may be missing this long before GRAFT |
| `THICKET_LAZY_TARGETS` | 2 | peers sent the IHAVE per heartbeat |
| `THICKET_RELAY_FLOOR_MS` | 5 | spacing of relay rounds |
| `THICKET_NACK_RETRY_MS` | 100 | wait before asking for a lost packet again |
| `THICKET_REPAIR_CELLS` | 256 | cells of the store slice a catching-up node asks for in one request |
| `THICKET_DIGEST_BUCKETS` | 64 | newest settled buckets of a rule a repair digest compares |
| `THICKET_DIGEST_PUSH_CELLS` | 1024 | most cells pushed back in answer to one digest frame |
| `THICKET_LEAF_CHILDREN_MIN` | 8 | leaf children an interior node takes at least before refusing an attach |
| `THICKET_MISSING_CAPACITY` | 4096 | announced changes tracked at once |
| `THICKET_GRAFT_ATTEMPTS` | 3 | GRAFTs sent for one change |
| `THICKET_PROBE_REPEATS` | 3 | heartbeats a PROBE is repeated after the last eager packet |
| `THICKET_CATCH_UP_ATTEMPTS` | 6 | requests a node that joined empty makes for its first slice |

`fanout` has no effect here, and `max_cells_per_tick` now caps the cells in
one frame to one neighbour. `GOSSIP_SEND_QUEUE_CAPACITY` is 512: a round
queues a frame for each tree neighbour, not for `⌈ln n + c⌉` picks.

### Adaptations

Each is the smallest change that makes the papers' protocol work on gabion's
data and UDP.

- **A message is a cell version, named end to end.** The papers' message ids
  need a cluster-wide name. `CellStore::merge_remote` now keeps the origin's
  sequence instead of re-stamping the cell with a local counter
  ([`CRDT.md`](CRDT.md)), so `(origin, incarnation, sequence)` plus the cell's
  rule, key and bucket names one change on every node.
- **Per-root eager sets**, as `riak_core_broadcast` does. A single shared tree
  is unstable with every node sending: wavefronts of different origins meet at
  different edges and prune each other.
- **Class from the address, view from discovery.** The class is a hash of the
  peer's address, so no handshake is needed and membership comes only from
  discovery events. A node's own class comes from `transport.local_addr()`, so
  a node bound to a wildcard address would disagree with its peers: a
  production deployment would carry the node id in the header and hash that.
  The backbone view of random class-mates stands in for HyParView, since
  membership is full.
- **UDP.** Both papers assume reliable links. Each eager packet carries a
  per-edge number (`Header::edge_sequence`); a receiver that sees one skipped
  sends a NACK (control frame, `wire::control`) and the sender resends from
  its last 512 packets. A heartbeat PROBE names the latest number so that the
  loss of a burst's tail shows, and is repeated for 3 heartbeats so that one
  lost probe does not hide it. A lost packet is asked for 3 times, and a GRAFT
  that nothing answered is sent again, up to 3 times.
- **IHAVE names a cell version.** Plumtree announces every message id, so a
  message lost below a later one is still announced. Here a message is a cell
  version, and an entry is that version's origin, incarnation and sequence
  and the cell's rule fingerprint, key hash and bucket. Every node announces
  every change it receives, so these entries were half of all bytes at 64
  bytes each; an IHAVE now groups its entries by origin and rule, sorted by
  sequence, so a change costs its key hash and its bucket and sequence past
  the group's first (22 bytes) plus a share of the group's 50. The
  receiver looks the cell up and starts the missing timer only if it holds
  neither that version nor a newer one. A cell that changed again since it was
  queued is named once, at its newest version, because the older one is
  superseded. A GRAFT names the same entries and is answered with the current
  cell of each. Only changes made here or pushed by a tree are announced, once,
  to 2 peers: cells delivered by a GRAFT or the repair lane are not.
- **Duplicates are exact.** A row is a duplicate only if the node holds
  exactly that version and it arrived by a tree edge or was made here. An
  older version arriving after a newer one that took a faster tree is a first
  copy, and a cell delivered by a pull or the repair lane says nothing about
  the trees. Counting either as a duplicate prunes the edge that delivered it.
- **Placement is sampled, not first-come.** Discovery delivers peers in a
  fixed order, and choosing among the peers known when each arrives made every
  node attach to the first peer of each class: the trees were stars and the
  views the same first few class-mates. A discovered peer instead takes a
  parent or view slot by reservoir sampling: a peer of another class replaces
  that tree's parent with probability `1/k` of the `k` peers of its class
  known, and a class-mate replaces a random view member with probability
  `view/mates`, so parents and views are uniform over the membership known at
  that moment whatever the order. A replaced parent is sent a PRUNE to detach.
  Thicket bounds the fanout of an interior node by its active view; here an
  interior node takes at most twice the even share of known leaves (never fewer
  than `THICKET_LEAF_CHILDREN_MIN`) and refuses the rest, so no node is a hub.
- **A leaf attaches with a GRAFT, and the parent can refuse it with a PRUNE.**
  The papers join a tree by asking a neighbour into the tree's overlay. Here a
  leaf sends an empty-entry GRAFT naming the tree to its chosen parent, and
  repeats it every heartbeat so a lost datagram or a parent that learned of the
  leaf later still attaches it. A GRAFT for the receiver's own tree from a
  non-class-mate is always this attach; the refusal is a PRUNE for that tree
  sent to a leaf whose parent it is. Both reuse the control kinds of the same
  meaning (join, leave) rather than adding a kind to the wire format.
- **Re-parenting on a timeout, only from a silent parent.** Discovery
  removes a crashed pod only after seconds, and a partition removes nothing,
  so a leaf whose parent is gone or cut off would neither receive that tree
  nor get its own changes into it. A missing timer that runs out is the
  paper's repair trigger: the leaf GRAFTs an announcer interior in the
  change's tree, which becomes its parent there. A missing change remembers
  the first peer that announced it, replaced by a later one only when that
  one is interior in the change's tree and the first is not. Unlike the
  paper, whose links are TCP and fail only with their node, a leaf re-parents
  only when nothing has arrived from its parent for a missing timeout: on UDP
  a change goes missing from a live tree too, and moving a leaf moves where
  its own changes enter the tree, whose eager links every root pruned around
  the old entry, so re-parenting on every loss scatters the trees. A leaf that
  re-parents, or whose parent discovery removes, sends its new parent its own
  changes of that tree made since it last heard the old one (less one
  heartbeat, since a change waits for the next round), which the old parent
  may never have passed on.
- **Repair digests (the `digest-repair` component).** An IHAVE is sent
  once, to 2 peers, so a node that missed both the push and the announcement
  (a packet lost past its NACKs and its announcement lost too, a partition, a
  parent that went silent, a joiner that peers did not know yet) would never
  hear of the change: the papers are a broadcast and move no state. The
  backstop is a Scuttlebutt-style digest exchange, adapted to cells. Once per
  bucket (at the first heartbeat after a rule's newest settled bucket moves) a
  node sends one random peer a digest of each rule's settled buckets: per
  origin, the sum of the counts it holds in each bucket (`wire::digest`; 20
  bytes per origin plus 4 per bucket). A bucket is settled once it closed at
  least one bucket ago, and stays so until it is the oldest in the window, so
  changes in flight and the moment each node expires a bucket do not make two
  up-to-date nodes disagree; at most the newest 64 are compared. Each frame
  lists origins in node id order and names the node ids it covers, so an
  origin in its range that it leaves out is one the sender holds none of. The
  peer pushes back, outside the trees, the cells of each listed origin and
  bucket where its sum is higher, and every settled cell of an origin in range
  that is not listed, up to 1,024 cells per frame. Counts only grow, so a
  higher sum means the peer holds something the sender lacks; equal sums of
  different cells, or each side lacking something, are left to the sender's
  next digest to another peer. The digest replaces the blind repair slice,
  which sent a random peer the next 256 cells of the store whatever it held:
  when nodes agree a digest costs a fraction of the slice's bytes, and it
  repairs within a round trip what the slice took tens of seconds to reach. A rule
  interned without a window (the fallback for a fingerprint no config names)
  has no settled buckets, so only the trees and IHAVE carry it.
- **Catching up.** The papers do not move state to a node that joins, because
  a broadcast has none. A node with an empty store that discovery adds a
  peer to asks a random peer for the next slice of its store (`Pull` control
  frame) and, as long as a slice teaches it something, for the next from the
  same peer, until one does not. A request
  that nothing answers within a heartbeat, for example because the peer has
  not yet heard of the newcomer from discovery, goes to another peer, up to 6
  times. Slices are paced by the round trip, so a joiner is not sent the
  whole store at once.
- **PRUNE on every duplicate.** As Plumtree does, with no spacing between
  PRUNEs to one sender.
- **Expired cells are not merged.** A tree forwards whatever it newly merges,
  so a cell taken back in after its window passed would circulate. Rows of a
  rule whose bucket is already out of the window are dropped before merging.

### Left as it was

The CRDT's peer frontier, `fill_gossip_frame*` and the repair cursor stay in
the library and its tests; this runtime no longer calls the frontier or the
frame fillers, and uses the repair cursor only to answer a catching-up node (`CellStoreConfig::peer_capacity` is part of the harness
contract and now sizes the peer table). `PeerEntry::peer_slot` is gone.

Tests that asserted the baseline's behaviour were removed: per-peer frame
pruning, peer sampling without replacement, coverage fanout, uniform sampling
and the peer-slot pairing invariant. The convergence, partition-heal,
loss, authentication and expiration properties still pass.

## Operator knobs

Most deployments leave the defaults alone, because they are chosen to
keep convergence well under a second at typical cluster sizes
(≤ 256). The table below is for the days when you do need to tune, and
it splits into the two adaptive halves of the protocol.

### Heartbeat cadence and fanout (coverage fanout)

| Knob                 | Default     | What it controls                                                                                                                       | When to tune                                                                                                  |
|----------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `tick_interval`      | 500 ms      | Heartbeat cadence — period between proactive gossip ticks.                                                                             | Bigger clusters tolerate longer intervals; lower it only if you need sub-100 ms convergence on cold rules.    |
| `fanout`             | 3           | No effect on this branch: changes travel down the trees ([design notes](#design-notes-thicket-dissemination)). The backbone view is sized `⌈ln(m)+c⌉` from `GOSSIP_COVERAGE_MARGIN`. | Leave alone. |
| `max_cells_per_tick` | 4 096       | Cap on cells in one frame to one tree neighbour. Cells over the cap are not sent to that neighbour, which finds the gap through the lazy IHAVE. | Raise if you run many rules and the dirty ring backlogs visibly under burst.                                  |
| `max_payload_bytes`  | 1 400       | UDP datagram budget. The codec splits a tick's frame across multiple packets when the cell list overflows.                             | Lower if your network path has a tighter MTU; never raise past the IPv4 safe floor of 1400.                   |

### Per-rule error budget (adaptive emit rate)

| Knob                 | Default     | What it controls                                                                                                                       | When to tune                                                                                                  |
|----------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `target_err_bps`     | 100 (= 1 %) | Per-rule error budget in basis points of the rule's limit. Tighter = more threshold fires, looser = more local lag.                    | Lower if a hot rule needs tighter convergence than 1 %; higher if bandwidth is the bottleneck.                |
| `min_emit_interval`  | 5 ms        | Floor between two threshold-fire emissions. Caps worst-case emit rate when ε saturates under adversarial load.                         | Raise if a bad client is pinning the gossip socket; lower to chase microsecond convergence on tiny clusters.  |

The `gabion::defaults` module is the single source of truth for these
constants. Both adapters read from it, so if you change a default the
two adapters move together. Per-adapter configuration translates each
knob to its surface name — see [`crates/server/README.md`](../server/README.md)
for the YAML keys and [`crates/nginx/README.md`](../nginx/README.md) for
the directives.

## What we measured

Every figure below is regenerated from current code by running the
gossip simulator across the suites; see
[`../gossip-bench/README.md`](../gossip-bench/README.md) for the
"how to re-run it" perspective. The figures here are committed to
`figures/` so this README renders correctly on GitHub without first
running the bench.

The suites split into four bundles. Steady-state convergence and scale
together show that the protocol meets its asymptotic bounds, while the
resilience suite shows that it survives the failure modes operators
see in practice. The adaptive machinery suites — `coverage_fanout`,
`error_budget`, `min_emit_clamp`, and `heartbeat_threshold_mix` — are
the empirical evidence for the two adaptive aspects of the protocol.

### Steady-state convergence

![convergence](figures/convergence.svg)

**Method.** Single write at node 0; measure rounds-to-converge as both
cluster size N and the static fanout floor sweep. Bandwidth is total
bytes sent across the run divided by `N · duration`.

**Takeaway.** Gabion sits at or below the Karp `log₂ N` lower bound at
every fanout above 1, because peer-frontier dedup lets the sender skip
cells the recipient already has. The empirical curve at f = 3 — well
under the f = 6 production default — is within one extra round of f = 8.

![fanout_sweep](figures/fanout_sweep.svg)

**Method.** Fix N = 32, sweep static `fanout` from 1 to 12, measure both
convergence rounds and bytes-per-node-per-second.

**Takeaway.** Rounds drop sharply between f = 1 and f = 3 and then
flatten, while bandwidth scales roughly linearly with `fanout`. The
sweet spot near f = 3–6 is where bandwidth stays modest while
convergence sits within a round of the asymptote.

### Scale

![scale_n](figures/scale_n.svg)

**Method.** Sweep N from 4 to 1024 at fixed `fanout = 3`. Time-to-
converge should track `log₂ N`; bytes-per-node-per-second should stay
roughly constant (SWIM's "constant load per node" property).

**Takeaway.** Rounds-to-converge sits at or below `log₂ N` all the way
through N = 1024, and per-node bandwidth is flat in N to within the
noise of a horizontal line.

### Resilience

![loss](figures/loss.svg)

**Method.** Three trials at each per-link drop probability from 0 % to
50 %. N = 16, f = 3, 20 s of virtual time.

**Takeaway.** Convergence rounds climb only a small constant as loss
rises, and gabion converges in every trial including the 50 % loss row.
Bimodal Multicast (Birman et al. 1999) reports stable delivery to
~25–30 %; gabion's empirical tolerance is roughly twice that, thanks
to peer-frontier dedup plus the rotating repair lane.

![partition](figures/partition.svg)

**Method.** Two equally-sized partitions, single write inside one,
heal at t = 10 s. Time-to-reconverge measured from the heal.

**Takeaway.** Once the links open, the next tick after heal drains the
partition, so reconvergence is bounded above by one `tick_interval`.

![staleness](figures/staleness.svg)

**Method.** Sustained writes from k sources concurrently; measure
per-hit p50 / p95 lag (Astrolabe-style staleness).

**Takeaway.** P50 and p95 lag both stay flat at a single `tick_interval`
regardless of the number of concurrent writers, because the dirty ring
drains in one round under steady state.

### Adaptive machinery

These four suites are the empirical evidence for the two adaptive
aspects of the protocol. `coverage_fanout` exercises coverage fanout;
`error_budget` and `min_emit_clamp` exercise adaptive emit rate; and
`heartbeat_threshold_mix` confirms the cold-rule heartbeat is
independent of the threshold path.

![coverage_fanout](figures/coverage_fanout.svg)

**Method.** Sweep two axes against a `DistinctKeyBurst` (each write
lands in its own cell store slot, so `local_dirty.len()` jumps to the
burst size in one step). Across cluster size `n ∈ {16, 64, 256}` at a
fixed burst, read the per-tick `peak_effective_fanout`. Across burst
size `cells ∈ {16, 256, 1024}` at a fixed `n`, read the effective fanout
(packets ÷ dirty-tick count). Static `fanout = 1` (the floor, which the
coverage threshold overrides).

**Takeaway.** The left panel: the observed peak fanout tracks
`⌈ln(n−1)+c⌉` — 6 at `n=16`, 8 at `n=64`, 9 at `n=256` — sitting right
on the predicted threshold and rising by ~1 per factor-of-e of cluster
growth, exactly the KMG law. The right panel: at fixed `n` the per-tick
fanout is **flat** across a 64× change in burst volume, because the
burst rides one fat frame rather than widening the peer pick — volume is
a frame-fill concern, not a fanout one. (Volume that overflows the dirty
ring is repaired by the rotating repair lane, not by fanning out wider.)

![error_budget](figures/error_budget.svg)

**Method.** Sustained workload at N = 16, `rule_limit = 10 000`,
`per_tick = 5` hits / source / sample. Sweep `target_err_bps` across
nearly three decades. Each scenario reports bandwidth,
threshold-fires/node, and the empirical
`max_lag = max(ground_truth − min(per_node_total))`, alongside the
theoretical `N · ε_R` bound.

**Takeaway.** Two regimes show up cleanly. While ε_R is small enough
that per-sample accumulated hits cross the budget (left of bps ≈ 100
here), the runtime fires nearly every sample and bandwidth climbs to
~130 kB/node/s. Past that crossover, ε_R is comfortably above the
per-sample hit count and the proactive heartbeat carries the work
alone — bandwidth drops to ~73 kB/node/s and threshold fires go to
zero. Empirical max lag plateaus at ~65 hits across the entire sweep
because the heartbeat alone keeps lag bounded by one `tick_interval`
× per-sample hit count — the budget only matters when it would force
gossip *faster* than that. Empirical max lag may briefly exceed
`N · ε_R` at the tight end of the sweep — those are hits in flight
between the bench's sample point and the next gossip apply, bounded by
one workload batch and absorbed by the following sample.

![min_emit_clamp](figures/min_emit_clamp.svg)

**Method.** Adversarial workload: drive 10 000 hits into 50 ms of
virtual time. The `BurstCompressed` workload calls `record(...)` with
`rule_limit = 1`, which forces `ε_R = max(1, 1 · bps / 80_000) = 1` at
every bps — so every hit crosses the budget. Sweep
`min_emit_interval ∈ {0, 1, 5, 10, 50}` ms.

**Takeaway.** Bandwidth scales inversely with the floor, which is what
caps worst-case emit rate. Threshold-fires-per-node drops by ~50×
across the sweep (6.1 at floor = 0 / 1 ms down to 0.1 at floor = 50 ms),
and final divergence stays at zero in every configuration — the cluster
still converges after the burst, just at a different bandwidth budget.

![heartbeat_threshold_mix](figures/heartbeat_threshold_mix.svg)

**Method.** Two rules side by side. The hot rule receives a saturating
burst every tick (threshold path); the cold rule receives one hit per
second (heartbeat path). Both must converge.

**Takeaway.** The left panel shows the hot rule converging
sub-heartbeat through threshold fires, while the right panel shows the
cold rule riding the proactive heartbeat. Cold-rule replication does
not depend on the threshold ever firing, because heartbeat and
threshold are independent code paths.

## References

In-tree primary sources:

- `src/gossip/runtime.rs` — the event loop. `GossipRuntime::run` is the
  outer `select!`; `handle_gossip_tick` carries the coverage-fanout
  pick; `handle_limit_request` carries the per-site safe zone and the
  threshold-fire flag.
- `src/wire.rs` and `src/wire/` — packet shape, encoders, HMAC
  authentication.
- `src/crdt.rs` and `src/crdt/` — counter store, dirty rings, peer
  frontier, expiry. The structures here are documented in depth in
  [`CRDT.md`](CRDT.md).
- [`../gossip-bench/REFERENCES.md`](../gossip-bench/REFERENCES.md) — the
  literature survey behind the benchmark methodology (Demers 1987,
  Karp 2000, SWIM 2002, HyParView / Plumtree 2007, Astrolabe 2003,
  Bimodal Multicast 1999, plus Sharfman/Schuster/Keren and
  Olston/Jiang/Widom for the error-budget machinery).
