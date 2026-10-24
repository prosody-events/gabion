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
  - [Owner aggregation](#owner-aggregation)
  - [Design notes](#design-notes)
- [Operator knobs](#operator-knobs)
- [What we measured](#what-we-measured)
- [References](#references)

## Module map

The library is divided into six modules, and every type that crosses an
adapter boundary lives in exactly one of them. The dependency graph is
a DAG, so no module imports from a module that imports it.

| Module       | Path                          | What it owns                                                                                                                              |
|--------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `crdt`       | `src/crdt/*`                  | The per-origin counter store. Robin Hood hash index, structure-of-arrays columns. Allocates only at construction. See [`CRDT.md`](CRDT.md). |
| `rules`      | `src/rules/*`                 | `Rule`, `RuleTable`, `Descriptor`, fingerprint hashing. Two nodes with the same rules produce identical `rule_fingerprint` values, which is what makes the CRDT addressable across the cluster. |
| `gossip`     | `src/gossip/*`                | `GossipRuntime`, `GossipClient`, `GossipTransport`, `Clock`, the `sim::SimTransport` used in tests, and the `AdminCommand` / `AdminSnapshot` observability surface. |
| `wire`       | `src/wire/*`                  | The on-the-wire codec. One UDP packet = header + rule dictionary + node dictionary + cell rows + optional HMAC tag. Each packet decodes independently. |
| `discovery`  | `src/discovery/*`             | `PeerDiscovery` trait and the Kubernetes EndpointSlice implementation. Adapters consume `PeerEvent` streams; the gossip runtime treats peer add/remove as a normal `select!` arm. |
| `defaults`   | `src/defaults.rs`             | Constants shared by both adapters: sync and broadcast cadence, payload caps, and the broadcast batch limit. |

`gossip` sits at the top of that graph and depends on every other
module, whereas `wire` and `crdt` are independent of each other.
`defaults` is a leaf: `rules` and `gossip` import it, and it imports
nothing else in this crate. Adapters consume the public surface of
`gossip`, `rules`, and `discovery`, together with the constants in
`defaults`.

## How gossip works

Gossip is the protocol that turns N independent counter stores into one
*eventually consistent* counter store. Every node admits requests
against its local view of the cluster aggregate, and gossip is what
keeps that view fresh. From the adapter's side it shows up as a single
call: admission writes a row through `GossipClient::record`, and the
runtime turns those rows into outbound frames. The design is key-owner
aggregation, described under [Owner aggregation](#owner-aggregation);
the frame it speaks is below.

### The wire frame

Every gossip frame is one self-describing UDP packet, with no session
state on either side: each frame decodes independently, even if its
predecessor was dropped. A lost sync or acknowledgement is re-sent, and
a lost broadcast is repaired on request (see
[Owner aggregation](#owner-aggregation)).

The header is fixed at 72 bytes. Every multi-byte field is encoded
little-endian; the first four bytes on the wire are `0x32 0x47 0x42
0x47` so a packet capture reads `2GBG`. (The numeric `MAGIC` literal
`0x4742_4732` spells "GBG2" when written most-significant byte first;
the LE encoding then writes those bytes in reverse.)

```
byte  field                  width
----  ---------------------  -----
0     MAGIC                  4   (0x4742_4732)
4     VERSION                2   (= 1)
6     flags                  2   (FLAG_MORE | FLAG_AUTHENTICATED, and at most
                                  one kind: FLAG_SYNC | FLAG_ACK | FLAG_NACK |
                                  FLAG_REPAIR | FLAG_SNAPSHOT)
8     cluster_id_hash       16
24    sender_node_id        16
40    sender_incarnation     4
44    count_width            4   (2, 4, or 8 — width of the count column;
                                  matches u16/u32/u64 `impl Count`)
48    cell_count             4
52    body_len               4
56    min_origin_sequence    8
64    max_origin_sequence    8
----                       ----
                            72
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

`FLAG_SYNC` and `FLAG_ACK` say what the cells are for; a frame sets at
most one kind bit. A frame with none is an owner's **broadcast**, whose
body carries per-bucket key totals instead of cells, as do a **repair**
(`FLAG_REPAIR`) and a **snapshot** (`FLAG_SNAPSHOT`). A **NACK**
(`FLAG_NACK`) has no body. These four use the sequence fields: a broadcast
packet's `max_origin_sequence` is its number in the owner's sequence (1,
2, 3, ...), a repair's is the number of the broadcast it re-sends, a
snapshot's is the last broadcast it covers (with the packet's index in the
snapshot in `min_origin_sequence`), and a NACK carries the highest number
its sender has seen in `max_origin_sequence` and a bitmap of the ones it
missed below it in `min_origin_sequence` (zero asks for a snapshot). The sender is their origin, so a
total row drops the node dictionary and the origin columns:

```
[rule_dict_len    : u8     ]    R rules in this frame (1–255)
[rule_dict        : u128×R ]    rule fingerprints
[rule_slot        : u8 ×N  ]    each < R
[key_hash         : u128×N ]
[bucket           : u32 ×N ]
[count            : C  ×N  ]    the owner's total for (rule, key, bucket)
```

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

### Owner aggregation

Every `(rule, key)` has one **owner**, the member with the highest
rendezvous (highest-random-weight) hash score over the discovered
membership. The score hashes the member's address with the rule
fingerprint and the key hash, so ownership is a pure function of the key
and the member set: nodes that have discovered the same members agree on
every owner without a message, and a join or leave moves only the keys
the changed member wins or held.

1. **Record.** `GossipClient::record` folds the hit into the node's own
   cell for `(rule, key, bucket)`, so the node's aggregate counts it at
   once. If another member owns the key, the cell is queued for sync.
2. **Sync.** Every `sync_wait` (100 ms) the node sends each unacknowledged
   queued cell to its owner, one batch per owner (`FLAG_SYNC`).
3. **Fold and acknowledge.** The owner max-merges the cells into its store
   (they stay tagged with the contributor's origin, so a re-sent or
   reordered sync cannot double count), adds whatever they rose by to the
   bucket's count of the others' hits, queues the keys that rose for
   broadcast, and echoes every cell it holds back to the sender
   (`FLAG_ACK`). A node that believes it is not the owner folds and
   acknowledges nothing.
4. **Broadcast.** Every `broadcast_interval` (100 ms) an owner sends the
   total of every live bucket of each key that changed since its last
   broadcast (its own hits plus the others'), at most
   `broadcast_batch_limit` (1 000) keys, to every peer. A receiver counts
   the total less its own hits as the others' count of that bucket,
   unless it already holds more, and relays nothing.
5. **Re-send.** A cell stays queued until its owner acknowledges the
   current count, so a lost sync or ack is re-sent every `sync_wait`. When
   membership changes, every live local cell is re-checked against the new
   owner, and because acknowledgements are per owner, every live local
   cell of a key whose owner moved is re-sent until the new owner
   acknowledges it, acknowledged by the old owner or not. The new owner
   rebuilds the key from these and broadcasts it. During a rolling
   restart this re-sends each moved key's live cells once per move.
6. **Repair.** An owner numbers its broadcast packets and remembers the
   key buckets of the last `GOSSIP_BROADCAST_HISTORY` (256). A receiver
   tracks, per owner, the highest number it has seen and which of the 64
   below it are missing. Every `sync_wait` it sends each owner it missed
   broadcasts from a NACK naming them, and the owner re-sends those
   buckets' current totals (`FLAG_REPAIR`), until none is missing. A
   receiver that has not yet heard from an owner since discovering it
   (every peer, for a node that just started), that fell more than 64
   broadcasts behind, or that asks for a broadcast the owner no longer
   remembers, gets a snapshot instead: the total of every bucket of every
   key the owner owns, as of its last broadcast, in up to 128 packets. A
   node asks at most `GOSSIP_SNAPSHOT_REQUESTS_PER_SYNC` (32) owners for a
   snapshot per sync, so the replies fit its receive buffer.

A node's view of a bucket is therefore its own hits plus one number for
everyone else's, which only rises and never exceeds the truth: an owner's
total cannot include more of the receiver's hits than the receiver has, and
an owner's sum of contributors' cells cannot exceed their hits. A node that
held a larger count from a previous owner keeps it until the new owner's
count passes it, so an ownership move never double counts and never drops
the view.

A node never forwards another node's hits, only an owner sends a key's
totals (broadcast, repair or snapshot), and nothing is sent to a
non-owner. A receiver learns that it missed an owner's broadcast from the
owner's next one, so a broadcast lost just before its owner falls silent
stays missing until the owner broadcasts again; the next change to the
key, which carries every live bucket of it, also repairs it.

### Design notes

This implementation follows Gubernator's `GLOBAL` behaviour (Mailgun,
[architecture](https://raw.githubusercontent.com/mailgun/gubernator/v2.4.0/docs/architecture.md),
[blog](https://www.mailgun.com/blog/gubernator-cloud-native-distributed-rate-limiting-microservices)),
adapted as little as the CRDT and UDP require:

- **Totals per bucket.** Gubernator's owner holds one total per key and
  broadcasts the key's current status. Here the owner holds the
  contributors' origin-tagged cells (so a re-sent sync cannot double
  count) and broadcasts the key's status as one total per live bucket,
  found through a per-key chain in the cell store; a bucketed window has
  one number per bucket where Gubernator's single window has one. A
  receiver holds one others count per bucket, raised to the total less its
  own hits, rather than a replica of every contributor's cell. An earlier
  version of this design broadcast every contributor's cell of a changed
  key, which cost about 5.6 times `main`'s bytes in the `long-window`
  regime (60 one-minute buckets, up to 60 contributors each); totals
  carry at most one row per bucket, at 25 bytes instead of 46 plus a
  16-byte origin. The batch limit counts keys, as Gubernator's does.
- **Cadence.** Gubernator batches hits over a sub-millisecond window to
  coalesce requests, which is not a staleness bound. `sync_wait` and
  `broadcast_interval` are both 100 ms, chosen from the sub-second
  staleness target.
- **UDP.** Gubernator sends hits over gRPC and gets delivery from TCP.
  Here the owner acknowledges each folded cell and the contributor
  re-sends until it does, which is the minimal substitute for the retry
  TCP gave the hit path.
- **Broadcast repair (adapted `edge-nack` component).** Gubernator's
  broadcasts also ride gRPC, so TCP retransmits a lost segment; only a
  failed RPC is dropped, and the key's next update carries its status. Over
  UDP a broadcast reaches each of N - 1 peers independently, so at 20 %
  loss almost none reaches all 182 peers of the `loss-20` regime, and a key
  that changes every few seconds left over a third of its hits short at
  some pod until they aged out. Each owner-to-peer broadcast is therefore
  numbered, and receivers NACK the gaps (the per-edge sequence number,
  NACK and selective resend of the `edge-nack` component), which is the
  substitute for TCP's retransmission on the broadcast path, at a cost
  proportional to the loss. Repairs carry the buckets' current totals, so
  a repair is never stale, and only the owner sends them.
- **Snapshots (adapted `pull-sweep-join` and `full-state-fallback`).**
  Gubernator has no catch-up: a node that joins learns a key at its next
  update. A node that starts, or that falls further behind than repair can
  track (a partition of a zone for 30 s does that), asks each owner for the
  totals of the keys it owns. It asks owners rather than `k` random peers
  because only an owner may send a key's total.
- **Batch limit.** Gubernator flushes a batch as soon as it holds
  `GlobalBatchLimit` keys and arms its timers on the first queued item.
  Here the sync and broadcast tickers are free-running and
  `broadcast_batch_limit` caps the keys of one broadcast, leaving the rest
  for the next tick; a sync is capped at `max_cells_per_tick` cells. Neither
  cap is reached at the loads in the benchmark regimes.
- **Queue overflow.** If the sync queue is full when a hit arrives, the
  cell is left to a sweep of every local cell at the next sync tick, so
  the hit is still re-sent until acknowledged once the queue has drained.
  The broadcast queue has no such sweep: an overflow there drops the
  change, and the key's next change repairs it.
- **Addresses.** Owners are computed from peers' discovered addresses
  (the one identity discovery gives a node for every other), so the
  transport's local address must be the address peers discover the node
  by.
- **Shared tunables.** Keys and defaults are `GOSSIP_SYNC_WAIT_MS`,
  `GOSSIP_BROADCAST_INTERVAL_MS` and `GOSSIP_BROADCAST_BATCH_LIMIT` in
  `gabion::defaults`; the runtime config fields are `sync_wait`,
  `broadcast_interval` and `broadcast_batch_limit`. Repair is tuned by
  `GOSSIP_BROADCAST_HISTORY` and `GOSSIP_SNAPSHOT_REQUESTS_PER_SYNC`, which
  are not runtime config. The local and
  forwarded dirty-ring capacities size the sync and broadcast queues, and
  `peer_capacity` sizes the membership table.
- **Unused knobs.** `fanout`, `rng_seed`, `target_err_bps` and
  `min_emit_interval` are accepted and ignored (there is no random
  fanout, no random numbers and no threshold-triggered emit);
  `tick_interval` is the period of the expiry sweep.
  `GossipClient::record`'s `rule_limit` is ignored. The admin snapshot's
  `threshold_fires` and `last_error_budget` are always zero;
  `ticks_total` counts sync and broadcast ticks, and the effective
  fanout is the size of the last (peak) broadcast audience.
- **Removed.** The dirty rings, the peer frontier, the repair lane and
  the per-frame peer pruning of the previous design are gone from the
  CRDT store; nothing in this design reads them.

## Operator knobs

Most deployments leave the defaults alone. The table is for the days
when you do need to tune.

| Knob                    | Default | What it controls                                                                                                   | When to tune                                                                                     |
|-------------------------|---------|--------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| `sync_wait`             | 100 ms  | How often a node sends its unacknowledged cells to their owners, and the retry interval for a lost sync or ack.    | Lower for fresher counts at more small packets; raise if packet rate is the bottleneck.          |
| `broadcast_interval`    | 100 ms  | How often an owner broadcasts the totals of its changed keys to every peer.                                         | Each broadcast costs one packet per peer per interval, so cost grows with cluster size.          |
| `broadcast_batch_limit` | 1 000   | Most keys in one owner broadcast; the rest wait for the next interval.                                             | Raise if owners' queues back up under bursts (`forwarded_dirty_len` in the admin snapshot).      |
| `tick_interval`         | 500 ms  | Period of the sweep that expires cells whose bucket left the window.                                               | Rarely.                                                                                          |
| `max_cells_per_tick`    | 4 096   | Most cells one sync sends; the rest wait for the next `sync_wait`.                                                 | Raise if many keys are active per node.                                                          |
| `max_payload_bytes`     | 1 400   | UDP datagram budget. The codec splits a batch across multiple packets when the cell list overflows.                | Lower if your network path has a tighter MTU; never raise past the IPv4 safe floor of 1400.      |

The `gabion::defaults` module is the single source of truth for these
constants. Both adapters read from it, so if you change a default the
two adapters move together. Per-adapter configuration translates each
knob to its surface name — see [`crates/server/README.md`](../server/README.md)
for the YAML keys and [`crates/nginx/README.md`](../nginx/README.md) for
the directives.

## What we measured

The figures in this section were measured on the previous, randomized
anti-entropy design; they do not describe owner aggregation. The study
harness in [`../gossip-bench/README.md`](../gossip-bench/README.md)
measures this design.

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
  outer `select!`; `handle_sync_tick` and `handle_broadcast_tick` carry
  the owner protocol. `membership.rs` holds the rendezvous owner,
  `outbox.rs` the sync and broadcast queues.
- `src/wire.rs` and `src/wire/` — packet shape, encoders, HMAC
  authentication.
- `src/crdt.rs` and `src/crdt/` — counter store and expiry. The structures here are documented in depth in
  [`CRDT.md`](CRDT.md).
- [`../gossip-bench/REFERENCES.md`](../gossip-bench/REFERENCES.md) — the
  literature survey behind the benchmark methodology (Demers 1987,
  Karp 2000, SWIM 2002, HyParView / Plumtree 2007, Astrolabe 2003,
  Bimodal Multicast 1999, plus Sharfman/Schuster/Keren and
  Olston/Jiang/Widom for the error-budget machinery).
