# Gossip & Anti-Entropy Literature: Reference Metrics for Gabion

Reference notes used to ground the evaluation framework for the gabion gossip layer. The gabion protocol under test is:

- Anti-entropy with **per-origin counter CRDTs** (counters merge by max; messages dedup by `(origin, seq)`).
- **Push gossip with peer-frontier dedup**: every peer remembers the highest sequence number per origin it has acked, so the sender prunes already-known cells before transmission.
- **Fanout-based peer sampling** drawn from the Kubernetes EndpointSlice — gabion does **not** implement membership or failure detection itself.
- A **repair lane** that rotates linearly over the entire active peer set so anti-entropy converges even when per-peer dirty rings overflow.

Each entry below captures (1) what the paper measures, (2) headline numerical results, (3) which metrics map onto gabion's design, and (4) methodology worth replicating in `gossip-bench`. The synthesis table at the end is the operational deliverable: the right-hand column is what gabion-bench should report.

---

## Foundational

### Demers et al. 1987, "Epidemic Algorithms for Replicated Database Maintenance" (PODC)

**What it measures.** Demers introduces three canonical metrics that every subsequent gossip paper inherits:

- **Residue**: fraction of nodes still unaware of an update once the protocol "stops" being interested in it. For deterministic anti-entropy this is 0; for rumor-mongering it is the tail of the SIR curve.
- **Traffic** (denoted *m*): average number of update messages a node sends/receives, expressed as a multiplier of the update payload — directly the byte-overhead-per-update.
- **Delay**: distribution of times to first delivery, split into **t_avg** (mean delivery time across nodes) and **t_last** (time to reach the last node).

**Headline numbers.** Demers shows that anti-entropy (pull or push-pull) converges in O(log N) rounds with high probability for every node, and quantifies the difference between push, pull, and push-pull strategies: pull dominates after roughly half the population is infected (the proportion of "ignorant" sites shrinks quadratically per round under pull versus linearly under push). Rumor-mongering with a "counter-k feedback" stopping rule trades residue for traffic — increasing k drops residue exponentially while traffic grows linearly. The Xerox Clearinghouse trace evaluation shows spatial gossip (preferring nearby peers) reduces link load on backbone links by an order of magnitude versus uniform random selection.

**Maps onto gabion.** Gabion is *anti-entropy*, not rumor-mongering — it never stops trying to deliver an update, because the repair lane keeps cycling. Residue should therefore be ~0 at steady state; traffic and delay are the relevant metrics. The push-pull intuition is partially relevant: gabion's peer-frontier dedup makes its push behave like pull-from-the-receiver's-perspective, since the sender already knows what the receiver has and only ships the delta.

**Methodology to replicate.** Report **t_avg** and **t_last** per origin, not just an aggregate. Demers' Xerox trace approach — running over a real network topology with measured link RTTs — is the right model for evaluating gabion under a realistic k8s EndpointSlice rather than a uniform-random graph. Track traffic in bytes/update-cell, not in number of messages, since gabion frames pack many cells.

Sources: [Demers 1987 (UPenn copy)](https://www.cis.upenn.edu/~bcpierce/courses/dd/papers/demers-epidemic.pdf), [ACM SIGOPS reprint](https://dl.acm.org/doi/10.1145/43921.43922).

---

### Karp, Schindelhauer, Shenker, Vöcking 2000, "Randomized Rumor Spreading" (FOCS)

**What it measures.** Time complexity (rounds) and message complexity (total transmissions) for spreading one rumor on the complete graph K_n under push, pull, and push-pull strategies, with an explicit lower bound for address-oblivious algorithms.

**Headline numbers.** The push-pull random-phone-call protocol delivers a rumor to all n nodes in **Θ(log n)** rounds with high probability, and uses **Θ(n log log n)** messages total — asymptotically tight. The protocol has two phases: an exponential push phase that informs ~n/2 nodes, then a pull phase where the uninformed population shrinks quadratically per round. The address-oblivious lower bound is Ω(n log log n) messages per rumor.

**Maps onto gabion.** Gabion runs continuously over many origin counters, not a single rumor, but the per-origin convergence shape is the same. The n log log n bound says: if every cell gets to all peers via gossip and you can't open back-channel addressing, you cannot do better than ~log log N redundancy. Gabion's peer-frontier dedup *is* a form of addressed knowledge — the sender knows what each receiver already has — so in principle it can beat the address-oblivious bound, which is worth quantifying empirically.

**Methodology to replicate.** Karp's analysis is asymptotic, but the "phases" framing maps directly to a per-origin trace: plot the fraction of peers that have acked a given `(origin, seq)` over rounds and look for the exponential-then-quadratic shape. Also: Karp's lower bound makes a useful baseline — "how close does gabion's per-update message multiplier come to log log N at network sizes 100, 1k, 10k peers?"

Sources: [Karp 2000 (Yale)](https://zoo.cs.yale.edu/classes/cs426/2013/bib/karp00randomized.pdf), [FOCS proceedings](https://dl.acm.org/doi/10.5555/795666.796561).

---

## Membership / failure detection (comparison set; gabion does **not** implement these)

### Das, Gupta, Motivala 2002, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" (DSN)

**What it measures.** Three numbers that have become the standard for cluster membership: **time to first detection** of a failure, **rate of false positives**, and **message load per member per protocol period**. SWIM's pitch is that all three are independent of group size N.

**Headline numbers.** With protocol period T' and k indirect-ping subgroup members, expected time to first detection is approximately T'/(1 - e^(-qf)) where qf is the fraction of live members successfully completing the ping; with typical parameters (T' ≈ 2s, k = 3) detection takes ~1.6 protocol periods, i.e., ~3 seconds. False-positive rate at 95% message delivery and k = 3 stays near 0.1%. Per-member outgoing bandwidth is constant: one direct ping, one ack, and (worst case) k indirect pings per period — independent of group size. Membership change dissemination, piggybacked on pings, reaches all members in O(log N) rounds.

**Maps onto gabion.** Nothing directly — Kubernetes EndpointSlice owns membership, and any time-to-detect-failure metric belongs to k8s, not gabion. What *is* portable is SWIM's framing: **constant per-node message load as N grows** and **bounded worst-case latency for one piece of information to reach everyone**. Gabion's analogue of "detection time" is **worst-case staleness**: how long after an update is observed at the origin does the last peer hold a stale value? The repair lane is gabion's bound on this.

**Methodology to replicate.** SWIM's experimental scale was 16–56 nodes on real hardware, plus simulation to larger sizes. The headline plot is "per-process outgoing bandwidth versus N" — flat for SWIM, growing for heartbeat. Gabion should produce the same plot: per-pod outgoing bytes/sec versus N, holding fanout fixed. Also replicate SWIM's false-positive-rate-vs-network-loss-rate sweep, except the gabion-relevant axis is **repair-lane lag versus dirty-ring drop rate**.

Sources: [SWIM (Cornell)](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf), [DSN 2002 entry](https://dl.acm.org/doi/10.5555/647883.738420), [Wikipedia summary](https://en.wikipedia.org/wiki/SWIM_Protocol).

---

### Leitão, Pereira, Rodrigues 2007, "HyParView" (DSN)

**What it measures.** Reliability of gossip broadcast (fraction of nodes that receive a given message) under **massive simultaneous failure**, with two view-size knobs: a small **active view** of size log(N) + c, and a much larger **passive view** of size k·(log(N) + c) holding backup links.

**Headline numbers.** Experiments at N = 10,000 nodes, sweeping simultaneous failure rates from 10% to 95%. HyParView preserves ~100% delivery reliability under failure rates up to **80%**, and maintains ~90% delivery even at **95% simultaneous failures**, while comparable protocols (Scamp, Cyclon) collapse to <50% reliability at the same failure rates. Fanout is small — around log(N), i.e., ~10 for N = 10k — which keeps per-node bandwidth low.

**Maps onto gabion.** Gabion does not implement HyParView, but EndpointSlice plays the role of the "passive view": when an active peer disappears, the next EndpointSlice update supplies the replacement. The relevant question for gabion is the **resilience under simultaneous pod loss** — when 50% / 80% / 95% of pods are evicted at once, how fast does the system reconverge once new pods register? The fanout = log(N) sizing is also directly relevant for the gabion fanout knob.

**Methodology to replicate.** The massive-failure injection sweep is the headline experiment. Gabion-bench should kill 10%, 50%, 80% of pods simultaneously and measure (a) the fraction of in-flight updates that still converge, and (b) the time-to-reconvergence after the EndpointSlice settles.

Sources: [HyParView DSN 2007](https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf).

---

### Leitão, Pereira, Rodrigues 2007, "Epidemic Broadcast Trees" / Plumtree (SRDS)

**What it measures.** Two metrics that directly motivate gabion's peer-frontier dedup:

- **RMR — Relative Message Redundancy**: (total payload messages − N + 1) / (N − 1). For a perfect spanning tree RMR = 0; for flat gossip with fanout f, RMR ≈ f − 1.
- **LDH — Last Delivery Hop**: hop count at which the last node receives a given broadcast. Equivalent to t_last in hops.

Plumtree replaces redundant eager-push edges with cheap **IHAVE / GRAFT / PRUNE** lazy-push announcements: a peer announces it has a message; if the receiver hasn't seen it, it GRAFTs the edge into the tree; if it has, it PRUNEs.

**Headline numbers.** N = 10,000 simulated nodes. Plumtree drives RMR essentially to zero in steady state (versus RMR ≈ fanout − 1 for flat gossip), while keeping LDH within ~1 hop of flat gossip. Under massive node failure (up to 80%), Plumtree heals the tree using IHAVE timeouts and reliability stays near 100%.

**Maps onto gabion.** This is the closest analogue to gabion's design. Gabion's **peer-frontier dedup** is structurally the same idea as Plumtree's IHAVE/PRUNE: the sender already knows the receiver's frontier and prunes cells the receiver has acked. The "tree" in gabion is implicit and per-origin — the set of senders that successfully delivered each cell. Therefore **RMR is the single most important metric for gabion**: bytes shipped divided by bytes of novel CRDT delta. Target should be RMR ≪ fanout.

**Methodology to replicate.** Report RMR and LDH as the headline pair. Sweep the massive-failure axis the same way Plumtree does — 10%, 50%, 80%, 95% — and verify that the **repair lane** plays the role of Plumtree's tree-healing layer.

Sources: [Plumtree SRDS 2007](https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf).

---

## Aggregation / large-scale state

### Van Renesse, Birman, Vogels 2003, "Astrolabe" (TOCS)

**What it measures.** Aggregation latency and per-node load for a hierarchically-zoned gossip system that computes SQL-like rollups over a tree of zones. Headline metrics: **propagation delay**, **gossip rate ρ**, **estimation accuracy** of aggregates, and **per-agent bandwidth**.

**Headline numbers.** Astrolabe scales to thousands (claimed millions in simulation) of nodes with information propagation delays in the **tens of seconds**. Gossip rate ρ is typically 5–10 seconds per round; latency is quantized in 1-second bins with peaks at 2-second intervals because agents gossip in rounds. Aggregate estimation accuracy holds within ~5% error. The depth of the zone hierarchy is log_b(N) where b is the per-zone fanout, so latency scales as ρ · log_b(N).

**Maps onto gabion.** Gabion is flat — there is no zone hierarchy — but the **propagation delay vs gossip rate** relationship is the same fundamental tradeoff: ρ · log(N) is the floor on time-to-convergence regardless of architecture. The 5–10 second gossip period is a useful upper-bound calibration point: gabion runs much faster (sub-second) per round, so its target convergence should be on the order of single-digit seconds even at thousands of pods.

**Methodology to replicate.** Astrolabe's experimental setup uses *both* simulation (for scale) and a small live deployment (for realism). Gabion-bench should do the same: simulate fanout-based gossip at 1k–10k peers, then validate the simulation against a smaller real k8s deployment of, say, 50–200 pods. Also: latency-quantized-in-rounds is a real artifact — gabion's reported latency distributions will have similar shape if gossip is round-based.

Sources: [Astrolabe TOCS (Cornell)](https://www.cs.cornell.edu/home/rvr/papers/astrolabe.pdf), [ACM TOCS entry](https://dl.acm.org/doi/10.1145/762483.762485).

---

### Jelasity, Voulgaris, Guerraoui, Kermarrec, van Steen 2007, "Gossip-based peer sampling" (TOCS)

**What it measures.** Quality of a **peer-sampling service** — the abstraction every gossip protocol depends on for "pick a random peer." Headline metrics: **in-degree distribution** of the induced overlay, **clustering coefficient**, **average path length**, **partitioning probability under churn**, and how close the local view stream is to a true uniform random sample.

**Headline numbers.** N up to 100,000 simulated nodes. The paper compares Cyclon, Newscast, and several other instantiations. Key finding: even though every protocol gives each node a *locally* uniform stream of peers, the **global in-degree distribution is not uniform** — some protocols (notably Newscast) produce heavy-tailed in-degree distributions where a small number of nodes are heavily over-selected. Average path length stays close to the random-graph baseline (~log_f(N)) for view sizes ≥ 20. Partitioning probability under heavy churn is highly sensitive to the swap/healing parameter choice.

**Maps onto gabion.** Gabion gets its peer set from the EndpointSlice, which is effectively a flat, uniform list. So gabion's peer sampling is "trivially" uniform — but the relevant question is **whether that uniformity holds under fanout**. When gabion picks F peers out of K for a given round, is the *resulting* in-degree (peers selected to receive from this node) balanced across the cluster? Heavy-tailed in-degree means some peers get hammered while others starve — directly a fairness/load-balance concern.

**Methodology to replicate.** The headline plot is the in-degree CDF across nodes at steady state. Replicate it for gabion: log every send, count in-degree per peer over a 5-minute window, plot the CDF. Expect ≈ flat under EndpointSlice + uniform sampling; deviations indicate a sampling bug.

Sources: [Gossip-based peer sampling (distributed-systems.net)](https://www.distributed-systems.net/my-data/papers/2007.tocs.pdf), [ACM TOCS entry](https://dl.acm.org/doi/10.1145/1275517.1275520).

---

### Birman, Hayden, Ozkasap, Xiao, Budiu, Minsky 1999, "Bimodal Multicast" (TOCS)

**What it measures.** Reliability of pbcast (a two-phase protocol: best-effort multicast followed by gossip-based repair) under **adversarial perturbation** of nodes. Key metrics: **throughput stability** (does throughput stay flat as nodes are perturbed?), **delivery latency distribution**, and the **bimodal delivery property** — almost every node receives the message quickly, or almost no node does.

**Headline numbers.** Experiments on Cornell's SP2 (up to ~160 nodes). pbcast holds the "ideal" rate of 200 msgs/sec even with **25% of members perturbed** (forced to sleep). Reliability stays high at **20% systemwide packet loss** at 100 msgs/sec. The latency distribution is genuinely bimodal: a tight tail at ~1–2 gossip rounds for the vast majority of nodes, with a small fraction that takes much longer if the initial multicast missed them.

**Maps onto gabion.** Gabion is also a two-lane system — direct push for fresh writes, repair lane for stragglers — so the bimodal latency property is the right framing. Don't report mean delivery latency; report the **two-mode distribution**: the fast-lane tail and the repair-lane tail. The throughput-under-perturbation experiment is exactly the right shape: hold offered write rate fixed, perturb 25% of pods (CPU starvation, network throttling), and observe whether end-to-end CRDT update throughput stays flat.

**Methodology to replicate.** The perturbation methodology — deliberately slow down a fraction of nodes rather than just kill them — is more representative of real production conditions than clean failure. Gabion-bench should include a CPU-throttled-pods experiment alongside the simultaneous-kill experiment.

Sources: [Bimodal Multicast (Princeton)](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/bimodal.pdf), [CMU copy](http://www.cs.cmu.edu/~mihaib/research/pbcast.pdf).

---

## Empirical CRDT / production systems

### Dynamo (DeCandia 2007), Cassandra, Riak / Scylla AAE

Treat this as one block: "what production systems actually publish about gossip."

**What they measure.** Production systems publish operational, not academic, numbers: **gossip period in seconds**, **fan-out per round**, **time-to-converge for membership changes**, **request-latency SLOs (99.9th percentile)**, and **Merkle-tree comparison cost during anti-entropy repair**.

**Headline numbers.**
- **Dynamo (SOSP 2007)** uses one gossip round per second; every node contacts a random peer per second and reconciles membership. Designated **seed nodes** prevent split-ring. Latency SLO is 300ms at the 99.9th percentile. Membership-change convergence is on the order of seconds for a ring of ~hundreds of nodes.
- **Cassandra** runs gossip every 1 second, exchanging EndpointState (versioned heartbeat) with **up to 3 peers per round** (one random live, one random unreachable, one seed). State versioning is a monotonic counter incremented per second.
- **Riak Active Anti-Entropy** and **Scylla Anti-Entropy** use Merkle trees over per-partition (vnode) data so two replicas can detect divergent ranges in O(log K) hash comparisons (K = number of keys per vnode) and only ship the differing ranges.

**Maps onto gabion.** Gabion's anti-entropy is closer to Cassandra's gossip (versioned per-origin state, exchange per round) than to Riak's Merkle AAE (which is a much heavier full-dataset reconciliation). Cassandra's "up to 3 peers per round" is essentially a fanout-3 push, matching gabion's small-fanout design. The Dynamo seed-node concept does not apply — k8s EndpointSlice replaces it.

**Methodology to replicate.** Production numbers are reported as **time-to-converge in wall-clock seconds for a ring of size R**, not in protocol rounds. This is the unit users actually care about, and gabion-bench should report convergence as wall-clock latency at a fixed gossip period, not as a round count. Also: report the **bytes-per-second sustained gossip traffic per node** at idle (no writes) and at offered load — this is the production-relevant cost.

Sources: [Dynamo SOSP 2007](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf), [Cassandra gossip protocol (DeepWiki)](https://deepwiki.com/apache/cassandra/5.1-gossip-protocol), [Riak Active Anti-Entropy docs](https://docs.riak.com/riak/kv/2.2.3/using/cluster-operations/active-anti-entropy/index.html), [Scylla Anti-Entropy docs](https://docs.scylladb.com/manual/stable/architecture/anti-entropy/).

---

## Synthesis table

| Metric | Demers / Karp | SWIM | HyParView | Plumtree | Astrolabe | Bimodal | Gossip-PS | What we should measure |
|---|---|---|---|---|---|---|---|---|
| Convergence time | t_avg, t_last; O(log N) rounds | T'/(1−e^(−qf)) ≈ 1.6·T' | recovery after massive churn | LDH (hop count) | ρ · log_b(N) ≈ tens of sec | bimodal latency dist. | path length ≈ log_f(N) | **t_avg & t_last in wall-clock seconds, per origin** |
| Per-node message load | traffic m (msgs/update) | constant in N | fanout ≈ log(N) + c | RMR (redundancy) | per-agent bw vs ρ | msgs/sec under perturbation | view size & swap rate | **bytes/sec per pod at idle and at load; flat in N** |
| Redundancy / overhead | residue + traffic | piggyback overhead | active view ≈ log N | **RMR**: (total − N+1)/(N−1) | est. error vs ρ | gossip-repair bandwidth | in-degree skew | **RMR ≡ bytes shipped / bytes of novel delta** |
| Reliability under failure | residue tail | false-positive rate | ~100% at 80% kill; ~90% at 95% | recovers from 80% kill | n/a (aggregation, not delivery) | 20% pkt loss tolerated | partitioning prob. | **delivery % at 10/50/80/95% simultaneous pod loss** |
| Throughput stability | n/a | constant in N | n/a | n/a | flat aggregate accuracy | flat under 25% perturbed | n/a | **CRDT-update throughput under 25% throttled pods** |
| Worst-case staleness | t_last | time-to-detect | tree-heal latency | LDH max | hierarchy depth × ρ | bimodal slow tail | n/a | **repair-lane catch-up time after dirty-ring overflow** |
| Sampling quality | uniform-random assumed | random subgroup k | active/passive views | derived from overlay | zone election | n/a | **in-degree CDF** | **in-degree CDF over EndpointSlice peer set** |
| Workload methodology | Xerox trace | 16–56 real nodes + sim | 10k-node simulation | 10k-node simulation | sim + small deployment | SP2 cluster ≤160 nodes | up to 100k sim | **k8s deploy 50–200 pods + sim up to 10k peers** |
| Failure injection | replica crash | random crash | massive simultaneous failure 10–95% | massive failure 10–95% | n/a | CPU-perturbation + packet loss | churn rate | **simultaneous pod loss sweep + CPU throttle of subset** |

## What gabion-bench should ship

The literature converges on five headline numbers that every gossip system reports in some form. Gabion-bench should produce one figure per row, parameterized by `(N, fanout, write_rate, churn_rate)`:

1. **t_avg and t_last per origin**, in wall-clock seconds. (Demers; production-systems units.)
2. **Per-pod sustained bytes/sec** at idle and under load, plotted against N at fixed fanout. (SWIM constant-load methodology.)
3. **RMR** ≡ bytes shipped / bytes of novel CRDT delta. The single most important measure of how well peer-frontier dedup is actually working. (Plumtree.)
4. **Delivery completeness and reconvergence time** under simultaneous pod loss at 10%, 50%, 80%, 95%. (HyParView / Plumtree massive-failure sweep.)
5. **In-degree CDF** across the peer set at steady state — verifies the EndpointSlice + uniform-sampling assumption. (Jelasity 2007.)

Two gabion-specific metrics with no direct precedent in the literature:

6. **Repair-lane catch-up time** after a forced dirty-ring overflow. This is the gabion analogue of SWIM's worst-case detection time: the bound the repair lane provides on staleness.
7. **CRDT-update throughput under 25% throttled pods** (CPU-perturbation methodology from Bimodal). Tests that slow pods do not drag down the cluster.

Out of scope: anything from SWIM about failure detection — Kubernetes EndpointSlice owns membership. Gabion-bench should explicitly note this so reviewers don't expect time-to-detect-a-failed-pod numbers.
