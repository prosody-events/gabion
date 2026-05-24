# Gossip & Anti-Entropy Literature: Reference Metrics for Gabion

Reference notes that ground gabion's gossip evaluation. The protocol under test combines anti-entropy over **per-origin counter CRDTs** (counters merge by max, messages dedup by `(origin, seq)`) with **push gossip and peer-frontier dedup**: the sender remembers the highest sequence number per origin that each peer has acknowledged and prunes already-known cells before transmission. Peers are sampled via a partial Fisher-Yates shuffle of the active set drawn from the Kubernetes EndpointSlice, so membership and failure detection live entirely outside gabion. A **repair lane** rotates a cursor linearly over the local cell store's slots, which keeps anti-entropy converging even when per-peer dirty rings overflow.

Each entry below records (1) what the paper measures, (2) headline numbers, (3) which metrics map onto gabion, and (4) methodology worth replicating. The synthesis table at the end is the operational deliverable, and its right-hand column is what gabion-bench should report.

---

## Foundational

### Demers et al. 1987, "Epidemic Algorithms for Replicated Database Maintenance" (PODC)

**What it measures.** Demers introduces three canonical metrics that every subsequent gossip paper inherits:

- **Residue**: fraction of nodes still unaware of an update once the protocol "stops" being interested in it. For deterministic anti-entropy this is 0; for rumor-mongering it is the tail of the SIR curve.
- **Traffic** (denoted *m*): average number of update messages a node sends/receives, expressed as a multiplier of the update payload — directly the byte-overhead-per-update.
- **Delay**: distribution of times to first delivery, split into **t_avg** (mean delivery time across nodes) and **t_last** (time to reach the last node).

**Headline numbers.** Demers shows that anti-entropy (pull or push-pull) converges in O(log N) rounds with high probability for every node, and quantifies the difference between push, pull, and push-pull strategies: pull dominates after roughly half the population is infected, because the proportion of "ignorant" sites shrinks quadratically per round under pull versus linearly under push. Rumor-mongering with a "counter-k feedback" stopping rule trades residue for traffic; increasing k drops residue exponentially while traffic grows linearly. The Xerox Clearinghouse trace evaluation shows that spatial gossip, which prefers nearby peers, reduces link load on backbone links by an order of magnitude relative to uniform random selection.

**Maps onto gabion.** Because gabion runs anti-entropy rather than rumor-mongering, and because the repair lane keeps cycling, the protocol never stops trying to deliver an update; residue should therefore be ~0 at steady state, which leaves traffic and delay as the relevant metrics. The push-pull intuition is partially relevant: peer-frontier dedup makes gabion's push behave like pull from the receiver's perspective, since the sender already knows what the receiver has and only ships the delta.

**Methodology to replicate.** Gabion should report **t_avg** and **t_last** per origin rather than as an aggregate, because the per-origin distribution is what actually exposes stragglers. Demers' Xerox-trace approach — running over a real network topology with measured link RTTs — is the right model for evaluating gabion under a realistic k8s EndpointSlice rather than a uniform-random graph. Traffic should be tracked in bytes per update cell rather than in number of messages, since gabion frames pack many cells into a single packet.

Sources: [Demers 1987 (UPenn copy)](https://www.cis.upenn.edu/~bcpierce/courses/dd/papers/demers-epidemic.pdf), [ACM SIGOPS reprint](https://dl.acm.org/doi/10.1145/43921.43922).

---

### Karp, Schindelhauer, Shenker, Vöcking 2000, "Randomized Rumor Spreading" (FOCS)

**What it measures.** Time complexity (rounds) and message complexity (total transmissions) for spreading one rumor on the complete graph K_n under push, pull, and push-pull strategies, with an explicit lower bound for address-oblivious algorithms.

**Headline numbers.** The push-pull random-phone-call protocol delivers a rumor to all n nodes in **Θ(log n)** rounds with high probability, and uses **Θ(n log log n)** messages total, which is asymptotically tight. The protocol has two phases: an exponential push phase that informs roughly n/2 nodes, followed by a pull phase in which the uninformed population shrinks quadratically per round. The address-oblivious lower bound is Ω(n log log n) messages per rumor.

**Maps onto gabion.** Although gabion runs continuously over many origin counters rather than spreading a single rumor, the per-origin convergence shape is the same. The n log log n bound says that if every cell must reach all peers via gossip and back-channel addressing is unavailable, no protocol can do better than roughly log log N redundancy. Because peer-frontier dedup is effectively a form of addressed knowledge — the sender knows what each receiver already has — gabion can in principle beat the address-oblivious bound, and the gap is worth quantifying empirically.

**Methodology to replicate.** Karp's analysis is asymptotic, but the "phases" framing maps directly to a per-origin trace: plotting the fraction of peers that have acked a given `(origin, seq)` over rounds should show the exponential-then-quadratic shape. The lower bound also provides a useful baseline, namely how close gabion's per-update message multiplier comes to log log N at network sizes of 100, 1k, and 10k peers.

Sources: [Karp 2000 (Yale)](https://zoo.cs.yale.edu/classes/cs426/2013/bib/karp00randomized.pdf), [FOCS proceedings](https://dl.acm.org/doi/10.5555/795666.796561).

---

## Membership / failure detection (comparison set; gabion does **not** implement these)

### Das, Gupta, Motivala 2002, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" (DSN)

**What it measures.** Three numbers that have become the standard for cluster membership: **time to first detection** of a failure, **rate of false positives**, and **message load per member per protocol period**. SWIM's pitch is that all three are independent of group size N.

**Headline numbers.** With protocol period T' and k indirect-ping subgroup members, expected time to first detection is approximately T'/(1 - e^(-qf)) where qf is the fraction of live members successfully completing the ping; with typical parameters (T' ≈ 2s, k = 3) detection takes ~1.6 protocol periods, i.e., ~3 seconds. False-positive rate at 95% message delivery and k = 3 stays near 0.1%. Per-member outgoing bandwidth is constant — one direct ping, one ack, and (worst case) k indirect pings per period — independent of group size. Membership change dissemination, piggybacked on pings, reaches all members in O(log N) rounds.

**Maps onto gabion.** None of SWIM's headline numbers map directly, because Kubernetes EndpointSlice owns membership and any time-to-detect-failure metric belongs to k8s rather than to gabion. What *is* portable is SWIM's framing of constant per-node message load as N grows, paired with a bounded worst-case latency for one piece of information to reach everyone. The gabion analogue of "detection time" is **worst-case staleness**, meaning how long after an update is observed at the origin the last peer still holds a stale value, and the repair lane is gabion's bound on it.

**Methodology to replicate.** SWIM's experimental scale was 16–56 nodes on real hardware, supplemented by simulation at larger sizes. The headline plot is "per-process outgoing bandwidth versus N", which is flat for SWIM and growing for heartbeat; gabion should produce the same plot, with per-pod outgoing bytes/sec on the y-axis at fixed fanout. The false-positive-rate-versus-network-loss-rate sweep is also worth replicating, except that the gabion-relevant axis is **repair-lane lag versus dirty-ring drop rate**.

Sources: [SWIM (Cornell)](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf), [DSN 2002 entry](https://dl.acm.org/doi/10.5555/647883.738420), [Wikipedia summary](https://en.wikipedia.org/wiki/SWIM_Protocol).

---

### Leitão, Pereira, Rodrigues 2007, "HyParView" (DSN)

**What it measures.** Reliability of gossip broadcast (fraction of nodes that receive a given message) under **massive simultaneous failure**, with two view-size knobs: a small **active view** of size log(N) + c, and a much larger **passive view** of size k·(log(N) + c) holding backup links.

**Headline numbers.** Experiments at N = 10,000 nodes, sweeping simultaneous failure rates from 10% to 95%. HyParView preserves ~100% delivery reliability under failure rates up to **80%**, and maintains ~90% delivery even at **95% simultaneous failures**, while comparable protocols (Scamp, Cyclon) collapse to under 50% reliability at the same failure rates. Fanout is small (around log(N), so roughly 10 for N = 10k), which keeps per-node bandwidth low.

**Maps onto gabion.** Although gabion does not implement HyParView, EndpointSlice effectively plays the role of the passive view: when an active peer disappears, the next EndpointSlice update supplies the replacement. The relevant question for gabion is therefore resilience under simultaneous pod loss — when 50%, 80%, or 95% of pods are evicted at once, how quickly does the system reconverge once new pods register? The fanout = log(N) sizing applies directly to gabion's fanout knob.

**Methodology to replicate.** The massive-failure injection sweep is the headline experiment. Gabion-bench should kill 10%, 50%, and 80% of pods simultaneously and measure both the fraction of in-flight updates that still converge and the time-to-reconvergence once the EndpointSlice settles.

Sources: [HyParView DSN 2007](https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf).

---

### Leitão, Pereira, Rodrigues 2007, "Epidemic Broadcast Trees" / Plumtree (SRDS)

**What it measures.** Two metrics that directly motivate gabion's peer-frontier dedup:

- **RMR — Relative Message Redundancy**: (total payload messages − N + 1) / (N − 1). For a perfect spanning tree RMR = 0; for flat gossip with fanout f, RMR ≈ f − 1.
- **LDH — Last Delivery Hop**: hop count at which the last node receives a given broadcast. Equivalent to t_last in hops.

Plumtree replaces redundant eager-push edges with cheap **IHAVE / GRAFT / PRUNE** lazy-push announcements: a peer announces that it has a message, the receiver GRAFTs the edge into the tree if it has not yet seen the message, and PRUNEs the edge if it has.

**Headline numbers.** N = 10,000 simulated nodes. Plumtree drives RMR essentially to zero in steady state (versus RMR ≈ fanout − 1 for flat gossip), while keeping LDH within roughly one hop of flat gossip. Under massive node failure (up to 80%), Plumtree heals the tree using IHAVE timeouts and reliability stays near 100%.

**Maps onto gabion.** This is the closest analogue to gabion's design, because gabion's peer-frontier dedup is structurally the same idea as Plumtree's IHAVE/PRUNE: the sender already knows the receiver's frontier and prunes cells the receiver has acked. The "tree" in gabion is implicit and per-origin, formed by the set of senders that successfully delivered each cell. RMR is therefore the single most important metric for gabion — bytes shipped divided by bytes of novel CRDT delta — and the target should be RMR well below fanout.

**Methodology to replicate.** RMR and LDH form the headline pair. The massive-failure axis should be swept the same way Plumtree does (10%, 50%, 80%, 95%), and the experiment should confirm that the repair lane plays the role of Plumtree's tree-healing layer.

Sources: [Plumtree SRDS 2007](https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf).

---

## Aggregation / large-scale state

### Van Renesse, Birman, Vogels 2003, "Astrolabe" (TOCS)

**What it measures.** Aggregation latency and per-node load for a hierarchically-zoned gossip system that computes SQL-like rollups over a tree of zones. Headline metrics: **propagation delay**, **gossip rate ρ**, **estimation accuracy** of aggregates, and **per-agent bandwidth**.

**Headline numbers.** Astrolabe scales to thousands of nodes (claimed millions in simulation) with information propagation delays in the tens of seconds. Gossip rate ρ is typically 5–10 seconds per round; latency is quantized in 1-second bins with peaks at 2-second intervals because agents gossip in rounds. Aggregate estimation accuracy holds within roughly 5% error. The depth of the zone hierarchy is log_b(N) where b is the per-zone fanout, so latency scales as ρ · log_b(N).

**Maps onto gabion.** Gabion has no zone hierarchy, but the propagation-delay-versus-gossip-rate relationship captures the same fundamental tradeoff: ρ · log(N) is the floor on time-to-convergence regardless of architecture. The 5–10 second gossip period in Astrolabe is a useful upper-bound calibration point, because gabion runs much faster (sub-second) per round and its target convergence should therefore be on the order of single-digit seconds even at thousands of pods.

**Methodology to replicate.** Astrolabe's experimental setup uses both simulation (for scale) and a small live deployment (for realism), and gabion-bench should follow the same recipe by simulating fanout-based gossip at 1k–10k peers and then validating the simulation against a smaller real k8s deployment of perhaps 50–200 pods. Latency quantized in rounds is a real artifact, and gabion's reported latency distributions will have a similar shape if gossip is round-based.

Sources: [Astrolabe TOCS (Cornell)](https://www.cs.cornell.edu/home/rvr/papers/astrolabe.pdf), [ACM TOCS entry](https://dl.acm.org/doi/10.1145/762483.762485).

---

### Jelasity, Voulgaris, Guerraoui, Kermarrec, van Steen 2007, "Gossip-based peer sampling" (TOCS)

**What it measures.** Quality of a **peer-sampling service** — the abstraction every gossip protocol depends on for "pick a random peer." Headline metrics: **in-degree distribution** of the induced overlay, **clustering coefficient**, **average path length**, **partitioning probability under churn**, and how close the local view stream is to a true uniform random sample.

**Headline numbers.** N up to 100,000 simulated nodes. The paper compares Cyclon, Newscast, and several other instantiations. The key finding is that even though every protocol gives each node a *locally* uniform stream of peers, the global in-degree distribution is not uniform: some protocols (notably Newscast) produce heavy-tailed in-degree distributions in which a small number of nodes are heavily over-selected. Average path length stays close to the random-graph baseline (~log_f(N)) for view sizes ≥ 20, and partitioning probability under heavy churn is highly sensitive to the swap/healing parameter choice.

**Maps onto gabion.** Gabion draws its peer set from the EndpointSlice, which is effectively a flat, uniform list, so peer sampling is uniform by construction. The interesting question is whether that uniformity survives under fanout: when gabion picks F peers out of K for a given round, the resulting in-degree (the number of peers selected to receive from a given node) should be balanced across the cluster, and a heavy-tailed in-degree distribution would mean some peers get hammered while others starve, which is directly a fairness and load-balance concern.

**Methodology to replicate.** The headline plot is the in-degree CDF across nodes at steady state, and replicating it for gabion is straightforward: log every send, count in-degree per peer over a 5-minute window, and plot the CDF. Under EndpointSlice plus uniform sampling the curve should be approximately flat, so any deviation indicates a sampling bug.

Sources: [Gossip-based peer sampling (distributed-systems.net)](https://www.distributed-systems.net/my-data/papers/2007.tocs.pdf), [ACM TOCS entry](https://dl.acm.org/doi/10.1145/1275517.1275520).

---

### Birman, Hayden, Ozkasap, Xiao, Budiu, Minsky 1999, "Bimodal Multicast" (TOCS)

**What it measures.** Reliability of pbcast (a two-phase protocol: best-effort multicast followed by gossip-based repair) under **adversarial perturbation** of nodes. Key metrics: **throughput stability** (does throughput stay flat as nodes are perturbed?), **delivery latency distribution**, and the **bimodal delivery property** — almost every node receives the message quickly, or almost no node does.

**Headline numbers.** Experiments on Cornell's SP2 (up to ~160 nodes). pbcast holds the "ideal" rate of 200 msgs/sec even with 25% of members perturbed (forced to sleep), and reliability stays high at 20% systemwide packet loss at 100 msgs/sec. The latency distribution is genuinely bimodal: a tight tail at ~1–2 gossip rounds for the vast majority of nodes, with a small fraction that takes much longer if the initial multicast missed them.

**Maps onto gabion.** Gabion is also a two-lane system, with direct push for fresh writes and a repair lane for stragglers, so the bimodal latency property is the right framing. Reports should therefore expose the two-mode distribution — the fast-lane tail and the repair-lane tail — rather than collapsing everything into a mean. The throughput-under-perturbation experiment is exactly the right shape: hold offered write rate fixed, perturb 25% of pods (CPU starvation, network throttling), and observe whether end-to-end CRDT update throughput stays flat.

**Methodology to replicate.** The perturbation methodology, which deliberately slows down a fraction of nodes rather than killing them outright, is more representative of real production conditions than clean failure. Gabion-bench should include a CPU-throttled-pods experiment alongside the simultaneous-kill experiment.

Sources: [Bimodal Multicast (Princeton)](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/bimodal.pdf), [CMU copy](http://www.cs.cmu.edu/~mihaib/research/pbcast.pdf).

---

## Empirical CRDT / production systems

### Dynamo (DeCandia 2007), Cassandra, Riak / Scylla AAE

This block is best read together as "what production systems actually publish about gossip."

**What they measure.** Production systems publish operational rather than academic numbers: **gossip period in seconds**, **fan-out per round**, **time-to-converge for membership changes**, **request-latency SLOs (99.9th percentile)**, and **Merkle-tree comparison cost during anti-entropy repair**.

**Headline numbers.**
- **Dynamo (SOSP 2007)** uses one gossip round per second; every node contacts a random peer per second and reconciles membership. Designated **seed nodes** prevent split-ring. Latency SLO is 300ms at the 99.9th percentile. Membership-change convergence is on the order of seconds for a ring of ~hundreds of nodes.
- **Cassandra** runs gossip every 1 second, exchanging EndpointState (versioned heartbeat) with **up to 3 peers per round** (one random live, one random unreachable, one seed). State versioning is a monotonic counter incremented per second.
- **Riak Active Anti-Entropy** and **Scylla Anti-Entropy** use Merkle trees over per-partition (vnode) data so two replicas can detect divergent ranges in O(log K) hash comparisons (K = number of keys per vnode) and only ship the differing ranges.

**Maps onto gabion.** Gabion's anti-entropy is closer to Cassandra's gossip — versioned per-origin state exchanged each round — than to Riak's Merkle AAE, which performs a much heavier full-dataset reconciliation. Cassandra's "up to 3 peers per round" is essentially a fanout-3 push, which matches gabion's small-fanout design. The Dynamo seed-node concept does not apply, because k8s EndpointSlice takes its place.

**Methodology to replicate.** Production numbers are reported as time-to-converge in wall-clock seconds for a ring of size R rather than in protocol rounds, and since that is the unit users actually care about, gabion-bench should report convergence as wall-clock latency at a fixed gossip period rather than as a round count. Sustained gossip traffic in bytes per second per node, measured both at idle (no writes) and under offered load, is the production-relevant cost worth reporting alongside it.

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

## Roadmap: what gabion-bench should ship

Five headline numbers from the literature plus two gabion-specific ones. Each item's status reflects what `Headline` in `src/metrics.rs` and the `SUITES` in `bench/plot.py` emit today: the shipped item is the one to defend, and the rest are open work.

1. ☐ **t_avg and t_last per origin** in wall-clock seconds (Demers). The bench currently reports cluster-wide `convergence_millis` and `final_divergence` rather than a per-origin breakdown.
2. ☑ **Per-pod sustained bytes/sec** at idle and under load versus N at fixed fanout (SWIM). Shipped as `bytes_per_node_per_second` in `Headline`, and surfaced in both `plot_scale_n` and `plot_convergence`.
3. ☐ **RMR** ≡ bytes shipped / bytes of novel CRDT delta (Plumtree). The bench tracks `bytes_sent_total` but has no novel-delta denominator yet, which leaves gabion's most direct test of peer-frontier dedup unimplemented.
4. ☐ **Delivery completeness and reconvergence** under simultaneous pod loss at 10/50/80/95 % (HyParView/Plumtree). `LinkAction::Block` can isolate a single node, but no suite sweeps the failure axis.
5. ☐ **In-degree CDF** across the peer set at steady state (Jelasity 2007). `CountingTransport` counts bytes per node, but it does not log which node sent to which, so the CDF cannot yet be constructed.
6. ☐ **Repair-lane catch-up time** after a forced dirty-ring overflow, which is gabion's analogue of SWIM's worst-case detection time. No suite currently forces overflow; `min_emit_clamp` stresses the emit-rate floor but stays below the ring.
7. ☐ **CRDT-update throughput under 25 % throttled pods** (Bimodal CPU perturbation). The bench has neither a CPU-throttle nor a per-pod-rate-limit primitive, so this experiment is not yet possible.

Out of scope: anything from SWIM about failure detection, because Kubernetes EndpointSlice owns membership and reviewers should not expect time-to-detect-a-failed-pod numbers.
