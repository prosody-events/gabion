# Gossip Propagation Benchmarks

The Kubernetes propagation benchmark is intended to prove the full deployed path, not just isolated library behavior. It runs only against the local OrbStack Kubernetes context and refuses to run if the current context is not `orbstack` or the API server is not localhost.

Run:

```sh
make kubernetes-gossip-bench
```

Clean up a hung run:

```sh
make kubernetes-clean
```

## Realism Requirements

The benchmark must exercise the same major systems used in production-like Kubernetes mode:

- NGINX runs as a Kubernetes Deployment with multiple replicas.
- The Gabion server runs as a separate Kubernetes Deployment with multiple replicas.
- Both deployments use Kubernetes EndpointSlice discovery and watch both Services.
- Traffic enters through the NGINX Service, not by calling the Gabion runtime directly.
- NGINX records real HTTP requests from an actual configured location.
- Gossip uses pod networking through the configured gossip ports.
- Server admin endpoints are used only for observation after the system has processed real traffic.
- The test starts NGINX below the final replica count and then scales the Deployment, so EndpointSlice membership changes are owned by Kubernetes and the run depends on peer-add watch events.

The default run uses unique request URIs under `/api/index.html`, which keeps the traffic path real while creating high-cardinality cells that make convergence visible. The benchmark limit is intentionally higher than the request count so rate limiting does not mask propagation measurements.

## Metrics

The benchmark follows the same style of questions used by SWIM-style membership evaluations: how fast information disseminates, how much peer state each participant learns, how much message work is required, and whether final state is accurate.

It records:

- Successful, limited, and failed HTTP responses from the in-cluster load Job.
- Per-server remote aggregate totals over time.
- Per-server convergence latency at 50%, 90%, 99%, and 100% of successful requests.
- Per-server discovered peer counts.
- Merge cells, send bytes, receive bytes, digest mismatches, and decode errors from the runtime.

The generated report is written under `target/gabion-gossip-bench/<timestamp>/`:

- `summary.json`
- `samples.csv`
- `remote-total.svg`
- `merge-cells.svg`
- `peer-count.svg`
- `index.html`

## Pass Criteria

A run fails if:

- The load Job does not attempt the requested number of requests.
- Any request returns a non-2xx/non-429 status.
- Any request is rate limited during the propagation benchmark.
- Any server pod fails to converge to the number of successful NGINX requests.
- Any server pod fails to discover every other NGINX and server gossip peer.

These are correctness gates. The latency and byte charts are performance measurements, and should be compared across replica counts, fanout values, linger settings, and request cardinality.

## Tunables

The Makefile target accepts environment overrides:

- `GABION_BENCH_SERVER_REPLICAS`, default `3`
- `GABION_BENCH_INITIAL_NGINX_REPLICAS`, default half of `GABION_BENCH_NGINX_REPLICAS`, minimum `1`
- `GABION_BENCH_NGINX_REPLICAS`, default `8`
- `GABION_BENCH_REQUESTS`, default `2000`
- `GABION_BENCH_CONCURRENCY`, default `32`
- `GABION_BENCH_LINGER_MS`, default `100`
- `GABION_BENCH_FANOUT`, default `8`
- `GABION_BENCH_SAMPLE_MS`, default `100`
- `GABION_BENCH_TIMEOUT_SECONDS`, default `90`
- `GABION_BENCH_KEEP_NAMESPACE=1` to preserve the namespace for debugging
- `GABION_BENCH_OUTPUT_DIR` to choose the report directory

Example scale run:

```sh
GABION_BENCH_NGINX_REPLICAS=16 \
GABION_BENCH_SERVER_REPLICAS=4 \
GABION_BENCH_REQUESTS=10000 \
GABION_BENCH_CONCURRENCY=64 \
make kubernetes-gossip-bench
```

## Analysis

Use `remote-total.svg` to inspect dissemination speed and skew between server replicas. A healthy run should show every server reaching the successful request count quickly, with similar curves.

Use `peer-count.svg` to verify EndpointSlice discovery. Each server pod should discover `server_replicas + nginx_replicas - 1` peers. If this stalls, the issue is discovery or peer event handling rather than CRDT merge correctness.

Use `merge-cells.svg`, `send_bytes`, and `recv_bytes` in `samples.csv` to compare gossip work across fanout and replica counts. A change that improves latency by flooding the cluster should show up as increased byte cost.
