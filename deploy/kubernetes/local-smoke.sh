#!/bin/sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repo_root"

context="$(kubectl config current-context)"
server="$(kubectl config view --minify -o 'jsonpath={.clusters[0].cluster.server}')"

case "$context" in
    kind-*) ;;
    *)
        printf '%s\n' "refusing to run: current kubernetes context is '$context', expected 'kind-*'" >&2
        exit 1 ;;
esac

case "$server" in
    https://127.0.0.1:*|https://localhost:*)
        ;;
    *)
        printf '%s\n' "refusing to run: kubernetes API server is '$server', expected localhost" >&2
        exit 1
        ;;
esac

sh deploy/kubernetes/local-clean.sh
trap 'sh deploy/kubernetes/local-clean.sh' EXIT

if ! cargo nextest --version >/dev/null 2>&1; then
    printf '%s\n' \
        'cargo-nextest is not installed.' \
        'Gabion uses nextest as its only test runner; cargo test is unsupported.' \
        'Install with: cargo install cargo-nextest --locked' >&2
    exit 1
fi

# Use the default feature set: `discovery-kubernetes` is what enables the
# `discovery::kubernetes::tests` module in the first place, and
# `transport-udp` is needed for the test crate to compile (it pulls
# `UdpTransport` and `tokio::net::UdpSocket` via the gossip tests file).
# The previous `--no-default-features` form gated out kubernetes entirely,
# so the filter matched zero tests.
env CARGO_BUILD_RUSTC_WRAPPER= cargo nextest run -p gabion discovery::kubernetes::tests

# Live-cluster integration: `EndpointSliceDiscovery` against the running
# kind cluster, end-to-end through `GossipRuntime` convergence. Marked
# `#[ignore]` so it doesn't fire on `make test`, and only run here under
# `local-smoke.sh` where we've already asserted kind is up.
env CARGO_BUILD_RUSTC_WRAPPER= cargo nextest run -p gabion \
    --run-ignored ignored-only \
    discovery::kubernetes::tests::local_kubernetes_endpoint_slice_watcher_drives_gossip_convergence

printf '%s\n' "local kubernetes EndpointSlice watcher and gossip convergence test passed on context '$context' ($server)"
