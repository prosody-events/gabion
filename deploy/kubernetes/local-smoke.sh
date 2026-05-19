#!/bin/sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repo_root"

context="$(kubectl config current-context)"
server="$(kubectl config view --minify -o 'jsonpath={.clusters[0].cluster.server}')"

if [ "$context" != "orbstack" ]; then
    printf '%s\n' "refusing to run: current kubernetes context is '$context', expected 'orbstack'" >&2
    exit 1
fi

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

env CARGO_BUILD_RUSTC_WRAPPER= cargo test -p gabion-discovery --no-default-features
env CARGO_BUILD_RUSTC_WRAPPER= cargo test -p gabion-bin local_kubernetes_endpoint_slice_watcher_drives_gossip_convergence --no-default-features -- --ignored

printf '%s\n' "local kubernetes EndpointSlice watcher and gossip convergence test passed on context '$context' ($server)"
