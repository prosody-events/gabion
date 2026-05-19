#!/bin/sh
set -eu

context="$(kubectl config current-context)"
server="$(kubectl config view --minify -o 'jsonpath={.clusters[0].cluster.server}')"

if [ "$context" != "orbstack" ]; then
    printf '%s\n' "refusing to clean: current kubernetes context is '$context', expected 'orbstack'" >&2
    exit 1
fi

case "$server" in
    https://127.0.0.1:*|https://localhost:*)
        ;;
    *)
        printf '%s\n' "refusing to clean: kubernetes API server is '$server', expected localhost" >&2
        exit 1
        ;;
esac

kubectl get namespace -o name \
    | while IFS= read -r namespace; do
        case "$namespace" in
            namespace/gabion-kube-e2e-*|namespace/gabion-local-smoke)
                kubectl delete "$namespace" --ignore-not-found=true
                ;;
        esac
    done
