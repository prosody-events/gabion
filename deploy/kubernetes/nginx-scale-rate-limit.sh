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

namespace="gabion-nginx-scale-$$"
port_forward_pid=""

cleanup() {
    # Dump pod state BEFORE namespace deletion so CI failures surface
    # the real cause instead of a generic timeout. See mixed script for
    # the rationale.
    if [ -n "${namespace:-}" ]; then
        printf '\n--- cleanup diagnostic dump (namespace=%s) ---\n' "$namespace" >&2
        kubectl -n "$namespace" get pods,events --sort-by=.lastTimestamp -o wide >&2 || true
        kubectl -n "$namespace" describe pods -l app=gabion-nginx >&2 || true
        # --tail=1000 because the smoke-with-bt wrapper emits a full gdb
        # backtrace on a native SIGSEGV that easily exceeds the prior
        # 200-line tail — and that backtrace is the only signal we have
        # for a config-phase crash inside the module's static
        # callbacks.
        kubectl -n "$namespace" logs --all-containers --tail=1000 -l app=gabion-nginx >&2 || true
        kubectl -n "$namespace" logs --all-containers --tail=1000 --previous -l app=gabion-nginx >&2 || true
        # Best-effort: copy /tmp/cores out of any pod that's still
        # Running. kubectl cp uses `kubectl exec`, so it can't reach a
        # pod that has already exited — the in-pod gdb dump in the
        # logs above is the primary signal; this is for offline
        # post-mortem when a pod survives long enough.
        mkdir -p /tmp/cores
        for pod in $(kubectl -n "$namespace" get pods -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}'); do
            kubectl -n "$namespace" cp "$pod:/tmp/cores" "/tmp/cores/${namespace}-${pod}" 2>/dev/null || true
        done
        printf -- '--- end cleanup diagnostic dump ---\n\n' >&2
    fi
    if [ -n "$port_forward_pid" ]; then
        kill "$port_forward_pid" 2>/dev/null || true
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true
}
trap cleanup EXIT

docker compose --profile module -f deploy/nginx/docker-compose.yml build nginx-module-request-smoke

kubectl create namespace "$namespace"

kubectl -n "$namespace" apply -f - <<'YAML'
# Bind watch permissions to the namespace's `default` ServiceAccount.
# kubelet always mounts the default SA's token into pods that don't set
# `serviceAccountName`, so no custom SA is needed — but the API server
# still authenticates each request as `system:serviceaccount:<ns>:default`,
# and that identity has zero permissions out of the box. The Role +
# RoleBinding below grants exactly the verbs `EndpointSliceDiscovery`
# uses (`crates/gabion/src/discovery/kubernetes.rs::watch_services` and
# `watch_target`). Discovery itself falls through env→DNS bootstrap so
# no `serviceAccountName` line, no `env` directive in nginx.smoke.conf,
# and no kubeconfig is needed inside the pod.
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gabion-endpointslice-reader
rules:
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: gabion-endpointslice-reader
subjects:
  - kind: ServiceAccount
    name: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: gabion-endpointslice-reader
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabion-nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gabion-nginx
  template:
    metadata:
      labels:
        app: gabion-nginx
    spec:
      containers:
        - name: nginx
          image: nginx-nginx-module-request-smoke:latest
          imagePullPolicy: Never
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
            - name: gabion
              containerPort: 9000
              protocol: UDP
---
# nginx HTTP frontend. ClusterIP load-balances inbound requests across the
# replica set. UDP isn't exposed here — gossip lives on its own headless
# Service below so peer discovery sees pod IPs, not the virtual ClusterIP.
apiVersion: v1
kind: Service
metadata:
  name: gabion-nginx
spec:
  selector:
    app: gabion-nginx
  ports:
    - name: http
      port: 8080
      targetPort: http
      protocol: TCP
---
# Gabion peer-discovery Service. The discovery code (see
# `crates/gabion/src/discovery/kubernetes.rs::is_gabion_udp`) tracks only
# Services exposing a port literally named "gabion" with protocol UDP — so
# the port name and protocol below are load-bearing, not cosmetic.
# clusterIP: None makes this headless: the EndpointSlice the discovery
# watches lists each replica's pod IP directly, which is what gossip needs
# in order to address peers individually.
apiVersion: v1
kind: Service
metadata:
  name: gabion
spec:
  clusterIP: None
  selector:
    app: gabion-nginx
  ports:
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
YAML

wait_for_endpoint_count() {
    expected="$1"
    attempts=0
    while [ "$attempts" -lt 60 ]; do
        count="$(
            kubectl -n "$namespace" get endpointslice \
                -l kubernetes.io/service-name=gabion-nginx \
                -o 'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}' \
                | sed '/^$/d' \
                | wc -l \
                | tr -d ' '
        )"
        if [ "$count" = "$expected" ]; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    printf '%s\n' "timed out waiting for $expected EndpointSlice addresses; saw $count" >&2
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}

wait_for_pods() {
    expected="$1"
    attempts=0
    while [ "$attempts" -lt 60 ]; do
        pods="$(kubectl -n "$namespace" get pods -l app=gabion-nginx -o 'jsonpath={range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}')"
        count="$(printf '%s\n' "$pods" | sed '/^$/d' | wc -l | tr -d ' ')"
        if [ "$count" = "$expected" ]; then
            printf '%s\n' "$pods" | sed '/^$/d'
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    printf '%s\n' "timed out waiting for $expected running pods; saw $count" >&2
    kubectl -n "$namespace" get pods -o wide >&2
    return 1
}

start_port_forward() {
    pod="$1"
    if [ -n "$port_forward_pid" ]; then
        kill "$port_forward_pid" 2>/dev/null || true
        port_forward_pid=""
    fi
    kubectl -n "$namespace" port-forward "pod/$pod" 18080:8080 >/tmp/gabion-nginx-port-forward.log 2>&1 &
    port_forward_pid="$!"

    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if curl -fsS -o /dev/null "http://127.0.0.1:18080/off/index.html"; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    cat /tmp/gabion-nginx-port-forward.log >&2 || true
    return 1
}

start_service_port_forward() {
    if [ -n "$port_forward_pid" ]; then
        kill "$port_forward_pid" 2>/dev/null || true
        port_forward_pid=""
    fi
    kubectl -n "$namespace" port-forward "svc/gabion-nginx" 18080:8080 >/tmp/gabion-nginx-port-forward.log 2>&1 &
    port_forward_pid="$!"

    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if curl -fsS -o /dev/null "http://127.0.0.1:18080/off/index.html"; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    cat /tmp/gabion-nginx-port-forward.log >&2 || true
    return 1
}

assert_pod_rate_limit() {
    pod="$1"
    start_port_forward "$pod"

    first="$(curl -fsS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
    second="$(curl -fsS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"

    # Capture headers + status on the rejected third request so we can
    # validate the rate-limit headers the migrated adapter emits.
    third_headers="$(mktemp)"
    third="$(curl -sS -D "$third_headers" -o /dev/null \
        -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"

    test "$first" = 200
    test "$second" = 200
    test "$third" = 429

    printf '\n  pod=%s rate-limit headers on 429:\n' "$pod"
    # The header order doesn't matter; print whichever the adapter set.
    grep -iE '^(X-RateLimit-Limit|X-RateLimit-Remaining|X-RateLimit-Reset|Retry-After):' \
        "$third_headers" | sed 's/^/    /' || true

    limit_h="$(grep -i '^X-RateLimit-Limit:' "$third_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    remaining_h="$(grep -i '^X-RateLimit-Remaining:' "$third_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    reset_h="$(grep -i '^X-RateLimit-Reset:' "$third_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    retry_h="$(grep -i '^Retry-After:' "$third_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"

    rm -f "$third_headers"

    if [ -z "$limit_h" ] || [ -z "$remaining_h" ] || [ -z "$reset_h" ] || [ -z "$retry_h" ]; then
        printf '%s\n' "FAIL: missing rate-limit header on 429 from pod $pod" >&2
        return 1
    fi
    # Rule (deploy/nginx/nginx.smoke.conf): uri_api 2r/m, window=60s.
    # Retry-After is the sliding-window delta until the oldest live hit
    # ages out, so it falls in [1, window].
    if [ "$limit_h" != "2" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Limit=$limit_h, expected 2" >&2
        return 1
    fi
    if [ "$remaining_h" != "0" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Remaining=$remaining_h, expected 0" >&2
        return 1
    fi
    if ! [ "$retry_h" -ge 1 ] || ! [ "$retry_h" -le 60 ]; then
        printf '%s\n' "FAIL: Retry-After=$retry_h, expected 1..60 (rule window)" >&2
        return 1
    fi
    # Reset is a unix timestamp; sanity-check it is "now-ish" (within
    # ±5 minutes of the local clock) so we catch a stale or absurd value.
    now_unix="$(date +%s)"
    delta=$((reset_h - now_unix))
    if [ "$delta" -lt 0 ]; then
        delta=$((-delta))
    fi
    if [ "$delta" -gt 300 ]; then
        printf '%s\n' "FAIL: X-RateLimit-Reset=$reset_h is too far from now=$now_unix (delta=$delta s)" >&2
        return 1
    fi
}

assert_cluster_rate_limit() {
    # The replicas=3 path: the rule's 2r/min budget on $uri is shared
    # across every pod via gossip, so the assertion is about the
    # cluster-wide aggregate, not any one pod. Route through the
    # ClusterIP Service so curl gets round-robined across the replica
    # set, sleep enough for gossip to propagate the running budget, and
    # confirm that the first 2 of 5 requests succeed and the rest are
    # rejected — same arithmetic as the single-pod case at N=1, just
    # observed through the load balancer.
    start_service_port_forward

    # Give the just-started pods one or two gossip ticks to discover each
    # other and converge on a shared aggregate. tick_interval default is
    # 100 ms; bench manifests use 100 ms too — a couple of seconds is
    # ample.
    sleep 2

    headers_4="$(mktemp)"
    headers_5="$(mktemp)"
    c1="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
    c2="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
    c3="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
    c4="$(curl -sS -D "$headers_4" -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
    c5="$(curl -sS -D "$headers_5" -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"

    allowed=0
    rejected=0
    for code in "$c1" "$c2" "$c3" "$c4" "$c5"; do
        case "$code" in
            200) allowed=$((allowed + 1)) ;;
            429) rejected=$((rejected + 1)) ;;
        esac
    done

    printf '\n  cluster-wide rate-limit (svc/gabion-nginx, 5 requests):\n'
    printf '    codes: %s %s %s %s %s\n' "$c1" "$c2" "$c3" "$c4" "$c5"
    printf '    allowed=%d  rejected=%d (expect allowed=2 rejected=3)\n' "$allowed" "$rejected"

    if [ "$allowed" != 2 ] || [ "$rejected" != 3 ]; then
        printf '%s\n' "FAIL: cluster-wide /api budget mismatch: allowed=$allowed rejected=$rejected, expected 2/3" >&2
        rm -f "$headers_4" "$headers_5"
        return 1
    fi

    # The 4th and 5th request must both be 429 (the rule limit is 2),
    # so either response is a valid carrier of the rate-limit headers.
    # Pick whichever one was a 429 — under N=3 gossip can race the very
    # first second so we tolerate the budget being burned slightly
    # later than position 3 in the burst.
    if [ "$c4" = "429" ]; then
        rej_headers="$headers_4"
    else
        rej_headers="$headers_5"
    fi

    printf '\n  rate-limit headers on cluster-wide 429:\n'
    grep -iE '^(X-RateLimit-Limit|X-RateLimit-Remaining|X-RateLimit-Reset|Retry-After):' \
        "$rej_headers" | sed 's/^/    /' || true

    limit_h="$(grep -i '^X-RateLimit-Limit:' "$rej_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    remaining_h="$(grep -i '^X-RateLimit-Remaining:' "$rej_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    reset_h="$(grep -i '^X-RateLimit-Reset:' "$rej_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"
    retry_h="$(grep -i '^Retry-After:' "$rej_headers" | head -n1 | awk '{print $2}' | tr -d '\r')"

    rm -f "$headers_4" "$headers_5"

    if [ -z "$limit_h" ] || [ -z "$remaining_h" ] || [ -z "$reset_h" ] || [ -z "$retry_h" ]; then
        printf '%s\n' "FAIL: missing rate-limit header on cluster-wide 429" >&2
        return 1
    fi
    if [ "$limit_h" != "2" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Limit=$limit_h, expected 2" >&2
        return 1
    fi
    if [ "$remaining_h" != "0" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Remaining=$remaining_h, expected 0" >&2
        return 1
    fi
    if ! [ "$retry_h" -ge 1 ] || ! [ "$retry_h" -le 60 ]; then
        printf '%s\n' "FAIL: Retry-After=$retry_h, expected 1..60 (rule window)" >&2
        return 1
    fi
    now_unix="$(date +%s)"
    delta=$((reset_h - now_unix))
    if [ "$delta" -lt 0 ]; then
        delta=$((-delta))
    fi
    if [ "$delta" -gt 300 ]; then
        printf '%s\n' "FAIL: X-RateLimit-Reset=$reset_h is too far from now=$now_unix (delta=$delta s)" >&2
        return 1
    fi
}

summarize_pod() {
    pod="$1"
    printf '\n=== pod %s ===\n' "$pod"

    printf '\n-- nginx error log (gabion tracing + leader status) --\n'
    kubectl -n "$namespace" logs --tail=120 "$pod" 2>&1 \
        | grep -iE 'gabion|leader|gossip|warn|error|notice' \
        | sed 's/^/  /' \
        | head -n 40 || true

    printf '\n-- pod info --\n'
    kubectl -n "$namespace" get pod "$pod" \
        -o 'jsonpath={.status.podIP}{"\t"}{.spec.nodeName}{"\n"}' \
        | sed 's/^/  ip=node=/'

    # Issue a burst so we can quote total / rejected counts back at the user.
    printf '\n-- burst sample (10 requests against /api) --\n'
    allowed=0
    rejected=0
    cardinality=0
    other=0
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        code="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080/api/index.html")"
        case "$code" in
            200) allowed=$((allowed + 1)) ;;
            429) rejected=$((rejected + 1)) ;;
            400) cardinality=$((cardinality + 1)) ;;
            *)   other=$((other + 1)) ;;
        esac
    done
    printf '  allowed=%d  rejected=%d  cardinality=%d  other=%d\n' \
        "$allowed" "$rejected" "$cardinality" "$other"
}

kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=120s
wait_for_endpoint_count 1
pods="$(wait_for_pods 1)"
for pod in $pods; do
    assert_pod_rate_limit "$pod"
    summarize_pod "$pod"
done

kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=0
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=120s
wait_for_endpoint_count 0

kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=3
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=120s
wait_for_endpoint_count 3
pods="$(wait_for_pods 3)"

# At N>1 the rule's 2r/min budget on $uri is shared across all pods via
# gossip — burning it on one pod rejects on the next. The per-pod
# assertion only holds at N=1; for N=3 we assert the cluster-wide
# aggregate via the ClusterIP Service.
assert_cluster_rate_limit

# Still pull per-pod diagnostics so an operator can see who participated.
for pod in $pods; do
    start_port_forward "$pod"
    summarize_pod "$pod"
done

# Cluster-wide overview at the end so the operator can confirm everything
# settled into the expected state.
printf '\n=== cluster overview ===\n'
printf '\n-- pods --\n'
kubectl -n "$namespace" get pods -o wide 2>&1 | sed 's/^/  /'
printf '\n-- service & endpoints --\n'
kubectl -n "$namespace" get svc,endpointslice -o wide 2>&1 | sed 's/^/  /'
printf '\n-- recent events --\n'
kubectl -n "$namespace" get events --sort-by=.lastTimestamp 2>&1 \
    | tail -n 12 | sed 's/^/  /' || true

kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=1
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=120s
wait_for_endpoint_count 1

printf '\nlocal kubernetes NGINX EndpointSlice scale and per-pod rate-limit test passed on context %s (%s)\n' \
    "$context" "$server"
