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

namespace="gabion-nginx-scale-$$"
port_forward_pid=""

cleanup() {
    if [ -n "$port_forward_pid" ]; then
        kill "$port_forward_pid" 2>/dev/null || true
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true
}
trap cleanup EXIT

docker compose --profile module -f deploy/nginx/docker-compose.yml build nginx-module-request-smoke

kubectl create namespace "$namespace"

kubectl -n "$namespace" apply -f - <<'YAML'
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
            - name: gossip
              containerPort: 9000
---
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
    - name: gossip
      port: 9000
      targetPort: gossip
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
    # Per the rule in deploy/nginx/nginx.module.conf: uri_api 2r/m.
    if [ "$limit_h" != "2" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Limit=$limit_h, expected 2" >&2
        return 1
    fi
    if [ "$remaining_h" != "0" ]; then
        printf '%s\n' "FAIL: X-RateLimit-Remaining=$remaining_h, expected 0" >&2
        return 1
    fi
    # Reset / Retry-After are in seconds; should be > 0 and not absurdly large.
    if [ "$reset_h" -le 0 ] 2>/dev/null; then
        printf '%s\n' "FAIL: X-RateLimit-Reset=$reset_h is not a positive integer" >&2
        return 1
    fi
    if [ "$retry_h" -le 0 ] 2>/dev/null; then
        printf '%s\n' "FAIL: Retry-After=$retry_h is not a positive integer" >&2
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
for pod in $pods; do
    assert_pod_rate_limit "$pod"
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
