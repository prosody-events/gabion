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
    https://127.0.0.1:*|https://localhost:*) ;;
    *)
        printf '%s\n' "refusing to run: kubernetes API server is '$server', expected localhost" >&2
        exit 1
        ;;
esac

: "${POD_COUNT:=3}"
: "${TENANTS:=20}"
: "${BUDGET_PER_TENANT:=20}"
: "${RPS_PER_TENANT:=100}"
: "${DURATION_S:=20}"
: "${WARMUP_S:=2}"
: "${RATE_LIMIT_WINDOW:=1s}"
: "${RATE_LIMIT_BUCKET:=1s}"
: "${WINDOW_MS:=1000}"
: "${GOSSIP_TICK_INTERVAL:=100ms}"
: "${KEEP_NAMESPACE:=0}"

namespace="gabiond-dist-$$"

cleanup() {
    if [ "$KEEP_NAMESPACE" = "1" ]; then
        printf '\nleaving namespace %s in place for debug (KEEP_NAMESPACE=1)\n' "$namespace"
        return
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true --wait=false
}
trap cleanup EXIT

docker build -f deploy/gabiond/Dockerfile -t gabiond:local .
docker build -f deploy/kubernetes/loader.Dockerfile -t gabion-loader:local .

kubectl create namespace "$namespace"

kubectl -n "$namespace" create configmap gabiond-config --from-file=config.yaml=/dev/stdin <<YAML
envoy_bind: 0.0.0.0:8081
admin_bind: 0.0.0.0:9090
gossip:
  bind: 0.0.0.0:9000
  tick_interval: ${GOSSIP_TICK_INTERVAL}
discovery:
  namespace_whitelist: ["$namespace"]
limits:
  - name: tenant_dist
    domain: nginx
    descriptors:
      - key: tenant
        value: "*"
    limit: ${BUDGET_PER_TENANT}
    window: ${RATE_LIMIT_WINDOW}
    bucket: ${RATE_LIMIT_BUCKET}
    mode: enforce
YAML

kubectl -n "$namespace" apply -f - <<YAML
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
  name: gabiond
spec:
  replicas: ${POD_COUNT}
  selector:
    matchLabels:
      app: gabiond
  template:
    metadata:
      labels:
        app: gabiond
    spec:
      containers:
        - name: gabiond
          image: gabiond:local
          imagePullPolicy: Never
          args: ["/etc/gabion/config.yaml"]
          ports:
            - name: envoy
              containerPort: 8081
              protocol: TCP
            - name: admin
              containerPort: 9090
              protocol: TCP
            - name: gabion
              containerPort: 9000
              protocol: UDP
          volumeMounts:
            - name: config
              mountPath: /etc/gabion
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: gabiond-config
---
apiVersion: v1
kind: Service
metadata:
  name: gabiond
spec:
  selector:
    app: gabiond
  ports:
    - name: envoy
      port: 8081
      targetPort: envoy
      protocol: TCP
    - name: admin
      port: 9090
      targetPort: admin
      protocol: TCP
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
YAML

wait_for_endpoint_count() {
    expected="$1"
    timeout="${2:-180}"
    attempts=0
    while [ "$attempts" -lt "$timeout" ]; do
        count="$(
            kubectl -n "$namespace" get endpointslice \
                -l kubernetes.io/service-name=gabiond \
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
    printf '%s\n' "timed out waiting for $expected gabiond EndpointSlice addresses; saw $count" >&2
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}

printf '\nwaiting for %s gabiond pods to roll out...\n' "$POD_COUNT"
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
wait_for_endpoint_count "$POD_COUNT"

printf '\nletting gossip converge for 15s...\n'
sleep 15

printf '\nlaunching rust gRPC loader (%s tenants × %s r/s for %ss after %ss warm-up)...\n' \
    "$TENANTS" "$RPS_PER_TENANT" "$DURATION_S" "$WARMUP_S"

kubectl -n "$namespace" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: loader
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: loader
          image: gabion-loader:local
          imagePullPolicy: Never
          env:
            - name: BACKEND
              value: "grpc"
            - name: GRPC_ADDR
              value: "gabiond:8081"
            - name: DOMAIN
              value: "nginx"
            - name: DESCRIPTOR_KEY
              value: "tenant"
            - name: TENANTS
              value: "${TENANTS}"
            - name: RPS_PER_TENANT
              value: "${RPS_PER_TENANT}"
            - name: BUDGET_PER_TENANT
              value: "${BUDGET_PER_TENANT}"
            - name: DURATION_S
              value: "${DURATION_S}"
            - name: WARMUP_S
              value: "${WARMUP_S}"
            - name: WINDOW_MS
              value: "${WINDOW_MS}"
            - name: ALIGN_WINDOW
              value: "1"
YAML

loader_timeout=$((WARMUP_S + DURATION_S + 180))
kubectl -n "$namespace" wait --for=condition=complete \
    --timeout="${loader_timeout}s" job/loader || {
        printf '\nloader did not reach Complete; current state:\n'
        kubectl -n "$namespace" get job/loader -o wide || true
        kubectl -n "$namespace" describe job/loader || true
        kubectl -n "$namespace" get pods -l job-name=loader -o wide || true
    }

loader_pod="$(kubectl -n "$namespace" get pods -l job-name=loader \
    -o 'jsonpath={.items[0].metadata.name}')"
printf '\n=== loader log ===\n'
kubectl -n "$namespace" logs "$loader_pod" | sed 's/^/  /'

if ! kubectl -n "$namespace" logs "$loader_pod" | grep -q '^---LOADER-SUMMARY-END---$'; then
    printf '\nFAIL: loader did not emit a summary block\n' >&2
    exit 1
fi

printf '\n=== gabiond /snapshot ===\n'
gabiond_pod="$(kubectl -n "$namespace" get pods -l app=gabiond \
    -o 'jsonpath={.items[0].metadata.name}')"
admin_pf_log="/tmp/gabiond-dist-admin-port-forward.log"
kubectl -n "$namespace" port-forward "pod/$gabiond_pod" 19090:9090 \
    >"$admin_pf_log" 2>&1 &
admin_pf_pid=$!
trap 'kill "$admin_pf_pid" 2>/dev/null || true; cleanup' EXIT

attempts=0
while [ "$attempts" -lt 30 ]; do
    if curl -fsS -o /dev/null "http://127.0.0.1:19090/snapshot" 2>/dev/null; then
        break
    fi
    attempts=$((attempts + 1))
    sleep 1
done

if curl -fsS "http://127.0.0.1:19090/snapshot" >/tmp/gabiond-dist-snapshot.json 2>/dev/null; then
    python3 - <<'PY'
import json
with open("/tmp/gabiond-dist-snapshot.json") as f:
    snap = json.load(f)
store = snap.get("store", {})
cell_store = store.get("cell_store", {})
print(f"  known_peers                {len(snap.get('peers') or [])}")
print(f"  aggregate_rows             {store.get('aggregate_rows', '?')}")
print(f"  active_cells               {cell_store.get('active_cells', '?')}")
print(f"  cell_capacity              {cell_store.get('cell_capacity', '?')}")
print(f"  cell_store_full_rejects    {cell_store.get('cell_store_full_rejects', '?')}")
print(f"  rule_dict_full_rejects     {cell_store.get('rule_dictionary_full_rejects', '?')}")
print(f"  node_dict_full_rejects     {cell_store.get('node_dictionary_full_rejects', '?')}")
PY
else
    printf '  FAIL: could not reach gabiond admin endpoint\n' >&2
    cat "$admin_pf_log" >&2 || true
fi

kill "$admin_pf_pid" 2>/dev/null || true
wait "$admin_pf_pid" 2>/dev/null || true
admin_pf_pid=""

printf '\n-- gabiond log highlights --\n'
kubectl -n "$namespace" logs --tail=200 "deployment/gabiond" 2>&1 \
    | grep -iE 'cluster|peer joined|peer accepted|cell|error|warn|overflow|too many' \
    | sed 's/^/    /' | head -n 40 || true

printf '\ngabiond distributed rate-limit test finished on context %s (%s)\n' "$context" "$server"
