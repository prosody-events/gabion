#!/bin/sh
# End-to-end distributed rate-limit test.
#
# Spins up N pods of the gabion nginx module sharing a cluster-wide
# budget keyed on `?tenant=<i>` across 100 distinct tenants, then drives
# 5x over-budget load from an in-cluster loader Job and reports the
# observed `allowed / expected_allowed` ratio + per-tenant spread.
#
# Tunables (env vars, all optional):
#   POD_COUNT           number of nginx replicas (default 20)
#   TENANTS             distinct tenant keys (default 100)
#   BUDGET_PER_TENANT   per-tenant requests/sec budget (default 20)
#   RPS_PER_TENANT      per-tenant requests/sec attempted (default 100)
#   DURATION_S          sustained measurement seconds (default 60)
#   WARMUP_S            unmeasured warm-up seconds (default 5)
#   KEEP_NAMESPACE=1    leave the ephemeral namespace behind for debug
#   BACKEND=nginx       run nginx/curl path (default)
#   BACKEND=gabiond     run gabiond/gRPC path
#
# The script refuses to run outside the local orbstack context so it
# never touches a shared cluster.

set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repo_root"

: "${BACKEND:=nginx}"
if [ "$BACKEND" = "gabiond" ]; then
    exec sh ./deploy/kubernetes/gabiond-distributed-rate-limit.sh
fi
if [ "$BACKEND" != "nginx" ]; then
    printf '%s\n' "unknown BACKEND='$BACKEND' (expected nginx or gabiond)" >&2
    exit 1
fi

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

: "${POD_COUNT:=20}"
: "${TENANTS:=100}"
: "${BUDGET_PER_TENANT:=20}"
: "${RPS_PER_TENANT:=100}"
: "${DURATION_S:=60}"
: "${WARMUP_S:=5}"
: "${KEEP_NAMESPACE:=0}"

namespace="gabion-nginx-dist-$$"

cleanup() {
    if [ "$KEEP_NAMESPACE" = "1" ]; then
        printf '\nleaving namespace %s in place for debug (KEEP_NAMESPACE=1)\n' "$namespace"
        return
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true --wait=false
}
trap cleanup EXIT

# Ensure both images are present:
# - the nginx module image (gabion baked in) is what the load is driven against
# - gabiond is the gossip peer + admin endpoint we use for cluster-wide
#   diagnostics; including it always means the test always has a
#   neutral observer that can report the aggregate truth even when the
#   nginx pods themselves drop cells locally.
docker compose --profile module -f deploy/nginx/docker-compose.yml build nginx-module-request-smoke
docker build -f deploy/gabiond/Dockerfile -t gabiond:local .
docker build -f deploy/kubernetes/loader.Dockerfile -t gabion-loader:local .

kubectl create namespace "$namespace"

# nginx conf override + gabiond config go in via
# ConfigMaps so we don't need to rebuild images to vary test parameters.
kubectl -n "$namespace" create configmap nginx-conf \
    --from-file=nginx.conf=deploy/nginx/nginx.distributed.conf

# gabiond config. Storage + gossip settings deliberately omitted so the
# server uses its built-in defaults (see crates/server/src/config.rs) —
# the nginx module's defaults now mirror these one-for-one, so the two
# sides gossip with identical tuning out of the box.
kubectl -n "$namespace" create configmap gabiond-config --from-file=config.yaml=/dev/stdin <<YAML
envoy_bind: 0.0.0.0:8081
admin_bind: 0.0.0.0:9090
gossip:
  bind: 0.0.0.0:9000
discovery:
  namespace_whitelist: ["$namespace"]
limits:
  - name: tenant_dist
    domain: nginx
    descriptors:
      - key: tenant
        value: "*"
    limit: ${BUDGET_PER_TENANT}
    window: 1s
    bucket: 1s
    mode: enforce
YAML

# RBAC + Deployment + Services. Same shape as nginx-scale-rate-limit.sh:
# bind discovery permissions to the namespace's default SA, run pods
# with no `serviceAccountName` override.
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
  name: gabion-nginx
spec:
  replicas: ${POD_COUNT}
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
          volumeMounts:
            - name: nginx-conf
              mountPath: /etc/nginx/nginx.conf
              subPath: nginx.conf
          resources:
            requests:
              cpu: "100m"
              memory: "64Mi"
            limits:
              cpu: "500m"
              memory: "256Mi"
      volumes:
        - name: nginx-conf
          configMap:
            name: nginx-conf
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
      protocol: TCP
---
apiVersion: v1
kind: Service
metadata:
  name: gabion
spec:
  # Headless: the EndpointSlice the discovery watches lists each replica's
  # pod IP directly, which is what gossip needs in order to address peers
  # individually. The port name + protocol below are load-bearing — the
  # discovery filter is 'name == "gabion" && protocol == "UDP"'
  # (crates/gabion/src/discovery/kubernetes.rs:311-313).
  clusterIP: None
  selector:
    app: gabion-nginx
  ports:
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
---
# gabiond Deployment. Runs alongside the nginx pods so the test always
# has a neutral observer in the gossip cluster — its admin endpoint
# reports the cluster-wide CellStore truth regardless of whether
# individual nginx pods locally overflow their stores.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabiond
spec:
  replicas: 1
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
          resources:
            requests:
              cpu: "100m"
              memory: "128Mi"
            limits:
              cpu: "1000m"
              memory: "512Mi"
      volumes:
        - name: config
          configMap:
            name: gabiond-config
---
# gabiond Service. Two pieces:
#   - the 'gabion' UDP port satisfies the same discovery filter as the
#     headless nginx service above, so the EndpointSlice for this
#     Service contributes the gabiond pod's IP into the gossip mesh
#     alongside the nginx pods.
#   - the 'admin' TCP port exposes /snapshot for diagnostics; the test
#     port-forwards to it to read CellStoreStats after the load run.
apiVersion: v1
kind: Service
metadata:
  name: gabiond
spec:
  clusterIP: None
  selector:
    app: gabiond
  ports:
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
    - name: admin
      port: 9090
      targetPort: admin
      protocol: TCP
    - name: envoy
      port: 8081
      targetPort: envoy
      protocol: TCP
YAML

wait_for_endpoint_count() {
    expected="$1"
    timeout="${2:-180}"
    attempts=0
    while [ "$attempts" -lt "$timeout" ]; do
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

printf '\nwaiting for %s nginx pods + gabiond to roll out…\n' "$POD_COUNT"
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=300s
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
wait_for_endpoint_count "$POD_COUNT"

# Give gossip a moment to converge before sending the warm-up batch.
# Each pod logs "Peer joined" lines as it learns peers; sleeping past
# that quiets the early portion of the test. Convergence at this scale
# takes longer than the previous nginx-only test — the gabiond peer +
# the larger pod count both push the time-to-stable.
printf '\nletting gossip converge for 15s…\n'
sleep 15

# In-cluster Job loader.
printf '\nlaunching in-cluster loader (target %s tenants × %s r/s for %ss after %ss warm-up)…\n' \
    "$TENANTS" "$RPS_PER_TENANT" "$DURATION_S" "$WARMUP_S"

# Job manifest. backoffLimit=0 so a failed loader doesn't get retried.
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
              value: "http"
            - name: TARGET_URL
              value: "http://gabion-nginx:8080/tenant/index.html?tenant={tenant}"
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
              value: "1000"
            - name: ALIGN_WINDOW
              value: "1"
          resources:
            requests:
              cpu: "500m"
              memory: "128Mi"
            limits:
              cpu: "2000m"
              memory: "512Mi"
YAML

# Wait for the loader to complete. Timeout is generous: warmup +
# duration + 60s slack for image pull and cleanup.
loader_timeout=$((WARMUP_S + DURATION_S + 120))
printf 'waiting up to %ss for the loader Job to complete…\n' "$loader_timeout"

# kubectl wait --for=condition=complete blocks until the Job's status
# reports complete=True. Capture failure case separately so we can dump
# logs either way.
wait_status=0
kubectl -n "$namespace" wait --for=condition=complete \
    --timeout="${loader_timeout}s" job/loader 2>/dev/null || wait_status=$?

if [ "$wait_status" -ne 0 ]; then
    # Maybe it failed instead of completing — print whatever logs exist.
    printf '\nloader Job did not reach Complete; current state:\n'
    kubectl -n "$namespace" get job/loader -o wide || true
    kubectl -n "$namespace" describe job/loader || true
fi

loader_pod="$(kubectl -n "$namespace" get pods -l job-name=loader \
    -o 'jsonpath={.items[0].metadata.name}')"

# The loader prints both a human-readable summary and a JSON block
# between sentinels. Dump the whole log; the summary is at the bottom.
printf '\n=== loader log ===\n'
kubectl -n "$namespace" logs "$loader_pod" | sed 's/^/  /'

# Sanity-check that the loader actually produced the summary block; if
# not, exit non-zero so CI catches it.
if ! kubectl -n "$namespace" logs "$loader_pod" \
        | grep -q '^---LOADER-SUMMARY-END---$'; then
    printf '\nFAIL: loader did not emit a summary block\n' >&2
    exit 1
fi
printf '\n  (pods=%s)\n' "$POD_COUNT"

# Pull cluster-wide truth from the gabiond /snapshot endpoint. Even when
# individual nginx pods overflow their local CellStore and silently fail
# open, the gabiond aggregates the cells it receives over gossip into a
# config-sized 131072-cell store — so its snapshot is the most trusted
# observer of cluster-wide state.
printf '\n=== gabiond /snapshot ===\n'
gabiond_pod="$(kubectl -n "$namespace" get pods -l app=gabiond \
    -o 'jsonpath={.items[0].metadata.name}')"
admin_pf_log="/tmp/gabion-dist-admin-port-forward.log"
kubectl -n "$namespace" port-forward "pod/$gabiond_pod" 19090:9090 \
    >"$admin_pf_log" 2>&1 &
admin_pf_pid=$!
trap 'kill "$admin_pf_pid" 2>/dev/null || true; cleanup' EXIT

# Wait for the port-forward to come up so the first curl doesn't race it.
attempts=0
while [ "$attempts" -lt 30 ]; do
    if curl -fsS -o /dev/null "http://127.0.0.1:19090/snapshot" 2>/dev/null; then
        break
    fi
    attempts=$((attempts + 1))
    sleep 1
done

if curl -fsS "http://127.0.0.1:19090/snapshot" >/tmp/gabion-dist-snapshot.json 2>/dev/null; then
    # Pretty-print and highlight the headline counters; full JSON follows.
    python3 - <<'PY'
import json
with open("/tmp/gabion-dist-snapshot.json") as f:
    snap = json.load(f)
store = snap.get("store", {})
cell_store = store.get("cell_store", {})
peers = snap.get("peers") or []
print(f"  known_peers                {len(peers)}")
print(f"  aggregate_rows             {store.get('aggregate_rows', '?')}")
print(f"  active_cells               {cell_store.get('active_cells', '?')}")
print(f"  cell_capacity              {cell_store.get('cell_capacity', '?')}")
print(f"  cell_store_full_rejects    {cell_store.get('cell_store_full_rejects', '?')}")
print(f"  rule_dict_full_rejects     {cell_store.get('rule_dictionary_full_rejects', '?')}")
print(f"  node_dict_full_rejects     {cell_store.get('node_dictionary_full_rejects', '?')}")
PY
    printf '\n  -- full snapshot --\n'
    sed 's/^/    /' /tmp/gabion-dist-snapshot.json | head -n 60
else
    printf '  FAIL: could not reach gabiond admin endpoint\n' >&2
    cat "$admin_pf_log" >&2 || true
fi
kill "$admin_pf_pid" 2>/dev/null || true
wait "$admin_pf_pid" 2>/dev/null || true
admin_pf_pid=""

# Cluster overview at the end. Skim a few pod logs for gabion lines.
printf '\n=== cluster overview ===\n'
printf '\n-- pods --\n'
kubectl -n "$namespace" get pods -o wide 2>&1 | sed 's/^/  /' | head -40
printf '\n-- service & endpoints --\n'
kubectl -n "$namespace" get svc,endpointslice -o wide 2>&1 | sed 's/^/  /' | head -30

printf '\n-- gabiond log highlights --\n'
kubectl -n "$namespace" logs --tail=200 "deployment/gabiond" 2>&1 \
    | grep -iE 'cluster|peer joined|peer accepted|cell|error|warn|too many' \
    | sed 's/^/    /' | head -n 20 || true

printf '\n-- nginx log highlights (first 3 pods) --\n'
n=0
for pod in $(kubectl -n "$namespace" get pods -l app=gabion-nginx -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'); do
    n=$((n + 1))
    if [ "$n" -gt 3 ]; then
        break
    fi
    printf '\n  --- %s ---\n' "$pod"
    kubectl -n "$namespace" logs --tail=100 "$pod" 2>&1 \
        | grep -iE 'cluster_size|peer joined|peer accepted|leader|error|warn|too many' \
        | sed 's/^/    /' \
        | head -n 12 || true
done

printf '\ndistributed rate-limit test finished on context %s (%s)\n' "$context" "$server"
