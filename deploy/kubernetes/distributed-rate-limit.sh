#!/bin/sh
# End-to-end distributed rate-limit test.
#
# BACKEND=nginx runs the nginx module over HTTP and includes one gabiond
# observer for admin snapshots. BACKEND=gabiond runs a gabiond cluster and
# drives the Envoy-compatible gRPC rate-limit service directly.

set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repo_root"

: "${BACKEND:=nginx}"
case "$BACKEND" in
    nginx|gabiond) ;;
    *)
        printf '%s\n' "unknown BACKEND='$BACKEND' (expected nginx or gabiond)" >&2
        exit 1
        ;;
esac

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

: "${POD_COUNT:=$([ "$BACKEND" = "gabiond" ] && printf 3 || printf 20)}"
: "${TENANTS:=$([ "$BACKEND" = "gabiond" ] && printf 20 || printf 100)}"
: "${BUDGET_PER_TENANT:=20}"
: "${RPS_PER_TENANT:=100}"
: "${DURATION_S:=$([ "$BACKEND" = "gabiond" ] && printf 20 || printf 60)}"
: "${WARMUP_S:=$([ "$BACKEND" = "gabiond" ] && printf 2 || printf 5)}"
: "${RATE_LIMIT_WINDOW:=1s}"
: "${RATE_LIMIT_BUCKET:=1s}"
: "${WINDOW_MS:=1000}"
: "${GOSSIP_TICK_INTERVAL:=100ms}"
: "${GOSSIP_FANOUT:=6}"
: "${GOSSIP_TARGET_ERR_BPS:=100}"
: "${GOSSIP_MIN_EMIT_INTERVAL:=5ms}"
: "${KEEP_NAMESPACE:=0}"

namespace="gabion-${BACKEND}-dist-$$"
admin_pf_pid=""
snapshot_path="/tmp/gabion-${BACKEND}-dist-snapshot.json"
admin_pf_log="/tmp/gabion-${BACKEND}-dist-admin-port-forward.log"

cleanup() {
    if [ -n "$admin_pf_pid" ]; then
        kill "$admin_pf_pid" 2>/dev/null || true
        wait "$admin_pf_pid" 2>/dev/null || true
    fi
    if [ "$KEEP_NAMESPACE" = "1" ]; then
        printf '\nleaving namespace %s in place for debug (KEEP_NAMESPACE=1)\n' "$namespace"
        return
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true --wait=false
}
trap cleanup EXIT

build_images() {
    if [ "$BACKEND" = "nginx" ]; then
        docker compose --profile module -f deploy/nginx/docker-compose.yml build nginx-module-request-smoke
    fi
    docker build -f deploy/gabiond/Dockerfile -t gabiond:local .
    docker build -f deploy/kubernetes/loader.Dockerfile -t gabion-loader:local .
}

create_configmaps() {
    if [ "$BACKEND" = "nginx" ]; then
        awk \
            -v budget="$BUDGET_PER_TENANT" \
            -v window="$RATE_LIMIT_WINDOW" \
            -v bucket="$RATE_LIMIT_BUCKET" \
            -v fanout="$GOSSIP_FANOUT" \
            -v tick="$GOSSIP_TICK_INTERVAL" \
            -v target_err_bps="$GOSSIP_TARGET_ERR_BPS" \
            -v min_emit="$GOSSIP_MIN_EMIT_INTERVAL" '
                /gabion_limit_rule tenant_dist / {
                    sub(/tenant_dist [0-9]+r\/s/, "tenant_dist " budget "r/s")
                    sub(/window=[^ ]+/, "window=" window)
                    sub(/bucket=[^ ;]+/, "bucket=" bucket)
                }
                /gabion_gossip_fanout [0-9]+;/ {
                    sub(/gabion_gossip_fanout [0-9]+;/, "gabion_gossip_fanout " fanout ";")
                    print
                    print "    gabion_gossip_tick_interval " tick ";"
                    print "    gabion_gossip_target_err_bps " target_err_bps ";"
                    print "    gabion_gossip_min_emit_interval " min_emit ";"
                    next
                }
                { print }
            ' deploy/nginx/nginx.distributed.conf \
            | kubectl -n "$namespace" create configmap nginx-conf --from-file=nginx.conf=/dev/stdin
    fi

    kubectl -n "$namespace" create configmap gabiond-config --from-file=config.yaml=/dev/stdin <<YAML
envoy_bind: 0.0.0.0:8081
admin_bind: 0.0.0.0:9090
gossip:
  bind: 0.0.0.0:9000
  fanout: ${GOSSIP_FANOUT}
  tick_interval: ${GOSSIP_TICK_INTERVAL}
  target_err_bps: ${GOSSIP_TARGET_ERR_BPS}
  min_emit_interval: ${GOSSIP_MIN_EMIT_INTERVAL}
discovery:
  namespace_allow: ["$namespace"]
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
}

apply_common_rbac() {
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
YAML
}

apply_nginx_backend() {
    kubectl -n "$namespace" apply -f - <<YAML
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
  clusterIP: None
  selector:
    app: gabion-nginx
  ports:
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
YAML
    apply_gabiond_deployment 1 Headless
}

apply_gabiond_backend() {
    apply_gabiond_deployment "$POD_COUNT" ClusterIP
}

apply_gabiond_deployment() {
    replicas="$1"
    service_type="$2"
    if [ "$service_type" = "Headless" ]; then
        cluster_ip="clusterIP: None"
    else
        cluster_ip=""
    fi
    kubectl -n "$namespace" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabiond
spec:
  replicas: ${replicas}
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
apiVersion: v1
kind: Service
metadata:
  name: gabiond
spec:
  ${cluster_ip}
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
}

endpoint_count() {
    service="$1"
    kubectl -n "$namespace" get endpointslice \
        -l "kubernetes.io/service-name=$service" \
        -o 'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}' \
        | sed '/^$/d' \
        | wc -l \
        | tr -d ' '
}

wait_for_endpoint_count() {
    service="$1"
    expected="$2"
    timeout="${3:-180}"
    attempts=0
    count=0
    while [ "$attempts" -lt "$timeout" ]; do
        count="$(endpoint_count "$service")"
        if [ "$count" = "$expected" ]; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    printf '%s\n' "timed out waiting for $expected EndpointSlice addresses on $service; saw $count" >&2
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}

loader_env() {
    if [ "$BACKEND" = "nginx" ]; then
        cat <<YAML
            - name: BACKEND
              value: "http"
            - name: TARGET_URL
              value: "http://gabion-nginx:8080/tenant/index.html?tenant={tenant}"
YAML
    else
        cat <<YAML
            - name: BACKEND
              value: "grpc"
            - name: GRPC_ADDR
              value: "gabiond:8081"
            - name: DOMAIN
              value: "nginx"
            - name: DESCRIPTOR_KEY
              value: "tenant"
YAML
    fi
}

apply_loader() {
    backend_env="$(loader_env)"
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
${backend_env}
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
            - name: HTTP_CONNECTIONS
              value: "${POD_COUNT}"
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
}

show_loader_log() {
    loader_pod="$(kubectl -n "$namespace" get pods -l job-name=loader -o 'jsonpath={.items[0].metadata.name}')"
    printf '\n=== loader log ===\n'
    kubectl -n "$namespace" logs "$loader_pod" | sed 's/^/  /'
    if ! kubectl -n "$namespace" logs "$loader_pod" | grep -q '^---LOADER-SUMMARY-END---$'; then
        printf '\nFAIL: loader did not emit a summary block\n' >&2
        exit 1
    fi
}

show_snapshot() {
    printf '\n=== gabiond /snapshot ===\n'
    gabiond_pod="$(kubectl -n "$namespace" get pods -l app=gabiond -o 'jsonpath={.items[0].metadata.name}')"
    kubectl -n "$namespace" port-forward "pod/$gabiond_pod" 19090:9090 >"$admin_pf_log" 2>&1 &
    admin_pf_pid=$!

    attempts=0
    while [ "$attempts" -lt 30 ]; do
        if curl -fsS -o /dev/null "http://127.0.0.1:19090/snapshot" 2>/dev/null; then
            break
        fi
        attempts=$((attempts + 1))
        sleep 1
    done

    if curl -fsS "http://127.0.0.1:19090/snapshot" >"$snapshot_path" 2>/dev/null; then
        python3 - "$snapshot_path" <<'PY'
import json
import sys
with open(sys.argv[1]) as f:
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
print(f"  decode_reject_count        {snap.get('decode_reject_count', '?')}")
PY
        printf '\n  -- full snapshot --\n'
        sed 's/^/    /' "$snapshot_path" | head -n 60
    else
        printf '  FAIL: could not reach gabiond admin endpoint\n' >&2
        cat "$admin_pf_log" >&2 || true
    fi

    kill "$admin_pf_pid" 2>/dev/null || true
    wait "$admin_pf_pid" 2>/dev/null || true
    admin_pf_pid=""
}

show_cluster_overview() {
    printf '\n=== cluster overview ===\n'
    kubectl -n "$namespace" get pods -o wide 2>&1 | sed 's/^/  /' | head -40
    printf '\n-- service & endpoints --\n'
    kubectl -n "$namespace" get svc,endpointslice -o wide 2>&1 | sed 's/^/  /' | head -40
    printf '\n-- gabiond log highlights --\n'
    kubectl -n "$namespace" logs --tail=200 "deployment/gabiond" 2>&1 \
        | grep -iE 'cluster|peer joined|peer accepted|cell|error|warn|overflow|too many' \
        | sed 's/^/    /' | head -40 || true
    if [ "$BACKEND" = "nginx" ]; then
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
                | sed 's/^/    /' | head -12 || true
        done
    fi
}

build_images
kubectl create namespace "$namespace"
create_configmaps
apply_common_rbac
if [ "$BACKEND" = "nginx" ]; then
    apply_nginx_backend
    printf '\nwaiting for %s nginx pods + gabiond to roll out...\n' "$POD_COUNT"
    kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=300s
    kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
    wait_for_endpoint_count gabion-nginx "$POD_COUNT"
else
    apply_gabiond_backend
    printf '\nwaiting for %s gabiond pods to roll out...\n' "$POD_COUNT"
    kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
    wait_for_endpoint_count gabiond "$POD_COUNT"
fi

printf '\nletting gossip converge for 15s...\n'
sleep 15

printf '\nlaunching %s loader (%s tenants x %s r/s for %ss after %ss warm-up)...\n' \
    "$BACKEND" "$TENANTS" "$RPS_PER_TENANT" "$DURATION_S" "$WARMUP_S"
apply_loader

loader_timeout=$((WARMUP_S + DURATION_S + 180))
printf 'waiting up to %ss for the loader Job to complete...\n' "$loader_timeout"
wait_status=0
kubectl -n "$namespace" wait --for=condition=complete --timeout="${loader_timeout}s" job/loader 2>/dev/null || wait_status=$?
if [ "$wait_status" -ne 0 ]; then
    printf '\nloader Job did not reach Complete; current state:\n'
    kubectl -n "$namespace" get job/loader -o wide || true
    kubectl -n "$namespace" describe job/loader || true
    kubectl -n "$namespace" get pods -l job-name=loader -o wide || true
fi

show_loader_log
printf '\n  (backend=%s pods=%s)\n' "$BACKEND" "$POD_COUNT"
show_snapshot
show_cluster_overview

printf '\ndistributed rate-limit test finished on context %s (%s)\n' "$context" "$server"
