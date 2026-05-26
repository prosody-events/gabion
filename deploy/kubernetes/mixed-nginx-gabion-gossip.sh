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
    https://127.0.0.1:*|https://localhost:*) ;;
    *)
        printf '%s\n' "refusing to run: kubernetes API server is '$server', expected localhost" >&2
        exit 1
        ;;
esac

namespace="gabion-mixed-$$"
admin_forward_pid=""
nginx_forward_pid=""

cleanup() {
    if [ -n "$admin_forward_pid" ]; then
        kill "$admin_forward_pid" 2>/dev/null || true
    fi
    if [ -n "$nginx_forward_pid" ]; then
        kill "$nginx_forward_pid" 2>/dev/null || true
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true
}
trap cleanup EXIT

docker compose --profile module -f deploy/nginx/docker-compose.yml build nginx-module-request-smoke
docker build -f deploy/gabiond/Dockerfile -t gabiond:local .

kubectl create namespace "$namespace"

kubectl -n "$namespace" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gabion
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gabion-endpointslice-reader
rules:
  - apiGroups: [""]
    resources: ["pods", "services"]
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
    name: gabion
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: gabion-endpointslice-reader
---
# gabiond server config in the new \`AppConfig\` schema. Admin HTTP at :9090,
# envoy gRPC at :8081, gossip UDP at :9000.
apiVersion: v1
kind: ConfigMap
metadata:
  name: gabiond-config
data:
  config.yaml: |
    envoy_bind: 0.0.0.0:8081
    admin_bind: 0.0.0.0:9090
    storage:
      max_cells: 4096
      rule_dictionary_capacity: 64
      node_dictionary_capacity: 256
      local_dirty_capacity: 2048
      forwarded_dirty_capacity: 8192
      peer_capacity: 64
      max_descriptor_count: 16
      max_descriptor_bytes: 512
      max_key_bytes: 128
    runtime:
      rng_seed: 12345
    discovery:
      namespace_allow: ["$namespace"]
      service_allow: ["gabiond", "gabion-nginx"]
    gossip:
      bind: 0.0.0.0:9000
      tick_interval: 100ms
      fanout: 6
      max_payload_bytes: 1400
      max_cells_per_frame: 1024
      max_cells_per_tick: 1024
      send_queue_capacity: 32
      limit_queue_capacity: 1024
      cluster_id_hash: 1
      target_err_bps: 100
      min_emit_interval: 5ms
    limits:
      - name: nginx_uri
        domain: nginx
        descriptors:
          - key: uri
            value: "*"
        rate: 2r/m
        bucket: 1s
        mode: enforce
---
# nginx config using the migrated directive set.
apiVersion: v1
kind: ConfigMap
metadata:
  name: gabion-nginx-config
data:
  nginx.conf: |
    load_module /etc/nginx/modules/ngx_http_gabion_module.so;

    worker_processes 2;
    error_log /dev/stderr info;

    events {
        worker_connections 128;
    }

    http {
        gabion_limit_zone zone=api:128m;
        gabion_limit_rule uri_api \$uri rate=2r/m bucket=1s;
        gabion_gossip_bind 0.0.0.0:9000;
        gabion_gossip_cluster 1;
        gabion_gossip_fanout 6;
        gabion_discovery_namespace_allow $namespace;

        server {
            listen 8080;

            location /api/ {
                gabion_limit uri_api;
                alias /usr/share/nginx/html/;
            }

            location /off/ {
                gabion_limit uri_api;
                gabion off;
                alias /usr/share/nginx/html/;
            }
        }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabiond
spec:
  replicas: 2
  selector:
    matchLabels:
      app: gabiond
  template:
    metadata:
      labels:
        app: gabiond
    spec:
      serviceAccountName: gabion
      containers:
        - name: gabiond
          image: gabiond:local
          imagePullPolicy: Never
          args: ["/etc/gabion/config.yaml"]
          ports:
            - { name: envoy,  containerPort: 8081 }
            - { name: admin,  containerPort: 9090 }
            - { name: gossip, containerPort: 9000, protocol: UDP }
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
    - { name: envoy,  port: 8081, targetPort: envoy }
    - { name: admin,  port: 9090, targetPort: admin }
    - { name: gossip, port: 9000, targetPort: gossip, protocol: UDP }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabion-nginx
spec:
  replicas: 2
  selector:
    matchLabels:
      app: gabion-nginx
  template:
    metadata:
      labels:
        app: gabion-nginx
    spec:
      serviceAccountName: gabion
      containers:
        - name: nginx
          image: nginx-nginx-module-request-smoke:latest
          imagePullPolicy: Never
          ports:
            - { name: http,   containerPort: 8080 }
            - { name: gossip, containerPort: 9000, protocol: UDP }
          volumeMounts:
            - name: config
              mountPath: /etc/nginx/nginx.conf
              subPath: nginx.conf
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: gabion-nginx-config
---
apiVersion: v1
kind: Service
metadata:
  name: gabion-nginx
spec:
  selector:
    app: gabion-nginx
  ports:
    - { name: http,   port: 8080, targetPort: http }
    - { name: gossip, port: 9000, targetPort: gossip, protocol: UDP }
YAML

wait_for_endpoint_count() {
    service="$1"
    expected="$2"
    attempts=0
    while [ "$attempts" -lt 90 ]; do
        count="$(
            kubectl -n "$namespace" get endpointslice \
                -l "kubernetes.io/service-name=$service" \
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
    printf '%s\n' "timed out waiting for $expected EndpointSlice addresses for $service; saw $count" >&2
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}

dump_snapshot() {
    label="$1"
    printf '\n=== gabiond /snapshot (%s) ===\n' "$label"
    if ! curl -fsS "http://127.0.0.1:19090/snapshot" 2>/tmp/gabiond-snapshot.err; then
        printf 'FAILED to fetch /snapshot:\n'
        cat /tmp/gabiond-snapshot.err
        return 1
    fi >/tmp/gabiond-snapshot.json 2>&1
    curl -fsS "http://127.0.0.1:19090/snapshot" \
        | python3 -m json.tool 2>/dev/null \
        | sed 's/^/  /' \
        || curl -fsS "http://127.0.0.1:19090/snapshot" | sed 's/^/  /'
}

assert_snapshot_has_peers() {
    state="$(curl -fsS "http://127.0.0.1:19090/snapshot")"
    if printf '%s' "$state" | python3 -c '
import json, sys
s = json.load(sys.stdin)
peers = s.get("peers", [])
if not peers:
    sys.exit(1)
print(f"peers={len(peers)}")
' >/tmp/peers.txt 2>/dev/null; then
        sed 's/^/  /' /tmp/peers.txt
        return 0
    fi
    printf '%s\n' "FAIL: gabiond /snapshot has no peers — gossip discovery is not working" >&2
    printf '%s\n' "$state" | sed 's/^/  /' >&2
    return 1
}

assert_snapshot_has_aggregate_rows() {
    state="$(curl -fsS "http://127.0.0.1:19090/snapshot")"
    rows="$(printf '%s' "$state" | python3 -c '
import json, sys
print(json.load(sys.stdin)["store"]["aggregate_rows"])
' 2>/dev/null || echo 0)"
    if [ "$rows" -gt 0 ] 2>/dev/null; then
        printf '  store.aggregate_rows=%s\n' "$rows"
        return 0
    fi
    printf '%s\n' "FAIL: gabiond /snapshot store.aggregate_rows=$rows" >&2
    printf '%s\n' "$state" | sed 's/^/  /' >&2
    return 1
}

drive_nginx_traffic() {
    # Burst against /api so the local nginx leader records hits and gossips
    # them to the gabiond peers.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        curl -sS -o /dev/null "http://127.0.0.1:18080/api/index.html" || true
    done
}

kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
wait_for_endpoint_count gabiond 2
wait_for_endpoint_count gabion-nginx 2

kubectl -n "$namespace" port-forward service/gabiond 19090:9090 \
    >/tmp/gabiond-admin-port-forward.log 2>&1 &
admin_forward_pid="$!"
kubectl -n "$namespace" port-forward service/gabion-nginx 18080:8080 \
    >/tmp/gabion-nginx-service-port-forward.log 2>&1 &
nginx_forward_pid="$!"

attempts=0
while [ "$attempts" -lt 60 ]; do
    if curl -fsS -o /dev/null "http://127.0.0.1:19090/snapshot" \
        && curl -fsS -o /dev/null "http://127.0.0.1:18080/off/index.html"; then
        break
    fi
    attempts=$((attempts + 1))
    sleep 1
done

# Initial state — peers may not have discovered each other yet.
dump_snapshot "before traffic"

# Push some traffic through nginx, then give gossip a beat to propagate.
drive_nginx_traffic
sleep 3

# After some gossip ticks the gabiond peers should know about each other
# (they discover via EndpointSlice) and the store should have rows for the
# nginx rule's fingerprint.
attempts=0
while [ "$attempts" -lt 30 ]; do
    if curl -fsS "http://127.0.0.1:19090/snapshot" \
        | python3 -c '
import json, sys
s = json.load(sys.stdin)
if s.get("peers") and s["store"]["aggregate_rows"] > 0:
    sys.exit(0)
sys.exit(1)
' 2>/dev/null; then
        break
    fi
    attempts=$((attempts + 1))
    drive_nginx_traffic
    sleep 1
done

# Now scale both deployments and confirm discovery + gossip still convergent.
kubectl -n "$namespace" scale deployment/gabiond --replicas=3
kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=3
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
wait_for_endpoint_count gabiond 3
wait_for_endpoint_count gabion-nginx 3

# Send a bit more traffic so the new pods participate.
drive_nginx_traffic
sleep 3
drive_nginx_traffic
sleep 2

dump_snapshot "after traffic + scale-out"

printf '\n=== assertions ===\n'
assert_snapshot_has_peers
assert_snapshot_has_aggregate_rows

# Dump pod logs so the operator can see what each side reported.
printf '\n=== gabiond logs (most recent gabion / gossip / warn lines) ===\n'
for pod in $(kubectl -n "$namespace" get pods -l app=gabiond -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    printf '\n-- %s --\n' "$pod"
    kubectl -n "$namespace" logs --tail=80 "$pod" 2>&1 \
        | grep -iE 'gossip|peer|warn|error|info|gabion' \
        | sed 's/^/  /' \
        | head -n 25 || true
done

printf '\n=== gabion-nginx logs (leader status) ===\n'
for pod in $(kubectl -n "$namespace" get pods -l app=gabion-nginx -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    printf '\n-- %s --\n' "$pod"
    kubectl -n "$namespace" logs --tail=40 "$pod" 2>&1 \
        | grep -iE 'gabion|leader|gossip' \
        | sed 's/^/  /' \
        | head -n 15 || true
done

printf '\n=== cluster overview ===\n'
kubectl -n "$namespace" get pods,svc,endpointslice -o wide 2>&1 | sed 's/^/  /'

printf '\nlocal kubernetes mixed NGINX + Gabion server gossip test passed on context %s (%s)\n' \
    "$context" "$server"
