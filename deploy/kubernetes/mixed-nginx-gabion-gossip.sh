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
    resources: ["pods"]
    verbs: ["get"]
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
    name: gabion
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: gabion-endpointslice-reader
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: gabiond-config
data:
  config.yaml: |
    storage:
      max_keys: 128
      max_cells: 1024
      dirty_ring_entries: 1024
    server:
      envoy_rls:
        enabled: true
        bind: 0.0.0.0:8081
      admin:
        enabled: true
        bind: 0.0.0.0:9090
    discovery:
      kind: kubernetes
      endpoint_slices:
        - namespace: $namespace
          service_name: gabiond
          port_name: gossip
        - namespace: $namespace
          service_name: gabion-nginx
          port_name: gossip
    gossip:
      enabled: true
      bind: 0.0.0.0:9000
      linger_ms: 100
      fanout: 8
      max_payload_bytes: 65536
      max_cells_per_frame: 1024
      cluster_id_hash: 1
    limits:
      - name: nginx_uri
        domain: nginx
        descriptors:
          - key: uri
            value: "*"
        limit: 2
        window: 60s
        bucket: 1s
        local_fallback_limit: 2
        local_absolute_limit: 2
        stale_after: 2s
        overflow_policy: aggregate
        mode: enforce
---
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
        gabion_limit_zone api 128m;
        gabion_limit_rule uri_api 2r/m key=\$uri window=60s bucket=1s overflow=aggregate;
        gabion_limit_rule ip_api 2r/m key=\$remote_addr window=60s bucket=1s overflow=aggregate;
        gabion_limit_rule tenant_api 1r/m key=\$arg_tenant key=\$uri window=60s bucket=1s overflow=aggregate;
        gabion_gossip_discovery kubernetes;
        gabion_gossip_bind 0.0.0.0:9000;
        gabion_gossip_cluster 1;
        gabion_gossip_fanout 8;
        gabion_gossip_payload 64k;
        gabion_gossip_max_cells 1024;
        gabion_gossip_linger 250ms;
        gabion_gossip_endpoint_slice $namespace gabiond gossip;
        gabion_gossip_endpoint_slice $namespace gabion-nginx gossip;

        server {
            listen 8080;

            location /api/ {
                gabion_limit uri_api;
                alias /usr/share/nginx/html/;
            }

            location /ip/ {
                gabion_limit ip_api;
                alias /usr/share/nginx/html/;
            }

            location /tenant/ {
                gabion_limit tenant_api;
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
            - name: envoy
              containerPort: 8081
            - name: admin
              containerPort: 9090
            - name: gossip
              containerPort: 9000
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
    - name: admin
      port: 9090
      targetPort: admin
    - name: gossip
      port: 9000
      targetPort: gossip
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
            - name: http
              containerPort: 8080
            - name: gossip
              containerPort: 9000
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
    - name: http
      port: 8080
      targetPort: http
    - name: gossip
      port: 9000
      targetPort: gossip
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

wait_for_metric_greater_than_zero() {
    metric="$1"
    attempts=0
    while [ "$attempts" -lt 90 ]; do
        value="$(
            curl -fsS "http://127.0.0.1:19090/metrics" \
                | awk -v metric="$metric" '$1 == metric { print int($2) }'
        )"
        if [ "${value:-0}" -gt 0 ]; then
            return 0
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    printf '%s\n' "timed out waiting for metric $metric to become positive" >&2
    curl -fsS "http://127.0.0.1:19090/debug/introspection?max_cells=16&max_peers=16" >&2 || true
    return 1
}

kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
wait_for_endpoint_count gabiond 2
wait_for_endpoint_count gabion-nginx 2

kubectl -n "$namespace" port-forward service/gabiond 19090:9090 >/tmp/gabiond-admin-port-forward.log 2>&1 &
admin_forward_pid="$!"
kubectl -n "$namespace" port-forward service/gabion-nginx 18080:8080 >/tmp/gabion-nginx-service-port-forward.log 2>&1 &
nginx_forward_pid="$!"

attempts=0
while [ "$attempts" -lt 30 ]; do
    if curl -fsS -o /dev/null "http://127.0.0.1:19090/readyz" \
        && curl -fsS -o /dev/null "http://127.0.0.1:18080/off/index.html"; then
        break
    fi
    attempts=$((attempts + 1))
    sleep 1
done

wait_for_metric_greater_than_zero limiter_peers

curl -fsS -o /dev/null "http://127.0.0.1:18080/api/index.html"
curl -fsS -o /dev/null "http://127.0.0.1:18080/api/index.html"
curl -sS -o /dev/null "http://127.0.0.1:18080/api/index.html" || true

kubectl -n "$namespace" scale deployment/gabiond --replicas=3
kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=3
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
wait_for_endpoint_count gabiond 3
wait_for_endpoint_count gabion-nginx 3

curl -fsS -o /dev/null "http://127.0.0.1:18080/api/index.html" || true
wait_for_metric_greater_than_zero gossip_merge_cells_total

state="$(curl -fsS "http://127.0.0.1:19090/debug/introspection?max_cells=16&max_peers=16")"
case "$state" in
    *'"remote_cells":[]'*)
        printf '%s\n' "gabiond admin endpoint did not expose remote cells after NGINX traffic" >&2
        printf '%s\n' "$state" >&2
        exit 1
        ;;
esac

printf '%s\n' "local kubernetes mixed NGINX plus Gabion server gossip test passed on context '$context' ($server)"
