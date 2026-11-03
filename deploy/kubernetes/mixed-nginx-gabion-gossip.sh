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
. deploy/kubernetes/lib.sh
admin_forward_pid=""
nginx_forward_pid=""

cleanup() {
    # If the script is failing, dump pod state BEFORE namespace deletion —
    # otherwise `kubectl delete namespace` wipes pod logs, events, and
    # describe output before we can read them, leaving CI with nothing
    # but a generic "rollout timed out" message.
    if [ "${cleanup_dump:-1}" = "1" ] && [ -n "${namespace:-}" ]; then
        printf '\n--- cleanup diagnostic dump (namespace=%s) ---\n' "$namespace" >&2
        kubectl -n "$namespace" get pods,events --sort-by=.lastTimestamp -o wide >&2 || true
        kubectl -n "$namespace" describe pods -l app=gabiond >&2 || true
        kubectl -n "$namespace" describe pods -l app=gabion-nginx >&2 || true
        # --tail=1000 because the smoke-with-bt wrapper prints a full
        # `gdb thread apply all bt full` on crash, which easily blows
        # past 200 lines and is the *only* signal for a native SIGSEGV
        # in the module's config-phase callbacks.
        kubectl -n "$namespace" logs --all-containers --tail=1000 -l app=gabiond >&2 || true
        kubectl -n "$namespace" logs --all-containers --tail=1000 --previous -l app=gabiond >&2 || true
        kubectl -n "$namespace" logs --all-containers --tail=1000 -l app=gabion-nginx >&2 || true
        kubectl -n "$namespace" logs --all-containers --tail=1000 --previous -l app=gabion-nginx >&2 || true
        # Best-effort: copy /tmp/cores out of any pod that's still
        # Running (kubectl cp uses `kubectl exec` under the hood, so a
        # crashed pod's cores are unreachable). The smoke wrapper
        # already prints a gdb backtrace into stdout above; this just
        # preserves the raw cores for offline post-mortem.
        mkdir -p /tmp/cores
        for pod in $(kubectl -n "$namespace" get pods -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}'); do
            kubectl -n "$namespace" cp "$pod:/tmp/cores" "/tmp/cores/${namespace}-${pod}" 2>/dev/null || true
        done
        printf -- '--- end cleanup diagnostic dump ---\n\n' >&2
    fi
    if [ -n "$admin_forward_pid" ]; then
        kill "$admin_forward_pid" 2>/dev/null || true
    fi
    if [ -n "$nginx_forward_pid" ]; then
        kill "$nginx_forward_pid" 2>/dev/null || true
    fi
    kubectl delete namespace "$namespace" --ignore-not-found=true
}
trap cleanup EXIT

build_and_load_images nginx-nginx-module-request-smoke:latest gabiond:local

# kind's node carries no zone label; the EndpointSlice controller copies it
# into each pod's entry, where both adapters read their zone.
label_unzoned_nodes kind-a

kubectl create namespace "$namespace"
apply_gabion_rbac

kubectl -n "$namespace" apply -f - <<YAML
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
      max_descriptor_count: 16
      max_descriptor_bytes: 512
      max_key_bytes: 128
    discovery:
      namespace_allow: ["$namespace"]
      service_allow: ["gabiond", "gabion-nginx"]
    gossip:
      bind: 0.0.0.0:9000
      tick_interval: 100ms
      max_payload_bytes: 1400
      limit_queue_capacity: 1024
      cluster_id_hash: 1
      target_err_bps: 100
      min_emit_interval: 5ms
    # The NGINX config's three rules in the opposite order; each pair counts
    # as one rule across the two adapters.
    limits:
      - name: acme_only
        domain: nginx
        descriptors:
          - key: tenant
            value: acme
        rate: 2r/m
        bucket: 1s
      - name: per_client
        domain: nginx
        descriptors:
          - key: client
        rate: 3r/m
        bucket: 1s
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
    worker_processes 2;
    error_log /dev/stderr info;

    events {
        worker_connections 128;
    }

    http {
        gabion_limit_zone zone=api:128m;
        gabion_limit_rule uri_api \$uri rate=2r/m bucket=1s;
        gabion_limit_rule per_client client:\$arg_c rate=3r/m bucket=1s;
        gabion_limit_rule acme_only tenant:\$arg_t rate=2r/m bucket=1s value=tenant:acme;
        gabion_gossip_bind 0.0.0.0:9000;
        gabion_gossip_cluster 1;
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

            location /client/ {
                gabion_limit per_client;
                alias /usr/share/nginx/html/;
            }

            location /acme/ {
                gabion_limit acme_only;
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
            # EndpointSliceDiscovery filters by the literal port name
            # "gabion" (see crates/gabion/src/discovery/kubernetes.rs).
            # Renaming this port disables auto-discovery silently.
            - { name: gabion, containerPort: 9000, protocol: UDP }
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
    - { name: gabion, port: 9000, targetPort: gabion, protocol: UDP }
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
            - { name: gabion, containerPort: 9000, protocol: UDP }
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
    - { name: gabion, port: 9000, targetPort: gabion, protocol: UDP }
YAML

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

drive_nginx_traffic() {
    # Burst against /api so the local nginx leader records hits and gossips
    # them to the gabiond peers.
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        curl -sS -o /dev/null "http://127.0.0.1:18080/api/index.html" || true
    done
}

# nginx_hit <path>: one request through NGINX; prints its HTTP status.
nginx_hit() {
    curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:18080$1"
}

# counter <name>: one of the forwarded gabiond's /snapshot counters.
counter() {
    curl -fsS "http://127.0.0.1:19090/snapshot" | python3 -c '
import json, sys
print(json.load(sys.stdin)["counters"][sys.argv[1]])
' "$1"
}

# merged_past <count>: gabiond has merged more than <count> cells, so it holds
# the NGINX hit sent after it read <count>; nothing else changes a cell here.
merged_past() {
    curl -fsS "http://127.0.0.1:19090/snapshot" | python3 -c '
import json, sys
sys.exit(json.load(sys.stdin)["counters"]["cells_merged"] <= int(sys.argv[1]))
' "$1" 2>/dev/null
}

# acked_after <heartbeats>: gabiond has run a heartbeat round since it had run
# <heartbeats>, which sends its hit, and every peer has acked its own stream,
# so the NGINX leader holds the hit.
acked_after() {
    curl -fsS "http://127.0.0.1:19090/snapshot" | python3 -c '
import json, sys
s = json.load(sys.stdin)
sent = s["counters"]["rounds_heartbeat"] > int(sys.argv[1])
sys.exit(not (sent and all(p["own_unacked"] == 0 for p in s["peers"])))
' "$1" 2>/dev/null
}

# shared_limit <limit> <nginx path> <domain> <key> <value>: alternate hits on
# one key between NGINX and gabiond, each held by the other adapter before the
# next. The two adapters count one rule, so the first <limit> hits pass and
# each adapter refuses the next.
shared_limit() {
    limit="$1"
    path="$2"
    shift 2
    hit=1
    while [ "$hit" -le "$limit" ]; do
        allowed=0
        hit_ms="$(now_ms)"
        if [ $((hit % 2)) -eq 1 ]; then
            merged="$(counter cells_merged)"
            answer="NGINX $(nginx_hit "$path")"
            [ "$answer" = "NGINX 429" ] || allowed=1
            held=merged_past floor="$merged"
        else
            answer="gabiond $(gabiond_hit 18081 "$@")"
            [ "$answer" != "gabiond OK" ] || allowed=1
            held=acked_after floor="$(counter rounds_heartbeat)"
        fi
        if [ "$allowed" = 0 ]; then
            printf 'FAIL: hit %s of %s on %s was refused (%s) before the shared limit\n' \
                "$hit" "$limit" "$path" "$answer" >&2
            exit 1
        fi
        retry 30 "$held" "$floor"
        printf '%s hit %s held by the other adapter after %s ms\n' \
            "${answer% *}" "$hit" "$(($(now_ms) - hit_ms))"
        hit=$((hit + 1))
    done
    status="$(nginx_hit "$path")"
    code="$(gabiond_hit 18081 "$@")"
    if [ "$status" != 429 ] || [ "$code" != OVER_LIMIT ]; then
        printf 'FAIL: after %s hits split between NGINX and gabiond on %s, NGINX answered %s and gabiond %s; both must refuse, or the two adapters count the rule separately\n' \
            "$limit" "$path" "$status" "$code" >&2
        exit 1
    fi
    printf 'one limit of %s across NGINX and gabiond on %s\n' "$limit" "$path"
}

port_forwards_ready() {
    curl -fsS -o /dev/null "http://127.0.0.1:19090/snapshot" \
        && curl -fsS -o /dev/null "http://127.0.0.1:18080/off/index.html"
}

# gossip_settled <min-peers>: push traffic through nginx, then check that
# gabiond knows at least <min-peers> peers (discovered via EndpointSlice)
# and holds rows for the nginx rule.
gossip_settled() {
    drive_nginx_traffic
    curl -fsS "http://127.0.0.1:19090/snapshot" \
        | python3 -c '
import json, sys
s = json.load(sys.stdin)
sys.exit(len(s["peers"]) < int(sys.argv[1]) or s["aggregate_rows"] == 0)
' "$1" 2>/dev/null
}

# nginx_zones_known: every live NGINX pod logged learning its zone from its
# EndpointSlice entry.
nginx_zones_known() {
    live_pods gabion-nginx | while read -r pod _ _; do
        kubectl -n "$namespace" logs "$pod" \
            | grep -E 'code="?zone_known' | grep -q 'endpoint_slice' || return 1
    done
}

kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
assert_pods_run_image app=gabiond gabiond:local
assert_pods_run_image app=gabion-nginx nginx-nginx-module-request-smoke:latest
wait_for_endpoint_count gabiond 2
wait_for_endpoint_count gabion-nginx 2

kubectl -n "$namespace" port-forward service/gabiond 19090:9090 18081:8081 \
    >/tmp/gabiond-admin-port-forward.log 2>&1 &
admin_forward_pid="$!"
kubectl -n "$namespace" port-forward service/gabion-nginx 18080:8080 \
    >/tmp/gabion-nginx-service-port-forward.log 2>&1 &
nginx_forward_pid="$!"

retry 60 port_forwards_ready

# Initial state — peers may not have discovered each other yet.
dump_snapshot "before traffic"

retry 60 gossip_settled 1
assert_zone_from_endpoint_slice gabiond
retry 60 nginx_zones_known
printf 'every gabion-nginx pod learned its zone from its EndpointSlice entry\n'
print_own_entry_wait gabiond

# Now scale both deployments and confirm discovery + gossip still convergent.
kubectl -n "$namespace" scale deployment/gabiond --replicas=3
kubectl -n "$namespace" scale deployment/gabion-nginx --replicas=3
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
kubectl -n "$namespace" rollout status deployment/gabion-nginx --timeout=180s
wait_for_endpoint_count gabiond 3
wait_for_endpoint_count gabion-nginx 3

# gabiond peers with every other pod, never itself: 3 before the scale-out,
# 5 after.
retry 60 gossip_settled 5

# Each rule is one limit across both adapters. Fresh keys: the traffic above
# spent /api/index.html's budget.
shared_limit 2 /api/parity nginx uri /api/parity
shared_limit 3 '/client/index.html?c=parity' nginx client parity
shared_limit 2 '/acme/index.html?t=acme' nginx tenant acme

dump_snapshot "after traffic + scale-out"

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
