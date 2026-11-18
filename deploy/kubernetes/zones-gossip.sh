#!/bin/sh
# Multi-zone smoke: six gabiond replicas, two in each zone of a kind cluster
# built from deploy/kubernetes/kind-zones.yaml. Through each pod's /snapshot
# it checks that every pod learned its zone from its own EndpointSlice entry,
# that each has one zone peer and one gateway in each other zone, that hits
# through a kind-a pod reject at the shared limit through a kind-c pod, and
# that all of it holds again after one pod per zone is replaced and after a
# gateway is deleted. Every wait polls against a deadline derived from the
# gossip settings below; timings are printed, never asserted.
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

namespace="gabion-zones-$$"
. deploy/kubernetes/lib.sh

# The gossip settings the pods run with, and the deadlines they give.
TICK_MS=100
SUSPECT_MS=1000
# Roles settle within a suspicion and a few dozen heartbeats; 20 s more
# covers scheduling and the EndpointSlice controller's writes.
SETTLE_S=$(((SUSPECT_MS + 50 * TICK_MS) / 1000 + 20))
# The mixed smoke's bound for a hit to reach another node.
CONVERGE_S=3
# A gateway is replaced within a suspicion and two heartbeats of the
# origin's next change.
FAILOVER_S=$(((SUSPECT_MS + 2 * TICK_MS) / 1000 + 5))
# The rule's limit: 5 hits a minute per tenant.
LIMIT=5
forward_pids=""

zone_count="$(kubectl get nodes \
    -o 'jsonpath={range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' \
    | sort -u | grep -c . || true)"
if [ "$zone_count" -lt 3 ]; then
    printf '%s\n' "refusing to run: the cluster behind context '$context' has $zone_count node \
zones, and this smoke needs three. Create one with \`kind create cluster --name gabion-zones \
--config deploy/kubernetes/kind-zones.yaml --kubeconfig <file>\`, then run \
\`KUBECONFIG=<file> make kubernetes-zones-test\`." >&2
    exit 1
fi

stop_forwards() {
    for pid in $forward_pids; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    forward_pids=""
}

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        printf '\n--- cleanup diagnostic dump (namespace=%s) ---\n' "$namespace" >&2
        kubectl -n "$namespace" get pods -o wide >&2 || true
        kubectl -n "$namespace" get events --sort-by=.metadata.creationTimestamp >&2 || true
        for pod in $(live_pods gabiond 2>/dev/null | awk '{ print $1 }'); do
            printf '\n-- %s /snapshot --\n' "$pod" >&2
            snapshot "$pod" >&2 || true
        done
        kubectl -n "$namespace" logs --all-containers --tail=200 -l app=gabiond >&2 || true
        printf -- '--- end cleanup diagnostic dump ---\n\n' >&2
    fi
    stop_forwards
    kubectl delete namespace "$namespace" --ignore-not-found=true --wait=false
}
trap cleanup EXIT

build_and_load_images gabiond:local
kubectl create namespace "$namespace"
apply_gabion_rbac
# The shipped manifests pass the API server's schema and admission checks.
for manifest in gabiond gabion-nginx; do
    sed "s/namespace: default/namespace: $namespace/" "deploy/kubernetes/$manifest.yaml" \
        | kubectl apply --dry-run=server -f -
done

kubectl -n "$namespace" apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: gabiond-config
data:
  config.yaml: |
    envoy_bind: 0.0.0.0:8081
    admin_bind: 0.0.0.0:9090
    discovery:
      namespace_allow: ["$namespace"]
      service_allow: ["gabiond"]
    gossip:
      bind: 0.0.0.0:9000
    limits:
      - name: zone_tenant
        domain: zones
        descriptors:
          - key: tenant
        rate: ${LIMIT}r/m
        bucket: 1s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabiond
spec:
  replicas: 6
  selector:
    matchLabels:
      app: gabiond
  template:
    metadata:
      labels:
        app: gabiond
    spec:
      serviceAccountName: gabion
      # Two pods in each zone.
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app: gabiond
      containers:
        - name: gabiond
          image: gabiond:local
          imagePullPolicy: Never
          args: ["/etc/gabion/config.yaml"]
          env:
            - { name: GABION_GOSSIP_TICK_INTERVAL, value: "${TICK_MS}ms" }
            - { name: GABION_GOSSIP_SUSPECT_AFTER, value: "${SUSPECT_MS}ms" }
          ports:
            - { name: envoy,  containerPort: 8081 }
            - { name: admin,  containerPort: 9090 }
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
    - { name: gabion, port: 9000, targetPort: gabion, protocol: UDP }
YAML

# pod_in_zone <zone>: a live gabiond pod on a node in <zone>.
pod_in_zone() {
    live_pods gabiond | awk -v zone="$1" '$3 == zone { print $1; exit }'
}

live_count_is() {
    [ "$(live_pods gabiond | grep -c .)" = "$1" ]
}

# forward <pod> <port>: forward localhost:<port> to the pod's gRPC port and
# <port>+1 to its admin port, once the admin port answers.
forward() {
    kubectl -n "$namespace" port-forward "pod/$1" "$2:8081" "$(($2 + 1)):9090" \
        >"/tmp/$namespace-forward-$2.log" 2>&1 &
    forward_pids="$forward_pids $!"
    retry 30 curl -fsS -o /dev/null "http://127.0.0.1:$(($2 + 1))/readyz"
}

# roles_settled: six live pods, each with one zone peer of its own zone, one
# gateway in each other zone, the other two peers remote, and none unheard.
roles_settled() {
    live_count_is 6 || return 1
    live_pods gabiond | while read -r pod _ _; do
        snapshot "$pod" | python3 -c '
import json, sys
s = json.load(sys.stdin)
want = {"unheard": 0, "zone": 1, "gateway": 2, "remote": 2, "incompatible": 0}
gateways = [p["zone"] for p in s["peers"] if p["role"] == "gateway"]
zone_peers = [p["zone"] for p in s["peers"] if p["role"] == "zone"]
settled = (s["peers_by_role"] == want and zone_peers == ["same"]
           and len(set(gateways)) == 2 and "same" not in gateways)
sys.exit(0 if settled else 1)
' || return 1
    done
}

none_unheard() {
    live_pods gabiond | while read -r pod _ _; do
        snapshot "$pod" | python3 -c '
import json, sys
sys.exit(json.load(sys.stdin)["peers_by_role"]["unheard"] != 0)
' || return 1
    done
}

refused_in_kind_c() {
    [ "$(gabiond_hit 18281 zones tenant "$1")" = OVER_LIMIT ] && return 0
    allowed_in_c=$((allowed_in_c + 1))
    if [ "$allowed_in_c" -ge "$LIMIT" ]; then
        printf '%s\n' "FAIL: the kind-c pod allowed $allowed_in_c hits on tenant $1, whose \
limit of $LIMIT the kind-a pod had used up: the two zones count separately" >&2
        exit 1
    fi
    return 1
}

# cross_zone_limit <tenant>: hits through a kind-a pod up to the limit, after
# which a kind-c pod refuses the tenant within the convergence bound.
cross_zone_limit() {
    from="$(pod_in_zone kind-a)"
    to="$(pod_in_zone kind-c)"
    forward "$from" 18181
    forward "$to" 18281
    hit=1
    while [ "$hit" -le "$LIMIT" ]; do
        answer="$(gabiond_hit 18181 zones tenant "$1")"
        if [ "$answer" != OK ]; then
            printf '%s\n' "FAIL: hit $hit of $LIMIT on tenant $1 through $from (kind-a) \
answered $answer before the limit" >&2
            exit 1
        fi
        hit=$((hit + 1))
    done
    started="$(now_ms)"
    allowed_in_c=0
    if ! retry "$CONVERGE_S" refused_in_kind_c "$1"; then
        printf '%s\n' "FAIL: $to (kind-c) still allowed tenant $1 ${CONVERGE_S}s after $from \
(kind-a) used up its limit: hits did not cross zones" >&2
        exit 1
    fi
    printf '%s\n' "tenant $1: $LIMIT hits through $from (kind-a) reject through $to (kind-c) \
after $(($(now_ms) - started)) ms"
    stop_forwards
}

# gateway_pod <pod> <zone>: the live pod <pod> sends its own stream to as its
# gateway in <zone>.
gateway_pod() {
    gateway_pods="$(live_pods gabiond)"
    snapshot "$1" | python3 -c '
import json, sys
by_ip = {}
for line in sys.argv[2].splitlines():
    name, ip, zone = line.split()
    by_ip[ip] = (name, zone)
for peer in json.load(sys.stdin)["peers"]:
    name, zone = by_ip.get(peer["addr"].rsplit(":", 1)[0], ("", ""))
    if peer["role"] == "gateway" and zone == sys.argv[1]:
        print(name)
' "$2" "$gateway_pods"
}

new_gateway_in_kind_b() {
    gateway="$(gateway_pod "$origin" kind-b)"
    [ -n "$gateway" ] && [ "$gateway" != "$old_gateway" ]
}

settled_after_join() {
    if [ -z "$unheard_ms" ] && none_unheard; then
        unheard_ms=$(($(now_ms) - replaced_at))
    fi
    roles_settled
}

# 1. Zone source.
kubectl -n "$namespace" rollout status deployment/gabiond --timeout=180s
assert_pods_run_image app=gabiond gabiond:local
wait_for_endpoint_count gabiond 6
assert_zone_from_endpoint_slice gabiond
print_own_entry_wait gabiond

# 2. Roles.
retry "$SETTLE_S" roles_settled
printf 'every pod has one zone peer and one gateway in each other zone\n'

# 3. Cross-zone counting.
cross_zone_limit before-join

# 4. Join: replace one pod in each zone.
for zone in kind-a kind-b kind-c; do
    kubectl -n "$namespace" delete pod "$(pod_in_zone "$zone")" --wait=false
done
retry 120 live_count_is 6
assert_pods_run_image app=gabiond gabiond:local
replaced_at="$(now_ms)"
unheard_ms=""
retry "$SETTLE_S" settled_after_join
printf '%s\n' "after one pod per zone was replaced, a peer stayed unheard for ${unheard_ms} ms \
once the replacements were ready; roles settled after $(($(now_ms) - replaced_at)) ms"
cross_zone_limit after-join

# 5. Gateway loss: an idle origin fails over only on its next change, so
# send one hit through it once its kind-b gateway is gone.
origin="$(pod_in_zone kind-a)"
old_gateway="$(gateway_pod "$origin" kind-b)"
if [ -z "$old_gateway" ]; then
    printf '%s\n' "FAIL: $origin (kind-a) has no gateway in kind-b" >&2
    exit 1
fi
kubectl -n "$namespace" delete pod "$old_gateway" --wait=false
started="$(now_ms)"
forward "$origin" 18181
gabiond_hit 18181 zones tenant failover-nudge >/dev/null
if ! retry "$FAILOVER_S" new_gateway_in_kind_b; then
    printf '%s\n' "FAIL: $origin (kind-a) found no new kind-b gateway within ${FAILOVER_S}s of \
losing $old_gateway" >&2
    exit 1
fi
stop_forwards
printf '%s\n' "$origin (kind-a) replaced kind-b gateway $old_gateway with $gateway after \
$(($(now_ms) - started)) ms"
cross_zone_limit after-failover

printf '\nlocal kubernetes three-zone gabiond gossip test passed on context %s (%s)\n' \
    "$context" "$server"
