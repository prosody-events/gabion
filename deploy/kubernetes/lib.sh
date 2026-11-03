# Helpers shared by the kind smokes, sourced after `context` and `namespace`
# are set. A cluster has no event we can block on from a shell, so each wait
# re-checks an observable condition once a second: a healthy cluster moves on
# as soon as it is ready, and the deadline only bounds failure.

# retry <seconds> <command...>: run the command until it succeeds; name it on
# stderr once <seconds> of wall time have passed.
retry() {
    retry_budget="$1"
    retry_deadline=$(($(date +%s) + retry_budget))
    shift
    until "$@"; do
        if [ "$(date +%s)" -ge "$retry_deadline" ]; then
            printf '%s\n' "gave up after ${retry_budget}s waiting for: $*" >&2
            return 1
        fi
        sleep 1
    done
}

# now_ms: wall-clock milliseconds, for the timings the smokes print.
now_ms() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

# kind_cluster: the kind cluster the guarded `kind-*` context names.
kind_cluster() {
    printf '%s\n' "${context#kind-}"
}

# build_and_load_images <tag>...: build each smoke image from this checkout
# and load it into the kind cluster, so the pods run this tree, not whatever
# image the cluster last held. CI builds and loads them itself, with the
# registry cache, and sets GABION_SKIP_IMAGE_BUILD=1.
build_and_load_images() {
    if [ "${GABION_SKIP_IMAGE_BUILD:-0}" = 1 ]; then
        return 0
    fi
    for tag in "$@"; do
        case "$tag" in
            gabiond:local)
                docker build -f deploy/gabiond/Dockerfile -t "$tag" . ;;
            gabion-loader:local)
                docker build -f deploy/kubernetes/loader.Dockerfile -t "$tag" . ;;
            nginx-nginx-module-request-smoke:latest)
                docker compose --profile module -f deploy/nginx/docker-compose.yml \
                    build nginx-module-request-smoke ;;
            *)
                printf 'build_and_load_images: no recipe builds %s\n' "$tag" >&2
                return 1 ;;
        esac
        kind load docker-image "$tag" --name "$(kind_cluster)"
    done
}

# image_config_digest <tag>: the hex digest of the image's config, which is
# how containerd names an image loaded from a Docker image store that keys
# images by manifest.
image_config_digest() {
    docker save "$1" | tar -xOf - manifest.json | python3 -c '
import json, sys
config = json.load(sys.stdin)[0]["Config"]
print(config.rsplit("/", 1)[-1].removesuffix(".json"))
'
}

# assert_pods_run_image <selector> <tag>: every started pod the label
# selector matches runs the image this run built and loaded as <tag>.
assert_pods_run_image() {
    want="$(docker image inspect "$2" --format '{{.Id}}')"
    want="${want#sha256:}"
    ran="$(kubectl -n "$namespace" get pods -l "$1" \
        -o 'jsonpath={range .items[*]}{.metadata.name} {.status.containerStatuses[*].imageID}{"\n"}{end}' \
        | awk 'NF == 2')"
    if printf '%s\n' "$ran" | grep -q . && ! printf '%s\n' "$ran" | grep -vq "$want"; then
        return 0
    fi
    want="$(image_config_digest "$2")"
    printf '%s\n' "$ran" | while read -r pod image; do
        case "$image" in
            *"$want"*) ;;
            *)
                printf '%s\n' "pod $pod runs image $image, but this run built and loaded $2 \
(ID sha256:$want). The cluster holds an older image: rerun with GABION_SKIP_IMAGE_BUILD \
unset so the script builds and loads this checkout." >&2
                return 1 ;;
        esac
    done
}

# apply_gabion_rbac: the ServiceAccount `gabion` and the Role discovery needs.
apply_gabion_rbac() {
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
YAML
}

# live_pods <app>: one `name ip zone` line per running, ready pod of the app
# that is not terminating, with the zone label of its node.
live_pods() {
    live_nodes="$(kubectl get nodes -o json)"
    kubectl -n "$namespace" get pods -l "app=$1" -o json | python3 -c '
import json, sys
label = "topology.kubernetes.io/zone"
zones = {n["metadata"]["name"]: n["metadata"].get("labels", {}).get(label, "")
         for n in json.loads(sys.argv[1])["items"]}
for pod in json.load(sys.stdin)["items"]:
    status = pod.get("status", {})
    ready = any(c["type"] == "Ready" and c["status"] == "True"
                for c in status.get("conditions", []))
    if "deletionTimestamp" in pod["metadata"] or status.get("phase") != "Running" or not ready:
        continue
    print(pod["metadata"]["name"], status.get("podIP", ""),
          zones.get(pod["spec"].get("nodeName", ""), "") or "-")
' "$live_nodes"
}

# snapshot <pod>: gabiond's /snapshot, read inside the pod.
snapshot() {
    kubectl -n "$namespace" exec "$1" -- wget -qO- http://127.0.0.1:9090/snapshot
}

# label_unzoned_nodes <zone>: give every node without a zone label <zone>, so
# each pod's EndpointSlice entry carries a zone. A zoned node is left alone.
label_unzoned_nodes() {
    unzoned="$(kubectl get nodes -l '!topology.kubernetes.io/zone' -o name)"
    if [ -n "$unzoned" ]; then
        # shellcheck disable=SC2086 # one node name per word
        kubectl label $unzoned "topology.kubernetes.io/zone=$1"
    fi
}

zones_from_endpoint_slice() {
    live_pods "$1" | while read -r pod _ zone; do
        snapshot "$pod" | python3 -c '
import json, sys
zone = json.load(sys.stdin)["zone"]
want = {"name": sys.argv[1], "source": "endpoint_slice"}
if zone != want:
    print(f"pod {sys.argv[2]} reports zone {zone}, expected {want}", file=sys.stderr)
    sys.exit(1)
' "$zone" "$pod" || return 1
    done
}

# assert_zone_from_endpoint_slice <app>: every live gabiond pod reports its
# node's zone, learned from its own EndpointSlice entry.
assert_zone_from_endpoint_slice() {
    retry 60 zones_from_endpoint_slice "$1"
    printf 'every %s pod learned its zone from its EndpointSlice entry\n' "$1"
}

# print_own_entry_wait <app>: how long each gabiond pod waited between
# starting and learning its zone, and the longest wait; `gossip.zone_grace`
# must stay at least five times that. Printed, never asserted.
print_own_entry_wait() {
    live_pods "$1" | while read -r pod _ _; do
        kubectl -n "$namespace" logs "$pod" | python3 -c '
import datetime, re, sys
start = known = None
for line in sys.stdin:
    stamp = line.split(" ", 1)[0]
    if start is None and "Starting gabion." in line:
        start = stamp
    if known is None and re.search(r"code=\"?zone_known", line):
        known = stamp
if start and known:
    t = lambda s: datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    print(int((t(known) - t(start)).total_seconds() * 1000))
' | sed "s/^/$pod /"
    done | awk '
        { printf "own EndpointSlice entry wait: %s %s ms\n", $1, $2; if ($2 > max) max = $2 }
        END { printf "longest own-entry wait: %d ms\n", max }'
}

# gabiond_hit <port> <domain> <key> <value>: one hit through a gabiond gRPC
# port forwarded to localhost:<port>; prints the response's overall code, OK
# or OVER_LIMIT. The request is encoded by hand, so the smoke needs only curl
# and python3.
gabiond_hit() {
    hit_port="$1"
    shift
    python3 -c '
import sys
def field(number, payload):  # length-delimited; every payload here is under 128 bytes
    return bytes([number << 3 | 2, len(payload)]) + payload
domain, key, value = (arg.encode() for arg in sys.argv[1:])
request = field(1, domain) + field(2, field(1, field(1, key) + field(2, value)))
sys.stdout.buffer.write(bytes(1) + len(request).to_bytes(4, "big") + request)
' "$@" \
        | curl -sS --http2-prior-knowledge -H 'content-type: application/grpc' -H 'te: trailers' \
            --data-binary @- \
            "http://127.0.0.1:$hit_port/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit" \
        | python3 -c '
import sys
reply = sys.stdin.buffer.read()
print({b"\x08\x01": "OK", b"\x08\x02": "OVER_LIMIT"}.get(reply[5:7], "UNKNOWN"))
'
}

endpoint_count_is() {
    [ "$(kubectl -n "$namespace" get endpointslice \
        -l "kubernetes.io/service-name=$1" \
        -o 'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}' \
        | grep -c .)" = "$2" ]
}

# wait_for_endpoint_count <service> <expected>
wait_for_endpoint_count() {
    retry 180 endpoint_count_is "$1" "$2" && return 0
    kubectl -n "$namespace" get endpointslice -o wide >&2
    return 1
}
