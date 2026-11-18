# Kubernetes manifests and smokes

This directory holds a starting set of manifests for running gabion on
Kubernetes, and the smoke tests that run gabion on a local kind cluster.

## Manifests

| File | What it holds |
|---|---|
| [`gabion-discovery-rbac.yaml`](gabion-discovery-rbac.yaml) | The `gabion` ServiceAccount, and the Role and RoleBinding that let discovery read Services and EndpointSlices. Both adapters need it. |
| [`gabiond.yaml`](gabiond.yaml) | `gabiond`: a ConfigMap with its YAML, a Deployment of three replicas with a `/readyz` readiness probe, and a Service exposing Envoy's gRPC port and the UDP port named `gabion`. |
| [`gabion-nginx.yaml`](gabion-nginx.yaml) | NGINX with the module: a ConfigMap with `nginx.conf`, a Deployment of three replicas on the published image, and a Service exposing HTTP and the UDP port named `gabion`. |
| [`gabion-networkpolicy.yaml`](gabion-networkpolicy.yaml) | A NetworkPolicy that admits gossip only from gabion pods, since gossip carries no authentication. |

Apply the RBAC, one adapter or both, and the NetworkPolicy:

```sh
kubectl apply -f deploy/kubernetes/gabion-discovery-rbac.yaml \
  -f deploy/kubernetes/gabiond.yaml \
  -f deploy/kubernetes/gabion-networkpolicy.yaml
```

Before you do, edit:

- **The image tag.** Pin a release (for example `:0.4`) instead of `:latest`,
  and run one release on every pod of the cluster.
- **The namespace.** Every object is in `default`; change every `namespace:`
  together, including the RoleBinding's subject in the RBAC file. Discovery
  watches the pods' own namespace unless `discovery.namespace_allow` says
  otherwise.
- **The cluster id.** Both adapter manifests set the same
  `cluster_id_hash`, so applied together they form one mixed cluster. Give
  each cluster its own id; two clusters in one namespace also need
  `discovery.service_allow` (`gabion_discovery_service_allow`) so each finds
  only its own Service.
- **The rules** in each ConfigMap, and the limit zone's SIZE for NGINX.
- **The nodes.** Every node needs a `topology.kubernetes.io/zone` label, and
  its `net.core.rmem_max` and `net.core.wmem_max` sysctls raised for gossip's
  socket buffers; both are node settings, not pod settings (the gabion
  README's [Socket buffers](../../crates/gabion/README.md#socket-buffers)).

The root README's [Kubernetes checklist](../../README.md#kubernetes-checklist)
says why each object is needed. A pod that finds no peers logs why; each
message has its entry in
[`crates/gabion/TROUBLESHOOTING.md`](../../crates/gabion/TROUBLESHOOTING.md).
The smokes below build their own workloads inline, but every smoke that
runs gabion pods applies `gabion-discovery-rbac.yaml` in its namespace, so
they prove its rules suffice; the three-zone smoke also checks `gabiond.yaml` and
`gabion-nginx.yaml` against the API server with a server-side dry run.

## Smokes

Each script runs against a local kind cluster. It refuses to run unless the
current context is `kind-*` and its API server is on localhost, and it works
in a namespace of its own that it deletes on exit; `make kubernetes-clean`
removes any an aborted run left behind.

| Make target | Script | Cluster | What it checks |
|---|---|---|---|
| `make kubernetes-test` | `local-smoke.sh` | any kind cluster | The EndpointSlice watcher reports a peer, against a live API server. |
| `make kubernetes-nginx-test` | `nginx-scale-rate-limit.sh` | any kind cluster | NGINX pods enforce a limit alone and as a three-pod cluster, and across a reload. |
| `make kubernetes-mixed-test` | `mixed-nginx-gabion-gossip.sh` | any kind cluster | NGINX and `gabiond` pods share each rule's limit; every pod learns its zone from its EndpointSlice entry (the script labels unzoned nodes `kind-a`) and prints how long that took. |
| `make kubernetes-zones-test` | `zones-gossip.sh` | three zones, from `kind-zones.yaml` | Six `gabiond` pods, two per zone: zone sources, roles, cross-zone counting, and both again after pods are replaced and a gateway is deleted. |

`distributed-rate-limit.sh` is a load test, run by hand with `BACKEND=nginx`
or `BACKEND=gabiond`.

The three-zone smoke needs its own cluster. Create it with a kubeconfig of
its own, so your current context is left alone:

```sh
kind create cluster --name gabion-zones --config deploy/kubernetes/kind-zones.yaml \
  --kubeconfig /tmp/gabion-zones.kubeconfig
KUBECONFIG=/tmp/gabion-zones.kubeconfig make kubernetes-zones-test
```

## Images

Each script builds the images it deploys from this checkout and loads them
into the kind cluster (`build_and_load_images` in `lib.sh`), then checks that
every pod runs the image it just built (`assert_pods_run_image`), so a pod
never tests an image left in the cluster by an older tree. The check compares
each pod's `imageID` with the image's ID, or, when the Docker image store keys
images by manifest, with the digest of the image's config.

CI builds the images itself, with the read-only registry cache, loads them,
and sets `GABION_SKIP_IMAGE_BUILD=1` so the scripts skip their own build;
the provenance check still runs. Set it locally only when you have just
loaded the images yourself: with a stale image loaded, the check fails and
says to rerun with `GABION_SKIP_IMAGE_BUILD` unset.
