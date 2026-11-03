# Kubernetes smokes

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
