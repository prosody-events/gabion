#!/usr/bin/env python3
import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


def env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = f"gabion-gossip-bench-{os.getpid()}"
OUTPUT_DIR = Path(
    os.environ.get(
        "GABION_BENCH_OUTPUT_DIR",
        ROOT / "target" / "gabion-gossip-bench" / time.strftime("%Y%m%d-%H%M%S"),
    )
)

SERVER_REPLICAS = env_int("GABION_BENCH_SERVER_REPLICAS", 3)
NGINX_REPLICAS = env_int("GABION_BENCH_NGINX_REPLICAS", 8)
INITIAL_NGINX_REPLICAS = env_int("GABION_BENCH_INITIAL_NGINX_REPLICAS", max(1, NGINX_REPLICAS // 2))
REQUESTS = env_int("GABION_BENCH_REQUESTS", 2000)
CONCURRENCY = env_int("GABION_BENCH_CONCURRENCY", 32)
LINGER_MS = env_int("GABION_BENCH_LINGER_MS", 100)
FANOUT = env_int("GABION_BENCH_FANOUT", 8)
SAMPLE_MS = env_int("GABION_BENCH_SAMPLE_MS", 100)
TIMEOUT_SECONDS = env_int("GABION_BENCH_TIMEOUT_SECONDS", 90)
KEEP_NAMESPACE = os.environ.get("GABION_BENCH_KEEP_NAMESPACE") == "1"

# Derived from the pid so parallel bench runs against the same kind
# cluster (CI's "bench matrix" + a local dev run) can't collide on the
# host-side port-forward ports. NAMESPACE already varies by pid so the
# k8s side is namespace-isolated; the only remaining shared resource is
# the host TCP port range.
ADMIN_BASE_PORT = 19100 + (os.getpid() % 100)
RULE_LIMIT = max(REQUESTS * 2, 10_000)
MAX_CELLS = max(8192, NGINX_REPLICAS * REQUESTS * 2)


def run(args, *, input_text=None, capture=False, check=True):
    kwargs = {
        "cwd": ROOT,
        "text": True,
        "check": check,
    }
    if input_text is not None:
        kwargs["input"] = input_text
    if capture:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    return subprocess.run(args, **kwargs)


def log(message):
    print(message, file=sys.stderr, flush=True)


def guard_local_kind():
    context = run(["kubectl", "config", "current-context"], capture=True).stdout.strip()
    server = run(
        ["kubectl", "config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"],
        capture=True,
    ).stdout.strip()
    if not context.startswith("kind-"):
        raise SystemExit(
            f"refusing to run: current kubernetes context is {context!r}, expected 'kind-*'"
        )
    if not (server.startswith("https://127.0.0.1:") or server.startswith("https://localhost:")):
        raise SystemExit(f"refusing to run: kubernetes API server is {server!r}, expected localhost")
    return context, server


def manifest():
    return f"""
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
    envoy_bind: 0.0.0.0:8081
    admin_bind: 0.0.0.0:9090
    storage:
      max_cells: {MAX_CELLS}
      rule_dictionary_capacity: 64
      node_dictionary_capacity: 256
      local_dirty_capacity: 2048
      forwarded_dirty_capacity: 8192
      peer_capacity: 64
    runtime:
      rng_seed: 12345
    discovery:
      namespace_allow: ["{NAMESPACE}"]
      service_allow: ["gabiond", "gabion-nginx"]
    gossip:
      bind: 0.0.0.0:9000
      tick_interval: {LINGER_MS}ms
      fanout: {FANOUT}
      send_queue_capacity: 32
      limit_queue_capacity: 1024
      cluster_id_hash: 1
    limits:
      - name: nginx_uri
        domain: nginx
        descriptors:
          - key: request_uri
            value: "*"
        rate: {RULE_LIMIT}r/m
        bucket: 1s
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

    events {{
        worker_connections 2048;
    }}

    http {{
        gabion_limit_zone zone=api:256m;
        gabion_limit_rule uri_api $request_uri rate={RULE_LIMIT}r/m bucket=1s;
        gabion_gossip_bind 0.0.0.0:9000;
        gabion_gossip_cluster 1;
        gabion_gossip_fanout {FANOUT};
        gabion_gossip_tick_interval {LINGER_MS}ms;
        gabion_discovery_namespace_allow {NAMESPACE};
        gabion_discovery_service_allow gabiond;
        gabion_discovery_service_allow gabion-nginx;

        server {{
            listen 8080;

            location /api/ {{
                gabion_limit uri_api;
                alias /usr/share/nginx/html/;
            }}
        }}
    }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabiond
spec:
  replicas: {SERVER_REPLICAS}
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
            # EndpointSliceDiscovery filters by literal name "gabion" and
            # protocol "UDP" (crates/gabion/src/discovery/kubernetes.rs).
            # Both fields are load-bearing — renaming or dropping the
            # protocol disables peer discovery silently.
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
    - name: admin
      port: 9090
      targetPort: admin
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gabion-nginx
spec:
  replicas: {INITIAL_NGINX_REPLICAS}
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
          # See nginx-scale-rate-limit.sh: the image's baked CMD is the
          # one-shot request smoke, which exits immediately under k8s.
          command: ["nginx", "-g", "daemon off;"]
          ports:
            - name: http
              containerPort: 8080
            - name: gabion
              containerPort: 9000
              protocol: UDP
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
    - name: gabion
      port: 9000
      targetPort: gabion
      protocol: UDP
"""


def load_job_manifest():
    per_worker = (REQUESTS + CONCURRENCY - 1) // CONCURRENCY
    return f"""
apiVersion: batch/v1
kind: Job
metadata:
  name: gabion-load
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: gabion-load
    spec:
      restartPolicy: Never
      containers:
        - name: load
          image: nginx-nginx-module-request-smoke:latest
          imagePullPolicy: Never
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -eu
              worker() {{
                worker_id="$1"
                index=0
                while [ "$index" -lt "{per_worker}" ]; do
                  request_id=$((worker_id * {per_worker} + index))
                  if [ "$request_id" -lt "{REQUESTS}" ]; then
                    curl -sS -o /dev/null -w '%{{http_code}}\\n' "http://gabion-nginx:8080/api/index.html?request_id=$request_id"
                  fi
                  index=$((index + 1))
                done
              }}
              worker_id=0
              while [ "$worker_id" -lt "{CONCURRENCY}" ]; do
                worker "$worker_id" &
                worker_id=$((worker_id + 1))
              done
              wait
"""


def wait_endpoint_count(service, expected):
    deadline = time.monotonic() + 120
    last = ""
    while time.monotonic() < deadline:
        result = run(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "get",
                "endpointslice",
                "-l",
                f"kubernetes.io/service-name={service}",
                "-o",
                'jsonpath={range .items[*].endpoints[*]}{.addresses[0]}{"\\n"}{end}',
            ],
            capture=True,
            check=False,
        )
        last = result.stdout
        count = len([line for line in last.splitlines() if line.strip()])
        if count == expected:
            return
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {expected} EndpointSlice addresses for {service}; saw {last!r}")


def pods_for_app(app):
    result = run(
        [
            "kubectl",
            "-n",
            NAMESPACE,
            "get",
            "pods",
            "-l",
            f"app={app}",
            "-o",
            'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
        ],
        capture=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def wait_http(url, timeout=30):
    """Poll an HTTP endpoint until it returns a non-5xx. Fails fast on
    4xx other than 408/429 because those are operator bugs (missing
    route, bad query) that won't heal by retrying — the previous
    blanket "swallow URLError" path turned a 404 into a 30s timeout,
    which is what masked the missing `/readyz` / `/debug/introspection`
    routes for the bench. 408 (Request Timeout) and 429 (Too Many
    Requests) are intentionally retried — they're transient by
    definition. Anything 5xx is also retried since the server may
    still be coming up.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status < 400:
                    return
                # 5xx is transient; 4xx (except 408/429) is fatal.
                if response.status >= 500 or response.status in (408, 429):
                    pass
                else:
                    raise RuntimeError(
                        f"{url} returned {response.status} — usually means the "
                        f"endpoint is missing or the URL is wrong. The bench "
                        f"won't retry 4xx other than 408/429."
                    )
        except urllib.error.HTTPError as err:
            if err.code >= 500 or err.code in (408, 429):
                pass
            else:
                raise RuntimeError(
                    f"{url} returned HTTP {err.code} — usually means the "
                    f"endpoint is missing or the URL is wrong. The bench "
                    f"won't retry 4xx other than 408/429."
                ) from err
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {url}")


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=1) as response:
        return json.loads(response.read().decode("utf-8"))


def start_port_forwards(pods):
    forwards = []
    for index, pod in enumerate(pods):
        port = ADMIN_BASE_PORT + index
        log(f"starting admin port-forward pod/{pod} on 127.0.0.1:{port}")
        proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                NAMESPACE,
                "port-forward",
                f"pod/{pod}",
                f"{port}:9090",
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        forwards.append((pod, port, proc))
    for pod, port, _ in forwards:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            proc = next(candidate for candidate in forwards if candidate[0] == pod)[2]
            if proc.poll() is not None:
                stdout, stderr = proc.communicate(timeout=1)
                raise RuntimeError(
                    f"port-forward for pod/{pod} exited with {proc.returncode}\n{stdout}{stderr}"
                )
            try:
                wait_http(f"http://127.0.0.1:{port}/readyz", timeout=1)
                break
            except RuntimeError:
                time.sleep(0.25)
        else:
            raise RuntimeError(f"timed out waiting for port-forward pod/{pod} on 127.0.0.1:{port}")
    return forwards


def stop_processes(processes):
    for _, _, proc in processes:
        proc.terminate()
    for _, _, proc in processes:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def sample_loop(forwards, rows, stop_event, started_at):
    # Log the first failure observed for each pod, then swallow the
    # rest. The previous implementation silently appended a row of
    # zeros on every failure, which made a missing `/debug/introspection`
    # endpoint look like "convergence never happened" rather than
    # "endpoint missing" — which is exactly the bug the bench tripped
    # over before /debug/introspection existed.
    logged_failures: set[str] = set()
    while not stop_event.is_set():
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        for pod, port, _ in forwards:
            try:
                data = fetch_json(
                    f"http://127.0.0.1:{port}/debug/introspection?max_cells={MAX_CELLS}&max_peers=256"
                )
                gossip = data.get("gossip") or {}
                peers = data.get("peers") or {}
                remote_cells = data.get("remote_cells") or []
                remote_total = sum(int(cell.get("count", 0)) for cell in remote_cells)
                rows.append(
                    {
                        "elapsed_ms": elapsed_ms,
                        "pod": pod,
                        "remote_total": remote_total,
                        "remote_active_cells": int(gossip.get("remote_active_cells", 0)),
                        "merge_cells": int(gossip.get("merge_cells", 0)),
                        "send_bytes": int(gossip.get("send_bytes", 0)),
                        "recv_bytes": int(gossip.get("recv_bytes", 0)),
                        "digest_mismatch": int(gossip.get("digest_mismatch", 0)),
                        "decode_errors": int(gossip.get("decode_errors", 0)),
                        "peers": len(peers.get("active_peers", [])),
                    }
                )
            except Exception as err:
                if pod not in logged_failures:
                    log(
                        f"sampler error for pod/{pod} on 127.0.0.1:{port}: "
                        f"{type(err).__name__}: {err} — subsequent failures "
                        f"for this pod will be silently zeroed."
                    )
                    logged_failures.add(pod)
                rows.append(
                    {
                        "elapsed_ms": elapsed_ms,
                        "pod": pod,
                        "remote_total": 0,
                        "remote_active_cells": 0,
                        "merge_cells": 0,
                        "send_bytes": 0,
                        "recv_bytes": 0,
                        "digest_mismatch": 0,
                        "decode_errors": 0,
                        "peers": 0,
                    }
                )
        stop_event.wait(SAMPLE_MS / 1000)


def parse_load_codes(logs):
    codes = [line.strip() for line in logs.splitlines() if line.strip().isdigit()]
    ok = sum(1 for code in codes if code.startswith("2"))
    limited = sum(1 for code in codes if code == "429")
    failures = len(codes) - ok - limited
    return {"attempted": len(codes), "ok": ok, "limited": limited, "failures": failures}


def write_csv(path, rows):
    fields = [
        "elapsed_ms",
        "pod",
        "remote_total",
        "remote_active_cells",
        "merge_cells",
        "send_bytes",
        "recv_bytes",
        "digest_mismatch",
        "decode_errors",
        "peers",
    ]
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def svg_line_chart(path, title, rows, y_field, expected=None):
    width = 1000
    height = 520
    left = 72
    right = 24
    top = 48
    bottom = 64
    plot_w = width - left - right
    plot_h = height - top - bottom
    by_pod = {}
    for row in rows:
        by_pod.setdefault(row["pod"], []).append(row)
    max_x = max((row["elapsed_ms"] for row in rows), default=1)
    max_y = max((row[y_field] for row in rows), default=1)
    if expected is not None:
        max_y = max(max_y, expected)
    max_y = max(max_y, 1)
    colors = ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#f59e0b", "#0891b2", "#4b5563", "#db2777"]

    def point(row):
        x = left + (row["elapsed_ms"] / max_x) * plot_w
        y = top + plot_h - (row[y_field] / max_y) * plot_h
        return f"{x:.2f},{y:.2f}"

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="28" font-family="sans-serif" font-size="20" font-weight="700">{title}</text>',
        f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#111827"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#111827"/>',
        f'<text x="{left}" y="{height - 18}" font-family="sans-serif" font-size="13">elapsed milliseconds</text>',
        f'<text x="12" y="{top + 16}" font-family="sans-serif" font-size="13">{y_field}</text>',
        f'<text x="{left - 8}" y="{top + plot_h + 20}" text-anchor="end" font-family="sans-serif" font-size="12">0</text>',
        f'<text x="{left - 8}" y="{top + 4}" text-anchor="end" font-family="sans-serif" font-size="12">{max_y}</text>',
        f'<text x="{left + plot_w}" y="{top + plot_h + 20}" text-anchor="end" font-family="sans-serif" font-size="12">{max_x} ms</text>',
    ]
    if expected is not None:
        y = top + plot_h - (expected / max_y) * plot_h
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#111827" stroke-dasharray="6 6"/>')
        parts.append(f'<text x="{left + plot_w - 6}" y="{y - 6:.2f}" text-anchor="end" font-family="sans-serif" font-size="12">expected {expected}</text>')
    for index, (pod, pod_rows) in enumerate(sorted(by_pod.items())):
        color = colors[index % len(colors)]
        points = " ".join(point(row) for row in pod_rows)
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points}"/>')
        parts.append(
            f'<text x="{left + 12}" y="{top + 22 + index * 18}" font-family="sans-serif" font-size="12" fill="{color}">{pod}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts))


def write_report(output_dir, summary, rows):
    write_csv(output_dir / "samples.csv", rows)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    svg_line_chart(output_dir / "remote-total.svg", "Remote Count Convergence", rows, "remote_total", summary["load"]["ok"])
    svg_line_chart(output_dir / "merge-cells.svg", "Gossip Merge Cells", rows, "merge_cells")
    svg_line_chart(output_dir / "peer-count.svg", "Discovered Peers Per Server Pod", rows, "peers")
    (output_dir / "index.html").write_text(
        """<!doctype html>
<html>
<head><meta charset="utf-8"><title>Gabion gossip propagation benchmark</title></head>
<body>
<h1>Gabion gossip propagation benchmark</h1>
<pre>{summary}</pre>
<img src="remote-total.svg" alt="remote count convergence">
<img src="merge-cells.svg" alt="merge cells">
<img src="peer-count.svg" alt="peer count">
</body>
</html>
""".format(summary=json.dumps(summary, indent=2, sort_keys=True))
    )


def convergence_summary(rows, target):
    by_pod = {}
    for row in rows:
        by_pod.setdefault(row["pod"], []).append(row)
    result = {}
    threshold_names = [("p50_ms", 0.50), ("p90_ms", 0.90), ("p99_ms", 0.99), ("full_ms", 1.00)]
    for pod, pod_rows in by_pod.items():
        pod_result = {}
        for name, fraction in threshold_names:
            threshold = int(target * fraction)
            hit = next((row["elapsed_ms"] for row in pod_rows if row["remote_total"] >= threshold), None)
            pod_result[name] = hit
        final = pod_rows[-1] if pod_rows else {}
        pod_result["final_remote_total"] = final.get("remote_total", 0)
        pod_result["final_accuracy"] = (final.get("remote_total", 0) / target) if target else 0
        pod_result["final_peers"] = final.get("peers", 0)
        result[pod] = pod_result
    return result


def benchmark_failures(load, convergence):
    failures = []
    if load["attempted"] != REQUESTS:
        failures.append(f"load attempted {load['attempted']} requests, expected {REQUESTS}")
    if load["failures"] != 0:
        failures.append(f"load had {load['failures']} non-2xx/non-429 responses")
    if load["limited"] != 0:
        failures.append(f"load had {load['limited']} 429 responses with benchmark limit {RULE_LIMIT}")
    if load["ok"] != REQUESTS:
        failures.append(f"load completed {load['ok']} successful requests, expected {REQUESTS}")
    for pod, pod_result in sorted(convergence.items()):
        if pod_result["final_remote_total"] < load["ok"]:
            failures.append(
                f"{pod} converged to {pod_result['final_remote_total']} remote counts, expected {load['ok']}"
            )
        if pod_result["full_ms"] is None:
            failures.append(f"{pod} never reached full convergence")
        if pod_result["final_peers"] < SERVER_REPLICAS + NGINX_REPLICAS - 1:
            failures.append(
                f"{pod} discovered {pod_result['final_peers']} peers, expected at least "
                f"{SERVER_REPLICAS + NGINX_REPLICAS - 1}"
            )
    return failures


def main():
    context, server = guard_local_kind()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    forwards = []
    stop_event = threading.Event()
    rows = []
    sampler = None
    started_at = time.monotonic()
    try:
        log("building local NGINX module image")
        run(["docker", "compose", "--profile", "module", "-f", "deploy/nginx/docker-compose.yml", "build", "nginx-module-request-smoke"])
        log("building local gabiond image")
        run(["docker", "build", "-f", "deploy/gabiond/Dockerfile", "-t", "gabiond:local", "."])

        log(f"creating namespace {NAMESPACE}")
        run(["kubectl", "create", "namespace", NAMESPACE])
        log("applying benchmark manifests")
        run(["kubectl", "-n", NAMESPACE, "apply", "-f", "-"], input_text=manifest())
        run(["kubectl", "-n", NAMESPACE, "rollout", "status", "deployment/gabiond", "--timeout=180s"])
        run(["kubectl", "-n", NAMESPACE, "rollout", "status", "deployment/gabion-nginx", "--timeout=180s"])
        log("waiting for initial EndpointSlices")
        wait_endpoint_count("gabiond", SERVER_REPLICAS)
        wait_endpoint_count("gabion-nginx", INITIAL_NGINX_REPLICAS)
        if INITIAL_NGINX_REPLICAS != NGINX_REPLICAS:
            log(f"scaling NGINX from {INITIAL_NGINX_REPLICAS} to {NGINX_REPLICAS} replicas")
            run(["kubectl", "-n", NAMESPACE, "scale", "deployment/gabion-nginx", f"--replicas={NGINX_REPLICAS}"])
            run(["kubectl", "-n", NAMESPACE, "rollout", "status", "deployment/gabion-nginx", "--timeout=180s"])
        log("waiting for final EndpointSlices")
        wait_endpoint_count("gabion-nginx", NGINX_REPLICAS)

        server_pods = pods_for_app("gabiond")
        log(f"sampling server pods: {', '.join(server_pods)}")
        forwards = start_port_forwards(server_pods)
        started_at = time.monotonic()
        sampler = threading.Thread(target=sample_loop, args=(forwards, rows, stop_event, started_at), daemon=True)
        sampler.start()

        log(f"starting in-cluster load job: requests={REQUESTS} concurrency={CONCURRENCY}")
        run(["kubectl", "-n", NAMESPACE, "apply", "-f", "-"], input_text=load_job_manifest())
        run(["kubectl", "-n", NAMESPACE, "wait", "--for=condition=complete", "job/gabion-load", f"--timeout={TIMEOUT_SECONDS}s"])
        logs = run(["kubectl", "-n", NAMESPACE, "logs", "job/gabion-load"], capture=True).stdout
        load = parse_load_codes(logs)
        log(f"load completed: {load}")

        deadline = time.monotonic() + TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            latest_by_pod = {}
            for row in rows:
                latest_by_pod[row["pod"]] = row
            if latest_by_pod and all(row["remote_total"] >= load["ok"] for row in latest_by_pod.values()):
                break
            time.sleep(SAMPLE_MS / 1000)

        stop_event.set()
        if sampler is not None:
            sampler.join(timeout=5)

        convergence = convergence_summary(rows, load["ok"])
        failures = benchmark_failures(load, convergence)
        summary = {
            "context": context,
            "server": server,
            "namespace": NAMESPACE,
            "parameters": {
                "server_replicas": SERVER_REPLICAS,
                "initial_nginx_replicas": INITIAL_NGINX_REPLICAS,
                "nginx_replicas": NGINX_REPLICAS,
                "requests": REQUESTS,
                "concurrency": CONCURRENCY,
                "linger_ms": LINGER_MS,
                "fanout": FANOUT,
                "sample_ms": SAMPLE_MS,
            },
            "load": load,
            "convergence": convergence,
            "failures": failures,
            "output_dir": str(OUTPUT_DIR),
        }
        write_report(OUTPUT_DIR, summary, rows)
        print(json.dumps(summary, indent=2, sort_keys=True))
        print(f"gabion gossip propagation benchmark wrote {OUTPUT_DIR}")
        if failures:
            raise SystemExit("gabion gossip propagation benchmark failed:\n" + "\n".join(failures))
    finally:
        stop_event.set()
        if sampler is not None:
            sampler.join(timeout=5)
        stop_processes(forwards)
        # Dump pod state BEFORE deleting the namespace — otherwise CI
        # logs lose every signal about why pods crashed.
        log(f"\n--- cleanup diagnostic dump (namespace={NAMESPACE}) ---")
        # --tail=1000 captures the smoke-with-bt wrapper's gdb output
        # on a native SIGSEGV; 200 was enough for plain "rollout timed
        # out" but truncates a full thread bt.
        for cmd in (
            ["kubectl", "-n", NAMESPACE, "get", "pods,events", "--sort-by=.lastTimestamp", "-o", "wide"],
            ["kubectl", "-n", NAMESPACE, "describe", "pods", "-l", "app=gabiond"],
            ["kubectl", "-n", NAMESPACE, "describe", "pods", "-l", "app=gabion-nginx"],
            ["kubectl", "-n", NAMESPACE, "logs", "--all-containers", "--tail=1000", "-l", "app=gabiond"],
            ["kubectl", "-n", NAMESPACE, "logs", "--all-containers", "--tail=1000", "--previous", "-l", "app=gabiond"],
            ["kubectl", "-n", NAMESPACE, "logs", "--all-containers", "--tail=1000", "-l", "app=gabion-nginx"],
            ["kubectl", "-n", NAMESPACE, "logs", "--all-containers", "--tail=1000", "--previous", "-l", "app=gabion-nginx"],
        ):
            run(cmd, check=False)
        # Best-effort: copy /tmp/cores out of any pod that's still
        # Running. Crashed pods are unreachable via `kubectl cp` (it
        # uses `kubectl exec` under the hood); the in-pod gdb dump in
        # the logs above is the primary signal.
        os.makedirs("/tmp/cores", exist_ok=True)
        running = run(
            [
                "kubectl", "-n", NAMESPACE, "get", "pods",
                "-o", 'jsonpath={.items[?(@.status.phase=="Running")].metadata.name}',
            ],
            capture=True,
            check=False,
        )
        for pod in (running.stdout or "").split():
            run(
                ["kubectl", "-n", NAMESPACE, "cp", f"{pod}:/tmp/cores", f"/tmp/cores/{NAMESPACE}-{pod}"],
                check=False,
            )
        log("--- end cleanup diagnostic dump ---\n")
        if KEEP_NAMESPACE:
            print(f"kept namespace {NAMESPACE}", file=sys.stderr)
        else:
            run(["kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found=true"], check=False)


if __name__ == "__main__":
    main()
