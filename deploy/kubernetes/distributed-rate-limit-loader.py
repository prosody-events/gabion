#!/usr/bin/env python3
"""In-cluster load generator for the distributed rate-limit test.

Spawns one keep-alive HTTP/1.1 client per tenant, each pacing itself at
`RPS_PER_TENANT` requests per second toward `TARGET_URL?tenant=<i>` for
`DURATION_S` seconds. The shared cluster-wide expected-allowed rate is
`TENANTS * BUDGET_PER_TENANT`; everything above that should land in 429s
once gossip has propagated.

Reports a JSON summary to stdout so the wrapping bash script can grep
out the headline numbers without parsing free text.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
from collections import Counter
from urllib.parse import urlsplit


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


TARGET_URL = os.environ.get("TARGET_URL", "http://gabion-nginx:8080/tenant/index.html")
TENANTS = env_int("TENANTS", 100)
RPS_PER_TENANT = env_float("RPS_PER_TENANT", 50.0)
BUDGET_PER_TENANT = env_float("BUDGET_PER_TENANT", 20.0)
DURATION_S = env_float("DURATION_S", 60.0)
WARMUP_S = env_float("WARMUP_S", 5.0)
REQUEST_TIMEOUT_S = env_float("REQUEST_TIMEOUT_S", 3.0)

split = urlsplit(TARGET_URL)
HOST = split.hostname or "gabion-nginx"
PORT = split.port or (443 if split.scheme == "https" else 80)
BASE_PATH = split.path or "/"


def worker(
    tenant_id: int,
    counts: Counter,
    measure_start: float,
    stop_at: float,
    interval_s: float,
    error_log: list,
) -> None:
    """Drive one tenant at a fixed RPS, keep-alive single connection.

    Counts only span [measure_start, stop_at) so warm-up traffic doesn't
    skew the ratio. A request that crosses the boundary still counts
    against the bucket it returns in.
    """
    path = f"{BASE_PATH}?tenant={tenant_id}"
    conn = http.client.HTTPConnection(HOST, PORT, timeout=REQUEST_TIMEOUT_S)
    next_at = time.monotonic()
    while True:
        now = time.monotonic()
        if now >= stop_at:
            break
        if now < next_at:
            time.sleep(min(next_at - now, 0.005))
            continue
        next_at += interval_s
        measuring = now >= measure_start
        try:
            conn.request("GET", path, headers={"Host": HOST})
            resp = conn.getresponse()
            code = resp.status
            # Drain body so the connection stays usable for keep-alive.
            resp.read()
        except (http.client.HTTPException, socket.error, OSError) as e:
            code = 0
            error_log.append(repr(e))
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(HOST, PORT, timeout=REQUEST_TIMEOUT_S)
        if measuring:
            counts[code] += 1
    try:
        conn.close()
    except Exception:
        pass


def main() -> int:
    print(
        f"loader: TARGET_URL={TARGET_URL} TENANTS={TENANTS} "
        f"RPS_PER_TENANT={RPS_PER_TENANT} BUDGET_PER_TENANT={BUDGET_PER_TENANT} "
        f"DURATION_S={DURATION_S} WARMUP_S={WARMUP_S}",
        flush=True,
    )

    interval_s = 1.0 / RPS_PER_TENANT
    started_at = time.monotonic()
    measure_start = started_at + WARMUP_S
    stop_at = measure_start + DURATION_S

    per_tenant: list[Counter] = [Counter() for _ in range(TENANTS)]
    per_tenant_errors: list[list] = [[] for _ in range(TENANTS)]

    threads = []
    for i in range(TENANTS):
        t = threading.Thread(
            target=worker,
            args=(
                i,
                per_tenant[i],
                measure_start,
                stop_at,
                interval_s,
                per_tenant_errors[i],
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Progress heartbeat every 5s during the measurement window so the
    # operator can confirm load is sustained.
    last_progress = measure_start
    while time.monotonic() < stop_at:
        now = time.monotonic()
        if now - last_progress >= 5.0:
            total = sum(sum(c.values()) for c in per_tenant)
            print(
                f"loader: t={now - measure_start:5.1f}s total_so_far={total}",
                flush=True,
            )
            last_progress = now
        time.sleep(0.5)

    for t in threads:
        t.join(timeout=REQUEST_TIMEOUT_S * 2)

    total = Counter()
    for c in per_tenant:
        total.update(c)

    measured_s = stop_at - measure_start
    allowed = total[200]
    rejected = total[429]
    failed = total[0]
    other = sum(v for k, v in total.items() if k not in (200, 429, 0))

    expected_allowed = TENANTS * BUDGET_PER_TENANT * measured_s
    expected_attempts = TENANTS * RPS_PER_TENANT * measured_s

    per_tenant_allowed = [c[200] for c in per_tenant]
    per_tenant_rejected = [c[429] for c in per_tenant]
    sorted_allowed = sorted(per_tenant_allowed)
    p50 = sorted_allowed[TENANTS // 2]
    p95 = sorted_allowed[int(TENANTS * 0.95) - 1] if TENANTS >= 20 else sorted_allowed[-1]
    p99 = sorted_allowed[int(TENANTS * 0.99) - 1] if TENANTS >= 100 else sorted_allowed[-1]

    summary = {
        "config": {
            "target_url": TARGET_URL,
            "tenants": TENANTS,
            "rps_per_tenant": RPS_PER_TENANT,
            "budget_per_tenant_per_sec": BUDGET_PER_TENANT,
            "duration_s": DURATION_S,
            "warmup_s": WARMUP_S,
            "measured_s": measured_s,
        },
        "totals": {
            "attempts": sum(total.values()),
            "expected_attempts": expected_attempts,
            "allowed_200": allowed,
            "expected_allowed": expected_allowed,
            "rejected_429": rejected,
            "failed_0": failed,
            "other": other,
            "status_codes": dict(total),
        },
        "ratio": {
            "allowed_over_expected": (
                allowed / expected_allowed if expected_allowed > 0 else None
            ),
            "actual_allowed_per_sec": allowed / measured_s if measured_s > 0 else 0,
            "expected_allowed_per_sec": TENANTS * BUDGET_PER_TENANT,
        },
        "per_tenant_allowed": {
            "min": min(per_tenant_allowed) if per_tenant_allowed else 0,
            "p50": p50,
            "p95": p95,
            "p99": p99,
            "max": max(per_tenant_allowed) if per_tenant_allowed else 0,
        },
        "per_tenant_rejected": {
            "min": min(per_tenant_rejected) if per_tenant_rejected else 0,
            "max": max(per_tenant_rejected) if per_tenant_rejected else 0,
        },
    }

    # Sample a few error messages if any connections failed.
    errors_sample: list[str] = []
    for errs in per_tenant_errors:
        for err in errs:
            if err not in errors_sample:
                errors_sample.append(err)
            if len(errors_sample) >= 5:
                break
        if len(errors_sample) >= 5:
            break
    if errors_sample:
        summary["errors_sample"] = errors_sample

    # Human-readable summary first so it's the headline a reader sees
    # when they `kubectl logs job/loader`. The structured JSON follows
    # between sentinels so a wrapping script can extract it cleanly.
    cfg = summary["config"]
    tot = summary["totals"]
    rat = summary["ratio"]
    pa = summary["per_tenant_allowed"]
    pr = summary["per_tenant_rejected"]
    print()
    print("=== distributed rate-limit test summary ===")
    print(f"  tenants                    {cfg['tenants']}")
    print(f"  budget per tenant (r/s)    {cfg['budget_per_tenant_per_sec']}")
    print(f"  load per tenant (r/s)      {cfg['rps_per_tenant']}")
    print(f"  measured window (s)        {cfg['measured_s']:.1f}")
    print()
    print(f"  expected attempts          {tot['expected_attempts']:.0f}")
    print(f"  actual attempts            {tot['attempts']}")
    print(f"  expected allowed           {tot['expected_allowed']:.0f}")
    print(f"  actual allowed (200)       {tot['allowed_200']}")
    print(f"  actual rejected (429)      {tot['rejected_429']}")
    print(f"  failed connections (0)     {tot['failed_0']}")
    print(f"  other status codes         {tot['other']}")
    print()
    ratio = rat["allowed_over_expected"]
    print(f"  allowed / expected         {ratio:.3f}" if ratio is not None else "  allowed / expected         n/a")
    print(f"  actual allowed/sec         {rat['actual_allowed_per_sec']:.1f}")
    print(f"  expected allowed/sec       {rat['expected_allowed_per_sec']:.1f}")
    print()
    print(
        "  per-tenant allowed         "
        f"min={pa['min']}  p50={pa['p50']}  p95={pa['p95']}  p99={pa['p99']}  max={pa['max']}"
    )
    print(f"  per-tenant rejected        min={pr['min']}  max={pr['max']}")
    if "errors_sample" in summary:
        print()
        print("  connection error samples:")
        for err in summary["errors_sample"]:
            print(f"    {err}")
    print()

    print("---LOADER-SUMMARY-BEGIN---", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    print("---LOADER-SUMMARY-END---", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
