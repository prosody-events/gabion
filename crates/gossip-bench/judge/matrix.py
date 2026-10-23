#!/usr/bin/env python3
"""Run the study harness across regimes x seeds for the checked-out solution.

Builds `gossip-bench` (release, `--no-default-features`), runs
`gossip-bench study run` for every (regime, seed) pair with `--jobs`
parallelism, and writes one stamped result per run to
`<out>/<solution>/<regime>/seed-<n>.json`. A run whose output already exists
with the same stamp (harness tree hash and solution commit) is skipped, so an
interrupted matrix resumes where it stopped.

The stamp identifies what produced the numbers: the harness tree hash
(`git rev-parse HEAD:crates/gossip-bench`), the solution label, branch and
commit. The judge refuses to compare results with different harness hashes.
`process` carries wall time, CPU time and peak RSS of the run as measured by
`wait4`; these are host-dependent and noisy, unlike everything else.

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path

HARNESS_DIR = "crates/gossip-bench"


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def stamp_for(repo: Path, solution: str, allow_dirty: bool) -> dict:
    harness_dirty = bool(git(repo, "status", "--porcelain", "--", HARNESS_DIR))
    if harness_dirty and not allow_dirty:
        sys.exit(
            f"error: {HARNESS_DIR} has uncommitted changes, so its tree hash would not "
            "describe the harness that ran. Commit or stash them, or pass --allow-dirty "
            "for a throwaway run (the judge rejects dirty stamps)."
        )
    return {
        "harness_tree_hash": git(repo, "rev-parse", f"HEAD:{HARNESS_DIR}"),
        "harness_dirty": harness_dirty,
        "solution": solution,
        "solution_branch": git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
        "solution_commit": git(repo, "rev-parse", "HEAD"),
        "solution_dirty": bool(git(repo, "status", "--porcelain")),
    }


def build(repo: Path) -> Path:
    subprocess.run(
        ["cargo", "build", "--release", "-p", "gossip-bench", "--no-default-features"],
        cwd=repo,
        check=True,
    )
    target = Path(os.environ.get("CARGO_TARGET_DIR", repo / "target"))
    return target / "release" / "gossip-bench"


def regimes(repo: Path, names: str) -> list[Path]:
    directory = repo / HARNESS_DIR / "regimes"
    available = {p.stem: p for p in sorted(directory.glob("*.json"))}
    if names == "all":
        return list(available.values())
    picked = []
    for name in names.split(","):
        if name not in available:
            sys.exit(f"error: no regime `{name}` in {directory}; have {', '.join(available)}")
        picked.append(available[name])
    return picked


def seeds(spec: str) -> list[int]:
    if "," in spec or "-" in spec:
        out = []
        for part in spec.split(","):
            lo, _, hi = part.partition("-")
            out.extend(range(int(lo), int(hi or lo) + 1))
        return out
    return list(range(1, int(spec) + 1))


def is_done(path: Path, stamp: dict) -> bool:
    try:
        existing = json.loads(path.read_text())["stamp"]
    except (OSError, ValueError, KeyError):
        return False
    return all(
        existing.get(k) == stamp[k] for k in ("harness_tree_hash", "solution_commit", "solution")
    )


def max_rss_bytes(ru_maxrss: int) -> int:
    # Darwin reports bytes, Linux kilobytes.
    return ru_maxrss if platform.system() == "Darwin" else ru_maxrss * 1024


def run_one(binary: Path, regime: Path, seed: int, out: Path, stamp: dict) -> str:
    out.parent.mkdir(parents=True, exist_ok=True)
    partial = out.with_suffix(".partial")
    start = time.monotonic()
    log = out.with_suffix(".stderr")
    with log.open("wb") as stderr_file:
        proc = subprocess.Popen(
            [str(binary), "study", "run", "--regime", str(regime), "--seed", str(seed),
             "--out", str(partial)],
            stderr=stderr_file,
        )
        _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    wall = time.monotonic() - start
    stderr = log.read_text(errors="replace")
    log.unlink(missing_ok=True)
    if proc.returncode != 0:
        return f"FAILED {regime.stem} seed {seed} (exit {proc.returncode}): {stderr.strip()}"
    result = json.loads(partial.read_text())
    result["stamp"] = stamp
    result["process"] = {
        "wall_s": round(wall, 3),
        "cpu_user_s": round(usage.ru_utime, 3),
        "cpu_sys_s": round(usage.ru_stime, 3),
        "max_rss_bytes": max_rss_bytes(usage.ru_maxrss),
        "host": platform.node(),
        "noisy": True,
    }
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(result, indent=1) + "\n")
    tmp.replace(out)
    partial.unlink(missing_ok=True)
    valid = "valid" if result.get("valid") else f"INVALID {result.get('invalid_reasons')}"
    return (
        f"{regime.stem} seed {seed}: {valid}, wall {wall:.1f} s, "
        f"rss {result['process']['max_rss_bytes'] / 2**20:.0f} MiB"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--solution", required=True,
                        help="label for this solution, e.g. `main` (the baseline)")
    parser.add_argument("--regimes", default="all", help="comma-separated ids, or `all`")
    parser.add_argument("--seeds", default="1",
                        help="count (`3` = seeds 1..3) or list/ranges (`1,4-6`)")
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--no-build", action="store_true",
                        help="reuse the existing release binary")
    args = parser.parse_args()

    repo = Path(git(args.repo, "rev-parse", "--show-toplevel"))
    stamp = stamp_for(repo, args.solution, args.allow_dirty)
    binary = (Path(os.environ.get("CARGO_TARGET_DIR", repo / "target")) / "release"
              / "gossip-bench") if args.no_build else build(repo)

    todo = []
    for regime in regimes(repo, args.regimes):
        for seed in seeds(args.seeds):
            out = args.out / args.solution / regime.stem / f"seed-{seed}.json"
            if is_done(out, stamp):
                print(f"skip {regime.stem} seed {seed}: already done", flush=True)
            else:
                todo.append((regime, seed, out))
    print(f"{len(todo)} runs, {args.jobs} at a time; harness {stamp['harness_tree_hash'][:12]}, "
          f"{stamp['solution']} @ {stamp['solution_commit'][:12]}", flush=True)

    lock = threading.Lock()
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(run_one, binary, r, s, o, stamp) for r, s, o in todo]
        for future in concurrent.futures.as_completed(futures):
            line = future.result()
            with lock:
                failures += line.startswith("FAILED")
                print(line, flush=True)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
