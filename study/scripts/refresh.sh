#!/usr/bin/env bash
# Rebuild study/data from matrix.py results with the harness judge:
# the final judge over every run, posterior.json, runs.json and self_posterior.json.
#
#   study/scripts/refresh.sh <harness-checkout> <baseline-results> <improved-results> <scratch-dir>
#
# <harness-checkout> is a checkout of sim/harness; the two results directories hold
# <solution>/<regime>/seed-<n>.json; <scratch-dir> is overwritten. Needs python3 and rsync.
set -euo pipefail
if [ $# -ne 4 ]; then
    sed -n '2,8p' "$0" >&2
    exit 2
fi
HARNESS=$1 BASE=$2 IMP=$3 WORK=$4
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=$HERE/../data
JUDGE=$HARNESS/crates/gossip-bench/judge/judge.py

rm -rf "$WORK" && mkdir -p "$WORK/all"
for d in "$BASE" "$IMP"; do
    rsync -a --include='*/' --include='seed-*.json' --exclude='*' "$d"/ "$WORK/all"/
done
python3 "$JUDGE" "$WORK/all" --baseline main --out "$WORK/final-judge" >/dev/null
cp "$WORK/final-judge/summary.csv" "$WORK/final-judge/pareto.md" "$WORK/final-judge/flags.md" \
    "$WORK/final-judge/rejected.md" "$DATA/judge/"
python3 "$HERE/export_posterior.py" --summary "$WORK/final-judge/summary.csv" \
    --metrics "$DATA/metrics.json" --out "$DATA/posterior.json"
python3 "$HERE/export_runs.py" --out "$DATA/runs.json" "$BASE" "$IMP"
python3 "$HERE/self_judge.py" --judge "$JUDGE" --baseline-results "$BASE" --improved-results "$IMP" \
    --arms "$DATA/arms.json" --metrics "$DATA/metrics.json" --work "$WORK/self" \
    --out "$DATA/self_posterior.json"
