#!/usr/bin/env bash
# Rebuild docs/study/data from matrix.py results with the harness judge: the final
# judge over every run, then posterior.json, runs.json, seed_metrics.json and
# self_posterior.json, compacted, then the study tests. It starts from the
# current study-data release (`make study-data`), which also holds the evidence
# refresh does not rebuild; publish the result as a new release (README, "Data").
#
#   docs/study/scripts/refresh.sh <harness-checkout> <baseline-results> <improved-results> <scratch-dir>
#
# <harness-checkout> is a checkout of sim/harness at 896ea20; the two results directories
# hold <solution>/<regime>/seed-<n>.json (symlinks are followed); <scratch-dir> is
# overwritten. Needs python3, rsync, and docs/study/tests/requirements.txt for the tests.
set -euo pipefail
if [ $# -ne 4 ]; then
    sed -n '2,12p' "$0" >&2
    exit 2
fi
HARNESS=$1 BASE=$2 IMP=$3 WORK=$4
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=$HERE/../data
JUDGE=$HARNESS/crates/gossip-bench/judge/judge.py
make -C "$HERE/../../.." study-data
RUN_AT=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["run_at_default"])' "$DATA/provenance.json")

rm -rf "$WORK" && mkdir -p "$WORK/all"
for d in "$BASE" "$IMP"; do
    rsync -aL --include='*/' --include='seed-*.json' --exclude='*' "$d"/ "$WORK/all"/
done
python3 "$JUDGE" "$WORK/all" --baseline main --out "$WORK/final-judge" >/dev/null
cp "$WORK/final-judge/summary.csv" "$WORK/final-judge/pareto.md" "$WORK/final-judge/flags.md" \
    "$WORK/final-judge/rejected.md" "$DATA/judge/"
python3 "$HERE/export_posterior.py" --summary "$WORK/final-judge/summary.csv" \
    --metrics "$DATA/metrics.json" --out "$DATA/posterior.json"
python3 "$HERE/export_runs.py" --out "$DATA/runs.json" --run-at-default "$RUN_AT" "$BASE" "$IMP"
python3 "$HERE/export_seeds.py" --judge "$JUDGE" --out "$DATA/seed_metrics.json" "$BASE" "$IMP"
python3 "$HERE/self_judge.py" --judge "$JUDGE" --baseline-results "$BASE" --improved-results "$IMP" \
    --arms "$DATA/arms.json" --metrics "$DATA/metrics.json" --work "$WORK/self" \
    --out "$DATA/self_posterior.json"
python3 "$HERE/compact.py" "$DATA"
make -C "$HERE/../../.." study-test
