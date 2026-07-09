#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage — Real Benchmark Numbers
#
#  The README's Benchmarks table originally shipped with hand-estimated ranges
#  ("expected on Apple Silicon M1"). This script replaces that guesswork: it
#  runs the actual JMH suite in the same container image used everywhere else,
#  and rewrites the README table with real numbers measured on your machine,
#  timestamped.
#
#  Usage:
#    ./scripts/update-benchmark-numbers.sh
#
#  Takes a few minutes — JMH runs multiple forks/warmup iterations per
#  benchmark by design, so the numbers are stable, not a single noisy sample.
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

RESULTS_DIR="$PROJECT_DIR/benchmark-results"
mkdir -p "$RESULTS_DIR"
RESULTS_JSON="$RESULTS_DIR/benchmark-results.json"
rm -f "$RESULTS_JSON"

echo "[bench] Building image..."
docker build -q -t aegis-storage:latest . >/dev/null

echo "[bench] Running JMH benchmarks — this takes a few minutes (warmup + measurement iterations)..."
docker run --rm \
    -v "${RESULTS_DIR}:/data" \
    aegis-storage:latest bench

if [[ ! -f "$RESULTS_JSON" ]]; then
    echo "[bench] ERROR: no results file produced at $RESULTS_JSON"
    echo "        check the container output above for a JMH error."
    exit 1
fi

echo "[bench] Parsing results and updating README.md..."
python3 "$PROJECT_DIR/scripts/render-benchmark-table.py" "$RESULTS_JSON" "$PROJECT_DIR/README.md"

echo "[bench] Done. README.md's Benchmarks table now reflects a real run on this machine."
echo "[bench] Raw JMH output kept at: $RESULTS_JSON"
