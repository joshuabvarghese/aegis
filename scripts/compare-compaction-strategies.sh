#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage — STCS vs LCS: Same Workload, Real Numbers
#
#  Runs the identical write/flush/read workload against two instances of the
#  same engine — one running STCS, one running LCS — and prints a real,
#  measured comparison of read amplification, write amplification, and level
#  structure. Not an argument, a measurement.
#
#  Usage:
#    ./scripts/compare-compaction-strategies.sh [write_count] [flush_every] [read_sample]
#
#  Defaults: 3000 writes, flush every 100, sample 300 reads.
# ═══════════════════════════════════════════════════════════════════════════════
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

WRITE_COUNT="${1:-3000}"
FLUSH_EVERY="${2:-100}"
READ_SAMPLE="${3:-300}"

IMAGE="aegis-storage:latest"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; RESET='\033[0m'
info() { echo -e "${CYAN}[compare]${RESET} $*"; }

cleanup() {
    docker rm -f aegis-cmp-stcs aegis-cmp-lcs >/dev/null 2>&1 || true
    docker volume rm aegis-cmp-stcs-data aegis-cmp-lcs-data >/dev/null 2>&1 || true
}
cleanup
trap cleanup EXIT

info "Building image..."
docker build -q -t "$IMAGE" . >/dev/null || { echo "docker build failed"; exit 1; }

run_workload() {
    local name="$1" port="$2" strategy="$3"
    local base="http://localhost:${port}"

    docker volume create "aegis-cmp-${name}-data" >/dev/null
    docker run -d --name "aegis-cmp-${name}" \
        -p "${port}:8080" \
        -e COMPACTION_STRATEGY="$strategy" \
        -v "aegis-cmp-${name}-data:/data" \
        "$IMAGE" serve >/dev/null

    for _ in $(seq 1 40); do
        curl -sf "${base}/healthz" >/dev/null 2>&1 && break
        sleep 0.5
    done

    info "[$strategy] Writing ${WRITE_COUNT} rows, flushing every ${FLUSH_EVERY}..."
    for i in $(seq 1 "$WRITE_COUNT"); do
        curl -sf -X POST "${base}/write?key=row-${i}&col=v&val=${i}" >/dev/null 2>&1
        if (( i % FLUSH_EVERY == 0 )); then
            curl -sf -X POST "${base}/flush" >/dev/null 2>&1
        fi
    done
    curl -sf -X POST "${base}/flush" >/dev/null 2>&1

    info "[$strategy] Letting the compaction daemon catch up..."
    sleep 6

    info "[$strategy] Reading back a ${READ_SAMPLE}-row sample..."
    step=$(( WRITE_COUNT / READ_SAMPLE ))
    [[ $step -lt 1 ]] && step=1
    i=1
    while [[ $i -le $WRITE_COUNT ]]; do
        curl -sf "${base}/read?key=row-${i}" >/dev/null 2>&1
        i=$(( i + step ))
    done

    curl -sf "${base}/stats"  > "/tmp/aegis-cmp-${name}-stats.json"
    curl -sf "${base}/levels" > "/tmp/aegis-cmp-${name}-levels.json"

    docker rm -f "aegis-cmp-${name}" >/dev/null 2>&1
}

run_workload stcs 18091 STCS
run_workload lcs  18092 LCS

echo ""
python3 "$PROJECT_DIR/scripts/render-compaction-comparison.py" \
    /tmp/aegis-cmp-stcs-stats.json /tmp/aegis-cmp-stcs-levels.json \
    /tmp/aegis-cmp-lcs-stats.json  /tmp/aegis-cmp-lcs-levels.json
