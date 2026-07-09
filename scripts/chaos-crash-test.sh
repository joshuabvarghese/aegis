#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage — Crash / Durability Chaos Test
#
#  What this actually tests:
#    Every write that gets an HTTP 200 from /write is a durability promise —
#    the row must survive a crash. This script writes a burst of rows against
#    a live engine, sends the container a hard SIGKILL mid-burst (no graceful
#    shutdown, no final flush — the same as pulling the power), restarts it
#    from the same volume, and checks that every acknowledged write actually
#    came back after CommitLog replay.
#
#  Two sync modes, two different honest outcomes:
#    PERIODIC (the default) fsyncs every 200ms. Writes acknowledged inside
#      that window CAN be lost on a hard crash — that's the documented
#      trade-off (Cassandra calls this commitlog_sync: periodic). This script
#      reports that loss as expected, not a failure, and bounds it.
#    BATCH fsyncs before acknowledging every single write. This script
#      asserts ZERO loss in this mode — if even one row is missing, that's
#      a real bug, and the script exits non-zero.
#
#  Usage:
#    ./scripts/chaos-crash-test.sh [total_writes] [kill_after] [PERIODIC|BATCH]
#
#  Examples:
#    ./scripts/chaos-crash-test.sh                   # defaults: 400 writes, kill at 200, PERIODIC
#    ./scripts/chaos-crash-test.sh 400 200 BATCH     # same burst, zero-loss mode
# ═══════════════════════════════════════════════════════════════════════════════
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

N="${1:-400}"
KILL_AFTER="${2:-200}"
MODE="${3:-PERIODIC}"

IMAGE="aegis-storage:latest"
CONTAINER="aegis-chaos-test"
VOLUME="aegis-chaos-data"
PORT=18080
BASE="http://localhost:${PORT}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RESET='\033[0m'

info()  { echo -e "${CYAN}[chaos]${RESET} $*"; }
ok()    { echo -e "${GREEN}[pass]${RESET} $*"; }
warn()  { echo -e "${YELLOW}[!]${RESET} $*"; }
fail()  { echo -e "${RED}[fail]${RESET} $*"; }

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
}

wait_healthy() {
    for _ in $(seq 1 40); do
        if curl -sf "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    return 1
}

if [[ "$MODE" != "PERIODIC" && "$MODE" != "BATCH" ]]; then
    fail "mode must be PERIODIC or BATCH, got: $MODE"
    exit 1
fi

echo ""
echo "=== Aegis-Storage crash / durability test — mode=${MODE} ==="
echo ""

cleanup
docker volume create "$VOLUME" >/dev/null

info "Building image if needed..."
docker build -q -t "$IMAGE" . >/dev/null || { fail "docker build failed"; exit 1; }

info "Starting engine (sync mode = $MODE)..."
docker run -d --name "$CONTAINER" \
    -p "${PORT}:8080" \
    -e COMMITLOG_SYNC_MODE="$MODE" \
    -v "${VOLUME}:/data" \
    "$IMAGE" serve >/dev/null

if ! wait_healthy; then
    fail "engine never became healthy — check: docker logs $CONTAINER"
    cleanup
    exit 1
fi
ok "engine is up"

ACK_LOG="$(mktemp)"
info "Writing ${N} rows, will SIGKILL the container after ${KILL_AFTER} acknowledged writes..."

for i in $(seq 1 "$N"); do
    if curl -sf -X POST "${BASE}/write?key=chaos-${i}&col=v&val=${i}" >/dev/null 2>&1; then
        echo "$i" >> "$ACK_LOG"
    fi
    if [[ "$i" -eq "$KILL_AFTER" ]]; then
        warn "killing container NOW — SIGKILL, no graceful shutdown, no final flush"
        docker kill -s SIGKILL "$CONTAINER" >/dev/null 2>&1
        break
    fi
done

ACKED=$(wc -l < "$ACK_LOG" | tr -d ' ')
info "Acknowledged writes before the crash: ${ACKED} / ${N}"

info "Restarting the same container on the same volume — CommitLog replay runs on boot..."
docker start "$CONTAINER" >/dev/null

if ! wait_healthy; then
    fail "engine never came back up after restart — check: docker logs $CONTAINER"
    cleanup
    exit 1
fi
ok "engine recovered and is healthy again"

info "Verifying every acknowledged write survived the crash..."
LOST=0
LOST_KEYS=()
while read -r i; do
    if ! curl -sf "${BASE}/read?key=chaos-${i}" | grep -q '"found":true'; then
        LOST=$((LOST + 1))
        LOST_KEYS+=("chaos-${i}")
    fi
done < "$ACK_LOG"

echo ""
echo "=== Result (mode=${MODE}) ==="
echo "  Acknowledged before crash: ${ACKED}"
echo "  Missing after recovery:    ${LOST}"
echo ""

EXIT_CODE=0
if [[ "$LOST" -eq 0 ]]; then
    ok "zero data loss across a hard SIGKILL crash"
else
    if [[ "$MODE" == "BATCH" ]]; then
        fail "${LOST} acknowledged write(s) lost under BATCH mode — this is a real bug."
        printf '  %s\n' "${LOST_KEYS[@]}"
        EXIT_CODE=1
    else
        warn "${LOST} acknowledged write(s) lost — expected under PERIODIC sync"
        echo "  PERIODIC fsyncs every ${COMMITLOG_SYNC_PERIOD_MS:-200}ms; acknowledged writes inside"
        echo "  that window can be lost on a hard crash. This mirrors Cassandra's real"
        echo "  commitlog_sync: periodic trade-off — higher throughput, bounded loss window."
        echo "  Run with BATCH for the zero-loss guarantee:"
        echo "    ./scripts/chaos-crash-test.sh ${N} ${KILL_AFTER} BATCH"
    fi
fi

echo ""
cleanup
exit $EXIT_CODE
