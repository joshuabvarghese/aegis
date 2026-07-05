#!/bin/sh
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage Container Entrypoint
#
#  Usage (via docker-compose):
#    CMD shell     → interactive CLI shell (default)
#    CMD demo      → run the automated 6-act demo then exit
#    CMD bench     → run JMH benchmarks then print results
# ═══════════════════════════════════════════════════════════════════════════════
set -e

JAR="/opt/aegis-storage/aegis-storage.jar"

# Use /data as home so StorageConfig paths resolve correctly
export HOME=/data

# Wait for MinIO to be reachable (only if cold tier is requested)
wait_for_minio() {
    echo "[entrypoint] Waiting for MinIO at ${MINIO_ENDPOINT:-http://minio:9000}..."
    for i in $(seq 1 30); do
        if wget -q --spider "${MINIO_ENDPOINT:-http://minio:9000}/minio/health/live" 2>/dev/null; then
            echo "[entrypoint] MinIO ready."
            return 0
        fi
        sleep 1
    done
    echo "[entrypoint] WARNING: MinIO not reachable — cold tier will be disabled."
    return 0
}

MODE="${1:-shell}"

case "$MODE" in
    shell)
        echo "[entrypoint] Starting interactive shell..."
        exec java $JAVA_OPTS \
            -Duser.home=/data \
            -jar "$JAR"
        ;;

    demo)
        echo "[entrypoint] Running automated demo..."
        # Pipe 'demo' command, then 'quit', into the shell
        printf 'demo\nquit\n' | java $JAVA_OPTS \
            -Duser.home=/data \
            -jar "$JAR"
        ;;

    demo-cold)
        wait_for_minio
        echo "[entrypoint] Running demo with cold tier enabled..."
        printf 'demo\nquit\n' | java $JAVA_OPTS \
            -Duser.home=/data \
            -jar "$JAR" --cold-tier
        ;;

    bench)
        echo "[entrypoint] Running JMH benchmarks..."
        java $JAVA_OPTS \
            -Duser.home=/data \
            -cp "$JAR" \
            org.openjdk.jmh.Main \
            "com.aegis.benchmark.StorageBenchmark" \
            -f 1 -wi 3 -i 5 -tu us \
            -rff /data/benchmark-results.json
        echo "[entrypoint] Benchmark results written to /data/benchmark-results.json"
        cat /data/benchmark-results.json 2>/dev/null || true
        ;;

    *)
        echo "[entrypoint] Unknown mode: $MODE"
        echo "  Valid modes: shell, demo, demo-cold, bench"
        exit 1
        ;;
esac
