#!/bin/sh
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage Container Entrypoint
#
#  Usage (via docker-compose):
#    CMD shell     → interactive CLI shell (default)
#    CMD demo      → run the automated 6-act demo then exit
#    CMD bench     → run JMH benchmarks then print results
#    CMD serve     → start the HTTP wrapper (used by Cloud Run)
# ═══════════════════════════════════════════════════════════════════════════════
set -e

JAR="/opt/aegis-storage/aegis-storage.jar"

# Use /data as home so StorageConfig paths resolve correctly
export HOME=/data

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

    serve)
        echo "[entrypoint] Starting HTTP server on port ${PORT:-8080}..."
        exec java $JAVA_OPTS \
            -Duser.home=/data \
            -cp "$JAR" \
            com.aegis.http.AegisHttpServer
        ;;

    *)
        echo "[entrypoint] Unknown mode: $MODE"
        echo "  Valid modes: shell, demo, bench, serve"
        exit 1
        ;;
esac
