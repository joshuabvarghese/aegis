#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage — Demo Runner
#  Cassandra LSM-Tree Storage Engine, built from scratch in Java 21
# ═══════════════════════════════════════════════════════════════════════════════
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR="$PROJECT_DIR/target/aegis-storage-1.0.0.jar"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'
YELLOW='\033[1;33m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[aegis]${RESET} $*"; }
success() { echo -e "${GREEN}[✓]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[!]${RESET} $*"; }
die()     { echo -e "${RED}[✗]${RESET} $*"; exit 1; }

# ─── Prerequisite Check ───────────────────────────────────────────────────────
check_java() {
    java --version &>/dev/null || die "Java 21+ not found. Install from https://adoptium.net"
    local ver
    ver=$(java --version 2>&1 | head -1 | awk '{print $2}' | cut -d. -f1 | tr -d '"')
    [[ "$ver" -lt 21 ]] && die "Java 21+ required. Found: $ver"
}

check_maven() {
    mvn --version &>/dev/null || die "Maven not found. Run: brew install maven"
}

# ─── Build ────────────────────────────────────────────────────────────────────
build() {
    info "Building Aegis-Storage..."
    check_java
    check_maven
    cd "$PROJECT_DIR"
    mvn -q package -DskipTests 2>/dev/null || mvn package -DskipTests
    success "Built: $JAR"
}

# ─── MinIO (Cold Tier) ────────────────────────────────────────────────────────
minio_start() {
    info "Starting MinIO (bare-metal Homebrew binary, no Docker)..."
    command -v minio &>/dev/null || {
        warn "MinIO not found. Installing via Homebrew..."
        brew install minio/stable/minio
        brew install minio/stable/mc
    }

    if curl -sf http://127.0.0.1:9000/minio/health/live &>/dev/null; then
        success "MinIO already running on :9000"; return
    fi

    MINIO_ROOT_USER=aegisadmin MINIO_ROOT_PASSWORD=aegisadmin \
        minio server ~/.aegis-storage/minio-data \
            --address ":9000" --console-address ":9001" \
            > /tmp/aegis-minio.log 2>&1 &
    echo $! > /tmp/aegis-minio.pid

    for i in {1..20}; do
        curl -sf http://127.0.0.1:9000/minio/health/live &>/dev/null && break
        sleep 0.5
    done

    mc alias set aegislocal http://127.0.0.1:9000 aegisadmin aegisadmin &>/dev/null || true
    mc mb aegislocal/aegis-storage-cold --ignore-existing &>/dev/null || true
    success "MinIO running (PID=$(cat /tmp/aegis-minio.pid))"
}

minio_stop() {
    if [[ -f /tmp/aegis-minio.pid ]]; then
        kill "$(cat /tmp/aegis-minio.pid)" 2>/dev/null && success "MinIO stopped"
        rm -f /tmp/aegis-minio.pid
    else
        warn "MinIO PID file not found"
    fi
}

# ─── Run Modes ────────────────────────────────────────────────────────────────
run_shell() {
    info "Launching interactive CLI shell..."
    java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -Djava.util.logging.SimpleFormatter.format="[%4\$s] %5\$s%n" \
         -jar "$JAR"
}

run_shell_cold() {
    info "Launching with cold tier enabled (MinIO must be running)..."
    minio_start
    java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -Djava.util.logging.SimpleFormatter.format="[%4\$s] %5\$s%n" \
         -jar "$JAR" --cold-tier
}

run_demo() {
    info "Running automated demo sequence..."
    echo ""
    echo -e "${BOLD}The demo shows all 6 acts:${RESET}"
    echo "  1. Write path  — CommitLog + MemTable"
    echo "  2. Flush       — MemTable → SSTable"
    echo "  3. Bloom filter — zero I/O for missing keys"
    echo "  4. Tombstones  — LSM delete semantics"
    echo "  5. Compaction  — STCS merge + tombstone purge"
    echo "  6. Stats       — full engine metrics"
    echo ""
    # Pipe the 'demo' command then drop into interactive mode
    { echo "demo"; cat; } | run_shell
}

run_tests() {
    info "Running JUnit 5 test suite..."
    check_maven
    cd "$PROJECT_DIR"
    mvn -q test --enable-preview 2>/dev/null || mvn test
    success "All tests passed"
}

run_bench() {
    info "Running JMH benchmarks (Apple Silicon M1)..."
    java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -jar "$JAR" \
         -cp target/aegis-storage-1.0.0.jar \
         org.openjdk.jmh.Main \
         "com.aegis.benchmark.StorageBenchmark" \
         -f 1 -wi 3 -i 5 -tu us \
         -rff aegis-benchmark-results.json 2>/dev/null || {
        # Fallback: run via Maven exec
        cd "$PROJECT_DIR"
        mvn -q exec:java \
            -Dexec.mainClass="com.aegis.benchmark.StorageBenchmark" \
            -Dexec.args="" \
            --enable-preview
    }
    success "Benchmark results: aegis-benchmark-results.json"
}

clean() {
    info "Cleaning build artifacts and data..."
    rm -rf ~/.aegis-storage
    cd "$PROJECT_DIR" && mvn -q clean
    success "Clean complete"
}

# ─── Dispatch ─────────────────────────────────────────────────────────────────
case "${1:-help}" in
    build)        build ;;
    shell)        build && run_shell ;;
    shell-cold)   build && run_shell_cold ;;
    demo)         build && run_demo ;;
    minio)        minio_start ;;
    minio-stop)   minio_stop ;;
    test)         run_tests ;;
    bench)        build && run_bench ;;
    clean)        clean ;;
    help|*)
        echo ""
        echo -e "  ${BOLD}Aegis-Storage — LSM-Tree Engine Demo Runner${RESET}"
        echo ""
        echo "  Usage: ./scripts/run.sh <command>"
        echo ""
        echo "  Commands:"
        echo "    build        Build the fat JAR (mvn package)"
        echo "    shell        Interactive CLI shell  ← start here"
        echo "    shell-cold   CLI with cold tier (starts MinIO automatically)"
        echo "    demo         Automated 6-act demo sequence"
        echo "    minio        Start MinIO (Homebrew, no Docker)"
        echo "    minio-stop   Stop MinIO"
        echo "    test         Run JUnit 5 test suite"
        echo "    bench        Run JMH benchmarks"
        echo "    clean        Remove build artifacts and data"
        echo ""
        echo "  JVM flags applied: --enable-preview -Xmx256m -XX:+UseZGC"
        echo "  MinIO: bare-metal Homebrew binary, no Docker required"
        echo ""
        ;;
esac
