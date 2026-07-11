#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
#  Aegis-Storage — Demo Runner
#  Cassandra LSM-Tree Storage Engine, built from scratch in Java 21
# ═══════════════════════════════════════════════════════════════════════════════
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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

# ─── Run Modes ────────────────────────────────────────────────────────────────
run_shell() {
    info "Launching interactive CLI shell..."
    java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -Djava.util.logging.SimpleFormatter.format="[%4\$s] %5\$s%n" \
         -jar "$JAR"
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
    info "Running JMH benchmarks..."
    java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -cp "$JAR" \
         org.openjdk.jmh.Main \
         "com.aegis.benchmark.StorageBenchmark" \
         -f 1 -wi 3 -i 5 -tu us \
         -rf json \
         -rff aegis-benchmark-results.json
    success "Benchmark results: aegis-benchmark-results.json"
}

run_serve() {
    info "Starting HTTP server on :${PORT:-8080}..."
    PORT="${PORT:-8080}" java --enable-preview \
         -Xmx256m \
         -XX:+UseZGC \
         -cp "$JAR" \
         com.aegis.http.AegisHttpServer
}

clean() {
    info "Cleaning build artifacts and data..."
    rm -rf ~/.aegis-storage
    cd "$PROJECT_DIR" && mvn -q clean
    success "Clean complete"
}

# ─── Dispatch ─────────────────────────────────────────────────────────────────
case "${1:-help}" in
    build)  build ;;
    shell)  build && run_shell ;;
    demo)   build && run_demo ;;
    test)   run_tests ;;
    bench)  build && run_bench ;;
    serve)  build && run_serve ;;
    clean)  clean ;;
    help|*)
        echo ""
        echo -e "  ${BOLD}Aegis-Storage — LSM-Tree Engine Demo Runner${RESET}"
        echo ""
        echo "  Usage: ./scripts/run.sh <command>"
        echo ""
        echo "  Commands:"
        echo "    build        Build the fat JAR (mvn package)"
        echo "    shell        Interactive CLI shell  ← start here"
        echo "    demo         Automated 6-act demo sequence"
        echo "    test         Run JUnit 5 test suite"
        echo "    bench        Run JMH benchmarks"
        echo "    serve        Run the HTTP wrapper on \$PORT (default 8080)"
        echo "    clean        Remove build artifacts and data"
        echo ""
        echo "  JVM flags applied: --enable-preview -Xmx256m -XX:+UseZGC"
        echo ""
        ;;
esac
