# ═══════════════════════════════════════════════════════════════════════════════
# Aegis-Storage — Multi-Stage Dockerfile
#
# Stage 1 (builder): Maven + JDK 21 on Eclipse Temurin — compiles the fat JAR
# Stage 2 (runtime): Lean JRE 21 image — runs the engine
#
# Final image is ~320MB (JRE only, no Maven, no source).
# ═══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# Copy POM first — Docker layer cache means dependencies are only re-downloaded
# when pom.xml changes, not every time source code changes.
COPY pom.xml .

# Download all dependencies into the layer cache
RUN mvn dependency:go-offline -B --no-transfer-progress 2>/dev/null || \
    (apk add --no-cache maven && mvn dependency:go-offline -B --no-transfer-progress)

# Copy source
COPY src ./src

# Build fat JAR, skip tests (tests run in a separate docker-compose service).
#
# IMPORTANT: --enable-preview is a javac/JVM flag — NOT a Maven CLI flag.
# Maven will reject it with "Unrecognized option" if passed on the command line.
# It is already configured in pom.xml:
#   maven-compiler-plugin  → <compilerArgs><arg>--enable-preview</arg></compilerArgs>
#   maven-surefire-plugin  → <argLine>--enable-preview</argLine>
# So we only need plain `mvn package` here.
RUN mvn package -DskipTests -B --no-transfer-progress

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

LABEL org.opencontainers.image.title="Aegis-Storage"
LABEL org.opencontainers.image.description="LSM-tree storage engine — Cassandra internals from scratch"
LABEL org.opencontainers.image.authors="aegis-storage"

# Create non-root user — production best practice
RUN addgroup -S aegis && adduser -S aegis -G aegis

WORKDIR /opt/aegis-storage

# Copy the fat JAR from builder
COPY --from=builder /build/target/aegis-storage-1.0.0.jar ./aegis-storage.jar

# Data directories — CommitLog, SSTables, cold-fetch temp files
RUN mkdir -p /data/commitlog /data/sstables /data/cold-fetch \
    && chown -R aegis:aegis /data /opt/aegis-storage

# Entrypoint script
COPY docker/entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

USER aegis

# --enable-preview goes here, on the JVM — this is correct.
# The $ signs are escaped so Docker doesn't try to expand them as shell variables.
ENV JAVA_OPTS="--enable-preview -Xmx256m -XX:+UseZGC -Djava.util.logging.SimpleFormatter.format=[%4\$s] %5\$s%n"
ENV AEGIS_DATA_DIR="/data"

EXPOSE 8888
EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
CMD ["shell"]
