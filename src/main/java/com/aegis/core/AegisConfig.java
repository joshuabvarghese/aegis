package com.aegis.core;

import java.nio.file.Path;

/**
 * Central configuration for Aegis-WAL.
 * No Spring @ConfigurationProperties — pure Java records.
 */
public final class AegisConfig {

    // --- Hot Storage ---
    public static final long SEGMENT_ROLL_BYTES = 10 * 1024 * 1024L; // 10 MB
    public static final int  FSYNC_INTERVAL_MS  = 200;
    public static final int  INDEX_INTERVAL_BYTES = 4096; // dense index every 4KB

    // --- Replication ---
    public static final int QUORUM_SIZE = 2; // floor(3/2)+1
    public static final int REPLICATION_TIMEOUT_MS = 500;

    // --- Cold Storage / MinIO ---
    public static final String MINIO_ENDPOINT   = "http://127.0.0.1:9000";
    public static final String MINIO_ACCESS_KEY = "aegisadmin";
    public static final String MINIO_SECRET_KEY = "aegisadmin";
    public static final String MINIO_BUCKET     = "aegis-wal-segments";

    // --- Node Ports ---
    public static final int[] NODE_PORTS = {9091, 9092, 9093};

    // --- JVM Constraints ---
    public static final long DIRECT_BUFFER_POOL_BYTES = 64 * 1024 * 1024L; // 64MB off-heap

    private AegisConfig() {}

    public static Path dataDir(int nodeId) {
        return Path.of(System.getProperty("user.home"), ".aegis-wal", "node-" + nodeId);
    }
}
