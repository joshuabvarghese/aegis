package com.aegis.core;

import java.nio.file.Path;

/**
 * Central configuration for Aegis-Storage.
 *
 * Every constant here has a direct parallel in cassandra.yaml.
 * The comments show the equivalent Cassandra configuration key so any
 * Instaclustr engineer reading this code immediately recognises the context.
 *
 * All sizes in bytes unless stated otherwise.
 */
public final class StorageConfig {

    // ─── CommitLog (cassandra.yaml: commitlog_*) ──────────────────────────────

    /**
     * Maximum size of a single CommitLog segment before a new one is created.
     * Cassandra default: commitlog_segment_size_in_mb = 32
     */
    public static final long COMMITLOG_SEGMENT_SIZE_BYTES = 32 * 1024 * 1024L;

    /**
     * How often to fsync the active CommitLog segment.
     * Cassandra equivalent: commitlog_sync = periodic
     *                        commitlog_sync_period_in_ms = 10000
     *
     * We use 200ms for demo visibility; set to 10_000 for Cassandra parity.
     */
    public static final int COMMITLOG_SYNC_PERIOD_MS = 200;

    /**
     * Cassandra supports two sync modes: PERIODIC (ack before fsync, risk up to
     * sync_period of data loss) and BATCH (fsync before ack, zero data loss, lower
     * throughput). We implement PERIODIC — the production default.
     */
    public static final CommitLogSyncMode COMMITLOG_SYNC_MODE = CommitLogSyncMode.PERIODIC;

    public enum CommitLogSyncMode { PERIODIC, BATCH }

    // ─── MemTable (cassandra.yaml: memtable_*) ────────────────────────────────

    /**
     * Heap size threshold at which the MemTable is flushed to an SSTable.
     * Cassandra default: memtable_heap_space_in_mb = 2048
     *
     * We use 8MB for demo so flushes occur quickly and are visible.
     */
    public static final long MEMTABLE_FLUSH_THRESHOLD_BYTES = 8 * 1024 * 1024L;

    /**
     * Cassandra supports on-heap and off-heap MemTable allocation.
     * We implement off-heap via ByteBuffer.allocateDirect() for row data,
     * keeping only the skip-list index on-heap — same as Cassandra's
     * memtable_allocation_type: offheap_objects mode.
     */
    public static final long MEMTABLE_OFFHEAP_POOL_BYTES = 64 * 1024 * 1024L;

    // ─── SSTable (cassandra.yaml: sstable_*) ──────────────────────────────────

    /**
     * Block size for SSTable data compression.
     * Cassandra default: chunk_length_in_kb = 16 (LZ4 compressor)
     *
     * We store uncompressed for clarity; compression is noted as an extension point.
     */
    public static final int SSTABLE_DATA_BLOCK_SIZE = 16 * 1024;

    /**
     * Sparse partition index sampling interval.
     * Cassandra default: column_index_size_in_kb = 64
     *
     * One index entry is written every INDEX_SAMPLE_BYTES of data, giving
     * O(data/sample) index size and O(sample) binary search resolution.
     */
    public static final int SSTABLE_INDEX_SAMPLE_BYTES = 4 * 1024;

    /**
     * Bloom filter false positive probability.
     * Cassandra default: bloom_filter_fp_chance = 0.01 (1%)
     *
     * At 1% FPP a Bloom filter for 1M keys needs ~9.6 bits/key = ~1.2MB.
     * We use 0.01 to match Cassandra's default.
     */
    public static final double BLOOM_FILTER_FP_PROBABILITY = 0.01;

    // ─── Compaction (cassandra.yaml: compaction = STCS) ───────────────────────

    /**
     * Size-Tiered Compaction Strategy (STCS) — Cassandra's default.
     *
     * Compaction is triggered when there are MIN_THRESHOLD SSTables of similar size.
     * Cassandra defaults: min_threshold = 4, max_threshold = 32
     */
    public static final int STCS_MIN_THRESHOLD = 4;
    public static final int STCS_MAX_THRESHOLD = 32;

    /**
     * Two SSTables are considered "similar size" if the smaller is at least
     * this fraction of the larger. Cassandra default: bucket_low = 0.5, bucket_high = 1.5
     */
    public static final double STCS_BUCKET_LOW  = 0.5;
    public static final double STCS_BUCKET_HIGH = 1.5;

    /**
     * GC grace seconds: tombstones are eligible for deletion after this period.
     * Cassandra default: gc_grace_seconds = 864000 (10 days)
     *
     * We use 0 for demo (immediate tombstone collection).
     */
    public static final long GC_GRACE_SECONDS = 0L;

    /**
     * How often the compaction daemon checks for eligible SSTable buckets.
     */
    public static final int COMPACTION_CHECK_INTERVAL_MS = 2_000;

    // ─── Cold Tier / Tiered Storage ───────────────────────────────────────────

    /**
     * SSTables older than this threshold in seconds are eligible for cold-tier offload.
     * Mirrors Kafka KIP-405 remote.log.segment.bytes semantics applied to SSTables.
     *
     * Cassandra equivalent: not native in open-source Cassandra; this mirrors what
     * Instaclustr implements in their managed platform's ClickHouse/OpenSearch tiering.
     */
    public static final long COLD_TIER_AGE_THRESHOLD_SECONDS = 30L;

    /**
     * MinIO / S3 connection settings.
     *
     * All four values are read from environment variables first, so the same
     * JAR works both locally (defaults below) and inside Docker Compose
     * (where MINIO_ENDPOINT=http://minio:9000 is injected by the service).
     *
     * Local default:  http://127.0.0.1:9000  (Homebrew minio server)
     * Docker default: http://minio:9000       (docker-compose service name)
     */
    public static String minioEndpoint() {
        String env = System.getenv("MINIO_ENDPOINT");
        return (env != null && !env.isBlank()) ? env : "http://127.0.0.1:9000";
    }

    public static String minioAccessKey() {
        String env = System.getenv("MINIO_ACCESS_KEY");
        return (env != null && !env.isBlank()) ? env : "aegisadmin";
    }

    public static String minioSecretKey() {
        String env = System.getenv("MINIO_SECRET_KEY");
        return (env != null && !env.isBlank()) ? env : "aegisadmin";
    }

    public static final String MINIO_BUCKET = "aegis-storage-cold";

    // ─── Paths ────────────────────────────────────────────────────────────────

    /**
     * Data root — overridable via AEGIS_DATA_DIR environment variable.
     *
     * Local:  ~/.aegis-storage/
     * Docker: /data/  (set by entrypoint via -Duser.home=/data)
     */
    private static String dataRoot() {
        String env = System.getenv("AEGIS_DATA_DIR");
        return (env != null && !env.isBlank())
            ? env
            : System.getProperty("user.home") + "/.aegis-storage";
    }

    public static Path dataDir() {
        return Path.of(dataRoot(), "data");
    }

    public static Path commitLogDir() {
        return Path.of(dataRoot(), "commitlog");
    }

    public static Path ssTableDir() {
        return Path.of(dataRoot(), "sstables");
    }

    private StorageConfig() {}
}
