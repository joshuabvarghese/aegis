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
     * sync_period of data loss on a crash — higher throughput) and BATCH (fsync
     * before ack, zero data loss, lower throughput). Both are implemented.
     * Overridable via the COMMITLOG_SYNC_MODE environment variable, so the same
     * JAR can demonstrate either trade-off without a rebuild.
     */
    public static CommitLogSyncMode commitLogSyncMode() {
        String env = System.getenv("COMMITLOG_SYNC_MODE");
        if (env != null && env.equalsIgnoreCase("BATCH")) return CommitLogSyncMode.BATCH;
        return CommitLogSyncMode.PERIODIC;
    }

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

    /**
     * Which compaction strategy the engine runs: STCS (default, Cassandra's
     * default) or LCS (Leveled — RocksDB/LevelDB's default, also selectable in
     * real Cassandra). Overridable via the COMPACTION_STRATEGY environment
     * variable so the same JAR can demonstrate either without a rebuild.
     */
    public static CompactionStrategyKind compactionStrategy() {
        // Property checked first so tests can override it in-process (same
        // pattern as user.home for data paths) without touching env vars,
        // which the JVM can't reliably mutate at runtime.
        String prop = System.getProperty("COMPACTION_STRATEGY");
        String value = (prop != null && !prop.isBlank()) ? prop : System.getenv("COMPACTION_STRATEGY");
        if (value != null && value.equalsIgnoreCase("LCS")) return CompactionStrategyKind.LCS;
        return CompactionStrategyKind.STCS;
    }

    public enum CompactionStrategyKind { STCS, LCS }

    // ─── Leveled Compaction Strategy (LCS) ────────────────────────────────────

    /**
     * L0 is compacted into L1 once this many L0 SSTables have accumulated.
     * L0 files can overlap in key range (they're raw flush output), so this
     * mirrors STCS_MIN_THRESHOLD to keep the two strategies comparable at the
     * same data volume.
     */
    public static final int LCS_L0_COMPACTION_TRIGGER = 4;

    /**
     * Target total size of L1. Each level above L1 targets
     * LCS_LEVEL_SIZE_MULTIPLIER times the level below it — the same
     * exponential level-size growth RocksDB and LevelDB use.
     *
     * Set small (128KB) relative to Cassandra's real defaults (~256MB) so a
     * demo-scale dataset actually produces a multi-level tree instead of
     * everything sitting in L1.
     */
    public static final long LCS_L1_MAX_BYTES = 128 * 1024L;

    public static final int LCS_LEVEL_SIZE_MULTIPLIER = 10;

    /** Target byte size for level N (N >= 1). Level 0 has no size target — it's bounded by file count instead. */
    public static long lcsLevelTargetBytes(int level) {
        if (level <= 0) return Long.MAX_VALUE;
        long target = LCS_L1_MAX_BYTES;
        for (int i = 1; i < level; i++) target *= LCS_LEVEL_SIZE_MULTIPLIER;
        return target;
    }

    // ─── Paths ────────────────────────────────────────────────────────────────

    /**
     * Data root — overridable via AEGIS_DATA_DIR environment variable.
     *
     * Local:  ~/.aegis-storage/
     * Docker: /data/  (set by entrypoint via -Duser.home=/data)
     */
    private static String dataRoot() {
        String prop = System.getProperty("AEGIS_DATA_DIR");
        if (prop != null && !prop.isBlank()) return prop;
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
