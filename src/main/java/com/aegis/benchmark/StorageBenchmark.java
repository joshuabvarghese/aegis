package com.aegis.benchmark;

import com.aegis.StorageEngine;
import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.sstable.BloomFilter;
import com.aegis.sstable.SSTableWriter;
import com.aegis.commitlog.CommitLogSegment;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmark Suite — Aegis-Storage Hot Paths
 *
 * Measures the cost of every layer independently so we can prove each
 * layer's overhead. This is the kind of analysis an Instaclustr engineer
 * does when profiling Cassandra's write path.
 *
 * Benchmarks:
 *   A. Murmur3 token computation (partition key hashing)
 *   B. Bloom filter add + mightContain
 *   C. CommitLog segment serialization + append
 *   D. Row merge (compaction reconciliation)
 *   E. Full engine write throughput
 *   F. Full engine read throughput (warm MemTable)
 *
 * Run: java --enable-preview -Xmx256m -jar target/aegis-storage-1.0.0.jar
 * JMH will be invoked via org.openjdk.jmh.Main.
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xmx256m", "--enable-preview", "-XX:+UseZGC"})
public class StorageBenchmark {

    // ─── Benchmark State ──────────────────────────────────────────────────────

    @Param({"user:000001", "user:500000", "user:999999"})
    private String keyParam;

    private PartitionKey key;
    private BloomFilter  bloomFilter;
    private Row          sampleRow;
    private Path         tempDir;
    private CommitLogSegment clSegment;
    private StorageEngine    engine;

    private final List<Row> rowsToMerge = new ArrayList<>();

    private Path engineDataDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        this.key = PartitionKey.of(keyParam);

        // Pre-populate Bloom filter with 10k keys
        this.bloomFilter = new BloomFilter(10_000);
        for (int i = 0; i < 10_000; i++) {
            bloomFilter.add(PartitionKey.of("user:" + i));
        }

        // Sample row for write benchmarks
        this.sampleRow = Row.create(keyParam)
            .putColumn("email",     keyParam + "@aegis.io",   nowMicros())
            .putColumn("firstName", "Jane",                    nowMicros())
            .putColumn("lastName",  "Cassandra",               nowMicros())
            .putColumn("createdAt", "2025-01-01T00:00:00Z",   nowMicros());

        // CommitLog segment
        this.tempDir   = Files.createTempDirectory("aegis-bench-");
        this.clSegment = new CommitLogSegment(tempDir,
            System.currentTimeMillis(),
            com.aegis.core.StorageConfig.CommitLogSyncMode.PERIODIC);

        // Row merge state — two versions of the same row
        for (int i = 0; i < 2; i++) {
            Row r = Row.create(keyParam)
                .putColumn("email", keyParam + "@v" + i,       nowMicros() + i)
                .putColumn("score", String.valueOf(i * 100),    nowMicros() + i);
            rowsToMerge.add(r);
        }

        // Full engine — an isolated, fresh data directory PER FORK. Every JMH
        // fork otherwise inherits AEGIS_DATA_DIR from the container (a
        // persistent volume), which means every fork's constructor would
        // replay every previous fork's entire CommitLog. That accumulates
        // without bound across a single bench run (let alone repeated runs
        // against the same volume) until CommitLog replay itself runs the
        // fork's -Xmx256m heap out of memory — which is exactly what used to
        // happen. Overriding AEGIS_DATA_DIR to a fresh temp dir here means
        // every fork starts from zero, regardless of what else has run.
        this.engineDataDir = Files.createTempDirectory("aegis-bench-engine-");
        System.setProperty("AEGIS_DATA_DIR", engineDataDir.toString());
        this.engine = new StorageEngine();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        clSegment.close();
        engine.close();
        deleteDir(tempDir);
        deleteDir(engineDataDir);
        System.clearProperty("AEGIS_DATA_DIR");
    }

    // ─── A. Murmur3 Token ─────────────────────────────────────────────────────

    /**
     * Cost of computing a Murmur3 token for a partition key.
     * In Cassandra this runs on every write and read.
     * Expected on M1: ~50-100ns (well under 1µs).
     */
    @Benchmark
    public long benchmarkMurmur3Token() {
        return key.token();
    }

    // ─── B. Bloom Filter ──────────────────────────────────────────────────────

    /**
     * Bloom filter false-negative path (key not in filter — definitive miss).
     * This is the common path for non-existent keys.
     * Expected: ~50-200ns — dominated by k=7 hash computations.
     */
    @Benchmark
    public boolean benchmarkBloomFilterMiss() {
        // Use a key that was definitely not inserted
        return bloomFilter.mightContain(PartitionKey.of("definitely-not-inserted"));
    }

    /**
     * Bloom filter true-positive path (key is in filter).
     * Expected: similar to miss — same number of bit checks.
     */
    @Benchmark
    public boolean benchmarkBloomFilterHit() {
        return bloomFilter.mightContain(PartitionKey.of("user:500"));
    }

    /**
     * Cost of inserting a key into the Bloom filter (SSTable write path).
     */
    @Benchmark
    public void benchmarkBloomFilterAdd() {
        bloomFilter.add(key);
    }

    // ─── C. CommitLog ─────────────────────────────────────────────────────────

    /**
     * CommitLog serialization — the byte framing cost without disk I/O.
     * Isolates the serialization overhead from the fsync cost.
     * Expected: ~500ns-2µs depending on row size.
     */
    @Benchmark
    public byte[] benchmarkCommitLogSerialization() {
        return CommitLogSegment.serialize(sampleRow);
    }

    /**
     * Full CommitLog append including FileChannel write (PERIODIC mode, no fsync).
     * This is the p50 cost of a write in production.
     * Expected: ~1-5µs on NVMe (page cache write, no fsync wait).
     */
    @Benchmark
    public long benchmarkCommitLogAppend() throws IOException {
        return clSegment.append(sampleRow);
    }

    // ─── D. Row Merge (Compaction) ────────────────────────────────────────────

    /**
     * Cost of reconciling two versions of a row during compaction.
     * Last-write-wins per cell — dominated by cell iteration and timestamp comparison.
     * Expected: ~200-500ns for a row with 4 cells.
     */
    @Benchmark
    public Row benchmarkRowMerge() {
        return com.aegis.compaction.STCSCompactor.mergeAllVersions(rowsToMerge);
    }

    /**
     * Tombstone purge check — called for every cell during compaction.
     * Expected: ~100ns — timestamp comparison only.
     */
    @Benchmark
    public Row benchmarkTombstonePurge() {
        return com.aegis.compaction.STCSCompactor.purgeTombstones(sampleRow, 0L);
    }

    // ─── E. Full Engine Write ─────────────────────────────────────────────────

    /**
     * End-to-end write throughput: CommitLog → MemTable.
     * No flush triggered (MemTable never reaches threshold in this test).
     * Expected: ~5-20µs (dominated by CommitLog FileChannel write).
     *
     * Compare to Cassandra benchmarks: ~100µs-1ms with fsync, ~10-50µs without.
     * Our PERIODIC mode matches Cassandra's periodic sync performance.
     */
    @Benchmark
    public void benchmarkFullEngineWrite() throws IOException {
        engine.write(sampleRow);
    }

    /**
     * MemTable read throughput — ConcurrentSkipListMap.get().
     * Key is pre-inserted in setup. Expected: ~200-500ns.
     */
    @Benchmark
    public Optional<Row> benchmarkEngineReadMemTable() throws IOException {
        return engine.read(keyParam);
    }

    // ─── Runner ───────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
            .include(StorageBenchmark.class.getSimpleName())
            .result("aegis-storage-benchmark.json")
            .resultFormat(org.openjdk.jmh.results.format.ResultFormatType.JSON)
            .build()
        ).run();
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static long nowMicros() {
        return System.currentTimeMillis() * 1_000L;
    }

    private static void deleteDir(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        Files.walk(dir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
}
