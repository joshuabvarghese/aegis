package com.aegis.compaction;

import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.core.StorageException.CompactionException;
import com.aegis.sstable.SSTableReader;
import com.aegis.sstable.SSTableWriter;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Size-Tiered Compaction Strategy (STCS) — Cassandra's default compaction algorithm.
 *
 * ─── Why Compaction is Necessary ─────────────────────────────────────────────
 *
 * Each MemTable flush produces one new SSTable. Without compaction, the number
 * of SSTables grows unboundedly. This causes:
 *   1. Read amplification: a lookup must check every SSTable's Bloom filter and
 *      potentially read from multiple files
 *   2. Space amplification: deleted data (tombstones) and overwritten values
 *      accumulate across multiple SSTables without being reclaimed
 *   3. Bloom filter memory growth: each SSTable requires its own Bloom filter
 *
 * Compaction merges multiple SSTables into one, eliminating duplicates and
 * tombstones past GC grace period. The output is one larger SSTable.
 *
 * ─── STCS Algorithm ──────────────────────────────────────────────────────────
 *
 * 1. Group SSTables into "buckets" by similar size:
 *    Two SSTables are similar if their sizes are within [BUCKET_LOW, BUCKET_HIGH]
 *    of each other. Cassandra defaults: bucket_low=0.5, bucket_high=1.5
 *
 * 2. A bucket is compaction-eligible if it has >= MIN_THRESHOLD SSTables (default: 4)
 *
 * 3. Select the hottest bucket (highest average read rate, or largest if tied)
 *    and merge up to MAX_THRESHOLD SSTables from it (default: 32)
 *
 * 4. The merge is a k-way merge sort over the sorted SSTable iterators.
 *    For duplicate partition keys, the cell with the highest timestamp wins
 *    (last-write-wins — same as Cassandra's reconciliation semantics).
 *
 * 5. Tombstones past GC grace period are dropped during the merge.
 *
 * ─── Compaction Output ────────────────────────────────────────────────────────
 *
 * The output is a new SSTable with a higher generation number. The input
 * SSTables are deleted after the output is fully written and synced.
 * This two-phase approach (write-then-delete) prevents data loss if the process
 * crashes mid-compaction — exactly how Cassandra handles it.
 */
public final class STCSCompactor implements CompactionStrategy {

    private static final Logger log = Logger.getLogger(STCSCompactor.class.getName());

    private final Path        sstableDir;
    private final AtomicLong  generationGen;

    // Compaction statistics
    private final AtomicLong compactionsRun      = new AtomicLong(0);
    private final AtomicLong sstablesCompacted   = new AtomicLong(0);
    private final AtomicLong partitionsMerged    = new AtomicLong(0);
    private final AtomicLong tombstonesPurged    = new AtomicLong(0);
    private final AtomicLong bytesReclaimed      = new AtomicLong(0);
    private final AtomicLong bytesWritten        = new AtomicLong(0);

    public STCSCompactor(Path sstableDir, AtomicLong generationGen) {
        this.sstableDir    = sstableDir;
        this.generationGen = generationGen;
    }

    // ─── Bucket Formation ─────────────────────────────────────────────────────

    /**
     * Group the given SSTables into size-tiered buckets.
     *
     * Two SSTables belong to the same bucket if:
     *   smaller.dataSizeBytes / larger.dataSizeBytes >= BUCKET_LOW (0.5)
     *
     * This is identical to Cassandra's SizeTieredCompactionStrategy.getBuckets().
     */
    public List<List<SSTableMetadata>> formBuckets(List<SSTableMetadata> sstables) {
        if (sstables.isEmpty()) return List.of();

        // Sort by data size ascending — makes bucket assignment O(n log n)
        List<SSTableMetadata> sorted = sstables.stream()
            .sorted(Comparator.comparingLong(SSTableMetadata::dataSizeBytes))
            .collect(Collectors.toCollection(ArrayList::new));

        List<List<SSTableMetadata>> buckets = new ArrayList<>();
        List<SSTableMetadata>       current = new ArrayList<>();
        current.add(sorted.get(0));

        for (int i = 1; i < sorted.size(); i++) {
            SSTableMetadata candidate = sorted.get(i);
            SSTableMetadata reference = current.get(0);

            double ratio = (double) reference.dataSizeBytes() / candidate.dataSizeBytes();

            if (ratio >= StorageConfig.STCS_BUCKET_LOW
                    && ratio <= StorageConfig.STCS_BUCKET_HIGH) {
                current.add(candidate);
            } else {
                buckets.add(current);
                current = new ArrayList<>();
                current.add(candidate);
            }
        }
        buckets.add(current);

        return buckets;
    }

    /**
     * Find the best bucket to compact — the one with the most candidates
     * above MIN_THRESHOLD. Among ties, prefer the bucket with largest average size
     * (higher priority in Cassandra's hotness ranking).
     *
     * Returns Optional.empty() if no bucket is eligible.
     */
    public Optional<List<SSTableMetadata>> selectCompactionBucket(
            List<List<SSTableMetadata>> buckets) {

        return buckets.stream()
            .filter(b -> b.size() >= StorageConfig.STCS_MIN_THRESHOLD)
            .max(Comparator
                .<List<SSTableMetadata>>comparingInt(b -> b.size())
                .thenComparingDouble(b -> b.stream()
                    .mapToLong(SSTableMetadata::dataSizeBytes)
                    .average()
                    .orElse(0)));
    }

    // ─── Compaction Execution ─────────────────────────────────────────────────

    /**
     * Run a compaction on the selected bucket.
     *
     * Performs a k-way merge sort over the SSTable iterators.
     * Tombstones past GC grace period are dropped.
     * The output SSTable is written before any inputs are deleted.
     *
     * @param bucket  the SSTables to merge (from selectCompactionBucket)
     * @return        metadata of the new output SSTable
     */
    public SSTableMetadata compact(List<SSTableMetadata> bucket) throws IOException {
        // Cap at MAX_THRESHOLD
        List<SSTableMetadata> toCompact = bucket.subList(
            0, Math.min(bucket.size(), StorageConfig.STCS_MAX_THRESHOLD));

        log.info("[COMPACTION] Starting STCS compaction of %d SSTables".formatted(toCompact.size()));

        // Phase 1: Open all input readers
        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableMetadata meta : toCompact) {
            readers.add(new SSTableReader(meta));
        }

        // Phase 2: Load all rows from all SSTables into a merge structure
        //   In production Cassandra this is a lazy k-way merge iterator to avoid
        //   loading everything into memory. For clarity we load all rows here,
        //   noting the production approach as an extension point.
        Map<PartitionKey, List<Row>> mergeMap = new TreeMap<>();
        long inputBytes = 0;

        for (SSTableReader reader : readers) {
            List<Row> rows = reader.scanAll();
            for (Row row : rows) {
                mergeMap.computeIfAbsent(row.key(), k -> new ArrayList<>()).add(row);
            }
            inputBytes += reader.metadata().dataSizeBytes();
            reader.close();
        }

        // Phase 3: Merge rows — for each partition key, reconcile all versions
        long expectedOutput = mergeMap.size();
        SSTableWriter writer = new SSTableWriter(sstableDir, generationGen.getAndIncrement());

        // Build a sorted NavigableMap for the writer
        TreeMap<PartitionKey, Row> outputMap = new TreeMap<>();
        long tombstonesPurgedLocal = 0;

        for (var entry : mergeMap.entrySet()) {
            PartitionKey key    = entry.getKey();
            List<Row>    versions = entry.getValue();

            Row merged = mergeAllVersions(versions);
            Row purged = purgeTombstones(merged, StorageConfig.GC_GRACE_SECONDS);

            // Skip entirely empty rows (all cells were tombstoned and purged)
            if (!purged.isEmpty()) {
                outputMap.put(key, purged);
            } else {
                tombstonesPurgedLocal++;
            }
        }

        // Track the oldest CommitLog position across all input SSTables —
        // the output SSTable inherits the latest position (newest write)
        var newestClPosition = toCompact.stream()
            .map(SSTableMetadata::commitLogPosition)
            .max(Comparator.naturalOrder())
            .orElse(com.aegis.commitlog.CommitLog.CommitLogPosition.NONE);

        writer.writeAll(outputMap, outputMap.size(), newestClPosition);
        SSTableMetadata outputMeta = writer.finish();
        writer.close();

        // Phase 4: Delete input SSTables (only after output is fully written + synced)
        for (SSTableMetadata meta : toCompact) {
            deleteSSTableFiles(meta);
        }

        // Update statistics
        compactionsRun.incrementAndGet();
        sstablesCompacted.addAndGet(toCompact.size());
        partitionsMerged.addAndGet(mergeMap.size());
        tombstonesPurged.addAndGet(tombstonesPurgedLocal);
        bytesReclaimed.addAndGet(inputBytes - outputMeta.dataSizeBytes());
        bytesWritten.addAndGet(outputMeta.dataSizeBytes());

        log.info(("[COMPACTION] Complete. input=%d SSTables, output=gen%d, " +
            "partitions=%d, tombstonesPurged=%d, bytesReclaimed=%d")
            .formatted(toCompact.size(), outputMeta.generation(),
                outputMap.size(), tombstonesPurgedLocal,
                inputBytes - outputMeta.dataSizeBytes()));

        return outputMeta;
    }

    // ─── Row Merge Logic ──────────────────────────────────────────────────────

    /**
     * Merge multiple versions of a row from different SSTables.
     *
     * For each column, the cell with the highest timestamp wins.
     * This is Cassandra's reconciliation rule: last-write-wins per cell.
     *
     * Tombstones are kept here — they are purged separately by purgeTombstones()
     * which only removes tombstones past GC grace period.
     */
    public static Row mergeAllVersions(List<Row> versions) {
        if (versions.size() == 1) return versions.get(0);

        // Start with the first version and merge all others into it
        Row base = versions.get(0);
        for (int i = 1; i < versions.size(); i++) {
            Row other = versions.get(i);
            for (var cellEntry : other.cells().entrySet()) {
                String colName     = cellEntry.getKey();
                Row.Cell otherCell = cellEntry.getValue();
                Row.Cell baseCell  = base.getCell(colName);

                if (baseCell == null || otherCell.timestamp() > baseCell.timestamp()) {
                    if (otherCell.isTombstone()) {
                        base.deleteColumn(colName, otherCell.timestamp());
                    } else {
                        byte[] val = otherCell.valueBytes();
                        if (val != null) base.putColumn(colName, val, otherCell.timestamp());
                    }
                }
            }
        }
        return base;
    }

    /**
     * Remove tombstones that are past the GC grace period.
     *
     * A tombstone can only be safely deleted during compaction if we are certain
     * that all replicas have seen the deletion. GC grace period gives enough time
     * for hints to be delivered and anti-entropy repair to propagate the tombstone.
     *
     * In our single-node implementation, GC_GRACE_SECONDS=0 means immediate purge.
     */
    public static Row purgeTombstones(Row row, long gcGraceSeconds) {
        Row purged = Row.create(row.key());
        for (var entry : row.cells().entrySet()) {
            Row.Cell cell = entry.getValue();
            if (!cell.isPurgeableTombstone(gcGraceSeconds)
                    && !cell.isExpired()) {
                // Keep this cell
                if (cell.isTombstone()) {
                    purged.deleteColumn(entry.getKey(), cell.timestamp());
                } else {
                    byte[] val = cell.valueBytes();
                    if (val != null) purged.putColumn(entry.getKey(), val, cell.timestamp());
                }
            }
            // else: purgeable tombstone or expired cell — drop it
        }
        return purged;
    }

    // ─── File Deletion ────────────────────────────────────────────────────────

    private void deleteSSTableFiles(SSTableMetadata meta) {
        Path[] components = {
            meta.dataPath(), meta.indexPath(), meta.filterPath(),
            meta.statsPath(), meta.summaryPath()
        };
        for (Path p : components) {
            try {
                java.nio.file.Files.deleteIfExists(p);
            } catch (IOException e) {
                log.warning("[COMPACTION] Failed to delete " + p + ": " + e.getMessage());
            }
        }
        log.info("[COMPACTION] Deleted input SSTable gen=%d".formatted(meta.generation()));
    }

    // ─── CompactionStrategy interface ─────────────────────────────────────────

    @Override
    public Optional<CompactionPlan> plan(List<SSTableMetadata> catalog) {
        if (catalog.size() < StorageConfig.STCS_MIN_THRESHOLD) return Optional.empty();

        List<List<SSTableMetadata>> buckets = formBuckets(catalog);
        return selectCompactionBucket(buckets)
            .map(bucket -> new CompactionPlan(bucket, 0,
                "size-tiered bucket, " + bucket.size() + " SSTables"));
    }

    @Override
    public SSTableMetadata execute(CompactionPlan plan) throws IOException {
        return compact(plan.inputs());
    }

    @Override
    public long bytesWritten() { return bytesWritten.get(); }

    @Override
    public String strategyName() { return "STCS"; }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    public long compactionsRun()    { return compactionsRun.get(); }
    public long sstablesCompacted() { return sstablesCompacted.get(); }
    public long partitionsMerged()  { return partitionsMerged.get(); }
    public long tombstonesPurged()  { return tombstonesPurged.get(); }
    public long bytesReclaimed()    { return bytesReclaimed.get(); }
}
