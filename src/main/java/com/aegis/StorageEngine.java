package com.aegis;

import com.aegis.commitlog.CommitLog;
import com.aegis.commitlog.CommitLog.CommitLogPosition;
import com.aegis.compaction.CompactionStrategy;
import com.aegis.compaction.CompactionStrategy.CompactionPlan;
import com.aegis.compaction.LeveledCompactor;
import com.aegis.compaction.STCSCompactor;
import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.memtable.MemTable;
import com.aegis.sstable.SSTableReader;
import com.aegis.sstable.SSTableWriter;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * StorageEngine — the top-level coordinator of the LSM-tree.
 *
 * ─── Architecture Overview ────────────────────────────────────────────────────
 *
 * This class orchestrates the complete Cassandra-style LSM-tree write and read paths.
 *
 * WRITE PATH:
 *   write(row)
 *     │
 *     ├─► 1. CommitLog.append(row)          durable before anything else
 *     │       returns CommitLogPosition
 *     │
 *     ├─► 2. activeMemTable.put(row, clPos)  in-memory, instantly readable
 *     │
 *     └─► 3. if memTable.shouldFlush()
 *                 triggerFlush()             async, Virtual Thread
 *                     └─► SSTableWriter.writeAll()
 *                     └─► commitLog.discardSegmentsUpTo(clPos)
 *
 * READ PATH:
 *   read(key)
 *     │
 *     ├─► 1. activeMemTable.get(key)         O(log n) skip-list lookup
 *     ├─► 2. flushingMemTable.get(key)        if one exists (being flushed)
 *     └─► 3. SSTables, grouped by level:
 *               level 0 (may overlap)         check every L0 table, newest first
 *               level 1+ (never overlap)      range-check finds at most one candidate per level
 *
 * COMPACTION:
 *   Background Virtual Thread, fires every COMPACTION_CHECK_INTERVAL_MS.
 *   Delegates to whichever CompactionStrategy is configured — STCSCompactor
 *   (default) or LeveledCompactor — via COMPACTION_STRATEGY. Both implement
 *   the same plan()/execute() contract, so the engine itself doesn't know or
 *   care which one is running.
 */
public final class StorageEngine implements Closeable {

    private static final Logger log = Logger.getLogger(StorageEngine.class.getName());

    // ─── Components ───────────────────────────────────────────────────────────

    private final CommitLog          commitLog;
    private final CompactionStrategy compactor;

    // MemTable management — read-write lock protects the swap from active → flushing
    private volatile MemTable     activeMemTable;
    private volatile MemTable     flushingMemTable; // null when no flush in progress
    private final ReentrantReadWriteLock memTableLock = new ReentrantReadWriteLock();

    // SSTable catalog — newest first (most recently flushed has highest priority in reads)
    private final List<SSTableMetadata> sstableCatalog = new CopyOnWriteArrayList<>();
    private final AtomicLong            generationGen  = new AtomicLong(
        System.currentTimeMillis());

    // Background executors — Virtual Thread pools
    private final ExecutorService flushExecutor      = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService compactionScheduler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("compaction-daemon").factory());
    // Global metrics
    private final AtomicLong totalWrites     = new AtomicLong(0);
    private final AtomicLong totalReads      = new AtomicLong(0);
    private final AtomicLong readHitsMemtable = new AtomicLong(0);
    private final AtomicLong readHitsSStable  = new AtomicLong(0);
    private final AtomicLong readMisses        = new AtomicLong(0);
    private final AtomicLong flushCount        = new AtomicLong(0);
    // Read amplification proxy: how many SSTables were actually opened and
    // checked to answer reads, in total. Divided by totalReads, this is the
    // number that makes the STCS-vs-LCS comparison concrete instead of a claim.
    private final AtomicLong sstablesScannedForReads = new AtomicLong(0);

    // ─── Construction ─────────────────────────────────────────────────────────

    public StorageEngine() throws IOException {
        // Initialise directories
        StorageConfig.commitLogDir().toFile().mkdirs();
        StorageConfig.ssTableDir().toFile().mkdirs();

        // Boot CommitLog
        this.commitLog = new CommitLog(
            StorageConfig.commitLogDir(), StorageConfig.commitLogSyncMode());

        // Create initial MemTable
        this.activeMemTable = new MemTable(generationGen.getAndIncrement());

        // Replay CommitLog to reconstruct MemTable from any crash
        replayCommitLog();

        // Scan SSTable directory and rebuild catalog
        rebuildCatalog();

        // Boot compactor — STCS (default) or LCS, selected via COMPACTION_STRATEGY env var
        this.compactor = switch (StorageConfig.compactionStrategy()) {
            case LCS  -> new LeveledCompactor(StorageConfig.ssTableDir(), generationGen);
            case STCS -> new STCSCompactor(StorageConfig.ssTableDir(), generationGen);
        };

        // Start background daemons
        startCompactionDaemon();

        log.info("[ENGINE] StorageEngine started. sstables=%d compaction=%s"
            .formatted(sstableCatalog.size(), compactor.strategyName()));
    }

    // ─── Write Path ───────────────────────────────────────────────────────────

    /**
     * Write a row into the storage engine.
     *
     * Implements the full Cassandra write path:
     *   CommitLog → MemTable → (trigger flush if needed)
     *
     * This method returns once the CommitLog write is durable and the MemTable
     * write is complete. The SSTable flush is asynchronous.
     */
    public void write(Row row) throws IOException {
        // Step 1: CommitLog — durability before anything touches memory
        CommitLogPosition clPosition = commitLog.append(row);

        // Step 2: MemTable — in-memory write, immediately readable
        memTableLock.readLock().lock();
        try {
            activeMemTable.put(row, clPosition);
        } finally {
            memTableLock.readLock().unlock();
        }

        totalWrites.incrementAndGet();

        // Step 3: Trigger flush if MemTable has reached its threshold
        if (activeMemTable.shouldFlush()) {
            triggerFlush(false);
        }
    }

    /**
     * Convenience write: single column, auto-timestamp.
     */
    public void write(String key, String column, String value) throws IOException {
        long tsMicros = System.currentTimeMillis() * 1_000L;
        Row row = Row.create(key).putColumn(column, value, tsMicros);
        write(row);
    }

    /**
     * Delete a column — inserts a tombstone.
     */
    public void delete(String key, String column) throws IOException {
        long tsMicros = System.currentTimeMillis() * 1_000L;
        Row row = Row.create(key).deleteColumn(column, tsMicros);
        write(row);
    }

    // ─── Read Path ────────────────────────────────────────────────────────────

    /**
     * Read a partition by key.
     *
     * Implements the full Cassandra read path:
     *   MemTable → (flushing MemTable) → SSTables (Bloom filter → Index → Data)
     *
     * Returns the reconciled row, or Optional.empty() if the key does not exist.
     */
    public Optional<Row> read(PartitionKey key) throws IOException {
        totalReads.incrementAndGet();

        // Step 1: Active MemTable — O(log n), always checked first
        memTableLock.readLock().lock();
        Optional<Row> memResult;
        try {
            memResult = activeMemTable.get(key);
            // Also check flushing MemTable if one exists
            if (memResult.isEmpty() && flushingMemTable != null) {
                memResult = flushingMemTable.get(key);
            }
        } finally {
            memTableLock.readLock().unlock();
        }

        if (memResult.isPresent()) {
            readHitsMemtable.incrementAndGet();
            return memResult;
        }

        // Step 2: SSTables.
        //
        // Level 0 holds raw flush output (and everything STCS produces, since
        // STCS is deliberately unleveled) — these can overlap in key range, so
        // every L0 SSTable must be checked, newest first.
        //
        // Level 1+ only exists under LeveledCompactor, which guarantees
        // SSTables within the same level never overlap in key range. That
        // means at most one SSTable per level can possibly contain the key —
        // we range-check to find it and stop at the first level that has one,
        // instead of checking every SSTable the way STCS has to.
        Map<Integer, List<SSTableMetadata>> byLevel = new TreeMap<>();
        for (SSTableMetadata meta : sstableCatalog) {
            if (!meta.dataPath().toFile().exists()) continue; // data file missing, skip
            byLevel.computeIfAbsent(meta.level(), l -> new ArrayList<>()).add(meta);
        }

        for (var levelEntry : byLevel.entrySet()) {
            List<SSTableMetadata> tables = levelEntry.getValue();

            if (levelEntry.getKey() == 0) {
                tables.sort(Comparator.comparingLong(SSTableMetadata::createdAtMs).reversed());
                for (SSTableMetadata meta : tables) {
                    Optional<Row> result = readFromSSTableIfInRange(meta, key);
                    if (result.isPresent()) return result;
                }
            } else {
                for (SSTableMetadata meta : tables) {
                    if (meta.mightContainKey(key)) {
                        Optional<Row> result = readFromSSTableIfInRange(meta, key);
                        if (result.isPresent()) return result;
                        break; // non-overlapping level — no other table here can contain this key
                    }
                }
            }
        }

        readMisses.incrementAndGet();
        return Optional.empty();
    }

    /** Range-checks, then opens and queries a single SSTable, counting it toward read amplification. */
    private Optional<Row> readFromSSTableIfInRange(SSTableMetadata meta, PartitionKey key) throws IOException {
        if (!meta.mightContainKey(key)) return Optional.empty();

        sstablesScannedForReads.incrementAndGet();
        try (SSTableReader reader = new SSTableReader(meta)) {
            Optional<Row> result = reader.get(key);
            if (result.isPresent()) {
                readHitsSStable.incrementAndGet();
                return result;
            }
        }
        return Optional.empty();
    }

    public Optional<Row> read(String key) throws IOException {
        return read(PartitionKey.of(key));
    }

    // ─── MemTable Flush ───────────────────────────────────────────────────────

    /**
     * Trigger an asynchronous MemTable flush.
     *
     * Swaps the active MemTable with a fresh one under write-lock (fast),
     * then flushes the old one to an SSTable on a Virtual Thread (slow, async).
     *
     * This mirrors Cassandra's ColumnFamilyStore.switchMemTableIfCurrent() +
     * flush() pattern.
     */
    private void triggerFlush(boolean force) {
        MemTable toFlush;

        memTableLock.writeLock().lock();
        try {
            // Bail out if there's nothing to do: either the threshold hasn't been
            // reached yet (and no one is forcing it), or the active MemTable is
            // already mid-flush / empty.
            if (!force && !activeMemTable.shouldFlush()) return;
            if (activeMemTable.status() != MemTable.Status.ACTIVE) return;
            if (activeMemTable.isEmpty()) return;

            toFlush = activeMemTable;
            toFlush.markFlushing();
            flushingMemTable = toFlush;

            // New active MemTable — writes continue without blocking
            activeMemTable = new MemTable(generationGen.getAndIncrement());
        } finally {
            memTableLock.writeLock().unlock();
        }

        // Flush on a Virtual Thread — does not block the write path
        final MemTable memTableToFlush = toFlush;
        flushExecutor.submit(() -> {
            try {
                flushToDisk(memTableToFlush);
            } catch (IOException e) {
                log.severe("[ENGINE] Flush failed: " + e.getMessage());
            }
        });
    }

    private void flushToDisk(MemTable memTable) throws IOException {
        if (memTable.isEmpty()) {
            memTable.markFlushed();
            flushingMemTable = null;
            return;
        }

        long generation = generationGen.getAndIncrement();
        log.info("[ENGINE] Flushing MemTable id=%d rows=%d to SSTable gen=%d"
            .formatted(memTable.id(), memTable.rowCount(), generation));

        SSTableWriter writer = new SSTableWriter(StorageConfig.ssTableDir(), generation);
        try {
            writer.writeAll(
                memTable.all(),
                memTable.rowCount(),
                memTable.maxCommitLogPosition()
            );
            SSTableMetadata meta = writer.finish();

            // Register in catalog — prepend so newest is first
            sstableCatalog.add(0, meta);

            // Notify CommitLog that segments up to this position can be recycled
            if (memTable.maxCommitLogPosition() != CommitLogPosition.NONE) {
                commitLog.discardSegmentsUpTo(memTable.maxCommitLogPosition());
            }

            memTable.markFlushed();
            flushingMemTable = null;
            flushCount.incrementAndGet();

            log.info("[ENGINE] Flush complete. SSTable gen=%d partitions=%d size=%dKB"
                .formatted(generation, meta.partitionCount(), meta.dataSizeBytes() / 1024));

        } finally {
            writer.close();
        }
    }

    /**
     * Force a synchronous flush of the current MemTable — used by CLI and tests.
     */
    public void forceFlush() throws IOException, InterruptedException {
        MemTable current = activeMemTable;
        if (current.isEmpty()) return;

        // force=true bypasses the size threshold — this is what makes "force" mean
        // something. Without it, forceFlush() was a no-op for any MemTable smaller
        // than MEMTABLE_FLUSH_THRESHOLD_BYTES (i.e. every demo/CLI invocation).
        triggerFlush(true);

        // Wait for flush to complete (poll)
        for (int i = 0; i < 50; i++) {
            if (current.status() == MemTable.Status.FLUSHED) break;
            Thread.sleep(100);
        }
    }

    // ─── Startup: CommitLog Replay ────────────────────────────────────────────

    private void replayCommitLog() throws IOException {
        log.info("[ENGINE] Starting CommitLog replay...");
        CommitLog.ReplayStats stats = commitLog.replay(row -> {
            try {
                activeMemTable.put(row, CommitLogPosition.NONE);
            } catch (Exception e) {
                log.warning("[ENGINE] Replay put failed for key=" + row.key() + ": " + e.getMessage());
            }
        });
        log.info("[ENGINE] CommitLog replay done. segments=%d rows=%d corruptions=%d"
            .formatted(stats.segmentsReplayed(), stats.rowsReplayed(), stats.corruptionEvents()));
    }

    // ─── Startup: SSTable Catalog Rebuild ────────────────────────────────────

    private void rebuildCatalog() throws IOException {
        Path dir = StorageConfig.ssTableDir();
        if (!dir.toFile().exists()) return;

        try (Stream<Path> paths = Files.list(dir)) {
            List<Path> statsFiles = paths
                .filter(p -> p.toString().endsWith("-Statistics.db"))
                .sorted(Comparator.reverseOrder()) // newest generation first
                .toList();

            for (Path statsPath : statsFiles) {
                try {
                    SSTableMetadata meta = loadMetadataFromStats(statsPath);
                    if (meta.dataPath().toFile().exists()) {
                        sstableCatalog.add(meta);
                        log.info("[ENGINE] Loaded SSTable gen=%d partitions=%d"
                            .formatted(meta.generation(), meta.partitionCount()));
                    }
                } catch (Exception e) {
                    log.warning("[ENGINE] Could not load SSTable from " + statsPath + ": " + e.getMessage());
                }
            }
        }
    }

    private SSTableMetadata loadMetadataFromStats(Path statsPath) throws IOException {
        byte[] bytes = Files.readAllBytes(statsPath);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);

        long generation    = buf.getLong();
        long partitions    = buf.getLong();
        long cells         = buf.getLong();
        long createdAt     = buf.getLong();
        long dataSize      = buf.getLong();
        long filterSize    = buf.getLong();

        int minKeyLen = buf.getShort() & 0xFFFF;
        byte[] minKeyBytes = new byte[minKeyLen];
        buf.get(minKeyBytes);

        int maxKeyLen = buf.getShort() & 0xFFFF;
        byte[] maxKeyBytes = new byte[maxKeyLen];
        buf.get(maxKeyBytes);

        long clSegmentId = buf.getLong();
        long clPosition  = buf.getLong();
        int  level       = buf.remaining() >= 4 ? buf.getInt() : 0;

        return new SSTableMetadata(
            generation,
            statsPath.getParent(),
            partitions, cells,
            minKeyLen > 0 ? PartitionKey.of(minKeyBytes) : null,
            maxKeyLen > 0 ? PartitionKey.of(maxKeyBytes) : null,
            new CommitLogPosition(clSegmentId, clPosition),
            createdAt, dataSize, filterSize,
            level
        );
    }

    // ─── Background Daemons ───────────────────────────────────────────────────

    private void startCompactionDaemon() {
        compactionScheduler.scheduleAtFixedRate(() -> {
            try {
                runCompactionCycle();
            } catch (Exception e) {
                log.warning("[COMPACTION] Cycle error: " + e.getMessage());
            }
        }, StorageConfig.COMPACTION_CHECK_INTERVAL_MS,
           StorageConfig.COMPACTION_CHECK_INTERVAL_MS,
           TimeUnit.MILLISECONDS);
    }

    private void runCompactionCycle() throws IOException {
        List<SSTableMetadata> localSSTables = new ArrayList<>(sstableCatalog);
        // Only compact local (non-offloaded) SSTables
        localSSTables.removeIf(m -> !m.dataPath().toFile().exists());

        Optional<CompactionPlan> plan = compactor.plan(localSSTables);
        if (plan.isEmpty()) return;

        SSTableMetadata output = compactor.execute(plan.get());

        // Update catalog: remove inputs, add output
        sstableCatalog.removeAll(plan.get().inputs());
        sstableCatalog.add(0, output);
    }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    public EngineStats stats() {
        return new EngineStats(
            totalWrites.get(), totalReads.get(),
            readHitsMemtable.get(), readHitsSStable.get(),
            readMisses.get(),
            flushCount.get(),
            activeMemTable.rowCount(),
            activeMemTable.estimatedSizeBytes(),
            sstableCatalog.size(),
            compactor.compactionsRun(),
            compactor.tombstonesPurged(),
            compactor.bytesReclaimed(),
            compactor.bytesWritten(),
            compactor.strategyName(),
            sstablesScannedForReads.get(),
            commitLog.totalWrites()
        );
    }

    /** Per-level SSTable counts and byte totals — meaningful under LCS; under STCS everything sits in level 0. */
    public Map<Integer, LeveledCompactor.LevelSummary> levelSummary() {
        return LeveledCompactor.summarizeLevels(new ArrayList<>(sstableCatalog));
    }

    /** Snapshot of the current SSTable catalog — used by tests and tooling to inspect per-level state directly. */
    public List<SSTableMetadata> catalogSnapshot() {
        return new ArrayList<>(sstableCatalog);
    }

    public record EngineStats(
        long totalWrites, long totalReads,
        long readHitsMemtable, long readHitsSStable,
        long readMisses,
        long flushCount,
        int  activeMemTableRows,
        long activeMemTableBytes,
        int  sstableCount,
        long compactionsRun,
        long tombstonesPurged,
        long bytesReclaimed,
        long compactionBytesWritten,
        String compactionStrategy,
        long sstablesScannedForReads,
        long commitLogWrites
    ) {
        public double readHitRatio() {
            long hits = readHitsMemtable + readHitsSStable;
            return totalReads == 0 ? 0.0 : (double) hits / totalReads;
        }

        /** Average number of SSTables actually opened per read that reached the SSTable tier — the read-amplification number. */
        public double avgSStablesScannedPerSStableRead() {
            long sstableReads = readHitsSStable + readMisses;
            return sstableReads == 0 ? 0.0 : (double) sstablesScannedForReads / sstableReads;
        }

        public void print() {
            System.out.println("┌─────────────────────────────────────────────────────────┐");
            System.out.println("│                  STORAGE ENGINE STATS                   │");
            System.out.println("├─────────────────────────────────────────────────────────┤");
            System.out.printf( "│  Compaction: %-6s                                      │%n", compactionStrategy);
            System.out.printf( "│  Writes:     %-10d  CommitLog writes: %-10d    │%n", totalWrites, commitLogWrites);
            System.out.printf( "│  Reads:      %-10d  Hit ratio:        %.1f%%              │%n", totalReads, readHitRatio()*100);
            System.out.printf( "│  MemTable:   %-6d hits    SSTables: %-6d hits          │%n", readHitsMemtable, readHitsSStable);
            System.out.printf( "│  Misses:     %-6d    Avg SSTables scanned/read: %.2f    │%n", readMisses, avgSStablesScannedPerSStableRead());
            System.out.println("├─────────────────────────────────────────────────────────┤");
            System.out.printf( "│  Active MemTable: %d rows / %d KB                        │%n", activeMemTableRows, activeMemTableBytes/1024);
            System.out.printf( "│  SSTables:        %d on disk                             │%n", sstableCount);
            System.out.printf( "│  Flushes:         %d                                     │%n", flushCount);
            System.out.printf( "│  Compactions:     %d  tombstones purged: %d              │%n", compactionsRun, tombstonesPurged);
            System.out.printf( "│  Bytes reclaimed: %d KB   bytes written: %d KB           │%n", bytesReclaimed/1024, compactionBytesWritten/1024);
            System.out.println("└─────────────────────────────────────────────────────────┘");
        }
    }

    @Override
    public void close() throws IOException {
        compactionScheduler.shutdown();
        flushExecutor.shutdown();
        commitLog.close();
        log.info("[ENGINE] StorageEngine shut down.");
    }
}
