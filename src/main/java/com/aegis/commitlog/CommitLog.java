package com.aegis.commitlog;

import com.aegis.core.Row;
import com.aegis.core.StorageConfig;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * CommitLog — the write-ahead durability layer.
 *
 * ─── Role in the LSM Write Path ──────────────────────────────────────────────
 *
 * Cassandra write path order (we implement the same):
 *
 *   Client write
 *       │
 *       ▼
 *   CommitLog.append(row)   ← THIS CLASS
 *       │  durable on disk before returning
 *       ▼
 *   MemTable.put(row)       ← next layer
 *       │  in-memory, lost on crash without CommitLog
 *       ▼
 *   [background] MemTable flush → SSTable
 *       │
 *       ▼
 *   CommitLog.discardSegmentsUpTo(segmentId)  ← segment recycling
 *
 * ─── Segment Lifecycle ───────────────────────────────────────────────────────
 *
 * CommitLog is divided into rolling segments (default 32MB each).
 * A segment is "dirty" if it contains writes not yet flushed to an SSTable.
 * A segment can be deleted (recycled) once all its writes have been flushed.
 *
 * Cassandra tracks this via ReplayPosition(segmentId, position) stored in each
 * flushed SSTable's metadata. We implement the same via CommitLogPosition.
 *
 * ─── Recovery ────────────────────────────────────────────────────────────────
 *
 * On startup, CrashRecovery calls replay(consumer) which:
 *   1. Finds all .clog segments in the commitlog directory
 *   2. Sorts them by segmentId (ascending = chronological order)
 *   3. Reads each segment, verifies CRC on each record
 *   4. Truncates torn writes at segment tails
 *   5. Calls the consumer for each valid row (which re-inserts into MemTable)
 */
public final class CommitLog implements Closeable {

    private static final Logger log = Logger.getLogger(CommitLog.class.getName());

    private final Path                  dir;
    private final StorageConfig.CommitLogSyncMode syncMode;
    private final AtomicLong            segmentIdGen = new AtomicLong(System.currentTimeMillis());

    private volatile CommitLogSegment   activeSegment;
    private final List<CommitLogSegment> inactiveSegments = new CopyOnWriteArrayList<>();

    // fsync scheduler — Virtual Thread based
    private final ScheduledExecutorService fsyncScheduler;

    // Metrics
    private final AtomicLong totalWrites     = new AtomicLong(0);
    private final AtomicLong totalBytesWritten = new AtomicLong(0);
    private final AtomicLong segmentRolls    = new AtomicLong(0);

    public CommitLog(Path dir, StorageConfig.CommitLogSyncMode syncMode) throws IOException {
        this.dir      = dir;
        this.syncMode = syncMode;
        dir.toFile().mkdirs();

        this.activeSegment = openNewSegment();

        // PERIODIC fsync scheduler
        this.fsyncScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("commitlog-fsync").factory()
        );

        if (syncMode == StorageConfig.CommitLogSyncMode.PERIODIC) {
            fsyncScheduler.scheduleAtFixedRate(
                this::periodicFsync,
                StorageConfig.COMMITLOG_SYNC_PERIOD_MS,
                StorageConfig.COMMITLOG_SYNC_PERIOD_MS,
                TimeUnit.MILLISECONDS
            );
        }

        log.info("[COMMITLOG] Opened. dir=%s syncMode=%s".formatted(dir, syncMode));
    }

    // ─── Write Path ───────────────────────────────────────────────────────────

    /**
     * Append a row to the active CommitLog segment.
     *
     * Returns a CommitLogPosition that the MemTable stores.
     * When the MemTable is flushed to an SSTable, the SSTable records this
     * position so segments up to it can be recycled.
     */
    public CommitLogPosition append(Row row) throws IOException {
        CommitLogSegment segment = activeSegment;
        long offset = segment.append(row);

        totalWrites.incrementAndGet();
        totalBytesWritten.addAndGet(row.estimatedSizeBytes());

        // Roll to a new segment if this one is full
        if (segment.isFull()) {
            rollSegment();
        }

        return new CommitLogPosition(segment.segmentId(), offset);
    }

    private synchronized void rollSegment() throws IOException {
        if (!activeSegment.isFull()) return;

        CommitLogSegment old = activeSegment;
        old.fsync(); // ensure all writes are durable before declaring inactive
        inactiveSegments.add(old);

        activeSegment = openNewSegment();
        segmentRolls.incrementAndGet();

        log.info("[COMMITLOG] Rolled to new segment. old=%d new=%d"
            .formatted(old.segmentId(), activeSegment.segmentId()));
    }

    private CommitLogSegment openNewSegment() throws IOException {
        long id = segmentIdGen.getAndIncrement();
        return new CommitLogSegment(dir, id, syncMode);
    }

    // ─── Segment Recycling ────────────────────────────────────────────────────

    /**
     * Mark all segments up to and including the given position as recyclable.
     *
     * Called by the MemTable flush callback after a successful SSTable write.
     * Cassandra calls this via CommitLog.discardCompletedSegments().
     */
    public void discardSegmentsUpTo(CommitLogPosition position) {
        inactiveSegments.removeIf(segment -> {
            boolean recyclable = segment.segmentId() <= position.segmentId();
            if (recyclable) {
                try {
                    segment.close();
                    Files.deleteIfExists(segment.filePath());
                    log.info("[COMMITLOG] Recycled segment %d".formatted(segment.segmentId()));
                } catch (IOException e) {
                    log.warning("[COMMITLOG] Failed to delete segment %d: %s"
                        .formatted(segment.segmentId(), e.getMessage()));
                }
            }
            return recyclable;
        });
    }

    // ─── Replay (Crash Recovery) ──────────────────────────────────────────────

    /**
     * Replay all CommitLog segments into the provided consumer.
     *
     * Called on startup by CrashRecovery before the MemTable is opened.
     * Equivalent to Cassandra's CommitLogReplayer.replay().
     *
     * @param replayConsumer  receives each valid Row in chronological order
     * @return                replay statistics
     */
    public ReplayStats replay(Consumer<Row> replayConsumer) throws IOException {
        List<Path> segmentPaths = findSegmentFiles();
        segmentPaths.sort(Comparator.comparing(p -> {
            String name = p.getFileName().toString().replace(".clog", "");
            try { return Long.parseLong(name); } catch (NumberFormatException e) { return 0L; }
        }));

        int segmentsReplayed  = 0;
        int rowsReplayed      = 0;
        int corruptionEvents  = 0;

        for (Path segPath : segmentPaths) {
            long segId = parseSegmentId(segPath);
            CommitLogSegment seg = new CommitLogSegment(segPath.getParent(), segId, syncMode);

            try {
                CommitLogSegment.ReplayResult result = seg.replay();

                if (result.hadCorruption()) {
                    corruptionEvents++;
                    log.warning("[COMMITLOG] Truncating torn write in segment %d at pos=%d"
                        .formatted(segId, result.lastGoodPosition()));
                    seg.truncateTo(result.lastGoodPosition());
                }

                for (Row row : result.rows()) {
                    replayConsumer.accept(row);
                    rowsReplayed++;
                }
                segmentsReplayed++;

            } finally {
                seg.close();
            }
        }

        log.info("[COMMITLOG] Replay complete. segments=%d rows=%d corruptions=%d"
            .formatted(segmentsReplayed, rowsReplayed, corruptionEvents));

        return new ReplayStats(segmentsReplayed, rowsReplayed, corruptionEvents);
    }

    // ─── fsync ────────────────────────────────────────────────────────────────

    private void periodicFsync() {
        try {
            activeSegment.fsync();
        } catch (IOException e) {
            log.warning("[COMMITLOG] fsync failed: " + e.getMessage());
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private List<Path> findSegmentFiles() throws IOException {
        if (!dir.toFile().exists()) return List.of();
        try (Stream<Path> stream = Files.walk(dir, 1)) {
            return stream.filter(p -> p.toString().endsWith(".clog"))
                         .sorted()
                         .toList();
        }
    }

    private long parseSegmentId(Path p) {
        String name = p.getFileName().toString().replace(".clog", "");
        try { return Long.parseLong(name); } catch (NumberFormatException e) { return 0L; }
    }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    public long totalWrites()      { return totalWrites.get(); }
    public long totalBytesWritten(){ return totalBytesWritten.get(); }
    public long segmentRolls()     { return segmentRolls.get(); }
    public long activeSegmentId()  { return activeSegment.segmentId(); }
    public int  inactiveSegments() { return inactiveSegments.size(); }

    @Override
    public void close() throws IOException {
        fsyncScheduler.shutdown();
        activeSegment.close();
        for (CommitLogSegment seg : inactiveSegments) seg.close();
    }

    // ─── Value Types ──────────────────────────────────────────────────────────

    /**
     * Position within the CommitLog — stored in SSTable metadata.
     * Cassandra uses ReplayPosition(segmentId, position) for the same purpose.
     */
    public record CommitLogPosition(long segmentId, long position)
        implements Comparable<CommitLogPosition> {

        public static final CommitLogPosition NONE = new CommitLogPosition(-1, -1);

        @Override
        public int compareTo(CommitLogPosition other) {
            int cmp = Long.compare(this.segmentId, other.segmentId);
            return cmp != 0 ? cmp : Long.compare(this.position, other.position);
        }

        @Override
        public String toString() {
            return "CLPos(%d:%d)".formatted(segmentId, position);
        }
    }

    public record ReplayStats(int segmentsReplayed, int rowsReplayed, int corruptionEvents) {}
}
