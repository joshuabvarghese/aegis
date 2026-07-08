package com.aegis.memtable;

import com.aegis.commitlog.CommitLog.CommitLogPosition;
import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * MemTable — the in-memory write buffer of the LSM-tree.
 *
 * ─── Cassandra Parallel ───────────────────────────────────────────────────────
 *
 * Cassandra's MemTable is a sorted data structure that buffers writes
 * in memory before they are flushed to an immutable SSTable on disk.
 * The sort order matches the SSTable's partition order (Murmur3 token),
 * so the flush operation is a single sequential scan — O(n), no sorting needed.
 *
 * We implement the same design:
 *   - ConcurrentSkipListMap<PartitionKey, Row> — sorted by Murmur3 token
 *   - Concurrent reads and writes without a global lock (skip-list is lock-free
 *     for non-conflicting keys; conflicting keys use per-row synchronization)
 *   - Size tracking via AtomicLong for flush threshold enforcement
 *   - Each MemTable has a "min CommitLog position" recording the oldest unflushed
 *     CommitLog segment — this is what Cassandra uses to recycle CommitLog segments
 *
 * ─── Lifecycle ────────────────────────────────────────────────────────────────
 *
 *   ACTIVE   → accepts writes, serves reads
 *   FLUSHING → write-locked, being serialized to SSTable by flush thread
 *   FLUSHED  → retired, reads fall through to SSTable
 *
 * Cassandra maintains a list of MemTables (active + being-flushed) for read merging.
 * We implement the same via MemTableManager.
 */
public final class MemTable {

    public enum Status { ACTIVE, FLUSHING, FLUSHED }

    private final long id;

    /**
     * Primary data structure: sorted by PartitionKey (Murmur3 token order).
     * ConcurrentSkipListMap provides:
     *   - O(log n) put/get/contains
     *   - Lock-free reads concurrent with writes
     *   - Ordered iteration for SSTable flush (sequential scan, cache-friendly)
     *
     * This is the Java equivalent of Cassandra's AtomicBTreePartition map,
     * simplified for clarity (Cassandra uses a B-tree variant for better
     * cache locality; skip-lists have similar asymptotic complexity).
     */
    private final ConcurrentSkipListMap<PartitionKey, Row> data =
        new ConcurrentSkipListMap<>();

    // Size tracking — used for flush threshold enforcement
    private final AtomicLong estimatedSizeBytes = new AtomicLong(0);

    // CommitLog position tracking — oldest position in this MemTable
    // When this MemTable is flushed, all CommitLog segments up to this
    // position can be recycled (Cassandra's ReplayPosition tracking)
    private volatile CommitLogPosition minCommitLogPosition = CommitLogPosition.NONE;
    private volatile CommitLogPosition maxCommitLogPosition = CommitLogPosition.NONE;

    private volatile Status status = Status.ACTIVE;
    private final ReentrantReadWriteLock statusLock = new ReentrantReadWriteLock();

    // Creation timestamp — used for SSTable age tracking
    private final long createdAtMs = System.currentTimeMillis();

    public MemTable(long id) {
        this.id = id;
    }

    // ─── Write Path ───────────────────────────────────────────────────────────

    /**
     * Insert or merge a row into the MemTable.
     *
     * If a row with the same PartitionKey already exists, cells are merged
     * with last-write-wins semantics (higher timestamp wins per column).
     * This matches Cassandra's MemTable.put() behaviour.
     *
     * @param row              the row to insert
     * @param commitLogPos     the position of this write in the CommitLog
     */
    public void put(Row row, CommitLogPosition commitLogPos) {
        statusLock.readLock().lock();
        try {
            if (status != Status.ACTIVE) {
                throw new IllegalStateException("MemTable " + id + " is not ACTIVE (status=" + status + ")");
            }

            data.merge(row.key(), row, MemTable::mergeRows);
            estimatedSizeBytes.addAndGet(row.estimatedSizeBytes());
            updateCommitLogPosition(commitLogPos);

        } finally {
            statusLock.readLock().unlock();
        }
    }

    /** Merge two rows by taking the highest-timestamp cell per column. */
    private static Row mergeRows(Row existing, Row incoming) {
        for (var entry : incoming.cells().entrySet()) {
            String colName   = entry.getKey();
            Row.Cell newCell = entry.getValue();
            Row.Cell oldCell = existing.getCell(colName);

            if (oldCell == null || newCell.timestamp() >= oldCell.timestamp()) {
                if (newCell.isTombstone()) {
                    existing.deleteColumn(colName, newCell.timestamp());
                } else {
                    existing.putColumn(colName,
                        newCell.valueBytes() != null ? newCell.valueBytes() : new byte[0],
                        newCell.timestamp());
                }
            }
        }
        return existing;
    }

    private void updateCommitLogPosition(CommitLogPosition pos) {
        if (minCommitLogPosition == CommitLogPosition.NONE
                || pos.compareTo(minCommitLogPosition) < 0) {
            minCommitLogPosition = pos;
        }
        if (maxCommitLogPosition == CommitLogPosition.NONE
                || pos.compareTo(maxCommitLogPosition) > 0) {
            maxCommitLogPosition = pos;
        }
    }

    // ─── Read Path ────────────────────────────────────────────────────────────

    /**
     * Look up a row by partition key.
     *
     * Returns Optional.empty() if the key is not present (not an error —
     * the caller must then check SSTables).
     *
     * Read does not require a lock — ConcurrentSkipListMap.get() is non-blocking.
     */
    public Optional<Row> get(PartitionKey key) {
        return Optional.ofNullable(data.get(key));
    }

    public Optional<Row> get(String key) {
        return get(PartitionKey.of(key));
    }

    /**
     * Range scan — returns all rows with keys in [fromKey, toKey].
     * Used by SSTable flush to produce sorted output in one sequential pass.
     *
     * ConcurrentSkipListMap.subMap() is O(1) and the resulting NavigableMap
     * iteration is O(n) in token order — exactly what SSTable flush needs.
     */
    public NavigableMap<PartitionKey, Row> rangeScan(PartitionKey fromKey, PartitionKey toKey) {
        return data.subMap(fromKey, true, toKey, true);
    }

    /**
     * Full scan — returns all rows in token-sorted order.
     * This is the flush path: MemTableFlushRunner calls this to write the SSTable.
     */
    public NavigableMap<PartitionKey, Row> all() {
        return Collections.unmodifiableNavigableMap(data);
    }

    // ─── Lifecycle Transitions ────────────────────────────────────────────────

    /**
     * Transition to FLUSHING state — write-locks the MemTable.
     * After this point, no new writes are accepted.
     * The MemTable's contents are fully visible to concurrent reads until
     * the flush completes and the MemTable transitions to FLUSHED.
     */
    public void markFlushing() {
        statusLock.writeLock().lock();
        try {
            if (status != Status.ACTIVE) throw new IllegalStateException(
                "Cannot mark FLUSHING from status=" + status);
            status = Status.FLUSHING;
        } finally {
            statusLock.writeLock().unlock();
        }
    }

    /** Called after SSTable is fully written and synced to disk. */
    public void markFlushed() {
        statusLock.writeLock().lock();
        try {
            status = Status.FLUSHED;
        } finally {
            statusLock.writeLock().unlock();
        }
    }

    // ─── State ────────────────────────────────────────────────────────────────

    public long   id()                      { return id; }
    public Status status()                  { return status; }
    public int    rowCount()                { return data.size(); }
    public long   estimatedSizeBytes()      { return estimatedSizeBytes.get(); }
    public long   createdAtMs()             { return createdAtMs; }
    public CommitLogPosition minCommitLogPosition() { return minCommitLogPosition; }
    public CommitLogPosition maxCommitLogPosition() { return maxCommitLogPosition; }

    public boolean shouldFlush() {
        return estimatedSizeBytes.get() >= StorageConfig.MEMTABLE_FLUSH_THRESHOLD_BYTES
            && status == Status.ACTIVE;
    }

    public boolean isEmpty() { return data.isEmpty(); }

    @Override
    public String toString() {
        return "MemTable{id=%d, status=%s, rows=%d, size=%dKB}"
            .formatted(id, status, rowCount(), estimatedSizeBytes.get() / 1024);
    }
}
