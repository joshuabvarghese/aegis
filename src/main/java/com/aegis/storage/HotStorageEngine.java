package com.aegis.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.core.WalRecord;

/**
 * Module A: Hot Storage Engine
 *
 * Manages the active WAL segment, rolls to new segments on threshold,
 * and schedules background fsync via Virtual Threads.
 *
 * All appends hold only a read-lock (concurrent appends are serialized
 * inside WalSegment.append()), while segment rolling holds the write-lock.
 */
public class HotStorageEngine {

    private final int nodeId;
    private final Path dataDir;
    private final TieringEngine tieringEngine;

    private volatile WalSegment activeSegment;
    private final List<WalSegment> closedSegments = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicLong nextOffset = new AtomicLong(0);
    private final AtomicLong totalBytesWritten = new AtomicLong(0);

    // Virtual Thread executor for fsync and housekeeping
    private final ScheduledExecutorService fsyncScheduler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("aegis-fsync-", 0).factory()
        );

    private TieringEngine tieringRef;

    public HotStorageEngine(int nodeId, TieringEngine tieringEngine) throws IOException {
        this.nodeId = nodeId;
        this.tieringEngine = tieringEngine;
        this.dataDir = AegisConfig.dataDir(nodeId);
        openOrRecoverActiveSegment();
        startFsyncScheduler();
    }

    private void openOrRecoverActiveSegment() throws IOException {
        dataDir.toFile().mkdirs();
        // Always start fresh segment based on current nextOffset
        activeSegment = new WalSegment(dataDir, nextOffset.get(), nodeId);
        EventBus.get().publish(EventBus.EventType.NODE_JOINED, nodeId,
            "Hot storage open, base_offset=" + nextOffset.get());
    }

    private void startFsyncScheduler() {
        fsyncScheduler.scheduleAtFixedRate(() -> {
            try {
                lock.readLock().lock();
                try {
                    activeSegment.fsync();
                } finally {
                    lock.readLock().unlock();
                }
            } catch (IOException e) {
                EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId, "fsync error: " + e.getMessage());
            }
        }, AegisConfig.FSYNC_INTERVAL_MS, AegisConfig.FSYNC_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Append a payload and return the assigned logical offset.
     * Will roll the segment if the current active segment is full.
     */
    public long append(byte[] payload) throws IOException {
        long offset = nextOffset.getAndIncrement();
        WalRecord record = WalRecord.of(offset, payload);

        lock.readLock().lock();
        try {
            activeSegment.append(record);
            totalBytesWritten.addAndGet(record.totalSize());
        } finally {
            lock.readLock().unlock();
        }

        // Check if we need to roll (outside read lock, then acquire write lock)
        if (activeSegment.isFull()) {
            rollSegment();
        }

        return offset;
    }

    private void rollSegment() throws IOException {
        lock.writeLock().lock();
        try {
            if (!activeSegment.isFull()) return; // double-check after acquiring write lock

            WalSegment rolledSegment = activeSegment;
            rolledSegment.fsync();
            closedSegments.add(rolledSegment);

            long newBase = nextOffset.get();
            activeSegment = new WalSegment(dataDir, newBase, nodeId);

            EventBus.get().publish(EventBus.EventType.SEGMENT_ROLLED, nodeId,
                "Segment #" + closedSegments.size() + " CLOSED base=" + rolledSegment.baseOffset()
                    + " size=" + rolledSegment.size(), rolledSegment.size());

            // Kick off async cold-tier upload via Virtual Thread
            if (tieringEngine != null) {
                WalSegment toUpload = rolledSegment;
                Thread.ofVirtual().name("tier-mover-" + rolledSegment.baseOffset())
                    .start(() -> {
                        try {
                            tieringEngine.uploadSegment(toUpload);
                        } catch (Exception e) {
                            EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                                "Tier upload failed: " + e.getMessage());
                        }
                    });
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Force a manual segment roll (for testing/CLI). */
    public void forceRoll() throws IOException {
        lock.writeLock().lock();
        try {
            WalSegment rolledSegment = activeSegment;
            rolledSegment.fsync();
            closedSegments.add(rolledSegment);

            long newBase = nextOffset.get();
            activeSegment = new WalSegment(dataDir, newBase, nodeId);

            EventBus.get().publish(EventBus.EventType.SEGMENT_ROLLED, nodeId,
                "FORCED ROLL segment #" + closedSegments.size(), rolledSegment.size());

            if (tieringEngine != null) {
                WalSegment toUpload = rolledSegment;
                Thread.ofVirtual().name("tier-mover-forced")
                    .start(() -> {
                        try {
                            tieringEngine.uploadSegment(toUpload);
                        } catch (Exception e) {
                            EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                                "Tier upload failed: " + e.getMessage());
                        }
                    });
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long currentOffset()      { return nextOffset.get(); }
    public long totalBytesWritten()  { return totalBytesWritten.get(); }
    public long activeSegmentSize()  { return activeSegment.size(); }
    public int  closedSegmentCount() { return closedSegments.size(); }
    public int  nodeId()             { return nodeId; }

    public List<WalSegment> closedSegments() {
        return Collections.unmodifiableList(closedSegments);
    }

    public void shutdown() throws IOException {
        fsyncScheduler.shutdown();
        lock.writeLock().lock();
        try {
            activeSegment.close();
            for (WalSegment seg : closedSegments) {
                seg.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
