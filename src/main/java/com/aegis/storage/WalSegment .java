package com.aegis.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.aegis.core.AegisConfig;
import com.aegis.core.CorruptRecordException;
import com.aegis.core.EventBus;
import com.aegis.core.WalRecord;

/**
 * Module A: Hot Storage Layer — a single WAL segment file.
 *
 * Frame format: [Length:4B][Offset:8B][Timestamp:8B][CRC32C:4B][Payload:varB]
 *
 * Each segment has:
 *   - a .log file (raw framed records, sequential appends)
 *   - a .index file (sparse: (logicalOffset -> filePosition) every INDEX_INTERVAL_BYTES)
 */
public class WalSegment implements Closeable {

    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private final long baseOffset;
    private final int nodeId;

    private final AtomicLong currentSize = new AtomicLong(0);
    private long lastIndexedFilePos = 0;

    // Index entry: 8B logicalOffset + 8B filePosition = 16 bytes
    private static final int INDEX_ENTRY_SIZE = 16;

    public WalSegment(Path dir, long baseOffset, int nodeId) throws IOException {
        this.baseOffset = baseOffset;
        this.nodeId = nodeId;

        dir.toFile().mkdirs();
        logPath   = dir.resolve("%020d.log".formatted(baseOffset));
        indexPath = dir.resolve("%020d.index".formatted(baseOffset));

        logChannel = FileChannel.open(logPath,
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        indexChannel = FileChannel.open(indexPath,
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Seek to end for append mode
        long existingSize = logChannel.size();
        logChannel.position(existingSize);
        currentSize.set(existingSize);
    }

    /**
     * Append a WAL record. Returns the file-position of the written frame.
     * Thread-safe via synchronized (only one active segment writer per node).
     */
    public synchronized long append(WalRecord record) throws IOException {
        ByteBuffer frame = record.serialize();
        long filePos = logChannel.position();

        while (frame.hasRemaining()) {
            logChannel.write(frame);
        }

        long newSize = currentSize.addAndGet(record.totalSize());

        // Write a sparse index entry if we've advanced past the index interval
        if (filePos - lastIndexedFilePos >= AegisConfig.INDEX_INTERVAL_BYTES) {
            ByteBuffer indexEntry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexEntry.putLong(record.offset());
            indexEntry.putLong(filePos);
            indexEntry.flip();
            indexChannel.write(indexEntry, indexChannel.size());
            lastIndexedFilePos = filePos;
        }

        EventBus.get().publish(EventBus.EventType.RECORD_APPENDED, nodeId,
            "APPEND offset=" + record.offset() + " size=" + record.totalSize() + "B", newSize);

        return filePos;
    }

    /**
     * Background fsync — called by the FsyncScheduler at SRE-configurable intervals.
     * force(false) flushes data but not file metadata (faster, sufficient for WAL).
     */
    public void fsync() throws IOException {
        logChannel.force(false);
        indexChannel.force(false);
        EventBus.get().publish(EventBus.EventType.FSYNC_COMPLETE, nodeId, "fsync OK segment=" + baseOffset);
    }

    /**
     * Read all valid records from this segment (used by recovery and historic reads).
     * Skips corrupt records with CRC mismatches (truncation mode for recovery).
     */
    public List<WalRecord> readAll(boolean skipCorrupt) throws IOException {
        List<WalRecord> records = new ArrayList<>();
        FileChannel readChannel = FileChannel.open(logPath, StandardOpenOption.READ);

        try (readChannel) {
            long size = readChannel.size();
            if (size == 0) return records;

            ByteBuffer buf = ByteBuffer.allocateDirect((int) Math.min(size, 32 * 1024 * 1024));
            readChannel.position(0);

            while (readChannel.position() < size) {
                buf.clear();
                int read = readChannel.read(buf);
                if (read <= 0) break;
                buf.flip();

                while (buf.remaining() >= WalRecord.HEADER_SIZE) {
                    buf.mark();
                    int payloadLen = buf.getInt();

                    if (payloadLen < 0 || payloadLen > 64 * 1024 * 1024) {
                        // Corrupt length prefix — stop scanning
                        break;
                    }

                    if (buf.remaining() < 8 + 8 + 4 + payloadLen) {
                        buf.reset();
                        break; // need more data
                    }

                    buf.reset();
                    try {
                        WalRecord record = WalRecord.deserialize(buf);
                        records.add(record);
                    } catch (CorruptRecordException e) {
                        if (!skipCorrupt) throw e;
                        // Skip this record and try to re-sync
                        buf.reset();
                        buf.getInt(); // skip length
                        if (buf.remaining() >= 8 + 8 + 4 + Math.max(0, payloadLen)) {
                            buf.position(buf.position() + 8 + 8 + 4 + payloadLen);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        return records;
    }

    /** Truncate the segment to the given file position (crash recovery). */
    public synchronized void truncateTo(long filePosition) throws IOException {
        logChannel.truncate(filePosition);
        currentSize.set(filePosition);
        logChannel.position(filePosition);
    }

    public long size()       { return currentSize.get(); }
    public long baseOffset() { return baseOffset; }
    public Path logPath()    { return logPath; }
    public boolean isFull()  { return currentSize.get() >= AegisConfig.SEGMENT_ROLL_BYTES; }

    @Override
    public void close() throws IOException {
        try { logChannel.force(true); } catch (IOException ignored) {}
        try { indexChannel.force(true); } catch (IOException ignored) {}
        logChannel.close();
        indexChannel.close();
    }
}
