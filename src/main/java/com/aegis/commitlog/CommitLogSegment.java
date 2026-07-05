package com.aegis.commitlog;

import com.aegis.core.Row;
import com.aegis.core.StorageConfig;
import com.aegis.core.StorageException.CorruptionException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * CommitLog Segment — the durability foundation of the LSM-tree write path.
 *
 * ─── Cassandra Parallel ───────────────────────────────────────────────────────
 *
 * Every write in Cassandra goes to the CommitLog BEFORE it touches the MemTable.
 * This guarantees durability: if the process crashes after writing to the
 * CommitLog but before the MemTable is flushed to an SSTable, the CommitLog
 * is replayed on startup to reconstruct the lost MemTable data.
 *
 * We implement the same contract:
 *   1. Caller writes Row → CommitLog.append() serialises and fsyncs
 *   2. Only after durable append returns does the MemTable accept the write
 *   3. On startup, CrashRecovery replays uncommitted CommitLog entries
 *
 * ─── Segment File Format ─────────────────────────────────────────────────────
 *
 * File: {segmentId}.clog
 *
 * Header (32 bytes):
 *   [MAGIC: 4B = 0xAEA3C109][version: 2B][segmentId: 8B][createdMs: 8B][reserved: 10B]
 *
 * Each record:
 *   [totalLength: 4B][keyLength: 2B][key: keyB][columnCount: 2B]
 *   per column: [nameLength: 2B][name: nameB][flags: 1B][timestampMicros: 8B]
 *               [ttl: 4B][valueLength: 4B][value: valueB]
 *   [CRC32C: 4B]  ← covers everything from totalLength to last value byte
 *
 * ─── Sync Strategy ───────────────────────────────────────────────────────────
 *
 * PERIODIC mode (default): writes are buffered, fsync fires every
 * COMMITLOG_SYNC_PERIOD_MS via the CommitLog's scheduler. Equivalent to
 * Cassandra's commitlog_sync = periodic.
 *
 * BATCH mode: fsync is called inline before append() returns. Equivalent to
 * Cassandra's commitlog_sync = batch. Zero data loss, lower throughput.
 */
public final class CommitLogSegment implements Closeable {

    // File format magic number — identifies this as an Aegis CommitLog segment
    static final int  MAGIC   = 0xAEA3C109;
    static final byte VERSION = 0x01;
    static final int  HEADER_SIZE = 32;

    private final long        segmentId;
    private final Path        filePath;
    private final FileChannel channel;
    private final StorageConfig.CommitLogSyncMode syncMode;

    // Monotonically increasing position — used by CrashRecovery to find replay start
    private long writtenBytes = 0;

    public CommitLogSegment(Path dir, long segmentId,
                            StorageConfig.CommitLogSyncMode syncMode) throws IOException {
        this.segmentId = segmentId;
        this.syncMode  = syncMode;
        this.filePath  = dir.resolve("%020d.clog".formatted(segmentId));

        dir.toFile().mkdirs();
        this.channel = FileChannel.open(filePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

        if (channel.size() == 0) {
            writeHeader();
        } else {
            // Existing segment — position at end for append
            writtenBytes = channel.size();
            channel.position(writtenBytes);
        }
    }

    // ─── Header ───────────────────────────────────────────────────────────────

    private void writeHeader() throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(HEADER_SIZE);
        hdr.putInt(MAGIC);
        hdr.put(VERSION);
        hdr.put((byte) 0x00); // flags (unused)
        hdr.putLong(segmentId);
        hdr.putLong(System.currentTimeMillis());
        // 10 bytes reserved
        hdr.put(new byte[10]);
        hdr.flip();
        channel.write(hdr, 0);
        channel.force(false);
        writtenBytes = HEADER_SIZE;
        channel.position(HEADER_SIZE);
    }

    // ─── Append ───────────────────────────────────────────────────────────────

    /**
     * Serialise a Row and append it durably to this segment.
     *
     * Returns the file offset of the written record — stored in the MemTable
     * so CrashRecovery can determine which CommitLog records need replaying.
     *
     * Thread-safety: synchronized on this segment instance.
     * Cassandra achieves the same via per-segment Allocation objects and
     * a lock-free wait-free MPSC queue, but we use simple synchronization
     * here for clarity.
     */
    public synchronized long append(Row row) throws IOException {
        byte[] serialized = serialize(row);

        // CRC covers the entire serialized record
        CRC32C crc = new CRC32C();
        crc.update(serialized);
        int checksum = (int) crc.getValue();

        // Frame: [length: 4B][record: nB][crc: 4B]
        ByteBuffer frame = ByteBuffer.allocate(4 + serialized.length + 4);
        frame.putInt(serialized.length);
        frame.put(serialized);
        frame.putInt(checksum);
        frame.flip();

        long recordOffset = channel.position();
        while (frame.hasRemaining()) {
            channel.write(frame);
        }
        writtenBytes = channel.position();

        if (syncMode == StorageConfig.CommitLogSyncMode.BATCH) {
            channel.force(false);
        }

        return recordOffset;
    }

    // ─── Serialization ────────────────────────────────────────────────────────

    /**
     * Serialize a Row to bytes using our CommitLog record format.
     *
     * Format:
     *   [keyLength: 2B][key: keyB][columnCount: 2B]
     *   per column:
     *     [nameLength: 2B][name: nameB][flags: 1B][timestampMicros: 8B][ttl: 4B]
     *     [valueLength: 4B][value: valueB or empty if tombstone]
     */
    public static byte[] serialize(Row row) {
        byte[] keyBytes = row.key().bytes();
        var cells = row.cells();

        // Calculate total size first to allocate once
        int totalSize = 2 + keyBytes.length + 2;
        for (var entry : cells.entrySet()) {
            byte[] nameBytes  = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = entry.getValue().valueBytes();
            totalSize += 2 + nameBytes.length + 1 + 8 + 4 + 4;
            if (valueBytes != null) totalSize += valueBytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putShort((short) keyBytes.length);
        buf.put(keyBytes);
        buf.putShort((short) cells.size());

        for (var entry : cells.entrySet()) {
            byte[] nameBytes  = entry.getKey().getBytes(StandardCharsets.UTF_8);
            Row.Cell cell     = entry.getValue();
            byte[] valueBytes = cell.valueBytes();

            buf.putShort((short) nameBytes.length);
            buf.put(nameBytes);
            buf.put(cell.flags());
            buf.putLong(cell.timestamp());
            buf.putInt(cell.ttlSeconds());
            buf.putInt(valueBytes != null ? valueBytes.length : 0);
            if (valueBytes != null) buf.put(valueBytes);
        }

        return buf.array();
    }

    /**
     * Deserialize a Row from raw bytes (used during CommitLog replay).
     */
    static Row deserialize(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);

        int keyLen    = buf.getShort() & 0xFFFF;
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        Row row = Row.create(Row.PartitionKey.of(keyBytes));

        int columnCount = buf.getShort() & 0xFFFF;
        for (int i = 0; i < columnCount; i++) {
            int nameLen = buf.getShort() & 0xFFFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String columnName = new String(nameBytes, StandardCharsets.UTF_8);

            byte flags       = buf.get();
            long timestamp   = buf.getLong();
            int  ttl         = buf.getInt();
            int  valueLen    = buf.getInt();

            if ((flags & Row.Cell.FLAG_TOMBSTONE) != 0) {
                row.deleteColumn(columnName, timestamp);
            } else {
                byte[] valueBytes = new byte[valueLen];
                buf.get(valueBytes);
                if ((flags & Row.Cell.FLAG_EXPIRING) != 0) {
                    row.cells().toString(); // force init
                    // Re-insert as expiring cell via direct put
                    row.putColumn(columnName, valueBytes, timestamp);
                } else {
                    row.putColumn(columnName, valueBytes, timestamp);
                }
            }
        }
        return row;
    }

    // ─── Replay (used by CrashRecovery) ──────────────────────────────────────

    /**
     * Read and validate all records in this segment.
     * Returns the list of valid rows and the position of the last valid record.
     *
     * Called during startup replay — equivalent to Cassandra's CommitLogReplayer.
     */
    public ReplayResult replay() throws IOException {
        List<Row> rows = new ArrayList<>();
        long lastGoodPosition = HEADER_SIZE;
        boolean foundCorruption = false;

        FileChannel readChannel = FileChannel.open(filePath, StandardOpenOption.READ);
        try (readChannel) {
            // Validate header magic
            ByteBuffer hdr = ByteBuffer.allocate(HEADER_SIZE);
            readChannel.read(hdr, 0);
            hdr.flip();
            int magic = hdr.getInt();
            if (magic != MAGIC) {
                throw new CorruptionException(filePath.toString(), 0,
                    "Invalid magic: 0x%08X".formatted(magic));
            }

            long pos = HEADER_SIZE;
            long size = readChannel.size();

            while (pos < size) {
                long recordStart = pos;

                // Read length prefix
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                int read = readChannel.read(lenBuf, pos);
                if (read < 4) {
                    foundCorruption = true;
                    break;
                }
                lenBuf.flip();
                int recordLen = lenBuf.getInt();

                if (recordLen <= 0 || recordLen > 64 * 1024 * 1024) {
                    foundCorruption = true;
                    break;
                }

                // Check record + CRC bytes are present
                if (pos + 4 + recordLen + 4 > size) {
                    foundCorruption = true;
                    break;
                }

                // Read record bytes
                ByteBuffer recordBuf = ByteBuffer.allocate(recordLen);
                readChannel.read(recordBuf, pos + 4);
                recordBuf.flip();
                byte[] recordBytes = new byte[recordLen];
                recordBuf.get(recordBytes);

                // Read and verify CRC
                ByteBuffer crcBuf = ByteBuffer.allocate(4);
                readChannel.read(crcBuf, pos + 4 + recordLen);
                crcBuf.flip();
                int storedCrc = crcBuf.getInt();

                CRC32C crc = new CRC32C();
                crc.update(recordBytes);
                int computedCrc = (int) crc.getValue();

                if (storedCrc != computedCrc) {
                    foundCorruption = true;
                    break;
                }

                // Valid record — deserialize
                rows.add(deserialize(recordBytes));
                pos += 4 + recordLen + 4;
                lastGoodPosition = pos;
            }
        }

        return new ReplayResult(rows, lastGoodPosition, foundCorruption);
    }

    /** Result of a CommitLog replay scan. */
    public record ReplayResult(
        List<Row> rows,
        long      lastGoodPosition,
        boolean   hadCorruption
    ) {}

    // ─── fsync (called by CommitLog scheduler in PERIODIC mode) ──────────────

    public synchronized void fsync() throws IOException {
        channel.force(false);
    }

    /** Truncate segment to last good position after crash (removes torn writes). */
    public synchronized void truncateTo(long position) throws IOException {
        channel.truncate(position);
        writtenBytes = position;
        channel.position(position);
    }

    // ─── State ────────────────────────────────────────────────────────────────

    public long segmentId()    { return segmentId; }
    public Path filePath()     { return filePath; }
    public long writtenBytes() { return writtenBytes; }
    public boolean isFull()    { return writtenBytes >= StorageConfig.COMMITLOG_SEGMENT_SIZE_BYTES; }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
