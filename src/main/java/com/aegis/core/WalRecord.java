package com.aegis.core;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record WalRecord(long offset, long timestamp, byte[] payload, int crc) {

    public static final int HEADER_SIZE = 4 + 8 + 8 + 4; // 24 bytes

    /** Serialize this record to a newly allocated ByteBuffer. */
    public ByteBuffer serialize() {
        int totalLen = HEADER_SIZE + payload.length;
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.putInt(payload.length);
        buf.putLong(offset);
        buf.putLong(timestamp);
        buf.putInt(crc);
        buf.put(payload);
        buf.flip();
        return buf;
    }

    /** Deserialize a record from a ByteBuffer positioned at the start of a frame. */
    public static WalRecord deserialize(ByteBuffer buf) {
        if (buf.remaining() < HEADER_SIZE) {
            throw new IllegalArgumentException("Buffer too small for WAL header");
        }
        int length = buf.getInt();
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        int storedCrc = buf.getInt();

        if (buf.remaining() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: expected %d payload bytes, got %d".formatted(length, buf.remaining())
            );
        }

        byte[] payload = new byte[length];
        buf.get(payload);

        int computedCrc = computeCrc(offset, timestamp, payload);
        if (computedCrc != storedCrc) {
            throw new CorruptRecordException(
                "CRC32C mismatch at offset %d: stored=0x%08X computed=0x%08X"
                    .formatted(offset, storedCrc, computedCrc)
            );
        }
        return new WalRecord(offset, timestamp, payload, storedCrc);
    }

    /** Factory: build a record from raw payload bytes (auto-computes CRC). */
    public static WalRecord of(long offset, byte[] payload) {
        long timestamp = System.currentTimeMillis();
        int crc = computeCrc(offset, timestamp, payload);
        return new WalRecord(offset, timestamp, payload, crc);
    }

    public static int computeCrc(long offset, long timestamp, byte[] payload) {
        CRC32C crc32c = new CRC32C();
        ByteBuffer tmp = ByteBuffer.allocate(8 + 8 + payload.length);
        tmp.putLong(offset);
        tmp.putLong(timestamp);
        tmp.put(payload);
        crc32c.update(tmp.array());
        return (int) crc32c.getValue();
    }

    public int totalSize() {
        return HEADER_SIZE + payload.length;
    }

    public String payloadAsString() {
        return new String(payload);
    }
}
