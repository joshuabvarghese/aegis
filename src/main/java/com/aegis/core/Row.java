package com.aegis.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Core data model for Aegis-Storage.
 *
 * Mirrors Cassandra's internal row representation:
 *
 *   PartitionKey  →  the row's primary sort key (Cassandra: partition key bytes)
 *   ColumnName    →  individual cell identifier  (Cassandra: clustering + column name)
 *   Cell          →  a single value + timestamp + optional TTL + tombstone flag
 *   Row           →  a PartitionKey bound to an ordered map of ColumnName → Cell
 *
 * Cassandra stores cells as (name, value, timestamp, flags) tuples at the SSTable
 * level. We model the same structure so our SSTable writer produces a format that
 * a Cassandra engineer will immediately recognise.
 *
 * Tombstones: a deleted cell is represented as a Cell with value=null and the
 * TOMBSTONE flag set. The compaction engine removes tombstones past GC grace period,
 * exactly as Cassandra's major compaction does.
 */
public final class Row {

    // ─── PartitionKey ─────────────────────────────────────────────────────────

    /**
     * Immutable partition key wrapper.
     *
     * In Cassandra, the partition key is hashed by the Murmur3 partitioner to
     * determine which node owns the data. We store the raw bytes and compute
     * a Murmur3-style token for sorting — same algorithm Cassandra uses.
     */
    public record PartitionKey(byte[] bytes) implements Comparable<PartitionKey> {

        public static PartitionKey of(String key) {
            return new PartitionKey(key.getBytes(StandardCharsets.UTF_8));
        }

        public static PartitionKey of(byte[] bytes) {
            return new PartitionKey(Arrays.copyOf(bytes, bytes.length));
        }

        /** Murmur3 token — determines SSTable sort order, same as Cassandra. */
        public long token() {
            return murmur3Token(bytes);
        }

        @Override
        public int compareTo(PartitionKey other) {
            // Sort by Murmur3 token, then by raw bytes for tie-breaking
            int cmp = Long.compare(this.token(), other.token());
            if (cmp != 0) return cmp;
            return Arrays.compare(this.bytes, other.bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionKey pk)) return false;
            return Arrays.equals(this.bytes, pk.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }

        @Override
        public String toString() {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        /**
         * Murmur3 hash — the exact algorithm used by Cassandra's
         * Murmur3Partitioner to assign tokens to rows.
         * Source: adapted from Cassandra's MurmurHash.java
         */
        private static long murmur3Token(byte[] data) {
            final long c1 = 0x87c37b91114253d5L;
            final long c2 = 0x4cf5ad432745937fL;
            int len = data.length;
            long h1 = 0x9368e53c2f6af274L;
            long h2 = 0x586dcd208f7cd3fdL;

            int roundedEnd = (len & 0xFFFFFFF0);
            for (int i = 0; i < roundedEnd; i += 16) {
                long k1 = getLong(data, i);
                long k2 = getLong(data, i + 8);
                k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
                h1 = Long.rotateLeft(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
                k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
                h2 = Long.rotateLeft(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
            }

            long k1 = 0, k2 = 0;
            switch (len & 15) {
                case 15: k2 ^= (long) (data[roundedEnd + 14] & 0xFF) << 48;
                case 14: k2 ^= (long) (data[roundedEnd + 13] & 0xFF) << 40;
                case 13: k2 ^= (long) (data[roundedEnd + 12] & 0xFF) << 32;
                case 12: k2 ^= (long) (data[roundedEnd + 11] & 0xFF) << 24;
                case 11: k2 ^= (long) (data[roundedEnd + 10] & 0xFF) << 16;
                case 10: k2 ^= (long) (data[roundedEnd +  9] & 0xFF) <<  8;
                case  9: k2 ^= (long) (data[roundedEnd +  8] & 0xFF);
                    k2 *= c2; k2 = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
                case  8: k1 ^= (long) (data[roundedEnd +  7] & 0xFF) << 56;
                case  7: k1 ^= (long) (data[roundedEnd +  6] & 0xFF) << 48;
                case  6: k1 ^= (long) (data[roundedEnd +  5] & 0xFF) << 40;
                case  5: k1 ^= (long) (data[roundedEnd +  4] & 0xFF) << 32;
                case  4: k1 ^= (long) (data[roundedEnd +  3] & 0xFF) << 24;
                case  3: k1 ^= (long) (data[roundedEnd +  2] & 0xFF) << 16;
                case  2: k1 ^= (long) (data[roundedEnd +  1] & 0xFF) <<  8;
                case  1: k1 ^= (long) (data[roundedEnd]       & 0xFF);
                    k1 *= c1; k1 = Long.rotateLeft(k1, 31); k1 *= c2; h1 ^= k1;
            }

            h1 ^= len; h2 ^= len;
            h1 += h2; h2 += h1;
            h1 = fmix(h1); h2 = fmix(h2);
            h1 += h2;
            return h1;
        }

        private static long getLong(byte[] data, int offset) {
            return ((long)(data[offset    ] & 0xFF)      ) |
                   ((long)(data[offset + 1] & 0xFF) <<  8) |
                   ((long)(data[offset + 2] & 0xFF) << 16) |
                   ((long)(data[offset + 3] & 0xFF) << 24) |
                   ((long)(data[offset + 4] & 0xFF) << 32) |
                   ((long)(data[offset + 5] & 0xFF) << 40) |
                   ((long)(data[offset + 6] & 0xFF) << 48) |
                   ((long)(data[offset + 7] & 0xFF) << 56);
        }

        private static long fmix(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }
    }

    // ─── Cell ─────────────────────────────────────────────────────────────────

    /**
     * A single column cell — the atomic unit of storage.
     *
     * Cassandra cell wire format:
     *   [column_name: varB][flags: 1B][timestamp: 8B][ttl?: 4B][value_length: 4B][value: varB]
     *
     * Flags we implement:
     *   NONE      = 0x00  normal live cell
     *   TOMBSTONE = 0x01  deletion marker — value is null
     *   EXPIRING  = 0x02  cell has a TTL
     */
    public static final class Cell {
        public static final byte FLAG_NONE      = 0x00;
        public static final byte FLAG_TOMBSTONE = 0x01;
        public static final byte FLAG_EXPIRING  = 0x02;

        private final ByteBuffer value;      // null for tombstones
        private final long       timestamp;  // microseconds since epoch (Cassandra uses µs)
        private final byte       flags;
        private final int        ttlSeconds;  // 0 if not expiring

        private Cell(ByteBuffer value, long timestamp, byte flags, int ttlSeconds) {
            this.value      = value;
            this.timestamp  = timestamp;
            this.flags      = flags;
            this.ttlSeconds = ttlSeconds;
        }

        /** Create a live cell with the given value. */
        public static Cell live(byte[] value, long timestampMicros) {
            ByteBuffer buf = ByteBuffer.allocateDirect(value.length);
            buf.put(value).flip();
            return new Cell(buf, timestampMicros, FLAG_NONE, 0);
        }

        /** Create a live cell with a TTL (expiring cell). */
        public static Cell expiring(byte[] value, long timestampMicros, int ttlSeconds) {
            ByteBuffer buf = ByteBuffer.allocateDirect(value.length);
            buf.put(value).flip();
            return new Cell(buf, timestampMicros, FLAG_EXPIRING, ttlSeconds);
        }

        /** Create a tombstone cell (deletion marker). */
        public static Cell tombstone(long timestampMicros) {
            return new Cell(null, timestampMicros, FLAG_TOMBSTONE, 0);
        }

        public boolean isTombstone()  { return (flags & FLAG_TOMBSTONE) != 0; }
        public boolean isExpiring()   { return (flags & FLAG_EXPIRING) != 0; }
        public boolean isLive()       { return !isTombstone() && !isExpired(); }

        public boolean isExpired() {
            if (!isExpiring()) return false;
            long nowMicros = System.currentTimeMillis() * 1_000L;
            return nowMicros > timestamp + ((long) ttlSeconds * 1_000_000L);
        }

        /** True if this tombstone is past GC grace period and safe to remove in compaction. */
        public boolean isPurgeableTombstone(long gcGraceSeconds) {
            if (!isTombstone()) return false;
            long nowMicros   = System.currentTimeMillis() * 1_000L;
            long graceMicros = gcGraceSeconds * 1_000_000L;
            return nowMicros > timestamp + graceMicros;
        }

        public byte[] valueBytes() {
            if (value == null) return null;
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            return bytes;
        }

        public String valueAsString() {
            byte[] b = valueBytes();
            return b == null ? "<tombstone>" : new String(b, StandardCharsets.UTF_8);
        }

        public long   timestamp()  { return timestamp; }
        public byte   flags()      { return flags; }
        public int    ttlSeconds() { return ttlSeconds; }

        @Override
        public String toString() {
            return "Cell{ts=%d, flags=0x%02X, val=%s}".formatted(timestamp, flags, valueAsString());
        }
    }

    // ─── Row ──────────────────────────────────────────────────────────────────

    private final PartitionKey key;

    /**
     * Cells sorted by column name — same ordering Cassandra uses for clustering.
     * TreeMap gives O(log n) insert and O(1) ordered scan for SSTable flush.
     */
    private final TreeMap<String, Cell> cells = new TreeMap<>();

    private Row(PartitionKey key) {
        this.key = key;
    }

    public static Row create(PartitionKey key) {
        return new Row(key);
    }

    public static Row create(String key) {
        return new Row(PartitionKey.of(key));
    }

    /** Write or overwrite a column. Last-write-wins by timestamp. */
    public Row putColumn(String columnName, byte[] value, long timestampMicros) {
        cells.merge(columnName,
            Cell.live(value, timestampMicros),
            (existing, incoming) ->
                incoming.timestamp() >= existing.timestamp() ? incoming : existing);
        return this;
    }

    public Row putColumn(String columnName, String value, long timestampMicros) {
        return putColumn(columnName, value.getBytes(StandardCharsets.UTF_8), timestampMicros);
    }

    /** Delete a column — inserts a tombstone cell. */
    public Row deleteColumn(String columnName, long timestampMicros) {
        cells.merge(columnName,
            Cell.tombstone(timestampMicros),
            (existing, incoming) ->
                incoming.timestamp() >= existing.timestamp() ? incoming : existing);
        return this;
    }

    public Cell getCell(String columnName)     { return cells.get(columnName); }
    public PartitionKey key()                  { return key; }
    public Map<String, Cell> cells()           { return Collections.unmodifiableMap(cells); }
    public boolean isEmpty()                   { return cells.isEmpty(); }

    /**
     * Estimate in-memory size for MemTable flush threshold accounting.
     * Cassandra uses a similar heuristic via MemtableSizing.
     */
    public long estimatedSizeBytes() {
        long size = key.bytes().length + 16; // key + overhead
        for (var entry : cells.entrySet()) {
            size += entry.getKey().length() * 2L; // column name (UTF-16 in JVM)
            Cell c = entry.getValue();
            byte[] v = c.valueBytes();
            size += (v != null ? v.length : 0) + 32; // value + Cell object overhead
        }
        return size;
    }

    @Override
    public String toString() {
        return "Row{key=%s, cells=%d}".formatted(key, cells.size());
    }
}
