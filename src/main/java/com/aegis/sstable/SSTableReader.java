package com.aegis.sstable;

import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageException.CorruptionException;
import com.aegis.core.StorageException.ReadException;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.zip.CRC32C;

/**
 * SSTable Reader — the read path into a sealed, immutable SSTable.
 *
 * ─── Cassandra Read Path Parallel ────────────────────────────────────────────
 *
 * Cassandra's single-key lookup follows exactly this sequence:
 *
 *   1. Check Bloom filter (Filter.db)
 *      → miss: return empty immediately (zero disk I/O)
 *      → hit:  proceed to index lookup
 *
 *   2. Binary search the Summary index (Summary.db) to find the index file range
 *
 *   3. Sequential scan the Index.db region to find the data file offset
 *
 *   4. Seek to data file offset in Data.db, deserialize the partition
 *
 * We implement the same four-step sequence. The result is that most lookups
 * for non-existent keys cost zero disk reads (Bloom filter eliminates them).
 * Lookups for existing keys cost:
 *   - 1 sequential Index.db scan of at most INDEX_SAMPLE_BYTES
 *   - 1 random seek + sequential read of the partition in Data.db
 *
 * ─── Concurrency ─────────────────────────────────────────────────────────────
 *
 * SSTableReader is thread-safe for concurrent reads. FileChannel.read(buf, pos)
 * with explicit position is atomic and does not require external synchronization —
 * multiple threads can read different positions of the same FileChannel concurrently.
 * The Bloom filter and index are read-only after construction.
 */
public final class SSTableReader implements Closeable {

    private final SSTableMetadata metadata;
    private final FileChannel     dataChannel;
    private final FileChannel     indexChannel;
    private final BloomFilter     bloomFilter;
    private final List<IndexEntry> summaryIndex; // in-memory sampled index

    // Read statistics
    private long bloomFilterHits   = 0;
    private long bloomFilterMisses = 0;
    private long diskReads         = 0;

    public SSTableReader(SSTableMetadata metadata) throws IOException {
        this.metadata = metadata;

        this.dataChannel  = FileChannel.open(metadata.dataPath(),  StandardOpenOption.READ);
        this.indexChannel = FileChannel.open(metadata.indexPath(), StandardOpenOption.READ);

        // Load Bloom filter into memory — stays in heap for lifetime of this reader
        this.bloomFilter  = loadBloomFilter(metadata);

        // Load summary index into memory — small (sampled every 128 index entries)
        this.summaryIndex = loadSummaryIndex(metadata);
    }

    // ─── Single-Key Lookup ────────────────────────────────────────────────────

    /**
     * Look up a single partition key.
     *
     * Implements the full Cassandra four-step read path:
     *   Bloom filter → Summary → Index → Data
     *
     * @return Optional.empty() if key is not in this SSTable (definitive or probable)
     */
    public Optional<Row> get(PartitionKey key) throws IOException {
        // Step 1: Bloom filter check — O(k) hash computations, zero I/O
        if (!bloomFilter.mightContain(key)) {
            bloomFilterMisses++;
            return Optional.empty(); // DEFINITELY not here
        }
        bloomFilterHits++;

        // Step 2: Binary search the in-memory summary to find where in Index.db
        // we should start scanning.
        long indexSearchFrom = findIndexSearchStart(key);

        // Step 3: Scan Index.db from that point. Index.db is itself a *sparse*
        // index (one entry per SSTABLE_INDEX_SAMPLE_BYTES), so most keys won't
        // have an exact Index.db entry — we may only learn the Data.db offset
        // of the largest indexed key that is <= our target.
        IndexScanResult scan = findDataOffset(key, indexSearchFrom);

        long dataOffset;
        if (scan.exactDataOffset() >= 0) {
            // Fast path: the key itself was in Index.db.
            dataOffset = scan.exactDataOffset();
        } else if (scan.lowerBoundDataOffset() >= 0) {
            // Step 4a: fall back to a sequential scan of Data.db starting from
            // the last known indexed offset <= target, since the exact key may
            // sit between two sparse index samples.
            Optional<Row> fromScan = scanDataForKey(scan.lowerBoundDataOffset(), key);
            if (fromScan.isEmpty()) return Optional.empty();
            diskReads++;
            return fromScan;
        } else {
            return Optional.empty(); // false positive from Bloom filter
        }

        // Step 4b: exact index hit — seek straight to the offset.
        diskReads++;
        return Optional.ofNullable(readPartitionAt(dataOffset, key));
    }

    public Optional<Row> get(String key) throws IOException {
        return get(PartitionKey.of(key));
    }

    // ─── Step 2: Summary Binary Search ───────────────────────────────────────

    /**
     * Binary search the in-memory summary index to find where in the Index.db
     * file we should start scanning for the target key.
     *
     * Returns the byte offset in Index.db to start scanning from.
     */
    private long findIndexSearchStart(PartitionKey targetKey) {
        if (summaryIndex.isEmpty()) return SSTableWriter.FILE_HEADER_SIZE;

        int lo = 0, hi = summaryIndex.size() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) / 2;
            if (summaryIndex.get(mid).key().compareTo(targetKey) <= 0) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        // lo is the last summary entry with key <= targetKey
        // The index entry we want is somewhere after this summary entry
        return lo < summaryIndex.size() ? summaryIndex.get(lo).dataOffset() : SSTableWriter.FILE_HEADER_SIZE;
    }

    // ─── Step 3: Index File Scan ──────────────────────────────────────────────

    /**
     * Scan the Index.db file starting from indexFromOffset, looking for targetKey.
     *
     * The index file contains entries in token-sorted order, but it is itself
     * sparse (one entry per SSTABLE_INDEX_SAMPLE_BYTES of data), so most keys
     * will not have an exact entry here. We therefore track two things:
     *   - an exact match, if the key happens to be indexed, and
     *   - the Data.db offset of the last indexed key <= target, which is the
     *     safe starting point for a fallback sequential scan of Data.db.
     *
     * Index.db is small by construction (sparse), so we scan it in full rather
     * than capping at an arbitrary byte limit — capping there was itself a
     * source of false negatives for SSTables with more than one index entry.
     */
    private IndexScanResult findDataOffset(PartitionKey targetKey, long indexFromOffset) throws IOException {
        long indexSize = indexChannel.size();
        long pos       = indexFromOffset;
        long lowerBoundDataOffset = -1;

        while (pos < indexSize) {
            // Read key length
            ByteBuffer lenBuf = ByteBuffer.allocate(2);
            int read = indexChannel.read(lenBuf, pos);
            if (read < 2) break;
            lenBuf.flip();
            int keyLen = lenBuf.getShort() & 0xFFFF;
            if (keyLen <= 0 || keyLen > 65535) break;

            // Read key bytes
            ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
            indexChannel.read(keyBuf, pos + 2);
            keyBuf.flip();
            byte[] keyBytes = new byte[keyLen];
            keyBuf.get(keyBytes);
            PartitionKey indexedKey = PartitionKey.of(keyBytes);

            // Read data file offset
            ByteBuffer offsetBuf = ByteBuffer.allocate(8);
            indexChannel.read(offsetBuf, pos + 2 + keyLen);
            offsetBuf.flip();
            long dataOffset = offsetBuf.getLong();

            int entrySize = 2 + keyLen + 8;

            int cmp = indexedKey.compareTo(targetKey);
            if (cmp == 0) {
                return new IndexScanResult(dataOffset, dataOffset); // exact match
            } else if (cmp > 0) {
                break; // passed the target key in Index.db — fall back to Data.db scan
            }

            lowerBoundDataOffset = dataOffset;
            pos += entrySize;
        }
        return new IndexScanResult(-1, lowerBoundDataOffset);
    }

    /** Result of scanning Index.db for a key: an exact hit, and/or a lower-bound Data.db offset to fall back to. */
    private record IndexScanResult(long exactDataOffset, long lowerBoundDataOffset) {}

    // ─── Step 4a: Data File Fallback Scan ─────────────────────────────────────

    /**
     * Sequentially scan Data.db starting at fromDataOffset, comparing each
     * partition's key against targetKey, until an exact match is found or a
     * key greater than targetKey is seen (partitions are token-sorted, so
     * that means targetKey isn't in this SSTable).
     *
     * This is the step that makes the sparse index actually work: between two
     * indexed keys there can be many un-indexed partitions, and this is where
     * we find them.
     */
    private Optional<Row> scanDataForKey(long fromDataOffset, PartitionKey targetKey) throws IOException {
        long pos  = fromDataOffset;
        long size = dataChannel.size();

        while (pos < size) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (dataChannel.read(lenBuf, pos) < 4) break;
            lenBuf.flip();
            int partitionLen = lenBuf.getInt();
            if (partitionLen <= 0 || partitionLen > 64 * 1024 * 1024) break;

            // Peek just the key at the start of the partition payload, without
            // reading/deserializing the (possibly much larger) cell data yet.
            ByteBuffer keyLenBuf = ByteBuffer.allocate(2);
            dataChannel.read(keyLenBuf, pos + 4);
            keyLenBuf.flip();
            int keyLen = keyLenBuf.getShort() & 0xFFFF;

            ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
            dataChannel.read(keyBuf, pos + 4 + 2);
            keyBuf.flip();
            byte[] keyBytes = new byte[keyLen];
            keyBuf.get(keyBytes);
            PartitionKey candidateKey = PartitionKey.of(keyBytes);

            int cmp = candidateKey.compareTo(targetKey);
            if (cmp == 0) {
                Row row = readPartitionAt(pos, targetKey);
                return Optional.ofNullable(row);
            } else if (cmp > 0) {
                return Optional.empty(); // passed it — not in this SSTable
            }

            pos += 4 + partitionLen + 4; // advance past this partition's frame
        }
        return Optional.empty();
    }

    // ─── Step 4: Data File Read ───────────────────────────────────────────────

    /**
     * Read and deserialize the partition at the given data file offset.
     *
     * Verifies the CRC before returning. Returns null if CRC fails
     * (false positive from the Bloom filter — treat as not-found).
     */
    private Row readPartitionAt(long dataOffset, PartitionKey expectedKey) throws IOException {
        // Read partition length
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        int read = dataChannel.read(lenBuf, dataOffset);
        if (read < 4) return null;
        lenBuf.flip();
        int partitionLen = lenBuf.getInt();

        if (partitionLen <= 0 || partitionLen > 64 * 1024 * 1024) {
            throw new CorruptionException(metadata.dataPath().toString(), dataOffset,
                "Invalid partition length: " + partitionLen);
        }

        // Read partition bytes
        ByteBuffer partBuf = ByteBuffer.allocate(partitionLen);
        dataChannel.read(partBuf, dataOffset + 4);
        partBuf.flip();
        byte[] partBytes = new byte[partitionLen];
        partBuf.get(partBytes);

        // Read and verify CRC
        ByteBuffer crcBuf = ByteBuffer.allocate(4);
        dataChannel.read(crcBuf, dataOffset + 4 + partitionLen);
        crcBuf.flip();
        int storedCrc = crcBuf.getInt();

        CRC32C crc = new CRC32C();
        crc.update(partBytes);
        int computedCrc = (int) crc.getValue();

        if (storedCrc != computedCrc) {
            throw new CorruptionException(metadata.dataPath().toString(), dataOffset,
                "CRC mismatch: stored=0x%08X computed=0x%08X".formatted(storedCrc, computedCrc));
        }

        // Deserialize and verify the key matches what we were looking for
        Row row = deserializePartition(ByteBuffer.wrap(partBytes));
        if (!row.key().equals(expectedKey)) {
            // Data at this offset is for a different key — false positive from index
            return null;
        }
        return row;
    }

    private Row deserializePartition(ByteBuffer buf) {
        int keyLen = buf.getShort() & 0xFFFF;
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        Row row = Row.create(PartitionKey.of(keyBytes));

        int cellCount = buf.getShort() & 0xFFFF;
        for (int i = 0; i < cellCount; i++) {
            int nameLen = buf.getShort() & 0xFFFF;
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String columnName = new String(nameBytes, StandardCharsets.UTF_8);

            byte flags     = buf.get();
            long timestamp = buf.getLong();
            int  ttl       = buf.getInt();
            int  valueLen  = buf.getInt();

            if ((flags & Row.Cell.FLAG_TOMBSTONE) != 0) {
                row.deleteColumn(columnName, timestamp);
            } else {
                byte[] valueBytes = new byte[valueLen];
                buf.get(valueBytes);
                row.putColumn(columnName, valueBytes, timestamp);
            }
        }
        return row;
    }

    // ─── Full Scan (used by Compaction) ───────────────────────────────────────

    /**
     * Iterate all partitions in token-sorted order.
     * Called by the compaction engine to merge multiple SSTables.
     */
    public List<Row> scanAll() throws IOException {
        List<Row> rows = new ArrayList<>();
        long pos  = SSTableWriter.FILE_HEADER_SIZE;
        long size = dataChannel.size();

        while (pos < size) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (dataChannel.read(lenBuf, pos) < 4) break;
            lenBuf.flip();
            int partitionLen = lenBuf.getInt();

            if (partitionLen <= 0 || partitionLen > 64 * 1024 * 1024) break;

            ByteBuffer partBuf = ByteBuffer.allocate(partitionLen);
            dataChannel.read(partBuf, pos + 4);
            partBuf.flip();
            byte[] partBytes = new byte[partitionLen];
            partBuf.get(partBytes);

            // Verify CRC
            ByteBuffer crcBuf = ByteBuffer.allocate(4);
            dataChannel.read(crcBuf, pos + 4 + partitionLen);
            crcBuf.flip();
            int storedCrc   = crcBuf.getInt();
            CRC32C crc      = new CRC32C();
            crc.update(partBytes);
            if ((int) crc.getValue() != storedCrc) {
                throw new CorruptionException(metadata.dataPath().toString(), pos,
                    "CRC mismatch during full scan");
            }

            rows.add(deserializePartition(ByteBuffer.wrap(partBytes)));
            pos += 4 + partitionLen + 4;
        }
        return rows;
    }

    // ─── Filter and Index Loading ─────────────────────────────────────────────

    private static BloomFilter loadBloomFilter(SSTableMetadata meta) throws IOException {
        if (!meta.filterPath().toFile().exists()) {
            return new BloomFilter(1); // empty filter — will pass everything
        }
        try (FileChannel fc = FileChannel.open(meta.filterPath(), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate((int) fc.size());
            fc.read(buf, 0);
            buf.flip();
            buf.getInt(); // skip magic
            byte[] filterBytes = new byte[buf.remaining()];
            buf.get(filterBytes);
            return BloomFilter.deserialize(filterBytes);
        }
    }

    private static List<IndexEntry> loadSummaryIndex(SSTableMetadata meta) throws IOException {
        List<IndexEntry> entries = new ArrayList<>();
        if (!meta.summaryPath().toFile().exists()) return entries;

        try (FileChannel fc = FileChannel.open(meta.summaryPath(), StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate((int) fc.size());
            fc.read(buf, 0);
            buf.flip();
            int count = buf.getInt();
            for (int i = 0; i < count && buf.hasRemaining(); i++) {
                int keyLen = buf.getShort() & 0xFFFF;
                byte[] keyBytes = new byte[keyLen];
                buf.get(keyBytes);
                long offset = buf.getLong();
                entries.add(new IndexEntry(PartitionKey.of(keyBytes), offset));
            }
        }
        return entries;
    }

    // ─── State ────────────────────────────────────────────────────────────────

    public SSTableMetadata metadata()       { return metadata; }
    public long generation()                { return metadata.generation(); }
    public long bloomFilterHits()           { return bloomFilterHits; }
    public long bloomFilterMisses()         { return bloomFilterMisses; }
    public long diskReads()                 { return diskReads; }
    public BloomFilter bloomFilter()        { return bloomFilter; }

    @Override
    public void close() throws IOException {
        dataChannel.close();
        indexChannel.close();
    }

    @Override
    public String toString() {
        return "SSTableReader{gen=%d, partitions=%d, bloomHits=%d, bloomMisses=%d, diskReads=%d}"
            .formatted(metadata.generation(), metadata.partitionCount(),
                bloomFilterHits, bloomFilterMisses, diskReads);
    }

    record IndexEntry(PartitionKey key, long dataOffset) {}
}
