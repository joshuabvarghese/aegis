package com.aegis.sstable;

import com.aegis.commitlog.CommitLog.CommitLogPosition;
import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.core.StorageException.FlushException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.zip.CRC32C;

/**
 * SSTable Writer — produces an immutable, sorted, on-disk SSTable from a MemTable flush.
 *
 * ─── Cassandra Parallel ───────────────────────────────────────────────────────
 *
 * A Cassandra SSTable is not a single file — it's a family of component files
 * that share a base name and generation number. We implement the same structure:
 *
 *   {generation}-Data.db      The actual row data, sorted by partition key token
 *   {generation}-Index.db     Sparse partition index: key → data file offset
 *   {generation}-Filter.db    Bloom filter for O(1) partition existence check
 *   {generation}-Statistics.db Metadata: row count, min/max key, CommitLog position
 *   {generation}-Summary.db   Sampled index summary for fast index binary search
 *
 * Cassandra's actual component names end in .db; we use the same extension for parity.
 *
 * ─── Data File Format ─────────────────────────────────────────────────────────
 *
 * The Data.db file stores rows in token-sorted order (ascending Murmur3 token).
 * Each partition is preceded by its length so readers can skip partitions without
 * deserializing their cells — same as Cassandra's partition header.
 *
 *   [MAGIC: 4B][generation: 8B][version: 2B]  ← file header
 *   per partition:
 *     [partitionLength: 4B]                    ← total bytes of this partition entry
 *     [keyLength: 2B][key: keyB]               ← partition key
 *     [cellCount: 2B]                          ← number of cells
 *     per cell:
 *       [nameLength: 2B][name: nameB]
 *       [flags: 1B][timestampMicros: 8B][ttl: 4B]
 *       [valueLength: 4B][value: valueB]
 *     [CRC32C: 4B]                             ← covers partition bytes
 *
 * ─── Index File Format ────────────────────────────────────────────────────────
 *
 * Sparse index — one entry every INDEX_SAMPLE_BYTES of data file bytes.
 * Cassandra's PartitionIndex uses the same sparse sampling strategy.
 *
 *   per index entry:
 *     [keyLength: 2B][key: keyB][dataFileOffset: 8B]
 *
 * ─── Immutability Guarantee ───────────────────────────────────────────────────
 *
 * Once write() returns, the SSTable is sealed — its files are never modified.
 * Compaction produces new SSTables; it never updates existing ones.
 * This immutability is fundamental to Cassandra's design: it enables safe
 * concurrent reads without locks, and makes crash recovery trivial
 * (a partially-written SSTable is detected by checking the Statistics file).
 */
public final class SSTableWriter implements Closeable {

    static final int  DATA_MAGIC   = 0xAE550001; // AE = Aegis, 55 = SSTable
    static final int  INDEX_MAGIC  = 0xAE550002;
    static final int  FILTER_MAGIC = 0xAE550003;
    static final byte VERSION      = 0x01;
    static final int  FILE_HEADER_SIZE = 4 + 8 + 1; // magic(4) + generation(8) + version(1) = 13 bytes

    private final long   generation;
    private final Path   baseDir;

    private final FileChannel dataChannel;
    private final FileChannel indexChannel;
    private final Path   dataPath;
    private final Path   indexPath;
    private final Path   filterPath;
    private final Path   statsPath;
    private final Path   summaryPath;

    // Bloom filter — built incrementally as partitions are written
    private BloomFilter bloomFilter;

    // Sparse index state
    private long bytesWrittenSinceLastIndexEntry = 0;
    private long indexFilePosition = FILE_HEADER_SIZE; // running write position within Index.db
    private long indexEntryCount   = 0;                // how many entries written to Index.db so far

    /** Every Nth Index.db entry is copied into the in-memory Summary. */
    static final int SUMMARY_SAMPLE_RATE = 128;

    // Summary index — sampled entries from the partition index.
    // NOTE: dataOffset() here stores the byte offset *within Index.db*, not
    // Data.db — the reader binary-searches this to find where to start
    // scanning Index.db, then Index.db entries themselves carry the Data.db
    // offset. Conflating these two was the cause of a nasty "key not found"
    // bug: mixing up which file an offset belongs to.
    private final List<IndexEntry> summaryEntries = new ArrayList<>();

    // Statistics gathered during write
    private long        partitionCount  = 0;
    private long        cellCount       = 0;
    private PartitionKey minKey         = null;
    private PartitionKey maxKey         = null;
    private CommitLogPosition commitLogPosition = CommitLogPosition.NONE;

    // Current data file write position
    private long dataFilePosition = FILE_HEADER_SIZE;

    public SSTableWriter(Path dir, long generation) throws IOException {
        this.generation = generation;
        this.baseDir    = dir;
        dir.toFile().mkdirs();

        this.dataPath    = dir.resolve(generation + "-Data.db");
        this.indexPath   = dir.resolve(generation + "-Index.db");
        this.filterPath  = dir.resolve(generation + "-Filter.db");
        this.statsPath   = dir.resolve(generation + "-Statistics.db");
        this.summaryPath = dir.resolve(generation + "-Summary.db");

        this.dataChannel  = openWrite(dataPath);
        this.indexChannel = openWrite(indexPath);

        writeDataHeader();
        writeIndexHeader();
    }

    // ─── Header Writing ───────────────────────────────────────────────────────

    private void writeDataHeader() throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);
        hdr.putInt(DATA_MAGIC);
        hdr.putLong(generation);
        hdr.put(VERSION);
        hdr.flip();
        dataChannel.write(hdr);
    }

    private void writeIndexHeader() throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(FILE_HEADER_SIZE);
        hdr.putInt(INDEX_MAGIC);
        hdr.putLong(generation);
        hdr.put(VERSION);
        hdr.flip();
        indexChannel.write(hdr);
    }

    // ─── Partition Writing ────────────────────────────────────────────────────

    /**
     * Write all partitions from a sorted NavigableMap to the SSTable.
     *
     * The map MUST be in token-sorted order (Murmur3 ascending) — which it is
     * when sourced from MemTable.all(), since MemTable uses ConcurrentSkipListMap
     * sorted by PartitionKey.compareTo() = token order.
     *
     * @param partitions   token-sorted map of key → row
     * @param expectedRows estimated row count for Bloom filter sizing
     * @param clPosition   CommitLog position of the newest write in this MemTable
     */
    public void writeAll(NavigableMap<PartitionKey, Row> partitions,
                         long expectedRows,
                         CommitLogPosition clPosition) throws IOException {
        this.bloomFilter       = new BloomFilter(Math.max(1, expectedRows));
        this.commitLogPosition = clPosition;

        for (var entry : partitions.entrySet()) {
            writePartition(entry.getKey(), entry.getValue());
        }
    }

    private void writePartition(PartitionKey key, Row row) throws IOException {
        byte[] partitionBytes = serializePartition(key, row);

        CRC32C crc = new CRC32C();
        crc.update(partitionBytes);
        int checksum = (int) crc.getValue();

        // Frame: [partitionLength: 4B][partitionBytes][CRC: 4B]
        ByteBuffer frame = ByteBuffer.allocate(4 + partitionBytes.length + 4);
        frame.putInt(partitionBytes.length);
        frame.put(partitionBytes);
        frame.putInt(checksum);
        frame.flip();

        // Write to data file at current position
        long partitionOffset = dataFilePosition;
        while (frame.hasRemaining()) {
            dataChannel.write(frame);
        }
        long frameSize = 4 + partitionBytes.length + 4;
        dataFilePosition += frameSize;

        // Add to Bloom filter
        bloomFilter.add(key);

        // Write sparse index entry if threshold exceeded
        bytesWrittenSinceLastIndexEntry += frameSize;
        if (bytesWrittenSinceLastIndexEntry >= StorageConfig.SSTABLE_INDEX_SAMPLE_BYTES
                || partitionCount == 0) {
            long indexOffset = writeIndexEntry(key, partitionOffset);
            bytesWrittenSinceLastIndexEntry = 0;

            // Summary: sample every SUMMARY_SAMPLE_RATE index entries.
            // Uses a running counter rather than summaryEntries.size() — checking
            // size() against itself after the list only grows when this same
            // condition is true meant it could only ever be true once (a
            // fixed point at size()==1), so no entry after the first one was
            // ever sampled.
            if (indexEntryCount % SUMMARY_SAMPLE_RATE == 0) {
                summaryEntries.add(new IndexEntry(key, indexOffset));
            }
            indexEntryCount++;
        }

        // Track statistics
        partitionCount++;
        cellCount += row.cells().size();
        if (minKey == null) minKey = key;
        maxKey = key;
    }

    private byte[] serializePartition(PartitionKey key, Row row) {
        byte[] keyBytes = key.bytes();
        var cells = row.cells();

        // Calculate size
        int size = 2 + keyBytes.length + 2;
        for (var entry : cells.entrySet()) {
            byte[] nameBytes  = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = entry.getValue().valueBytes();
            size += 2 + nameBytes.length + 1 + 8 + 4 + 4;
            if (valueBytes != null) size += valueBytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
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

    /** Writes one (key -> Data.db offset) entry to Index.db, returning this entry's own byte offset within Index.db. */
    private long writeIndexEntry(PartitionKey key, long dataOffset) throws IOException {
        long entryOffset = indexFilePosition;
        byte[] keyBytes = key.bytes();
        ByteBuffer entry = ByteBuffer.allocate(2 + keyBytes.length + 8);
        entry.putShort((short) keyBytes.length);
        entry.put(keyBytes);
        entry.putLong(dataOffset);
        entry.flip();
        indexChannel.write(entry);
        indexFilePosition += 2 + keyBytes.length + 8;
        return entryOffset;
    }

    // ─── Finalization ─────────────────────────────────────────────────────────

    /**
     * Seal the SSTable — write the Filter, Summary, and Statistics files.
     * After this returns, the SSTable is immutable and ready for reads.
     *
     * Returns the SSTableMetadata describing this SSTable for the catalog.
     */
    public SSTableMetadata finish() throws IOException {
        return finish(0);
    }

    /**
     * Seal the SSTable at a specific level. Level 0 for a fresh flush or STCS
     * output (unleveled); 1+ for LeveledCompactor output.
     */
    public SSTableMetadata finish(int level) throws IOException {
        // Force data and index files
        dataChannel.force(true);
        indexChannel.force(true);

        // Write Bloom filter
        writeFilterFile();

        // Write summary index
        writeSummaryFile();

        // Write statistics
        SSTableMetadata metadata = new SSTableMetadata(
            generation, baseDir, partitionCount, cellCount,
            minKey, maxKey, commitLogPosition,
            System.currentTimeMillis(), dataFilePosition,
            bloomFilter != null ? bloomFilter.memorySizeBytes() : 0,
            level
        );
        writeStatsFile(metadata);

        return metadata;
    }

    private void writeFilterFile() throws IOException {
        if (bloomFilter == null) bloomFilter = new BloomFilter(1);
        byte[] filterBytes = bloomFilter.serialize();

        ByteBuffer buf = ByteBuffer.allocate(4 + filterBytes.length);
        buf.putInt(FILTER_MAGIC);
        buf.put(filterBytes);
        buf.flip();

        try (FileChannel fc = openWrite(filterPath)) {
            fc.write(buf);
            fc.force(true);
        }
    }

    private void writeSummaryFile() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + summaryEntries.stream()
            .mapToInt(e -> 2 + e.key().bytes().length + 8).sum());
        buf.putInt(summaryEntries.size());
        for (IndexEntry entry : summaryEntries) {
            byte[] kb = entry.key().bytes();
            buf.putShort((short) kb.length);
            buf.put(kb);
            buf.putLong(entry.dataOffset());
        }
        buf.flip();

        try (FileChannel fc = openWrite(summaryPath)) {
            fc.write(buf);
            fc.force(true);
        }
    }

    private void writeStatsFile(SSTableMetadata meta) throws IOException {
        byte[] minKeyBytes = meta.minKey() != null ? meta.minKey().bytes() : new byte[0];
        byte[] maxKeyBytes = meta.maxKey() != null ? meta.maxKey().bytes() : new byte[0];

        ByteBuffer buf = ByteBuffer.allocate(
            8 + 8 + 8 + 8 + 8 + 8 + 2 + minKeyBytes.length + 2 + maxKeyBytes.length + 8 + 8 + 4
        );
        buf.putLong(meta.generation());
        buf.putLong(meta.partitionCount());
        buf.putLong(meta.cellCount());
        buf.putLong(meta.createdAtMs());
        buf.putLong(meta.dataSizeBytes());
        buf.putLong(meta.bloomFilterSizeBytes());
        buf.putShort((short) minKeyBytes.length);
        buf.put(minKeyBytes);
        buf.putShort((short) maxKeyBytes.length);
        buf.put(maxKeyBytes);
        buf.putLong(meta.commitLogPosition().segmentId());
        buf.putLong(meta.commitLogPosition().position());
        buf.putInt(meta.level());
        buf.flip();

        try (FileChannel fc = openWrite(statsPath)) {
            fc.write(buf);
            fc.force(true);
        }
    }

    // ─── Utility ──────────────────────────────────────────────────────────────

    private static FileChannel openWrite(Path path) throws IOException {
        return FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void close() throws IOException {
        dataChannel.close();
        indexChannel.close();
    }

    // ─── Value Types ──────────────────────────────────────────────────────────

    /** A single sparse index entry: key → data file offset. */
    record IndexEntry(PartitionKey key, long dataOffset) {}

    /**
     * Metadata describing a completed SSTable.
     * Stored in the Statistics.db file and cached in-memory by the SSTableManager.
     *
     * `level` is 0 for a fresh MemTable flush and for any STCS compaction output
     * (STCS is deliberately unleveled — see STCSCompactor). LeveledCompactor
     * assigns 1, 2, 3... and guarantees SSTables within the same level >= 1 never
     * overlap in key range, which is what lets a leveled read skip straight to
     * the one SSTable per level that could possibly contain a given key.
     */
    public record SSTableMetadata(
        long           generation,
        Path           baseDir,
        long           partitionCount,
        long           cellCount,
        PartitionKey   minKey,
        PartitionKey   maxKey,
        CommitLogPosition commitLogPosition,
        long           createdAtMs,
        long           dataSizeBytes,
        long           bloomFilterSizeBytes,
        int            level
    ) {
        public Path dataPath()    { return baseDir.resolve(generation + "-Data.db"); }
        public Path indexPath()   { return baseDir.resolve(generation + "-Index.db"); }
        public Path filterPath()  { return baseDir.resolve(generation + "-Filter.db"); }
        public Path statsPath()   { return baseDir.resolve(generation + "-Statistics.db"); }
        public Path summaryPath() { return baseDir.resolve(generation + "-Summary.db"); }

        public long ageSeconds() {
            return (System.currentTimeMillis() - createdAtMs) / 1_000;
        }

        /** True if this SSTable's [minKey, maxKey] range overlaps another's. Null keys are treated as "could contain anything" (conservative). */
        public boolean overlaps(SSTableMetadata other) {
            if (minKey == null || maxKey == null || other.minKey == null || other.maxKey == null) return true;
            return minKey.compareTo(other.maxKey) <= 0 && other.minKey.compareTo(maxKey) <= 0;
        }

        /** True if key falls within this SSTable's [minKey, maxKey] range. */
        public boolean mightContainKey(PartitionKey key) {
            if (minKey == null || maxKey == null) return true;
            return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
        }

        @Override
        public String toString() {
            return "SSTable{gen=%d, level=%d, partitions=%d, cells=%d, size=%dKB, age=%ds}"
                .formatted(generation, level, partitionCount, cellCount,
                    dataSizeBytes / 1024, ageSeconds());
        }
    }
}
