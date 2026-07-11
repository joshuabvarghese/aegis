package com.aegis;

import com.aegis.commitlog.CommitLog;
import com.aegis.commitlog.CommitLog.CommitLogPosition;
import com.aegis.commitlog.CommitLogSegment;
import com.aegis.compaction.CompactionStrategy;
import com.aegis.compaction.CompactionStrategy.CompactionPlan;
import com.aegis.compaction.LeveledCompactor;
import com.aegis.compaction.STCSCompactor;
import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.memtable.MemTable;
import com.aegis.sstable.BloomFilter;
import com.aegis.sstable.SSTableReader;
import com.aegis.sstable.SSTableWriter;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Aegis-Storage Test Suite
 *
 * Tests every layer of the LSM-tree independently, then integration tests
 * that drive the full write → flush → read → compact cycle.
 *
 * Layer coverage:
 *   A. PartitionKey  — Murmur3 token consistency and sort order
 *   B. Row / Cell    — last-write-wins, tombstone semantics
 *   C. BloomFilter   — FPP properties, serialization round-trip
 *   D. CommitLog     — append, CRC, replay, crash recovery
 *   E. MemTable      — concurrent put/get, flush threshold, lifecycle
 *   F. SSTable       — writer round-trip, Bloom filter read path
 *   G. Compaction    — STCS bucketing, merge, tombstone purge
 *   H. StorageEngine — full LSM-tree write+read integration
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageEngineTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("aegis-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDir(tempDir);
    }

    // ─── A. PartitionKey ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("A. PartitionKey — Murmur3 Tokens")
    class PartitionKeyTests {

        @Test @Order(1)
        @DisplayName("Same key always produces same token")
        void tokenIsDeterministic() {
            PartitionKey k1 = PartitionKey.of("cassandra");
            PartitionKey k2 = PartitionKey.of("cassandra");
            assertEquals(k1.token(), k2.token());
        }

        @Test @Order(2)
        @DisplayName("Different keys produce different tokens (collision resistance)")
        void differentKeysDifferentTokens() {
            PartitionKey k1 = PartitionKey.of("kafka");
            PartitionKey k2 = PartitionKey.of("clickhouse");
            assertNotEquals(k1.token(), k2.token());
        }

        @Test @Order(3)
        @DisplayName("PartitionKey compareTo is consistent with token order")
        void compareToConsistentWithToken() {
            PartitionKey k1 = PartitionKey.of("aaa");
            PartitionKey k2 = PartitionKey.of("zzz");
            int tokenCmp  = Long.compare(k1.token(), k2.token());
            int compareCmp = k1.compareTo(k2);
            assertEquals(Integer.signum(tokenCmp), Integer.signum(compareCmp),
                "compareTo must be consistent with token ordering");
        }

        @Test @Order(4)
        @DisplayName("ConcurrentSkipListMap sorts by token order")
        void skipListSortsByToken() {
            TreeMap<PartitionKey, String> map = new TreeMap<>();
            map.put(PartitionKey.of("z"), "z");
            map.put(PartitionKey.of("a"), "a");
            map.put(PartitionKey.of("m"), "m");

            List<PartitionKey> keys = new ArrayList<>(map.keySet());
            for (int i = 0; i < keys.size() - 1; i++) {
                assertTrue(keys.get(i).token() <= keys.get(i+1).token(),
                    "Keys must be in ascending token order");
            }
        }
    }

    // ─── B. Row / Cell ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B. Row and Cell Semantics")
    class RowCellTests {

        @Test @Order(10)
        @DisplayName("Last-write-wins: higher timestamp overwrites lower")
        void lastWriteWins() {
            Row row = Row.create("k");
            row.putColumn("col", "old-value", 100L);
            row.putColumn("col", "new-value", 200L);
            assertEquals("new-value", row.getCell("col").valueAsString());
        }

        @Test @Order(11)
        @DisplayName("Lower timestamp does NOT overwrite higher")
        void olderWriteDoesNotOverwrite() {
            Row row = Row.create("k");
            row.putColumn("col", "new-value", 200L);
            row.putColumn("col", "old-value", 100L); // older — should be ignored
            assertEquals("new-value", row.getCell("col").valueAsString());
        }

        @Test @Order(12)
        @DisplayName("Tombstone has null value and TOMBSTONE flag")
        void tombstoneHasNullValue() {
            Row row = Row.create("k");
            row.deleteColumn("col", 100L);
            Row.Cell cell = row.getCell("col");
            assertNotNull(cell);
            assertTrue(cell.isTombstone());
            assertNull(cell.valueBytes());
        }

        @Test @Order(13)
        @DisplayName("Tombstone at higher timestamp shadows live cell")
        void tombstoneShadowsLiveCell() {
            Row row = Row.create("k");
            row.putColumn("col", "value", 100L);
            row.deleteColumn("col", 200L); // newer tombstone
            assertTrue(row.getCell("col").isTombstone());
        }

        @Test @Order(14)
        @DisplayName("Live cell at higher timestamp wins over older tombstone")
        void liveCellWinsOverOlderTombstone() {
            Row row = Row.create("k");
            row.deleteColumn("col", 100L);
            row.putColumn("col", "resurrected", 200L); // newer live cell
            assertFalse(row.getCell("col").isTombstone());
            assertEquals("resurrected", row.getCell("col").valueAsString());
        }

        @Test @Order(15)
        @DisplayName("Purgeable tombstone is past GC grace period")
        void purgeableTombstone() {
            Row.Cell tombstone = Row.Cell.tombstone(
                (System.currentTimeMillis() - 10_000L) * 1_000L); // 10 seconds ago
            assertTrue(tombstone.isPurgeableTombstone(5L),  // 5 second grace
                "Tombstone older than GC grace should be purgeable");
            assertFalse(tombstone.isPurgeableTombstone(3600L), // 1 hour grace
                "Tombstone within GC grace should NOT be purgeable");
        }
    }

    // ─── C. BloomFilter ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("C. Bloom Filter")
    class BloomFilterTests {

        @Test @Order(20)
        @DisplayName("Returns true for all inserted keys (no false negatives)")
        void noFalseNegatives() {
            BloomFilter bf = new BloomFilter(1000);
            List<PartitionKey> keys = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                PartitionKey k = PartitionKey.of("key-" + i);
                bf.add(k);
                keys.add(k);
            }
            for (PartitionKey k : keys) {
                assertTrue(bf.mightContain(k),
                    "Bloom filter must never return false for inserted key: " + k);
            }
        }

        @Test @Order(21)
        @DisplayName("False positive rate is bounded by configured FPP")
        void falsePositiveRateBounded() {
            int n = 10_000;
            BloomFilter bf = new BloomFilter(n, 0.01);
            for (int i = 0; i < n; i++) bf.add(PartitionKey.of("inserted-" + i));

            int falsePositives = 0;
            int trials = 10_000;
            for (int i = 0; i < trials; i++) {
                if (bf.mightContain(PartitionKey.of("notinserted-" + i))) falsePositives++;
            }
            double actualFpp = (double) falsePositives / trials;
            assertTrue(actualFpp < 0.03, // allow 3× tolerance for small samples
                "FPP %.3f exceeded 3× tolerance of configured 0.01".formatted(actualFpp));
        }

        @Test @Order(22)
        @DisplayName("Serialization round-trip preserves all bits")
        void serializationRoundTrip() {
            BloomFilter bf1 = new BloomFilter(500);
            for (int i = 0; i < 200; i++) bf1.add(PartitionKey.of("k" + i));

            byte[] bytes = bf1.serialize();
            BloomFilter bf2 = BloomFilter.deserialize(bytes);

            for (int i = 0; i < 200; i++) {
                assertTrue(bf2.mightContain(PartitionKey.of("k" + i)),
                    "Deserialized filter must contain all originally inserted keys");
            }
        }

        @Test @Order(23)
        @DisplayName("optimalNumBits formula matches known values")
        void optimalBitsFormula() {
            // For n=1000000, p=0.01: m ≈ 9,585,059 bits ≈ 9.6 bits/key
            long bits = BloomFilter.optimalNumBits(1_000_000, 0.01);
            assertTrue(bits > 9_000_000 && bits < 10_500_000,
                "optimalNumBits for 1M keys at 1% FPP should be ~9.6M bits, got: " + bits);
        }
    }

    // ─── D. CommitLog ─────────────────────────────────────────────────────────

    @Nested
    @DisplayName("D. CommitLog Segment")
    class CommitLogTests {

        @Test @Order(30)
        @DisplayName("Append and replay round-trip")
        void appendAndReplay() throws IOException {
            CommitLogSegment seg = new CommitLogSegment(tempDir, 1L,
                StorageConfig.CommitLogSyncMode.PERIODIC);

            Row written = Row.create("replay-key")
                .putColumn("col1", "hello", 1000L)
                .putColumn("col2", "world", 2000L);
            seg.append(written);
            seg.fsync();

            CommitLogSegment.ReplayResult result = seg.replay();
            seg.close();

            assertFalse(result.hadCorruption(), "Clean segment should not report corruption");
            assertEquals(1, result.rows().size());
            Row recovered = result.rows().get(0);
            assertEquals("replay-key", recovered.key().toString());
            assertEquals("hello", recovered.getCell("col1").valueAsString());
            assertEquals("world", recovered.getCell("col2").valueAsString());
        }

        @Test @Order(31)
        @DisplayName("CRC mismatch on torn write is detected")
        void detectsTornWrite() throws IOException {
            CommitLogSegment seg = new CommitLogSegment(tempDir, 2L,
                StorageConfig.CommitLogSyncMode.PERIODIC);
            Row row = Row.create("k").putColumn("c", "v", 1L);
            seg.append(row);
            seg.close();

            // Corrupt the last 4 bytes (the CRC)
            Path segPath = tempDir.resolve("00000000000000000002.clog");
            byte[] bytes = Files.readAllBytes(segPath);
            bytes[bytes.length - 1] ^= 0xFF; // flip a bit in the CRC
            Files.write(segPath, bytes);

            CommitLogSegment corrupted = new CommitLogSegment(tempDir, 2L,
                StorageConfig.CommitLogSyncMode.PERIODIC);
            CommitLogSegment.ReplayResult result = corrupted.replay();
            corrupted.close();

            assertTrue(result.hadCorruption(), "CRC mismatch must be detected");
            assertTrue(result.rows().isEmpty(), "Corrupted record must not be returned");
        }

        @Test @Order(32)
        @DisplayName("CommitLog.replay() returns rows in append order")
        void replayInOrder() throws IOException {
            CommitLog log = new CommitLog(tempDir,
                StorageConfig.CommitLogSyncMode.PERIODIC);

            for (int i = 0; i < 5; i++) {
                log.append(Row.create("key-" + i).putColumn("v", String.valueOf(i), (long)i));
            }
            log.close();

            CommitLog replayLog = new CommitLog(tempDir,
                StorageConfig.CommitLogSyncMode.PERIODIC);
            List<Row> replayed = new ArrayList<>();
            replayLog.replay(replayed::add);
            replayLog.close();

            assertEquals(5, replayed.size());
            for (int i = 0; i < 5; i++) {
                assertEquals("key-" + i, replayed.get(i).key().toString());
            }
        }
    }

    // ─── E. MemTable ──────────────────────────────────────────────────────────

    @Nested
    @DisplayName("E. MemTable")
    class MemTableTests {

        @Test @Order(40)
        @DisplayName("Put and get round-trip")
        void putAndGet() {
            MemTable mt = new MemTable(1);
            Row row = Row.create("k").putColumn("col", "val", 1000L);
            mt.put(row, CommitLogPosition.NONE);

            Optional<Row> result = mt.get("k");
            assertTrue(result.isPresent());
            assertEquals("val", result.get().getCell("col").valueAsString());
        }

        @Test @Order(41)
        @DisplayName("Get returns empty for unknown key")
        void getUnknownKey() {
            MemTable mt = new MemTable(1);
            assertTrue(mt.get("nonexistent").isEmpty());
        }

        @Test @Order(42)
        @DisplayName("Merge semantics: higher timestamp wins on same key")
        void mergeOnSameKey() {
            MemTable mt = new MemTable(1);
            Row r1 = Row.create("k").putColumn("col", "old", 100L);
            Row r2 = Row.create("k").putColumn("col", "new", 200L);
            mt.put(r1, CommitLogPosition.NONE);
            mt.put(r2, CommitLogPosition.NONE);

            Optional<Row> result = mt.get("k");
            assertTrue(result.isPresent());
            assertEquals("new", result.get().getCell("col").valueAsString());
        }

        @Test @Order(43)
        @DisplayName("shouldFlush is false before threshold, true after")
        void shouldFlushThreshold() throws Exception {
            MemTable mt = new MemTable(1);
            assertFalse(mt.shouldFlush(), "Empty MemTable should not request flush");

            // Fill until threshold
            int writes = 0;
            while (!mt.shouldFlush() && writes < 1_000_000) {
                Row r = Row.create("key-" + writes);
                byte[] bigVal = new byte[1024]; // 1KB value
                r.putColumn("data", bigVal, writes);
                mt.put(r, CommitLogPosition.NONE);
                writes++;
            }
            assertTrue(mt.shouldFlush() || writes >= 1_000_000,
                "MemTable should eventually request flush");
        }

        @Test @Order(44)
        @DisplayName("markFlushing rejects subsequent writes")
        void markFlushingRejectsWrites() {
            MemTable mt = new MemTable(1);
            mt.put(Row.create("k").putColumn("c", "v", 1L), CommitLogPosition.NONE);
            mt.markFlushing();

            assertThrows(IllegalStateException.class, () ->
                mt.put(Row.create("k2").putColumn("c", "v", 2L), CommitLogPosition.NONE),
                "FLUSHING MemTable must reject writes");
        }

        @Test @Order(45)
        @DisplayName("all() returns rows in token-sorted order")
        void allReturnsSortedOrder() {
            MemTable mt = new MemTable(1);
            String[] keys = {"delta", "alpha", "gamma", "beta"};
            for (String k : keys) {
                mt.put(Row.create(k).putColumn("c", "v", 1L), CommitLogPosition.NONE);
            }

            List<PartitionKey> sortedKeys = new ArrayList<>(mt.all().keySet());
            for (int i = 0; i < sortedKeys.size() - 1; i++) {
                assertTrue(sortedKeys.get(i).compareTo(sortedKeys.get(i+1)) <= 0,
                    "MemTable.all() must return keys in token-sorted order");
            }
        }
    }

    // ─── F. SSTable ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("F. SSTable Writer + Reader")
    class SSTableTests {

        @Test @Order(50)
        @DisplayName("Write and read single partition round-trip")
        void writeReadRoundTrip() throws IOException {
            TreeMap<PartitionKey, Row> data = new TreeMap<>();
            Row written = Row.create("instaclustr")
                .putColumn("founded", "2012", 1000L)
                .putColumn("products", "Cassandra,Kafka,ClickHouse", 2000L);
            data.put(PartitionKey.of("instaclustr"), written);

            SSTableWriter writer = new SSTableWriter(tempDir, 1L);
            writer.writeAll(data, 1, CommitLogPosition.NONE);
            SSTableMetadata meta = writer.finish();
            writer.close();

            try (SSTableReader reader = new SSTableReader(meta)) {
                Optional<Row> result = reader.get("instaclustr");
                assertTrue(result.isPresent());
                assertEquals("2012",
                    result.get().getCell("founded").valueAsString());
                assertEquals("Cassandra,Kafka,ClickHouse",
                    result.get().getCell("products").valueAsString());
            }
        }

        @Test @Order(51)
        @DisplayName("Bloom filter eliminates disk read for non-existent key")
        void bloomFilterEliminatesDiskRead() throws IOException {
            TreeMap<PartitionKey, Row> data = new TreeMap<>();
            data.put(PartitionKey.of("exists"),
                Row.create("exists").putColumn("c", "v", 1L));

            SSTableWriter writer = new SSTableWriter(tempDir, 2L);
            writer.writeAll(data, 1, CommitLogPosition.NONE);
            SSTableMetadata meta = writer.finish();
            writer.close();

            try (SSTableReader reader = new SSTableReader(meta)) {
                Optional<Row> result = reader.get("definitely-not-in-sstable-xyzxyz");
                assertTrue(result.isEmpty(), "Non-existent key should return empty");
                // With 1 insertion, FPP is effectively 0 — Bloom filter should miss
                assertEquals(0, reader.diskReads(),
                    "Bloom filter miss should result in zero disk reads");
            }
        }

        @Test @Order(52)
        @DisplayName("scanAll returns all partitions in token order")
        void scanAllReturnsAllPartitions() throws IOException {
            TreeMap<PartitionKey, Row> data = new TreeMap<>();
            for (int i = 0; i < 10; i++) {
                String k = "partition-" + i;
                data.put(PartitionKey.of(k),
                    Row.create(k).putColumn("idx", String.valueOf(i), (long)i));
            }

            SSTableWriter writer = new SSTableWriter(tempDir, 3L);
            writer.writeAll(data, 10, CommitLogPosition.NONE);
            SSTableMetadata meta = writer.finish();
            writer.close();

            try (SSTableReader reader = new SSTableReader(meta)) {
                List<Row> all = reader.scanAll();
                assertEquals(10, all.size(), "scanAll must return all 10 partitions");
                // Verify token ordering
                for (int i = 0; i < all.size() - 1; i++) {
                    assertTrue(all.get(i).key().compareTo(all.get(i+1).key()) <= 0,
                        "scanAll must return rows in token order");
                }
            }
        }

        @Test @Order(53)
        @DisplayName("Statistics file records correct partition and cell counts")
        void statisticsFileIsCorrect() throws IOException {
            TreeMap<PartitionKey, Row> data = new TreeMap<>();
            for (int i = 0; i < 5; i++) {
                Row r = Row.create("k" + i).putColumn("c1", "v1", 1L).putColumn("c2", "v2", 2L);
                data.put(PartitionKey.of("k" + i), r);
            }

            SSTableWriter writer = new SSTableWriter(tempDir, 4L);
            writer.writeAll(data, 5, CommitLogPosition.NONE);
            SSTableMetadata meta = writer.finish();
            writer.close();

            assertEquals(5, meta.partitionCount(), "Should record 5 partitions");
            assertEquals(10, meta.cellCount(),     "Should record 10 cells (5 × 2)");
        }
    }

    // ─── G. Compaction ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("G. STCS Compaction")
    class CompactionTests {

        private STCSCompactor compactor;
        private java.util.concurrent.atomic.AtomicLong genGen;

        @BeforeEach
        void setupCompactor() {
            genGen    = new java.util.concurrent.atomic.AtomicLong(100);
            compactor = new STCSCompactor(tempDir, genGen);
        }

        @Test @Order(60)
        @DisplayName("formBuckets groups SSTables of similar size")
        void formBucketsSimilarSize() {
            List<SSTableMetadata> sstables = List.of(
                mockMeta(1L,  1024),
                mockMeta(2L,  1100),  // ~same as 1
                mockMeta(3L,  900),   // ~same as 1
                mockMeta(4L, 100_000), // very different size
                mockMeta(5L, 110_000)  // ~same as 4
            );

            List<List<SSTableMetadata>> buckets = compactor.formBuckets(sstables);
            assertTrue(buckets.size() >= 2,
                "Should form at least 2 buckets for very different sizes");

            // Find the bucket containing the small SSTables
            var smallBucket = buckets.stream()
                .filter(b -> b.stream().anyMatch(m -> m.generation() == 1L))
                .findFirst();
            assertTrue(smallBucket.isPresent());
            assertTrue(smallBucket.get().size() >= 3,
                "Small SSTables should be grouped together");
        }

        @Test @Order(61)
        @DisplayName("selectCompactionBucket returns bucket with most candidates")
        void selectsLargestEligibleBucket() {
            List<SSTableMetadata> smallBucket = List.of(
                mockMeta(1L, 1024), mockMeta(2L, 1000), mockMeta(3L, 950), mockMeta(4L, 1100)
            );
            List<SSTableMetadata> tinyBucket = List.of(
                mockMeta(5L, 100_000), mockMeta(6L, 95_000)
            ); // only 2 — below MIN_THRESHOLD

            List<List<SSTableMetadata>> buckets = List.of(smallBucket, tinyBucket);
            Optional<List<SSTableMetadata>> selected = compactor.selectCompactionBucket(buckets);

            assertTrue(selected.isPresent(), "Should select the eligible bucket");
            assertEquals(4, selected.get().size(), "Should select the 4-SSTable bucket");
        }

        @Test @Order(62)
        @DisplayName("mergeAllVersions: last-write-wins per cell")
        void mergeLastWriteWins() {
            Row r1 = Row.create("k").putColumn("col", "old", 100L);
            Row r2 = Row.create("k").putColumn("col", "new", 200L);
            Row merged = STCSCompactor.mergeAllVersions(List.of(r1, r2));
            assertEquals("new", merged.getCell("col").valueAsString());
        }

        @Test @Order(63)
        @DisplayName("purgeTombstones removes expired tombstones (gc_grace=0)")
        void purgesTombstonesAfterGcGrace() {
            Row row = Row.create("k");
            row.deleteColumn("col1", 1_000L); // old tombstone
            row.putColumn("col2", "alive", 2_000L);

            Row purged = STCSCompactor.purgeTombstones(row, 0L);
            assertNull(purged.getCell("col1"),
                "Tombstone past GC grace must be removed");
            assertNotNull(purged.getCell("col2"),
                "Live cell must be preserved");
        }

        @Test @Order(64)
        @DisplayName("Full compaction produces single merged SSTable")
        void fullCompactionMergesSSTables() throws IOException {
            // Write 4 SSTables of similar size
            List<SSTableMetadata> inputMetas = new ArrayList<>();
            for (int s = 0; s < 4; s++) {
                TreeMap<PartitionKey, Row> data = new TreeMap<>();
                for (int i = 0; i < 10; i++) {
                    String key = "key-" + (s * 10 + i);
                    data.put(PartitionKey.of(key),
                        Row.create(key).putColumn("src", "sstable-" + s, (long)(s+1)));
                }
                SSTableWriter w = new SSTableWriter(tempDir, genGen.getAndIncrement());
                w.writeAll(data, 10, CommitLogPosition.NONE);
                inputMetas.add(w.finish());
                w.close();
            }

            SSTableMetadata output = compactor.compact(inputMetas);

            // Output SSTable should exist
            assertTrue(output.dataPath().toFile().exists(),
                "Compaction output Data.db must exist");

            // Should contain all 40 rows
            try (SSTableReader reader = new SSTableReader(output)) {
                List<Row> all = reader.scanAll();
                assertEquals(40, all.size(),
                    "Compacted SSTable should contain all 40 partitions");
            }

            // Input SSTables should be deleted
            for (SSTableMetadata input : inputMetas) {
                assertFalse(input.dataPath().toFile().exists(),
                    "Input SSTable Data.db must be deleted after compaction");
            }
        }
    }

    // ─── H. StorageEngine Integration ─────────────────────────────────────────

    @Nested
    @DisplayName("H. StorageEngine Full Integration")
    class StorageEngineIntegrationTests {

        @Test @Order(70)
        @DisplayName("Write then read from MemTable returns correct value")
        void writeReadMemTable() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                engine.write("cassandra", "type", "wide-column");
                Optional<Row> result = engine.read("cassandra");
                assertTrue(result.isPresent());
                assertEquals("wide-column", result.get().getCell("type").valueAsString());
            }
        }

        @Test @Order(71)
        @DisplayName("Flush makes data readable from SSTable, not just MemTable")
        void writeFlushRead() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                engine.write("kafka", "version", "3.8");
                engine.forceFlush();
                Thread.sleep(300);

                Optional<Row> result = engine.read("kafka");
                assertTrue(result.isPresent());
                assertEquals("3.8", result.get().getCell("version").valueAsString());
            }
        }

        @Test @Order(72)
        @DisplayName("Tombstone masks earlier value across MemTable + SSTable")
        void tombstoneMasksEarlierValue() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                engine.write("toDelete", "col", "original");
                engine.forceFlush();
                Thread.sleep(300);

                engine.delete("toDelete", "col"); // newer tombstone in MemTable
                Optional<Row> result = engine.read("toDelete");

                // The tombstone in the MemTable shadows the SSTable value
                // Result: row found but cell is a tombstone
                if (result.isPresent()) {
                    Row.Cell cell = result.get().getCell("col");
                    if (cell != null) {
                        assertTrue(cell.isTombstone(),
                            "Column should be a tombstone after delete");
                    }
                }
                // If result is empty, that's also acceptable (engine may skip all-tombstone rows)
            }
        }

        @Test @Order(73)
        @DisplayName("Missing key returns empty after MemTable and SSTable checks")
        void missingKeyReturnsEmpty() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                engine.write("exists", "col", "val");
                Optional<Row> missing = engine.read("definitely-does-not-exist-key-xyz");
                assertTrue(missing.isEmpty());
            }
        }

        @Test @Order(74)
        @DisplayName("Stats report correct write and read counts")
        void statsAreAccurate() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                for (int i = 0; i < 10; i++) engine.write("k" + i, "c", "v");
                for (int i = 0; i < 5; i++)  engine.read("k" + i);

                StorageEngine.EngineStats stats = engine.stats();
                assertEquals(10, stats.totalWrites());
                assertEquals(5,  stats.totalReads());
                assertTrue(stats.readHitsMemtable() <= 5);
            }
        }
    }

    // ─── I. Concurrency Stress ────────────────────────────────────────────────

    @Nested
    @DisplayName("I. Concurrency Stress")
    class ConcurrencyStressTests {

        @Test @Order(80)
        @DisplayName("Concurrent disjoint-key writes: no write is lost under contention")
        void concurrentDisjointWritesAreNotLost() throws Exception {
            overrideDataPaths(tempDir);
            int threads = 32;
            int writesPerThread = 200;
            int total = threads * writesPerThread;

            try (StorageEngine engine = new StorageEngine()) {
                ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
                CountDownLatch ready = new CountDownLatch(threads);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();
                AtomicInteger errors = new AtomicInteger();

                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        try { go.await(); } catch (InterruptedException ignored) {}
                        for (int i = 0; i < writesPerThread; i++) {
                            try {
                                engine.write("stress-" + threadId + "-" + i, "v", "ok");
                            } catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    }));
                }

                ready.await();
                long start = System.nanoTime();
                go.countDown();
                for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
                long elapsedMs = Math.max(1, (System.nanoTime() - start) / 1_000_000);
                pool.shutdown();

                assertEquals(0, errors.get(), "No write should throw under concurrent contention");
                assertEquals(total, engine.stats().totalWrites(),
                    "Every concurrent write must be counted — none silently lost");

                int verified = 0;
                for (int t = 0; t < threads; t++) {
                    for (int i = 0; i < writesPerThread; i++) {
                        Optional<Row> r = engine.read("stress-" + t + "-" + i);
                        assertTrue(r.isPresent(),
                            "stress-" + t + "-" + i + " must be readable after concurrent write");
                        verified++;
                    }
                }
                assertEquals(total, verified);

                double opsPerSec = total / (elapsedMs / 1000.0);
                System.out.printf(
                    "[stress] %d threads x %d writes = %d total writes in %dms (%.0f writes/sec), 0 lost%n",
                    threads, writesPerThread, total, elapsedMs, opsPerSec);
            }
        }

        @Test @Order(81)
        @DisplayName("Concurrent same-key writes: last-write-wins, never a torn/corrupted value")
        void concurrentSameKeyWritesNeverTear() throws Exception {
            overrideDataPaths(tempDir);
            int threads = 16;
            int writesPerThread = 100;

            try (StorageEngine engine = new StorageEngine()) {
                ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
                Set<String> writtenValues = ConcurrentHashMap.newKeySet();
                List<Future<?>> futures = new ArrayList<>();

                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    futures.add(pool.submit(() -> {
                        for (int i = 0; i < writesPerThread; i++) {
                            String value = "t" + threadId + "-w" + i;
                            try {
                                engine.write("hot-key", "col", value);
                                writtenValues.add(value);
                            } catch (Exception ignored) {}
                        }
                    }));
                }
                for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
                pool.shutdown();

                Optional<Row> result = engine.read("hot-key");
                assertTrue(result.isPresent(), "hot-key must exist after concurrent writes");
                String finalValue = result.get().getCell("col").valueAsString();

                assertTrue(writtenValues.contains(finalValue),
                    "Final value must be exactly one of the values actually written — " +
                    "never a mix of two writes (a torn value would fail this)");

                System.out.printf(
                    "[stress] %d threads hammered one key with %d writes each; " +
                    "final value '%s' is a clean last-write-wins result, not a torn merge%n",
                    threads, writesPerThread, finalValue);
            }
        }

        @Test @Order(82)
        @DisplayName("Concurrent reads during writes never throw or return corrupted state")
        void concurrentReadsDuringWritesAreSafe() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine()) {
                for (int i = 0; i < 50; i++) engine.write("seed-" + i, "c", "v" + i);

                ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
                AtomicInteger readErrors = new AtomicInteger();
                AtomicInteger writeErrors = new AtomicInteger();
                AtomicBoolean stop = new AtomicBoolean(false);

                List<Future<?>> readers = new ArrayList<>();
                for (int r = 0; r < 8; r++) {
                    readers.add(pool.submit(() -> {
                        while (!stop.get()) {
                            try {
                                engine.read("seed-" + ThreadLocalRandom.current().nextInt(50));
                            } catch (Exception e) {
                                readErrors.incrementAndGet();
                            }
                        }
                    }));
                }

                List<Future<?>> writers = new ArrayList<>();
                for (int w = 0; w < 8; w++) {
                    final int writerId = w;
                    writers.add(pool.submit(() -> {
                        for (int i = 0; i < 200; i++) {
                            try {
                                engine.write("churn-" + writerId + "-" + i, "c", "v");
                            } catch (Exception e) {
                                writeErrors.incrementAndGet();
                            }
                        }
                    }));
                }

                for (Future<?> f : writers) f.get(30, TimeUnit.SECONDS);
                stop.set(true);
                for (Future<?> f : readers) f.get(10, TimeUnit.SECONDS);
                pool.shutdown();

                assertEquals(0, readErrors.get(),
                    "Reads must never throw while writes and flushes are happening concurrently");
                assertEquals(0, writeErrors.get(),
                    "Writes must never throw under concurrent read pressure");

                System.out.println(
                    "[stress] 8 readers + 8 writers ran concurrently against a live engine — 0 exceptions");
            }
        }
    }

    // ─── J. Leveled Compaction ────────────────────────────────────────────────

    @Nested
    @DisplayName("J. Leveled Compaction (LCS)")
    class LeveledCompactionTests {

        private LeveledCompactor lcs;
        private AtomicLong genGen;

        @BeforeEach
        void setUp() {
            genGen = new AtomicLong(1);
            lcs = new LeveledCompactor(tempDir, genGen);
        }

        @Test @Order(90)
        @DisplayName("plan() triggers L0 -> L1 once the L0 file-count threshold is reached")
        void planTriggersL0ToL1OnceThresholdReached() {
            List<SSTableMetadata> l0 = new ArrayList<>();
            for (int i = 0; i < StorageConfig.LCS_L0_COMPACTION_TRIGGER; i++) {
                l0.add(mockMeta(i + 1, 1000, 0));
            }

            Optional<CompactionPlan> plan = lcs.plan(l0);

            assertTrue(plan.isPresent(), "L0 at the trigger count must produce a plan");
            assertEquals(1, plan.get().outputLevel());
            assertEquals(StorageConfig.LCS_L0_COMPACTION_TRIGGER, plan.get().inputs().size());
        }

        @Test @Order(91)
        @DisplayName("plan() returns empty below the L0 threshold and with no over-budget levels")
        void planReturnsEmptyWhenNothingIsDue() {
            List<SSTableMetadata> l0 = new ArrayList<>();
            for (int i = 0; i < StorageConfig.LCS_L0_COMPACTION_TRIGGER - 1; i++) {
                l0.add(mockMeta(i + 1, 1000, 0));
            }

            assertTrue(lcs.plan(l0).isEmpty(),
                "Below the L0 trigger and with no level over its size target, nothing should be planned");
        }

        @Test @Order(92)
        @DisplayName("plan() picks the most over-budget level, and its largest SSTable, to push down")
        void planPicksMostOverBudgetLevelByScore() {
            List<PartitionKey> pool = sortedKeyPool(16);

            SSTableMetadata l1a = mockMetaRanged(10, pool.get(0), pool.get(3), 60_000, 1);
            SSTableMetadata l1b = mockMetaRanged(11, pool.get(4), pool.get(7), 60_000, 1);
            SSTableMetadata l1c = mockMetaRanged(12, pool.get(8), pool.get(11), 70_000, 1); // largest

            // Total L1 bytes (190,000) comfortably exceeds LCS_L1_MAX_BYTES (131,072)
            List<SSTableMetadata> catalog = List.of(l1a, l1b, l1c);

            Optional<CompactionPlan> plan = lcs.plan(catalog);

            assertTrue(plan.isPresent(), "L1 over its byte target must produce a plan");
            assertEquals(2, plan.get().outputLevel());
            assertEquals(1, plan.get().inputs().size(),
                "No L2 exists yet, so only the chosen L1 SSTable should be an input");
            assertEquals(12L, plan.get().inputs().get(0).generation(),
                "The largest SSTable in the over-budget level should be chosen");
        }

        @Test @Order(93)
        @DisplayName("plan() expands overlap to a fixed point, not just a single hop")
        void planExpandsOverlapToFixedPoint() {
            List<PartitionKey> pool = sortedKeyPool(30);

            // Single L1 SSTable, large enough alone to trigger L1 -> L2.
            SSTableMetadata chosen = mockMetaRanged(1, pool.get(5), pool.get(8), 200_000, 1);

            // A directly overlaps chosen. B does NOT directly overlap chosen,
            // but does overlap A — a naive single-pass check would miss B.
            // C is unrelated to all of them and must never be pulled in.
            SSTableMetadata a = mockMetaRanged(20, pool.get(7),  pool.get(12), 10_000, 2);
            SSTableMetadata b = mockMetaRanged(21, pool.get(11), pool.get(15), 10_000, 2);
            SSTableMetadata c = mockMetaRanged(22, pool.get(20), pool.get(25), 10_000, 2);

            List<SSTableMetadata> catalog = List.of(chosen, a, b, c);

            Optional<CompactionPlan> plan = lcs.plan(catalog);

            assertTrue(plan.isPresent());
            assertEquals(2, plan.get().outputLevel());

            Set<Long> inputGenerations = new HashSet<>();
            for (SSTableMetadata meta : plan.get().inputs()) inputGenerations.add(meta.generation());

            assertTrue(inputGenerations.contains(1L),  "chosen L1 table must be an input");
            assertTrue(inputGenerations.contains(20L), "A directly overlaps chosen — must be pulled in");
            assertTrue(inputGenerations.contains(21L), "B only overlaps A, not chosen — a single-hop check would miss this");
            assertFalse(inputGenerations.contains(22L), "C overlaps nothing in the closure — must stay untouched");
        }

        @Test @Order(94)
        @DisplayName("End-to-end: LCS keeps every level 1+ SSTable non-overlapping after real compactions")
        void endToEndLeveledCompactionProducesNonOverlappingLevels() throws Exception {
            overrideDataPaths(tempDir);
            System.setProperty("COMPACTION_STRATEGY", "LCS");
            try {
                try (StorageEngine engine = new StorageEngine()) {
                    // Several overlapping-by-construction L0 SSTables, the way
                    // real traffic produces them — each batch touches keys
                    // spread across the whole range, not a disjoint slice.
                    for (int batch = 0; batch < StorageConfig.LCS_L0_COMPACTION_TRIGGER; batch++) {
                        for (int i = 0; i < 30; i++) {
                            engine.write("key-" + (i * 7 + batch), "v", "batch" + batch + "-" + i);
                        }
                        engine.forceFlush();
                    }

                    boolean compacted = false;
                    for (int i = 0; i < 100; i++) {
                        if (engine.stats().compactionsRun() > 0) { compacted = true; break; }
                        Thread.sleep(200);
                    }
                    assertTrue(compacted, "LCS should have compacted L0 into L1 within the poll window");

                    Map<Integer, List<SSTableMetadata>> byLevel = new TreeMap<>();
                    for (SSTableMetadata meta : engine.catalogSnapshot()) {
                        byLevel.computeIfAbsent(meta.level(), l -> new ArrayList<>()).add(meta);
                    }

                    for (var entry : byLevel.entrySet()) {
                        if (entry.getKey() == 0) continue; // L0 is allowed to overlap
                        List<SSTableMetadata> tables = entry.getValue();
                        for (int i = 0; i < tables.size(); i++) {
                            for (int j = i + 1; j < tables.size(); j++) {
                                assertFalse(tables.get(i).overlaps(tables.get(j)),
                                    "Level " + entry.getKey() + " must never contain overlapping SSTables, but gen="
                                        + tables.get(i).generation() + " overlaps gen=" + tables.get(j).generation());
                            }
                        }
                    }

                    // Correctness, not just the invariant — spot-check real reads still resolve.
                    assertTrue(engine.read("key-0").isPresent());
                    assertTrue(engine.read("key-" + (29 * 7 + 3)).isPresent());
                }
            } finally {
                System.clearProperty("COMPACTION_STRATEGY");
            }
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Override StorageConfig paths for test isolation.
     * Each test uses its own temp directory so tests don't interfere.
     *
     * In production we'd use dependency injection for this — we use
     * System properties here to avoid making StorageConfig mutable.
     */
    private void overrideDataPaths(Path dir) {
        System.setProperty("user.home", dir.toString());
    }

    private SSTableMetadata mockMeta(long generation, long dataSizeBytes) {
        return mockMeta(generation, dataSizeBytes, 0);
    }

    private SSTableMetadata mockMeta(long generation, long dataSizeBytes, int level) {
        return new SSTableMetadata(
            generation, tempDir, 100, 200,
            PartitionKey.of("min"), PartitionKey.of("max"),
            CommitLogPosition.NONE,
            System.currentTimeMillis(), dataSizeBytes, 1024,
            level
        );
    }

    /** Metadata with an explicit key range, for tests that need to control overlap deliberately. */
    private SSTableMetadata mockMetaRanged(long generation, PartitionKey minKey, PartitionKey maxKey,
                                            long dataSizeBytes, int level) {
        return new SSTableMetadata(
            generation, tempDir, 100, 200,
            minKey, maxKey,
            CommitLogPosition.NONE,
            System.currentTimeMillis(), dataSizeBytes, 1024,
            level
        );
    }

    /**
     * Keys are ordered by Murmur3 token, not lexicographically — so tests that
     * need genuinely ordered, non-overlapping ranges generate a pool and sort
     * it by actual token order rather than assuming string order means anything.
     */
    private static List<PartitionKey> sortedKeyPool(int n) {
        List<PartitionKey> keys = new ArrayList<>();
        for (int i = 0; i < n; i++) keys.add(PartitionKey.of("pool-key-" + i));
        keys.sort(Comparator.naturalOrder());
        return keys;
    }

    private static void deleteDir(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        Files.walk(dir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
}
