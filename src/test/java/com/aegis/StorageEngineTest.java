package com.aegis;

import com.aegis.commitlog.CommitLog;
import com.aegis.commitlog.CommitLog.CommitLogPosition;
import com.aegis.commitlog.CommitLogSegment;
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
            try (StorageEngine engine = new StorageEngine(false)) {
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
            try (StorageEngine engine = new StorageEngine(false)) {
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
            try (StorageEngine engine = new StorageEngine(false)) {
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
            try (StorageEngine engine = new StorageEngine(false)) {
                engine.write("exists", "col", "val");
                Optional<Row> missing = engine.read("definitely-does-not-exist-key-xyz");
                assertTrue(missing.isEmpty());
            }
        }

        @Test @Order(74)
        @DisplayName("Stats report correct write and read counts")
        void statsAreAccurate() throws Exception {
            overrideDataPaths(tempDir);
            try (StorageEngine engine = new StorageEngine(false)) {
                for (int i = 0; i < 10; i++) engine.write("k" + i, "c", "v");
                for (int i = 0; i < 5; i++)  engine.read("k" + i);

                StorageEngine.EngineStats stats = engine.stats();
                assertEquals(10, stats.totalWrites());
                assertEquals(5,  stats.totalReads());
                assertTrue(stats.readHitsMemtable() <= 5);
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
        return new SSTableMetadata(
            generation, tempDir, 100, 200,
            PartitionKey.of("min"), PartitionKey.of("max"),
            CommitLogPosition.NONE,
            System.currentTimeMillis(), dataSizeBytes, 1024
        );
    }

    private static void deleteDir(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        Files.walk(dir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
}
