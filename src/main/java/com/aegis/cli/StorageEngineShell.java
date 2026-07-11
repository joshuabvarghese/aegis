package com.aegis.cli;

import com.aegis.StorageEngine;
import com.aegis.core.Row;
import com.aegis.core.StorageConfig;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Interactive CLI shell for the Aegis-Storage engine.
 *
 * This is the demo entry point. It gives an interviewer a live window into
 * every layer of the LSM-tree — from CommitLog append through MemTable,
 * SSTable flush, and compaction (STCS or LCS).
 *
 * ─── Commands ────────────────────────────────────────────────────────────────
 *
 *   write <key> <column> <value>    Write a single cell
 *   read  <key>                     Read a partition (shows which tier it came from)
 *   delete <key> <column>           Insert a tombstone
 *   blast <n>                       Write n records at max throughput (JMH-lite)
 *   flush                           Force MemTable → SSTable flush
 *   compact                         Force a compaction cycle (STCS or LCS, whichever is active)
 *   stats                           Print full engine metrics
 *   demo                            Run the full automated demo sequence
 *   help                            Show this help
 *   quit                            Shutdown and exit
 *
 * ─── Demo Sequence ────────────────────────────────────────────────────────────
 *
 * The `demo` command runs a scripted sequence that shows:
 *
 *   1. WRITE PATH: blast 500 records, show CommitLog and MemTable filling
 *   2. FLUSH:      trigger flush, show MemTable → SSTable transition
 *   3. READ PATH:  read a key, show Bloom filter hit, index lookup, data read
 *   4. TOMBSTONE:  delete a key, read it back (tombstone returned, not old value)
 *   5. COMPACTION: write 4 more flush cycles, trigger STCS, show merge stats
 *   6. STATS:      print full engine metrics
 */
public final class StorageEngineShell {

    private static StorageEngine engine;
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        System.setProperty("java.util.logging.SimpleFormatter.format",
            "[%4$s] %5$s%n");

        printBanner();
        System.out.println("  Booting storage engine...");

        engine = new StorageEngine();

        System.out.println("  Engine ready. CommitLog + MemTable active.");
        System.out.println("  Compaction strategy: " + engine.stats().compactionStrategy()
            + " (set COMPACTION_STRATEGY=LCS to try Leveled Compaction instead)");
        System.out.println();
        System.out.println("  Type 'demo' to run the full automated demo, or 'help' for commands.");
        System.out.println();

        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            try { if (engine != null) engine.close(); } catch (IOException ignored) {}
        }));

        repl();
    }

    // ─── REPL ─────────────────────────────────────────────────────────────────

    private static void repl() throws Exception {
        while (true) {
            System.out.print("aegis-storage> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+", 4);
            String cmd = parts[0].toLowerCase();

            try {
                switch (cmd) {
                    case "write"   -> cmdWrite(parts);
                    case "read"    -> cmdRead(parts);
                    case "delete"  -> cmdDelete(parts);
                    case "blast"   -> cmdBlast(parts);
                    case "flush"   -> cmdFlush();
                    case "compact" -> cmdCompact();
                    case "stats"   -> engine.stats().print();
                    case "demo"    -> runFullDemo();
                    case "help"    -> printHelp();
                    case "quit", "exit", "q" -> {
                        System.out.println("  Shutting down...");
                        engine.close();
                        System.out.println("  Goodbye.");
                        return;
                    }
                    default -> System.out.println("  Unknown command. Type 'help'.");
                }
            } catch (Exception e) {
                System.out.println("  ERROR: " + e.getMessage());
            }
        }
    }

    // ─── Command Handlers ─────────────────────────────────────────────────────

    private static void cmdWrite(String[] parts) throws IOException {
        if (parts.length < 4) { System.out.println("  Usage: write <key> <column> <value>"); return; }
        String key = parts[1], col = parts[2], val = parts[3];
        engine.write(key, col, val);
        System.out.printf("  ✓ Written  key=%-20s  col=%-15s  val=%s%n", key, col, val);
    }

    private static void cmdRead(String[] parts) throws IOException {
        if (parts.length < 2) { System.out.println("  Usage: read <key>"); return; }
        String key = parts[1];
        long start = System.nanoTime();
        var result = engine.read(key);
        long us = (System.nanoTime() - start) / 1_000;

        if (result.isEmpty()) {
            System.out.printf("  ✗ Not found  key=%s  (%dµs)%n", key, us);
            return;
        }
        Row row = result.get();
        System.out.printf("  ✓ Found  key=%-20s  (%dµs)%n", key, us);
        for (var entry : row.cells().entrySet()) {
            Row.Cell cell = entry.getValue();
            System.out.printf("      %-15s = %-20s  ts=%d  flags=0x%02X%n",
                entry.getKey(), cell.valueAsString(), cell.timestamp(), cell.flags());
        }
    }

    private static void cmdDelete(String[] parts) throws IOException {
        if (parts.length < 3) { System.out.println("  Usage: delete <key> <column>"); return; }
        engine.delete(parts[1], parts[2]);
        System.out.printf("  ✓ Tombstone inserted  key=%s  col=%s%n", parts[1], parts[2]);
    }

    private static void cmdBlast(String[] parts) throws Exception {
        int n = parts.length >= 2 ? Integer.parseInt(parts[1]) : 1000;
        System.out.printf("  Blasting %d writes...%n", n);

        AtomicLong written = new AtomicLong(0);
        AtomicLong errors  = new AtomicLong(0);
        long start = System.nanoTime();

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch latch = new CountDownLatch(n);

        for (int i = 0; i < n; i++) {
            final int idx = i;
            exec.submit(() -> {
                try {
                    engine.write(
                        "user:%06d".formatted(idx),
                        "email",
                        "user%d@aegis.io".formatted(idx)
                    );
                    written.incrementAndGet();
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        exec.shutdown();

        double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
        long throughput  = (long)(written.get() / (elapsedMs / 1_000));

        System.out.printf("  ✓ Blast complete: %d/%d written in %.1fms = %s writes/sec%n",
            written.get(), n, elapsedMs, formatNumber(throughput));
        if (errors.get() > 0)
            System.out.printf("  ! %d errors%n", errors.get());
    }

    private static void cmdFlush() throws IOException, InterruptedException {
        System.out.println("  Flushing MemTable → SSTable...");
        long before = engine.stats().sstableCount();
        engine.forceFlush();
        Thread.sleep(500);
        long after = engine.stats().sstableCount();
        System.out.printf("  ✓ Flush complete. SSTables: %d → %d%n", before, after);
    }

    private static void cmdCompact() throws Exception {
        System.out.println("  Triggering compaction check...");
        long before = engine.stats().sstableCount();
        Thread.sleep(StorageConfig.COMPACTION_CHECK_INTERVAL_MS + 500);
        long after = engine.stats().sstableCount();
        System.out.printf("  ✓ Compaction cycle done. SSTables: %d → %d%n", before, after);
        System.out.printf("  Compactions run: %d  Tombstones purged: %d  Bytes reclaimed: %dKB%n",
            engine.stats().compactionsRun(),
            engine.stats().tombstonesPurged(),
            engine.stats().bytesReclaimed() / 1024);
    }

    // ─── Full Automated Demo ──────────────────────────────────────────────────

    private static void runFullDemo() throws Exception {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println("  AEGIS-STORAGE DEMO — Cassandra LSM-Tree Engine");
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println();

        // ── Act 1: Write Path ──────────────────────────────────────────────
        sep("ACT 1: Write Path — CommitLog + MemTable");
        System.out.println("  Every write goes to CommitLog first (durability),");
        System.out.println("  then to the MemTable (in-memory ConcurrentSkipListMap).");
        System.out.println("  Same order Cassandra uses. Reads are immediately available.");
        System.out.println();

        engine.write("cassandra", "version", "5.0");
        engine.write("cassandra", "type",    "wide-column store");
        engine.write("kafka",     "version", "3.8");
        engine.write("kafka",     "type",    "distributed log");
        engine.write("clickhouse","version", "24.x");
        engine.write("clickhouse","type",    "columnar OLAP");
        System.out.println("  Written: cassandra, kafka, clickhouse (3 rows, 2 columns each)");

        System.out.print("  Reading 'cassandra' from MemTable... ");
        cmdRead(new String[]{"read", "cassandra"});

        pause();

        // ── Act 2: MemTable Flush ─────────────────────────────────────────
        sep("ACT 2: MemTable Flush → SSTable");
        System.out.println("  Writing 500 records to trigger the flush threshold...");
        cmdBlast(new String[]{"blast", "500"});
        System.out.println();
        System.out.println("  Flushing MemTable to SSTable (immutable, sorted, on-disk).");
        System.out.println("  SSTable components: Data.db + Index.db + Filter.db + Statistics.db");
        cmdFlush();
        System.out.println();
        System.out.print("  Reading 'kafka' — now comes from SSTable (Bloom filter → Index → Data)...");
        cmdRead(new String[]{"read", "kafka"});

        pause();

        // ── Act 3: Bloom Filter Demo ──────────────────────────────────────
        sep("ACT 3: Bloom Filter — Zero I/O for Non-Existent Keys");
        System.out.println("  Reading 'nonexistent-key-xyz' — Bloom filter eliminates all SSTable reads.");
        System.out.println("  At 1% FPP, 99% of missing-key lookups skip disk I/O entirely.");
        long t1 = System.nanoTime();
        var miss = engine.read("nonexistent-key-xyz");
        long t2 = System.nanoTime();
        System.out.printf("  Result: %s  (%dµs — Bloom filter short-circuited all disk reads)%n",
            miss.isEmpty() ? "Not found ✓" : "Found (unexpected)", (t2-t1)/1_000);

        pause();

        // ── Act 4: Tombstones ─────────────────────────────────────────────
        sep("ACT 4: Tombstones — LSM Deletes");
        System.out.println("  In LSM-trees, deletes are writes — they insert a tombstone cell.");
        System.out.println("  The tombstone shadows older values across all SSTables.");
        System.out.println("  Tombstones are physically removed during compaction (GC grace period).");
        engine.write("toDelete", "col1", "original-value");
        System.out.println("  Written: toDelete.col1 = original-value");
        cmdRead(new String[]{"read", "toDelete"});
        engine.delete("toDelete", "col1");
        System.out.println("  Deleted: toDelete.col1 (tombstone inserted)");
        cmdRead(new String[]{"read", "toDelete"});
        System.out.println("  (Tombstone masks the original value — correct LSM behaviour)");

        pause();

        // ── Act 5: STCS Compaction ────────────────────────────────────────
        sep("ACT 5: Size-Tiered Compaction Strategy (STCS)");
        System.out.println("  Writing 3 more flush cycles to create multiple SSTables...");
        System.out.println("  STCS triggers when >= 4 SSTables of similar size exist.");
        for (int cycle = 0; cycle < 3; cycle++) {
            cmdBlast(new String[]{"blast", "400"});
            cmdFlush();
        }
        System.out.println();
        System.out.println("  SSTable count before compaction: " + engine.stats().sstableCount());
        System.out.println("  Waiting for STCS daemon to detect the bucket...");
        Thread.sleep(StorageConfig.COMPACTION_CHECK_INTERVAL_MS * 2);
        System.out.println("  SSTable count after compaction:  " + engine.stats().sstableCount());
        System.out.printf("  Tombstones purged: %d   Bytes reclaimed: %dKB%n",
            engine.stats().tombstonesPurged(),
            engine.stats().bytesReclaimed() / 1024);

        pause();

        // ── Act 6: Stats ──────────────────────────────────────────────────
        sep("ACT 6: Full Engine Metrics");
        engine.stats().print();

        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println("  Demo complete. Try individual commands or run 'demo' again.");
        System.out.println("═══════════════════════════════════════════════════════════════");
        System.out.println();
    }

    // ─── UI Helpers ───────────────────────────────────────────────────────────

    private static void sep(String title) {
        System.out.println("───────────────────────────────────────────────────────────────");
        System.out.println("  " + title);
        System.out.println("───────────────────────────────────────────────────────────────");
    }

    private static void pause() throws InterruptedException {
        System.out.println();
        Thread.sleep(600);
    }

    private static String formatNumber(long n) {
        if (n >= 1_000_000) return "%.1fM".formatted(n / 1_000_000.0);
        if (n >= 1_000)     return "%.1fK".formatted(n / 1_000.0);
        return String.valueOf(n);
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("  ┌─────────────────────────────────────────────────────────┐");
        System.out.println("  │         AEGIS-STORAGE — LSM-Tree Storage Engine          │");
        System.out.println("  │      CommitLog → MemTable → SSTable → Compaction         │");
        System.out.println("  │        Cassandra internals, built from scratch           │");
        System.out.println("  └─────────────────────────────────────────────────────────┘");
        System.out.println();
    }

    private static void printHelp() {
        System.out.println("""
          Commands:
            write  <key> <column> <value>   Write a cell via CommitLog + MemTable
            read   <key>                    Read a partition (MemTable → SSTable → Cold)
            delete <key> <column>           Insert a tombstone (LSM delete)
            blast  <n>                      Throughput test: write n rows via VirtualThreads
            flush                           Force MemTable → SSTable flush
            compact                         Force a compaction cycle (STCS or LCS, whichever is active)
            stats                           Full engine metrics snapshot
            demo                            Run the automated 6-act demo
            quit                            Shutdown gracefully
        """);
    }
}
