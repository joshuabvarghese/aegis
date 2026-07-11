package com.aegis.compaction;

import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.sstable.SSTableReader;
import com.aegis.sstable.SSTableWriter;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Leveled Compaction Strategy (LCS) — RocksDB and LevelDB's default compaction
 * algorithm. Real Cassandra also offers it (LeveledCompactionStrategy) as an
 * alternative to its own STCS default.
 *
 * ─── Why a Second Strategy Exists ─────────────────────────────────────────────
 *
 * Every LSM-tree compaction strategy is a choice about which two points of the
 * write/read/space amplification triangle to optimise for, at the expense of
 * the third:
 *
 *   STCS (see STCSCompactor) — low write amplification, but SSTables overlap
 *   freely, so a point read may have to check every SSTable, and deleted or
 *   overwritten data can sit unreclaimed across many files at once.
 *
 *   LCS (this class) — SSTables within the same level (L1+) never overlap in
 *   key range, by construction. That makes a point read touch at most one
 *   SSTable per level (huge read-amplification win), and bounds how much
 *   stale/overwritten data can exist at once (space-amplification win). The
 *   cost is write amplification: the same row gets rewritten every time its
 *   level gets compacted into the next, and level sizes grow exponentially so
 *   deep trees compact the same bytes many times over their lifetime.
 *
 * ─── The Algorithm ────────────────────────────────────────────────────────────
 *
 * L0 is special: it holds raw MemTable-flush output, which can overlap freely
 * (Cassandra and RocksDB both do this). Once L0 accumulates
 * LCS_L0_COMPACTION_TRIGGER files, all of them are merged into L1 along with
 * any L1 files whose range they overlap.
 *
 * L1 and above: each level has a target total size (StorageConfig.lcsLevelTargetBytes),
 * growing by LCS_LEVEL_SIZE_MULTIPLIER per level. Whichever level is furthest
 * over its target gets compacted: pick its largest SSTable, merge it with
 * every overlapping SSTable in the next level down, write one new output file
 * at that next level.
 *
 * ─── A Subtlety Worth Naming: Overlap Expansion Must Reach a Fixed Point ─────
 *
 * It's tempting to compute "which next-level SSTables overlap the file we're
 * pushing down" with a single pass. That's wrong. Pulling in one overlapping
 * table can widen the merged key range enough to newly overlap a *second*
 * table that didn't intersect the original range at all — and if that second
 * table is left behind, the non-overlap invariant for that level is silently
 * broken. `expandToOverlapClosure` below keeps adding overlapping tables until
 * a full pass adds none, which is the actual correctness requirement.
 *
 * ─── Known Simplifications vs Production LCS ─────────────────────────────────
 *
 * 1. One output SSTable per compaction round, never split into fixed-size
 *    output files the way RocksDB does — acceptable at demo data volumes,
 *    would need addressing at real-world scale (a single level-4 compaction
 *    could otherwise produce one enormous file).
 * 2. Picks the *largest* SSTable in an over-budget level to push down, rather
 *    than LevelDB's persistent round-robin "compaction pointer" that cycles
 *    through the whole key range across successive compactions. Simpler, and
 *    it shrinks the over-budget level fastest, but a real implementation
 *    would want the round-robin cursor to guarantee every key range
 *    eventually gets compacted, not just the ones that happen to sort largest.
 */
public final class LeveledCompactor implements CompactionStrategy {

    private static final Logger log = Logger.getLogger(LeveledCompactor.class.getName());

    private final Path       sstableDir;
    private final AtomicLong generationGen;

    private final AtomicLong compactionsRun   = new AtomicLong(0);
    private final AtomicLong tombstonesPurged = new AtomicLong(0);
    private final AtomicLong bytesReclaimed   = new AtomicLong(0);
    private final AtomicLong bytesWritten     = new AtomicLong(0);

    public LeveledCompactor(Path sstableDir, AtomicLong generationGen) {
        this.sstableDir    = sstableDir;
        this.generationGen = generationGen;
    }

    // ─── Planning ──────────────────────────────────────────────────────────────

    @Override
    public Optional<CompactionPlan> plan(List<SSTableMetadata> catalog) {
        Map<Integer, List<SSTableMetadata>> byLevel = groupByLevel(catalog);

        // L0 -> L1: triggered by file count, not size (L0 files can overlap,
        // so "total bytes" isn't a meaningful budget the way it is for L1+).
        List<SSTableMetadata> l0 = byLevel.getOrDefault(0, List.of());
        if (l0.size() >= StorageConfig.LCS_L0_COMPACTION_TRIGGER) {
            List<SSTableMetadata> l1 = byLevel.getOrDefault(1, List.of());
            List<SSTableMetadata> overlappingL1 = expandToOverlapClosure(l0, l1);

            List<SSTableMetadata> inputs = new ArrayList<>(l0);
            inputs.addAll(overlappingL1);

            return Optional.of(new CompactionPlan(inputs, 1,
                "L0 has " + l0.size() + " SSTables (trigger=" + StorageConfig.LCS_L0_COMPACTION_TRIGGER + ")"));
        }

        // Ln -> Ln+1 (n >= 1): triggered by size score = bytes-in-level / target-bytes.
        // The most over-budget level compacts first.
        int    bestLevel = -1;
        double bestScore  = 1.0; // only levels at or past their target are eligible
        for (var entry : byLevel.entrySet()) {
            int level = entry.getKey();
            if (level == 0) continue;
            long totalBytes = entry.getValue().stream().mapToLong(SSTableMetadata::dataSizeBytes).sum();
            double score = (double) totalBytes / StorageConfig.lcsLevelTargetBytes(level);
            if (score > bestScore) {
                bestScore = score;
                bestLevel = level;
            }
        }

        if (bestLevel == -1) return Optional.empty();

        List<SSTableMetadata> levelTables = byLevel.get(bestLevel);
        SSTableMetadata chosen = levelTables.stream()
            .max(Comparator.comparingLong(SSTableMetadata::dataSizeBytes))
            .orElseThrow();

        List<SSTableMetadata> nextLevelTables = byLevel.getOrDefault(bestLevel + 1, List.of());
        List<SSTableMetadata> overlapping = expandToOverlapClosure(List.of(chosen), nextLevelTables);

        List<SSTableMetadata> inputs = new ArrayList<>();
        inputs.add(chosen);
        inputs.addAll(overlapping);

        return Optional.of(new CompactionPlan(inputs, bestLevel + 1,
            "L%d is %.1fx over its %dKB target".formatted(
                bestLevel, bestScore, StorageConfig.lcsLevelTargetBytes(bestLevel) / 1024)));
    }

    private static Map<Integer, List<SSTableMetadata>> groupByLevel(List<SSTableMetadata> catalog) {
        Map<Integer, List<SSTableMetadata>> byLevel = new TreeMap<>();
        for (SSTableMetadata meta : catalog) {
            byLevel.computeIfAbsent(meta.level(), l -> new ArrayList<>()).add(meta);
        }
        return byLevel;
    }

    /**
     * Starting from `seeds`, keep adding any not-yet-included candidate that
     * overlaps something already in the set — seeds or previously-added
     * candidates alike — until a full pass adds nothing. See the class
     * javadoc for why a single pass is not sufficient.
     */
    private static List<SSTableMetadata> expandToOverlapClosure(
            List<SSTableMetadata> seeds, List<SSTableMetadata> candidates) {

        List<SSTableMetadata> included = new ArrayList<>();
        boolean changed = true;

        while (changed) {
            changed = false;
            for (SSTableMetadata candidate : candidates) {
                if (included.contains(candidate)) continue;
                boolean overlapsAny =
                    seeds.stream().anyMatch(s -> s.overlaps(candidate)) ||
                    included.stream().anyMatch(s -> s.overlaps(candidate));
                if (overlapsAny) {
                    included.add(candidate);
                    changed = true;
                }
            }
        }
        return included;
    }

    // ─── Execution ─────────────────────────────────────────────────────────────

    @Override
    public SSTableMetadata execute(CompactionPlan plan) throws IOException {
        List<SSTableMetadata> inputs     = plan.inputs();
        int                   outputLevel = plan.outputLevel();

        log.info("[LCS] Compacting %d SSTables into L%d (%s)"
            .formatted(inputs.size(), outputLevel, plan.reason()));

        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableMetadata meta : inputs) {
            readers.add(new SSTableReader(meta));
        }

        Map<PartitionKey, List<Row>> mergeMap = new TreeMap<>();
        long inputBytes = 0;
        for (SSTableReader reader : readers) {
            for (Row row : reader.scanAll()) {
                mergeMap.computeIfAbsent(row.key(), k -> new ArrayList<>()).add(row);
            }
            inputBytes += reader.metadata().dataSizeBytes();
            reader.close();
        }

        SSTableWriter writer = new SSTableWriter(sstableDir, generationGen.getAndIncrement());
        TreeMap<PartitionKey, Row> outputMap = new TreeMap<>();
        long tombstonesPurgedLocal = 0;

        for (var entry : mergeMap.entrySet()) {
            // Reuse STCS's row reconciliation — last-write-wins-per-cell is
            // identical regardless of which compaction strategy is merging.
            Row merged = STCSCompactor.mergeAllVersions(entry.getValue());
            Row purged = STCSCompactor.purgeTombstones(merged, StorageConfig.GC_GRACE_SECONDS);

            if (!purged.isEmpty()) {
                outputMap.put(entry.getKey(), purged);
            } else {
                tombstonesPurgedLocal++;
            }
        }

        var newestClPosition = inputs.stream()
            .map(SSTableMetadata::commitLogPosition)
            .max(Comparator.naturalOrder())
            .orElse(com.aegis.commitlog.CommitLog.CommitLogPosition.NONE);

        writer.writeAll(outputMap, outputMap.size(), newestClPosition);
        SSTableMetadata outputMeta = writer.finish(outputLevel);
        writer.close();

        for (SSTableMetadata meta : inputs) {
            deleteSSTableFiles(meta);
        }

        compactionsRun.incrementAndGet();
        tombstonesPurged.addAndGet(tombstonesPurgedLocal);
        bytesReclaimed.addAndGet(inputBytes - outputMeta.dataSizeBytes());
        bytesWritten.addAndGet(outputMeta.dataSizeBytes());

        log.info(("[LCS] Complete. input=%d SSTables -> L%d gen=%d, partitions=%d, tombstonesPurged=%d")
            .formatted(inputs.size(), outputLevel, outputMeta.generation(), outputMap.size(), tombstonesPurgedLocal));

        return outputMeta;
    }

    private void deleteSSTableFiles(SSTableMetadata meta) {
        Path[] components = {
            meta.dataPath(), meta.indexPath(), meta.filterPath(),
            meta.statsPath(), meta.summaryPath()
        };
        for (Path p : components) {
            try {
                java.nio.file.Files.deleteIfExists(p);
            } catch (IOException e) {
                log.warning("[LCS] Failed to delete " + p + ": " + e.getMessage());
            }
        }
    }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    @Override public long compactionsRun()   { return compactionsRun.get(); }
    @Override public long tombstonesPurged() { return tombstonesPurged.get(); }
    @Override public long bytesReclaimed()   { return bytesReclaimed.get(); }
    @Override public long bytesWritten()     { return bytesWritten.get(); }
    @Override public String strategyName()   { return "LCS"; }

    /** Per-level SSTable counts and byte totals — used by tests and the STCS-vs-LCS comparison benchmark. */
    public static Map<Integer, LevelSummary> summarizeLevels(List<SSTableMetadata> catalog) {
        Map<Integer, List<SSTableMetadata>> byLevel = groupByLevel(catalog);
        Map<Integer, LevelSummary> summary = new TreeMap<>();
        for (var entry : byLevel.entrySet()) {
            long bytes = entry.getValue().stream().mapToLong(SSTableMetadata::dataSizeBytes).sum();
            summary.put(entry.getKey(), new LevelSummary(entry.getKey(), entry.getValue().size(), bytes));
        }
        return summary;
    }

    public record LevelSummary(int level, int sstableCount, long totalBytes) {}
}
