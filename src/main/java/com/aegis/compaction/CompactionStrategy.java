package com.aegis.compaction;

import com.aegis.sstable.SSTableWriter.SSTableMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Shared contract for a compaction strategy. Aegis-Storage ships two:
 *
 *   STCSCompactor     — Size-Tiered Compaction Strategy (Cassandra's default).
 *                       Optimises for write amplification: SSTables of similar
 *                       size are merged together, regardless of key range.
 *                       Simple, but a point read may have to check every
 *                       SSTable, since ranges overlap freely.
 *
 *   LeveledCompactor  — Leveled Compaction Strategy (RocksDB/LevelDB's default,
 *                       also available in Cassandra as an alternative to STCS).
 *                       Optimises for read and space amplification: SSTables
 *                       within the same level (L1+) never overlap in key
 *                       range, so a point read touches at most one SSTable
 *                       per level. The cost is much higher write amplification
 *                       — data gets rewritten repeatedly as it moves down levels.
 *
 * Neither strategy is "better" — this is the fundamental LSM-tree trade-off
 * triangle (write amp / read amp / space amp), and picking one is picking
 * which two you optimise for at the expense of the third.
 */
public interface CompactionStrategy {

    /**
     * Look at the current catalog and decide whether a compaction should run
     * right now, and if so, exactly which SSTables it would involve.
     * Returns empty if nothing is due.
     */
    Optional<CompactionPlan> plan(List<SSTableMetadata> catalog);

    /**
     * Execute a previously-returned plan: merge the plan's inputs, write the
     * output SSTable(s), delete the inputs. Returns the new output metadata.
     */
    SSTableMetadata execute(CompactionPlan plan) throws IOException;

    long compactionsRun();
    long tombstonesPurged();
    long bytesReclaimed();

    /** Total bytes written by compaction output SSTables — the write-amplification numerator. */
    long bytesWritten();

    /** Short display name, e.g. "STCS" or "LCS", for stats/logging. */
    String strategyName();

    /**
     * A planned compaction job: which SSTables to merge, and — for leveled
     * compaction — which level the output belongs to. STCS ignores
     * outputLevel and always produces level-0 (unleveled) output.
     */
    record CompactionPlan(List<SSTableMetadata> inputs, int outputLevel, String reason) {}
}
