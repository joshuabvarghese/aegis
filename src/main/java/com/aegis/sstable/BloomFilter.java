package com.aegis.sstable;

import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;

import java.util.BitSet;

/**
 * Bloom Filter — probabilistic partition key existence check.
 *
 * ─── Cassandra Parallel ───────────────────────────────────────────────────────
 *
 * Every Cassandra SSTable has an associated Bloom filter (.Filter.db component).
 * Before doing any disk I/O to look up a partition, the read path checks the
 * Bloom filter. If it returns false, the SSTable definitely does not contain
 * that partition — skip it entirely. If it returns true, the partition probably
 * exists (with configurable false positive probability).
 *
 * At bloom_filter_fp_chance = 0.01 (1% FPP, Cassandra default), a filter for
 * 1 million keys requires approximately 9.6 bits per key = 1.2MB.
 *
 * ─── Implementation ──────────────────────────────────────────────────────────
 *
 * Standard Bloom filter with k hash functions derived from double-hashing:
 *   h_i(x) = (h1(x) + i * h2(x)) mod m
 *
 * where h1 and h2 are independent hash functions derived from the key's Murmur3
 * token (same hash algorithm Cassandra uses for partition tokens).
 *
 * Parameters calculated from expected insertions n and FPP p:
 *   m = -n * ln(p) / (ln 2)^2   (bit array size)
 *   k = (m/n) * ln 2             (number of hash functions)
 *
 * These are the same formulas used in Cassandra's BloomFilter.java.
 *
 * ─── Memory Budget ───────────────────────────────────────────────────────────
 *
 * At default FPP=0.01, memory cost per key ≈ 9.6 bits = ~1.2 bytes.
 * A filter for 1M keys = 1.2MB — well within our 256MB heap budget.
 * The BitSet is stored on-heap; for very large SSTables Cassandra uses
 * off-heap allocation (we note this as an extension point).
 */
public final class BloomFilter {

    private final BitSet  bits;
    private final int     numHashFunctions;  // k
    private final long    numBits;           // m
    private final double  fpp;
    private long          insertions = 0;

    /**
     * Create a Bloom filter sized for the given expected number of insertions
     * at the configured false positive probability.
     *
     * @param expectedInsertions  n — number of keys expected to be inserted
     */
    public BloomFilter(long expectedInsertions) {
        this(expectedInsertions, StorageConfig.BLOOM_FILTER_FP_PROBABILITY);
    }

    public BloomFilter(long expectedInsertions, double fpp) {
        this.fpp = fpp;
        this.numBits = optimalNumBits(expectedInsertions, fpp);
        this.numHashFunctions = optimalNumHashFunctions(expectedInsertions, numBits);
        this.bits = new BitSet((int) numBits);
    }

    // ─── Core Operations ──────────────────────────────────────────────────────

    /**
     * Add a partition key to the filter.
     * Called during SSTable write for every partition key.
     */
    public void add(PartitionKey key) {
        long hash1 = key.token();                     // Murmur3 token (h1)
        long hash2 = spread(hash1);                   // derived second hash (h2)

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + (long) i * hash2;
            // Map to [0, numBits) — mod on negative values needs care
            int bitIndex = (int) ((combinedHash & Long.MAX_VALUE) % numBits);
            bits.set(bitIndex);
        }
        insertions++;
    }

    /**
     * Test whether a partition key might be in this SSTable.
     *
     * Returns false  → key is DEFINITELY NOT in this SSTable (skip all disk I/O)
     * Returns true   → key is PROBABLY in this SSTable (with FPP chance of being wrong)
     *
     * This is the critical hot-path optimization: at 1% FPP, 99% of non-existent
     * key lookups skip the SSTable entirely, eliminating random disk reads.
     */
    public boolean mightContain(PartitionKey key) {
        long hash1 = key.token();
        long hash2 = spread(hash1);

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + (long) i * hash2;
            int bitIndex = (int) ((combinedHash & Long.MAX_VALUE) % numBits);
            if (!bits.get(bitIndex)) return false; // definitive miss
        }
        return true; // probable hit
    }

    // ─── Serialization ────────────────────────────────────────────────────────

    /**
     * Serialize the Bloom filter to bytes for writing to the SSTable's
     * .filter component file.
     *
     * Format:
     *   [numBits: 8B][numHashFunctions: 4B][fpp: 8B][bitSetLength: 4B][bits: varB]
     */
    public byte[] serialize() {
        byte[] bitBytes = bits.toByteArray();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8 + 4 + 8 + 4 + bitBytes.length);
        buf.putLong(numBits);
        buf.putInt(numHashFunctions);
        buf.putDouble(fpp);
        buf.putInt(bitBytes.length);
        buf.put(bitBytes);
        return buf.array();
    }

    /**
     * Deserialize a Bloom filter from bytes (loaded when opening an SSTable).
     */
    public static BloomFilter deserialize(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        long   numBits          = buf.getLong();
        int    numHashFunctions = buf.getInt();
        double fpp              = buf.getDouble();
        int    bitBytesLen      = buf.getInt();
        byte[] bitBytes         = new byte[bitBytesLen];
        buf.get(bitBytes);

        BloomFilter filter = new BloomFilter(1, fpp); // dummy insertions
        // Override the computed values with deserialized ones
        return new BloomFilter(numBits, numHashFunctions, fpp, BitSet.valueOf(bitBytes));
    }

    private BloomFilter(long numBits, int numHashFunctions, double fpp, BitSet bits) {
        this.numBits          = numBits;
        this.numHashFunctions = numHashFunctions;
        this.fpp              = fpp;
        this.bits             = bits;
    }

    // ─── Parameter Calculation ────────────────────────────────────────────────

    /**
     * Optimal bit array size: m = -n * ln(p) / (ln 2)^2
     * Same formula as Cassandra's BloomCalculations.computeBloomSpec().
     */
    public static long optimalNumBits(long n, double p) {
        if (p == 0) p = Double.MIN_VALUE;
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Optimal number of hash functions: k = (m/n) * ln 2
     */
    public static int optimalNumHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Wang/Jenkins hash to derive a second independent hash from the first.
     * Avoids the cost of a second full Murmur3 invocation.
     */
    private static long spread(long hash) {
        hash = (~hash) + (hash << 21);
        hash ^= (hash >>> 24);
        hash = (hash + (hash << 3)) + (hash << 8);
        hash ^= (hash >>> 14);
        hash = (hash + (hash << 2)) + (hash << 4);
        hash ^= (hash >>> 28);
        hash += (hash << 31);
        return hash;
    }

    // ─── Metrics ──────────────────────────────────────────────────────────────

    public long   numBits()          { return numBits; }
    public int    numHashFunctions() { return numHashFunctions; }
    public double fpp()              { return fpp; }
    public long   insertions()       { return insertions; }
    public long   memorySizeBytes()  { return bits.size() / 8; }

    /**
     * Estimated current false positive probability given actual insertions.
     * Useful for monitoring filter saturation.
     */
    public double currentFpp() {
        return Math.pow(1 - Math.exp(-(double) numHashFunctions * insertions / numBits),
            numHashFunctions);
    }

    @Override
    public String toString() {
        return "BloomFilter{bits=%d, k=%d, fpp=%.4f, insertions=%d, memKB=%d}"
            .formatted(numBits, numHashFunctions, fpp, insertions, memorySizeBytes() / 1024);
    }
}
