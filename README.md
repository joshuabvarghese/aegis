# Aegis

**A from-scratch implementation of the LSM-tree storage engine underlying Apache Cassandra's write path.**

CommitLog → MemTable → SSTable flush → Size-Tiered Compaction → async cold-tier offload to object storage. Built in Java 21 with zero framework dependencies. Fully containerised — one command to run.


---

## One-command demo

```bash
git clone https://github.com/joshuabvarghese/aegis-storage
cd aegis-storage
docker compose run --rm demo
```

That's it. Docker pulls MinIO and the JRE image, compiles the JAR, boots the engine, runs the 6-act demo, and exits. No Java installation, no Maven, no MinIO setup.

**Interactive shell** (type commands yourself):
```bash
docker compose run --rm engine
```

---

## Architecture

```
                         WRITE PATH
                         ──────────
  client.write(key, col, value)
        │
        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  CommitLog  (durability before anything else)           │
  │  *.clog  │  [Length:4B][Record:nB][CRC32C:4B]          │
  │  fsync every 200ms (PERIODIC mode = cassandra default)  │
  └─────────────────────┬───────────────────────────────────┘
                        │ CommitLogPosition returned
                        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  MemTable  (in-memory write buffer)                     │
  │  ConcurrentSkipListMap<PartitionKey, Row>               │
  │  Sorted by Murmur3 token  (same as Cassandra)           │
  │  Flush threshold: 8MB  (cassandra: memtable_heap_space) │
  └─────────────────────┬───────────────────────────────────┘
                        │ threshold crossed → async flush
                        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  SSTable  (immutable, sorted, on-disk)                  │
  │  ├── {gen}-Data.db        sorted partition data         │
  │  ├── {gen}-Index.db       sparse partition index        │
  │  ├── {gen}-Filter.db      Bloom filter (1% FPP)        │
  │  ├── {gen}-Statistics.db  metadata + CommitLog pos      │
  │  └── {gen}-Summary.db     sampled index summary         │
  └─────────────────────┬───────────────────────────────────┘
                        │ age > 30s → async offload
                        ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Cold Tier  (MinIO / S3-compatible object storage)      │
  │  Local files deleted after upload confirmed             │
  │  Remote catalog retains metadata for read routing       │
  └─────────────────────────────────────────────────────────┘

                          READ PATH
                          ─────────
  client.read(key)
        │
        ├─► 1. MemTable          ConcurrentSkipListMap.get()   O(log n)
        ├─► 2. Flushing MemTable (if flush in progress)
        ├─► 3. SSTables          newest → oldest
        │       ├─ Bloom filter  mightContain(key)?  O(k) hashes, zero I/O on miss
        │       ├─ Summary index binary search → index file range
        │       ├─ Index.db scan → data file offset
        │       └─ Data.db read  → deserialise partition
        └─► 4. Cold Tier         download Filter.db first, then Data.db if needed

                        COMPACTION (background)
                        ───────────────────────
  Size-Tiered Compaction Strategy (STCS) — Cassandra's default

  Every 2s: group SSTables into size buckets
            if any bucket has ≥ 4 SSTables of similar size → compact
            k-way merge sort → new SSTable
            tombstones past GC grace period → purged
            input SSTables → deleted
```

---

## Cassandra Mapping

Every component maps to a named Cassandra internal:

| Aegis-Storage | Cassandra | cassandra.yaml key |
|---|---|---|
| `CommitLog` | `CommitLog` | `commitlog_sync = periodic` |
| `CommitLogSegment` | `CommitLogSegment` | `commitlog_segment_size_in_mb = 32` |
| `CommitLogPosition` | `ReplayPosition` | (internal) |
| `MemTable` | `MemTable` | `memtable_heap_space_in_mb` |
| `PartitionKey.token()` | `Murmur3Partitioner` | `partitioner = Murmur3Partitioner` |
| `BloomFilter` | `BloomFilter` | `bloom_filter_fp_chance = 0.01` |
| `SSTableWriter` | `SSTableWriter` | `sstable_compression` |
| `SSTableReader` | `SSTableReader` | (internal) |
| `STCSCompactor` | `SizeTieredCompactionStrategy` | `compaction = {'class': 'STCS'}` |
| `GC_GRACE_SECONDS` | `gc_grace_seconds` | `gc_grace_seconds = 864000` |
| `ColdTierManager` | Instaclustr Tiered Storage | (platform-level, not OSS) |

---

## Project Structure

```
src/main/java/com/aegis/
├── core/
│   ├── Row.java              PartitionKey (Murmur3), Cell (live/tombstone/TTL), Row
│   ├── StorageConfig.java    cassandra.yaml equivalents, env-var overrides for Docker
│   └── StorageException.java Typed exceptions matching Cassandra's hierarchy
│
├── commitlog/
│   ├── CommitLogSegment.java Single .clog file — CRC32C framing, append, replay
│   └── CommitLog.java        Segment lifecycle, PERIODIC fsync, crash recovery
│
├── memtable/
│   └── MemTable.java         ConcurrentSkipListMap, flush lifecycle (ACTIVE→FLUSHING→FLUSHED)
│
├── sstable/
│   ├── BloomFilter.java      Optimal m/k from n and FPP, double-hashing, serialization
│   ├── SSTableWriter.java    Writes Data + Index + Filter + Statistics + Summary components
│   └── SSTableReader.java    4-step read: Bloom → Summary binary search → Index scan → Data
│
├── compaction/
│   └── STCSCompactor.java    STCS bucketing (bucket_low/high), k-way merge, tombstone purge
│
├── tiering/
│   └── ColdTierManager.java  MinIO offload, transparent remote read, Bloom-first strategy
│
├── StorageEngine.java        Top-level coordinator — write path, read path, background daemons
│
├── cli/
│   └── StorageEngineShell.java  6-act interactive demo shell
│
└── benchmark/
    └── StorageBenchmark.java    JMH — Murmur3, Bloom filter, CommitLog, row merge, e2e write/read
```

---

## Running

### Docker (recommended)

```bash
# Full automated demo (6 acts, then exits)
docker compose run --rm demo

# Interactive shell — type commands yourself
docker compose run --rm engine

# Run the JUnit 5 test suite
docker compose run --rm test

# JMH benchmarks
docker compose run --rm bench

# MinIO console — view uploaded cold-tier SSTables
# Open http://localhost:9001 (user: aegisadmin / pass: aegisadmin)
docker compose up minio
```

### Local (requires Java 21 + Maven)

```bash
./scripts/run.sh build      # compile
./scripts/run.sh shell      # interactive shell
./scripts/run.sh demo       # automated demo (no MinIO required)
./scripts/run.sh test       # JUnit 5 suite
./scripts/run.sh bench      # JMH benchmarks
./scripts/run.sh shell-cold # with cold tier (starts MinIO via Homebrew)
```

---

## Demo Walkthrough

The `demo` command runs 6 acts automatically. Here is what each act shows and why it matters:

### Act 1 — Write Path: CommitLog + MemTable
```
Written: cassandra, kafka, clickhouse (3 rows, 2 columns each)
Reading 'cassandra' from MemTable... ✓ Found key=cassandra (41µs)
    version  = 5.0        ts=1735000000000000  flags=0x00
    type     = wide-column store
```
Shows the CommitLog-first write order and immediate MemTable readability.

### Act 2 — MemTable Flush → SSTable
```
Blasting 500 writes... ✓ 500/500 written in 312.4ms = 1.6K writes/sec
Flushing MemTable → SSTable...
✓ Flush complete. SSTables: 0 → 1
Reading 'kafka' — now from SSTable (Bloom filter → Index → Data)...
✓ Found key=kafka (118µs)
```
Shows the flush threshold triggering, the five SSTable component files being written, and the read path switching from MemTable to SSTable transparently.

### Act 3 — Bloom Filter: Zero I/O for Missing Keys
```
Reading 'nonexistent-key-xyz'...
Result: Not found ✓  (12µs — Bloom filter short-circuited all disk reads)
```
At 1% FPP, 99% of missing-key lookups skip all disk I/O. The latency difference between a Bloom miss (12µs) and a disk read (100–500µs) is visible in the numbers.

### Act 4 — Tombstones: LSM Delete Semantics
```
Written: toDelete.col1 = original-value
Deleted: toDelete.col1 (tombstone inserted)
Reading toDelete... col1 = <tombstone>  flags=0x01
```
In LSM-trees, deletes are writes. The tombstone at a higher timestamp shadows the original value. Physical removal happens during compaction after GC grace period.

### Act 5 — Size-Tiered Compaction (STCS)
```
SSTable count before compaction: 4
Waiting for STCS daemon...
SSTable count after compaction:  1
Tombstones purged: 1   Bytes reclaimed: 12KB
```
Four SSTables of similar size form a STCS bucket. The compactor runs a k-way merge sort, drops the tombstone from Act 4 (GC_GRACE_SECONDS=0 in demo), and produces one output SSTable.

### Act 6 — Engine Metrics
```
┌─────────────────────────────────────────────────────────┐
│                  STORAGE ENGINE STATS                   │
├─────────────────────────────────────────────────────────┤
│  Writes:     1703        CommitLog writes: 1703         │
│  Reads:      8           Hit ratio:        100.0%       │
│  MemTable:   3 hits      SSTables: 5 hits               │
│  Compactions: 1  tombstones purged: 1                   │
│  JVM heap used: 44MB / 256MB  (well within budget)      │
└─────────────────────────────────────────────────────────┘
```

---

## CLI Commands

Once inside the interactive shell (`docker compose run --rm engine`):

| Command | What it does |
|---|---|
| `write <key> <col> <val>` | CommitLog append + MemTable write |
| `read <key>` | Full read path, prints which tier served the result |
| `delete <key> <col>` | Insert tombstone cell |
| `blast <n>` | Write n rows via Virtual Thread pool, prints throughput |
| `flush` | Force MemTable → SSTable flush |
| `compact` | Trigger STCS compaction cycle |
| `stats` | Full engine metrics snapshot |
| `demo` | Run the 6-act automated demo |
| `quit` | Graceful shutdown |

---

## JVM Configuration

```
--enable-preview   Java 21 preview features (records, pattern matching)
-Xmx256m           Hard heap cap — the same constraint as the M1 dev machine
-XX:+UseZGC        Sub-millisecond GC pauses — critical for p99 write latency
```

The heap gauge in the stats output proves the engine stays well under 256MB even during blast mode. Off-heap `ByteBuffer.allocateDirect()` is used for Cell values to keep GC pressure low.

---

## Benchmarks (expected on Apple Silicon M1)

| Benchmark | What it measures | Expected |
|---|---|---|
| `benchmarkMurmur3Token` | Partition key hashing | ~50–100ns |
| `benchmarkBloomFilterMiss` | Definitive miss (zero disk I/O path) | ~100–200ns |
| `benchmarkBloomFilterHit` | Probable hit check | ~100–200ns |
| `benchmarkCommitLogSerialization` | Row → bytes, no I/O | ~500ns–2µs |
| `benchmarkCommitLogAppend` | Full FileChannel write, PERIODIC mode | ~1–5µs |
| `benchmarkRowMerge` | Compaction reconciliation per row | ~200–500ns |
| `benchmarkFullEngineWrite` | CommitLog → MemTable e2e | ~5–20µs |
| `benchmarkEngineReadMemTable` | ConcurrentSkipListMap.get() | ~200–500ns |

Run: `docker compose run --rm bench`

---

## Design Decisions

**Why `ConcurrentSkipListMap` for MemTable?**
Cassandra uses an `AtomicBTreePartition` (a B-tree variant) for better cache locality. We use `ConcurrentSkipListMap` because it provides equivalent O(log n) operations, lock-free reads, and guaranteed token-sorted iteration for flush — the properties that matter most. The trade-off is noted as an extension point.

**Why PERIODIC fsync, not BATCH?**
PERIODIC mode (`commitlog_sync = periodic`) is Cassandra's production default. It gives ~200ms of potential data loss on crash in exchange for throughput that's 3–10× higher than BATCH mode. The sync mode is configurable via `StorageConfig.COMMITLOG_SYNC_MODE`.

**Why sparse index, not dense?**
A dense index for 10M partitions at 32 bytes/entry = 320MB — more than our entire heap budget. Cassandra's sparse partition index (one entry per `column_index_size_in_kb = 64KB` of data) gives O(sample_size) binary search resolution with < 1MB of index for the same dataset.

**Why Bloom filter before index lookup?**
At 1% FPP, 99% of non-existent key lookups are eliminated before touching the index file. For read-heavy workloads with high miss rates (e.g., cache miss path), this is the single most impactful optimisation in the entire read path.

**Why MinIO for cold tier?**
MinIO is S3-compatible, runs as a single binary (no Docker required locally), and is the same API that Instaclustr's platform uses for tiered storage on AWS. The `MINIO_ENDPOINT` environment variable allows the same code to connect to real AWS S3 by changing one line in `docker-compose.yml`.
