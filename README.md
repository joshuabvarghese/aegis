# Aegis

**A from-scratch implementation of the LSM-tree storage engine underlying Apache Cassandra's write path.**

CommitLog → MemTable → SSTable flush → Size-Tiered Compaction. Built in Java 21 with zero framework dependencies. Fully containerised — one command to run.


---

## One-command demo

```bash
git clone https://github.com/joshuabvarghese/aegis
cd aegis
docker compose run --rm demo
```

That's it. Docker pulls the JDK/JRE base image, compiles the JAR, boots the engine, runs the 6-act demo, and exits. No Java installation, no Maven setup needed.

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
  │  *.clog  │  [Length:4B][Record:nB][CRC32C:4B]           │
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
  │  ├── {gen}-Filter.db      Bloom filter (1% FPP)         │
  │  ├── {gen}-Statistics.db  metadata + CommitLog pos      │
  │  └── {gen}-Summary.db     sampled index summary         │
  └─────────────────────────────────────────────────────────┘

                          READ PATH
                          ─────────
  client.read(key)
        │
        ├─► 1. MemTable          ConcurrentSkipListMap.get()   O(log n)
        ├─► 2. Flushing MemTable (if flush in progress)
        └─► 3. SSTables          newest → oldest
                ├─ Bloom filter  mightContain(key)?  O(k) hashes, zero I/O on miss
                ├─ Summary index binary search → index file range
                ├─ Index.db scan → data file offset
                └─ Data.db read  → deserialise partition

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
| `LeveledCompactor` | `LeveledCompactionStrategy` | `compaction = {'class': 'LCS'}` |
| `GC_GRACE_SECONDS` | `gc_grace_seconds` | `gc_grace_seconds = 864000` |

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
│   ├── CompactionStrategy.java  Shared interface: plan()/execute(), implemented by both below
│   ├── STCSCompactor.java       STCS bucketing (bucket_low/high), k-way merge, tombstone purge
│   └── LeveledCompactor.java    LCS — non-overlapping levels, fixed-point overlap expansion
│
├── StorageEngine.java        Top-level coordinator — write path, read path, background daemons
│
├── http/
│   └── AegisHttpServer.java  Zero-dependency HTTP wrapper (com.sun.net.httpserver) — used by Cloud Run
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

# HTTP server on http://localhost:8080 (same wrapper used on Cloud Run)
docker compose up serve
```

### Local (requires Java 21 + Maven)

```bash
./scripts/run.sh build      # compile
./scripts/run.sh shell      # interactive shell
./scripts/run.sh demo       # automated demo
./scripts/run.sh test       # JUnit 5 suite
./scripts/run.sh bench      # JMH benchmarks
./scripts/run.sh serve      # HTTP server on $PORT (default 8080)
```

---

## Crash / Durability Testing

Every write that gets an HTTP 200 from `/write` is a durability promise. `scripts/chaos-crash-test.sh` verifies that promise for real: it writes a burst of rows against a live engine, sends the container a hard `SIGKILL` mid-burst (no graceful shutdown, no final flush — the same as pulling the power), restarts it from the same volume, and checks that every acknowledged write actually came back after CommitLog replay.

```bash
# Default: 400 writes, kill at 200, PERIODIC sync (the production default)
./scripts/chaos-crash-test.sh

# Zero-loss guarantee: fsync before every acknowledged write
./scripts/chaos-crash-test.sh 400 200 BATCH
```

The two sync modes give two different honest outcomes, matching Cassandra's real `commitlog_sync` trade-off:

- **PERIODIC** (default, fsyncs every 200ms) — writes acknowledged inside that 200ms window can legitimately be lost on a hard crash. The script reports this as expected behaviour, not a failure, and shows exactly how many (and which) writes fell in that window.
- **BATCH** (fsyncs before acknowledging every write) — the script asserts **zero** data loss. If even one acknowledged row goes missing in this mode, the script exits non-zero, because that would be a real bug.

This is the same crash-recovery guarantee every real LSM-tree database depends on — the CommitLog is what makes an in-memory MemTable safe to use at all.

The JUnit suite also includes a concurrency stress section (`I. Concurrency Stress` in `StorageEngineTest.java`) that hammers the engine with dozens of virtual threads doing simultaneous writes, same-key contention, and reads-during-writes, asserting zero lost writes, zero torn values, and zero exceptions under load. Run it with `docker compose run --rm test` or `./scripts/run.sh test`.

---

## Compaction Strategies: STCS vs LCS

Every LSM-tree compaction strategy is a choice about which two points of the **write / read / space amplification** triangle to optimise for, at the expense of the third. Aegis-Storage ships two real implementations of the trade-off, not just one with the other described in a comment:

- **STCS** (Size-Tiered, the default — Cassandra's default too) merges SSTables of similar size together, regardless of key range. Low write amplification, but SSTables can overlap freely, so a point read may have to check every SSTable on disk.
- **LCS** (Leveled — RocksDB/LevelDB's default, also selectable in real Cassandra) enforces that SSTables within the same level (L1+) never overlap in key range. A point read touches at most one SSTable per level — a large read-amplification win — at the cost of much higher write amplification, since data gets rewritten every time its level compacts into the next.

Switch strategies with an environment variable, no rebuild required:

```bash
docker compose run --rm -e COMPACTION_STRATEGY=LCS engine   # interactive shell, LCS active
docker run -e COMPACTION_STRATEGY=LCS aegis-storage:latest serve
```

Both strategies implement the same `CompactionStrategy` interface (`plan()` / `execute()`), so `StorageEngine` itself doesn't know or care which one is running — and the read path is level-aware regardless of which strategy produced the catalog: level 0 is always checked exhaustively (newest first, since it can overlap), while level 1+ is range-checked to find the single SSTable that could possibly hold the key.

**A real numbers, not a claim:**

```bash
./scripts/compare-compaction-strategies.sh
```

This runs an identical write → flush → read workload against two containers — one on STCS, one on LCS — and prints a measured side-by-side comparison: average SSTables opened per read, total bytes written during compaction, and the resulting per-level structure of each. Increase the write count (first argument) if your run doesn't show LCS splitting past L1 yet — the read-amplification gap widens as the dataset grows, which is the whole point.

**Known simplifications versus production LCS** (documented in `LeveledCompactor`'s class javadoc too): a single output SSTable per compaction round rather than splitting into fixed-size files the way RocksDB does, and picking the *largest* SSTable in an over-budget level to push down rather than LevelDB's persistent round-robin cursor. Both are reasonable at this project's data volumes; a production implementation would need to revisit them at real-world scale.

One correctness subtlety worth calling out explicitly: expanding "which next-level SSTables overlap the one being pushed down" cannot stop after a single pass. Pulling in one overlapping table can widen the merged range enough to newly overlap a *second* table that didn't intersect the original range at all — miss that, and the non-overlap invariant for the level silently breaks. `LeveledCompactor.expandToOverlapClosure()` keeps adding overlapping tables until a full pass adds none, which is the actual requirement, not a single-hop check. `StorageEngineTest`'s `J. Leveled Compaction` section tests this directly, along with an end-to-end test that runs real writes and compactions through the full engine and asserts no two SSTables in the same level ever overlap.

---

## Live Dashboard

`dashboard/index.html` is a single, dependency-free static file that polls `/stats` and renders the engine's live read path and throughput. It works against either a local `serve` container or a deployed Cloud Run URL — just point it at the right address.

```bash
docker compose up serve          # engine listening on :8080
open dashboard/index.html        # (or just double-click it)
```

By default it polls `http://localhost:8080`. Change the URL field at the top-right to point at any running instance, including your Cloud Run service URL.

---

## Deploying to Google Cloud Run

The project ships with a one-command deploy script that builds the image with Cloud Build (no local Docker needed) and deploys it to Cloud Run's always-free tier.

```bash
# Prerequisites: gcloud CLI installed and authenticated (gcloud auth login),
# and a GCP project with billing enabled (required to enable the APIs below,
# even though the deployment itself stays within the free tier).

./deploy/cloudrun-deploy.sh YOUR_GCP_PROJECT_ID [REGION]
# REGION defaults to us-central1
```

This enables the Cloud Run, Artifact Registry, and Cloud Build APIs, builds the image, and deploys it running in `serve` mode (the HTTP wrapper), scaling to zero when idle. At the end it prints the public service URL.

Once deployed:
```bash
curl https://YOUR-SERVICE-URL/healthz
curl -X POST "https://YOUR-SERVICE-URL/write?key=cassandra&col=version&val=5.0"
curl "https://YOUR-SERVICE-URL/read?key=cassandra"
curl -X POST "https://YOUR-SERVICE-URL/flush"
curl "https://YOUR-SERVICE-URL/stats"
curl "https://YOUR-SERVICE-URL/levels"
```

**Storage is ephemeral** — each new revision or scale-to-zero cold start begins with an empty CommitLog/MemTable/SSTables, since data lives on container-local disk. That's expected for a demo/portfolio deployment like this one, not a place to keep data you care about.

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
| `compact` | Trigger a compaction cycle (STCS or LCS, whichever is active) |
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

## Benchmarks

These numbers are measured, not guessed — run `./scripts/update-benchmark-numbers.sh` on your own machine any time to regenerate this table from a real JMH run in the same container image used everywhere else in this project. Until you run it, the table below reflects whichever machine last ran it.

<!-- BENCHMARK_TABLE_START -->
_Measured 2026-07-11 02:10 UTC on Darwin arm64, inside the same container image used everywhere else in this project. Re-run `./scripts/update-benchmark-numbers.sh` any time to refresh these numbers on your own hardware._

| Benchmark | What it measures | Avg time/op | Throughput |
|---|---|---|---|
| `benchmarkBloomFilterAdd` | Insert into Bloom filter | 0.026 us/op | 31.5 ops/us |
| `benchmarkBloomFilterHit` | Probable hit check | 0.0244 us/op | 39.1 ops/us |
| `benchmarkBloomFilterMiss` | Definitive miss (zero disk I/O path) | 0.02 us/op | 50.6 ops/us |
| `benchmarkCommitLogAppend` | Full FileChannel write, PERIODIC mode | 1.48 us/op | 0.669 ops/us |
| `benchmarkCommitLogSerialization` | Row to bytes, no I/O | 0.184 us/op | 4.96 ops/us |
| `benchmarkEngineReadMemTable` | ConcurrentSkipListMap.get() | 0.0265 us/op | 32.9 ops/us |
| `benchmarkFullEngineWrite` | CommitLog to MemTable end-to-end | 2.45 us/op | 0.404 ops/us |
| `benchmarkMurmur3Token` | Partition key hashing | 0.00782 us/op | 130 ops/us |
| `benchmarkRowMerge` | Compaction reconciliation per row | 0.0202 us/op | 50.9 ops/us |
| `benchmarkTombstonePurge` | Tombstone GC-grace check | 1.58 us/op | 0.677 ops/us |
<!-- BENCHMARK_TABLE_END -->

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
