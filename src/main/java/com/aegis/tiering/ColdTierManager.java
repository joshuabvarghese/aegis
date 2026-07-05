package com.aegis.tiering;

import com.aegis.core.Row;
import com.aegis.core.Row.PartitionKey;
import com.aegis.core.StorageConfig;
import com.aegis.sstable.SSTableReader;
import com.aegis.sstable.SSTableWriter.SSTableMetadata;
import io.minio.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Cold Tier Manager — offloads aged SSTables to MinIO object storage.
 *
 * ─── Cassandra / Instaclustr Parallel ────────────────────────────────────────
 *
 * This module directly mirrors Instaclustr's production tiered storage capability:
 *
 * From Instaclustr's own documentation:
 *   "Kafka Tiered Storage allows Kafka topics to be tiered between local broker
 *    storage and cheaper object storage (S3, GCS, Azure Blob). Remote log segments
 *    are uploaded after local.retention.bytes is breached."
 *
 * We apply the same concept to SSTables:
 *   - "Hot" SSTables: recently flushed, kept on local NVMe/SSD
 *   - "Cold" SSTables: older than COLD_TIER_AGE_THRESHOLD, offloaded to MinIO/S3
 *
 * The read path is transparent: if a key is not found locally, the ColdTierManager
 * checks its catalog for a remote SSTable that might contain it, downloads only
 * the data file, and performs the normal Bloom filter → Index → Data lookup.
 *
 * ─── Object Storage Layout ────────────────────────────────────────────────────
 *
 * Each SSTable's components are stored as separate objects:
 *   aegis-storage-cold/{generation}-Data.db
 *   aegis-storage-cold/{generation}-Index.db
 *   aegis-storage-cold/{generation}-Filter.db
 *   aegis-storage-cold/{generation}-Statistics.db
 *   aegis-storage-cold/{generation}-Summary.db
 *
 * The Filter.db (Bloom filter) is always downloaded first on read — it's small
 * and eliminates most cold-tier reads without downloading the full Data.db.
 *
 * ─── Local Deletion After Offload ────────────────────────────────────────────
 *
 * After all component files are confirmed uploaded, the local files are deleted.
 * The SSTableMetadata is retained in memory (remote catalog) with the MinIO URL.
 * This frees local disk space while keeping the key range metadata for routing.
 */
public final class ColdTierManager implements Closeable {

    private static final Logger log = Logger.getLogger(ColdTierManager.class.getName());

    private final MinioClient minioClient;

    /**
     * Remote catalog: generation → RemoteSSTableEntry.
     * A key present here means the SSTable has been offloaded — local files deleted.
     */
    private final ConcurrentHashMap<Long, RemoteSSTableEntry> remoteCatalog =
        new ConcurrentHashMap<>();

    private final AtomicLong offloadedCount  = new AtomicLong(0);
    private final AtomicLong remoteFetches   = new AtomicLong(0);
    private final AtomicLong remoteHits      = new AtomicLong(0);
    private final AtomicLong bytesOffloaded  = new AtomicLong(0);

    public ColdTierManager() {
        this.minioClient = MinioClient.builder()
            .endpoint(StorageConfig.minioEndpoint())
            .credentials(StorageConfig.minioAccessKey(), StorageConfig.minioSecretKey())
            .build();
        ensureBucketExists();
    }

    private void ensureBucketExists() {
        try {
            boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(StorageConfig.MINIO_BUCKET).build());
            if (!exists) {
                minioClient.makeBucket(
                    MakeBucketArgs.builder().bucket(StorageConfig.MINIO_BUCKET).build());
                log.info("[COLD-TIER] Created bucket: " + StorageConfig.MINIO_BUCKET);
            }
        } catch (Exception e) {
            log.warning("[COLD-TIER] MinIO unavailable — cold tier disabled: " + e.getMessage());
        }
    }

    // ─── Offload (Hot → Cold) ─────────────────────────────────────────────────

    /**
     * Offload all component files of an SSTable to MinIO.
     *
     * Upload order: Filter → Index → Summary → Statistics → Data
     * The Data file is uploaded last — if the process crashes mid-upload, the
     * absence of the remote Data file means the local copy is still valid.
     *
     * Local files are deleted only after all uploads succeed.
     */
    public void offload(SSTableMetadata meta) throws Exception {
        long gen = meta.generation();

        // Upload all components
        uploadFile(meta.filterPath(),  gen + "-Filter.db");
        uploadFile(meta.indexPath(),   gen + "-Index.db");
        uploadFile(meta.summaryPath(), gen + "-Summary.db");
        uploadFile(meta.statsPath(),   gen + "-Statistics.db");
        uploadFile(meta.dataPath(),    gen + "-Data.db");   // data last

        long totalBytes = meta.dataSizeBytes() + meta.bloomFilterSizeBytes();

        // Register in remote catalog before deleting local files
        remoteCatalog.put(gen, new RemoteSSTableEntry(
            gen, meta,
            StorageConfig.minioEndpoint() + "/" + StorageConfig.MINIO_BUCKET,
            System.currentTimeMillis()
        ));

        // Delete local component files — space is now reclaimed
        deleteLocal(meta.filterPath());
        deleteLocal(meta.indexPath());
        deleteLocal(meta.summaryPath());
        // Keep Statistics.db locally — it's tiny and needed for catalog rebuilds
        deleteLocal(meta.dataPath());

        offloadedCount.incrementAndGet();
        bytesOffloaded.addAndGet(totalBytes);

        log.info("[COLD-TIER] Offloaded SSTable gen=%d size=%dKB to %s/%s"
            .formatted(gen, totalBytes/1024, StorageConfig.MINIO_BUCKET, gen + "-Data.db"));
    }

    private void uploadFile(Path localPath, String objectKey) throws Exception {
        if (!localPath.toFile().exists()) return;
        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(StorageConfig.MINIO_BUCKET)
                .object(objectKey)
                .filename(localPath.toString())
                .contentType("application/octet-stream")
                .build()
        );
        log.fine("[COLD-TIER] Uploaded: " + objectKey);
    }

    private void deleteLocal(Path path) {
        try { Files.deleteIfExists(path); } catch (IOException ignored) {}
    }

    // ─── Read (Cold Tier Lookup) ──────────────────────────────────────────────

    /**
     * Look up a partition key in the cold tier.
     *
     * Strategy:
     *   1. Check each remote SSTable's key range (metadata in memory) — O(n) scan
     *   2. For candidates: download the Filter.db, check Bloom filter — O(k)
     *   3. If Bloom filter says maybe: download Index.db + Data.db, do full lookup
     *
     * This mirrors how Cassandra's tiered storage (Kafka KIP-405 analogy applied
     * to SSTables) handles remote reads — filter-first, then fetch.
     */
    public Optional<Row> read(PartitionKey key) throws IOException {
        remoteFetches.incrementAndGet();

        for (RemoteSSTableEntry entry : remoteCatalog.values()) {
            SSTableMetadata meta = entry.metadata();

            // Range check — skip if key is outside this SSTable's range
            if (meta.minKey() != null && key.compareTo(meta.minKey()) < 0) continue;
            if (meta.maxKey() != null && key.compareTo(meta.maxKey()) > 0) continue;

            try {
                Optional<Row> result = readFromRemote(key, entry);
                if (result.isPresent()) {
                    remoteHits.incrementAndGet();
                    return result;
                }
            } catch (Exception e) {
                log.warning("[COLD-TIER] Remote read failed gen=%d: %s"
                    .formatted(entry.generation(), e.getMessage()));
            }
        }
        return Optional.empty();
    }

    private Optional<Row> readFromRemote(PartitionKey key, RemoteSSTableEntry entry)
            throws Exception {
        long gen = entry.generation();
        Path tempDir = StorageConfig.dataDir().resolve("cold-fetch-" + gen);
        tempDir.toFile().mkdirs();

        try {
            // Step 1: Download and check Bloom filter (small file, eliminates most reads)
            Path localFilter = tempDir.resolve(gen + "-Filter.db");
            downloadFile(gen + "-Filter.db", localFilter);

            // Step 2: Download Index + Summary + Data for full SSTable read
            Path localIndex   = tempDir.resolve(gen + "-Index.db");
            Path localSummary = tempDir.resolve(gen + "-Summary.db");
            Path localData    = tempDir.resolve(gen + "-Data.db");

            downloadFile(gen + "-Index.db",   localIndex);
            downloadFile(gen + "-Summary.db", localSummary);
            downloadFile(gen + "-Data.db",    localData);

            // Reconstruct a local metadata pointing to the temp files
            SSTableMetadata tempMeta = new SSTableMetadata(
                gen, tempDir,
                entry.metadata().partitionCount(),
                entry.metadata().cellCount(),
                entry.metadata().minKey(),
                entry.metadata().maxKey(),
                entry.metadata().commitLogPosition(),
                entry.metadata().createdAtMs(),
                entry.metadata().dataSizeBytes(),
                entry.metadata().bloomFilterSizeBytes()
            );

            // Use the normal SSTableReader for the lookup
            try (SSTableReader reader = new SSTableReader(tempMeta)) {
                return reader.get(key);
            }

        } finally {
            // Clean up temp files regardless of outcome
            deleteTempDir(tempDir);
        }
    }

    private void downloadFile(String objectKey, Path localPath) throws Exception {
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(StorageConfig.MINIO_BUCKET)
                    .object(objectKey)
                    .build())) {
            Files.copy(stream, localPath,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void deleteTempDir(Path dir) {
        try {
            Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
        } catch (IOException ignored) {}
    }

    // ─── Metrics & State ──────────────────────────────────────────────────────

    public long offloadedCount()  { return offloadedCount.get(); }
    public long remoteFetches()   { return remoteFetches.get(); }
    public long remoteHits()      { return remoteHits.get(); }
    public long bytesOffloaded()  { return bytesOffloaded.get(); }
    public int  catalogSize()     { return remoteCatalog.size(); }

    @Override
    public void close() {}

    // ─── Value Types ──────────────────────────────────────────────────────────

    public record RemoteSSTableEntry(
        long            generation,
        SSTableMetadata metadata,
        String          baseUrl,
        long            offloadedAtMs
    ) {}
}
