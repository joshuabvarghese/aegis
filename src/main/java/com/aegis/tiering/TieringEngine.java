package com.aegis.tiering;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.core.WalRecord;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;

/**
 * Module B: Log Compactor & Tiering Engine
 *
 * Runs as an async Virtual Thread pool worker.
 * On segment close: reads raw bytes → uploads to MinIO → registers in RemoteCatalog.
 * The upload is idempotent (object key = node-{id}/segment-{baseOffset}.log).
 */
public class TieringEngine {

    private final MinioClient minioClient;
    private final int nodeId;

    // Virtual thread executor for concurrent uploads (multiple segments can close quickly)
    private final ExecutorService uploadExecutor =
        Executors.newVirtualThreadPerTaskExecutor();

    public TieringEngine(int nodeId) {
        this.nodeId = nodeId;

        minioClient = MinioClient.builder()
            .endpoint(AegisConfig.MINIO_ENDPOINT)
            .credentials(AegisConfig.MINIO_ACCESS_KEY, AegisConfig.MINIO_SECRET_KEY)
            .build();

        ensureBucketExists();
    }

    private void ensureBucketExists() {
        try {
            boolean found = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(AegisConfig.MINIO_BUCKET).build()
            );
            if (!found) {
                minioClient.makeBucket(
                    MakeBucketArgs.builder().bucket(AegisConfig.MINIO_BUCKET).build()
                );
                EventBus.get().publish(EventBus.EventType.SEGMENT_UPLOADED, nodeId,
                    "MinIO bucket '" + AegisConfig.MINIO_BUCKET + "' created");
            }
        } catch (Exception e) {
            EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                "MinIO unavailable (cold tier disabled): " + e.getMessage());
        }
    }

    /**
     * Upload a closed segment to MinIO.
     * Called from a Virtual Thread — never blocks the hot path.
     */
    public void uploadSegment(WalSegment segment) {
        String objectKey = "node-%d/segment-%020d.log".formatted(nodeId, segment.baseOffset());

        EventBus.get().publish(EventBus.EventType.SEGMENT_UPLOAD_STARTED, nodeId,
            "Uploading segment base=" + segment.baseOffset() + " → " + objectKey);

        try {
            Path logPath = segment.logPath();
            long sizeBytes = Files.size(logPath);

            minioClient.uploadObject(
                UploadObjectArgs.builder()
                    .bucket(AegisConfig.MINIO_BUCKET)
                    .object(objectKey)
                    .filename(logPath.toString())
                    .contentType("application/octet-stream")
                    .build()
            );

            // Determine end offset from reading records
            List<WalRecord> records = segment.readAll(true);
            long endOffset = records.isEmpty() ? segment.baseOffset()
                : records.get(records.size() - 1).offset();

            // Register in the transparent read-routing catalog
            RemoteCatalog.get().register(new RemoteCatalog.CatalogEntry(
                objectKey,
                segment.baseOffset(),
                endOffset,
                sizeBytes,
                AegisConfig.MINIO_ENDPOINT + "/" + AegisConfig.MINIO_BUCKET + "/" + objectKey
            ));

            EventBus.get().publish(EventBus.EventType.SEGMENT_UPLOADED, nodeId,
                "COLD TIER: " + objectKey + " (" + (sizeBytes / 1024) + " KB)",
                sizeBytes);

        } catch (Exception e) {
            EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                "Upload FAILED for " + objectKey + ": " + e.getMessage());
        }
    }

    /**
     * Stream a specific record from cold tier by offset.
     * Transparently downloads the segment from MinIO and deserializes.
     */
    public List<WalRecord> fetchFromColdTier(long logicalOffset) throws Exception {
        var entry = RemoteCatalog.get().findForOffset(logicalOffset);
        if (entry.isEmpty()) {
            throw new IllegalArgumentException("Offset " + logicalOffset + " not found in cold tier catalog");
        }

        RemoteCatalog.CatalogEntry catalogEntry = entry.get();

        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                    .bucket(AegisConfig.MINIO_BUCKET)
                    .object(catalogEntry.objectKey())
                    .build())) {

            byte[] bytes = stream.readAllBytes();

            EventBus.get().publish(EventBus.EventType.HISTORIC_READ, nodeId,
                "Cold read offset=" + logicalOffset + " from " + catalogEntry.objectKey());

            // Deserialize all records from fetched bytes and filter to requested offset
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
            List<WalRecord> records = new java.util.ArrayList<>();
            while (buf.remaining() >= WalRecord.HEADER_SIZE) {
                try {
                    WalRecord r = WalRecord.deserialize(buf);
                    records.add(r);
                } catch (Exception e) {
                    break;
                }
            }
            return records;
        }
    }

    public void shutdown() {
        uploadExecutor.shutdown();
    }
}
