package com.aegis.core;

/**
 * Typed exceptions for Aegis-Storage.
 *
 * Cassandra uses a hierarchy of StorageException types internally.
 * We model the same pattern so the exception semantics are familiar
 * to anyone who has read Cassandra's source code.
 */
public final class StorageException {

    /** Thrown when CRC validation fails on a CommitLog segment or SSTable block. */
    public static final class CorruptionException extends RuntimeException {
        private final String filePath;
        private final long   fileOffset;

        public CorruptionException(String filePath, long fileOffset, String message) {
            super("Corruption at %s+%d: %s".formatted(filePath, fileOffset, message));
            this.filePath   = filePath;
            this.fileOffset = fileOffset;
        }

        public CorruptionException(String filePath, long fileOffset, String message, Throwable cause) {
            super("Corruption at %s+%d: %s".formatted(filePath, fileOffset, message), cause);
            this.filePath   = filePath;
            this.fileOffset = fileOffset;
        }

        public String filePath()   { return filePath; }
        public long   fileOffset() { return fileOffset; }
    }

    /** Thrown when a flush, compaction, or write fails due to an I/O error. */
    public static final class FlushException extends RuntimeException {
        public FlushException(String message, Throwable cause) { super(message, cause); }
        public FlushException(String message)                   { super(message); }
    }

    /** Thrown when a read fails — key not found is NOT an exception, returns Optional.empty(). */
    public static final class ReadException extends RuntimeException {
        public ReadException(String message, Throwable cause) { super(message, cause); }
        public ReadException(String message)                   { super(message); }
    }

    /** Thrown when compaction encounters unrecoverable SSTable data. */
    public static final class CompactionException extends RuntimeException {
        public CompactionException(String message, Throwable cause) { super(message, cause); }
        public CompactionException(String message)                   { super(message); }
    }

    private StorageException() {}
}
