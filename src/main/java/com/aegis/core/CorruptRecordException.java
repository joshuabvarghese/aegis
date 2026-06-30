package com.aegis.core;

public class CorruptRecordException extends RuntimeException {
    public CorruptRecordException(String message) {
        super(message);
    }
    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
