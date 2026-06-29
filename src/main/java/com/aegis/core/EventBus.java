package com.aegis.core;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.aegis.core.EventBus.AegisEvent;

/**
 * Lightweight in-process event bus.
 * Components publish typed events; the TUI subscribes to render updates
 * without any coupling to business logic.
 */
public class EventBus {

    public enum EventType {
        RECORD_APPENDED,
        RECORD_REPLICATED,
        QUORUM_ACHIEVED,
        QUORUM_FAILED,
        SEGMENT_ROLLED,
        SEGMENT_UPLOAD_STARTED,
        SEGMENT_UPLOADED,
        NODE_JOINED,
        NODE_FAILED,
        NODE_RECOVERED,
        RECOVERY_SCAN,
        FSYNC_COMPLETE,
        CHAOS_KILL,
        CHAOS_PARTITION,
        HISTORIC_READ
    }

    public record AegisEvent(EventType type, int nodeId, String message, long value) {
        public static AegisEvent of(EventType type, int nodeId, String message) {
            return new AegisEvent(type, nodeId, message, 0L);
        }
        public static AegisEvent of(EventType type, int nodeId, String message, long value) {
            return new AegisEvent(type, nodeId, message, value);
        }
    }

    private static final EventBus INSTANCE = new EventBus();
    private final CopyOnWriteArrayList<Consumer<AegisEvent>> listeners = new CopyOnWriteArrayList<>();

    private EventBus() {}

    public static EventBus get() { return INSTANCE; }

    public void subscribe(Consumer<AegisEvent> listener) {
        listeners.add(listener);
    }

    public void publish(AegisEvent event) {
        for (Consumer<AegisEvent> listener : listeners) {
            try {
                listener.accept(event);
            } catch (Exception ignored) {
                // Never let a bad TUI listener crash the storage engine
            }
        }
    }

    public void publish(EventType type, int nodeId, String message) {
        publish(AegisEvent.of(type, nodeId, message));
    }

    public void publish(EventType type, int nodeId, String message, long value) {
        publish(AegisEvent.of(type, nodeId, message, value));
    }
}
