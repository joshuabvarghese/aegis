package com.aegis;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.recovery.CrashRecovery;
import com.aegis.replication.PrimaryReplicator;
import com.aegis.replication.ReplicationServer;
import com.aegis.storage.HotStorageEngine;
import com.aegis.tiering.TieringEngine;
import com.aegis.tiering.RemoteCatalog;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Top-level orchestrator for a single Aegis-WAL node.
 *
 * Lifecycle:
 *   1. Run crash recovery scan
 *   2. Start tiering engine
 *   3. Open hot storage at recovered offset
 *   4. Start replication server (listen for peers)
 *   5. If primary: connect to follower peers
 */
public class AegisNode {

    public enum Role { PRIMARY, FOLLOWER }

    private final int nodeId;
    private final Role role;
    private final AtomicBoolean alive = new AtomicBoolean(false);
    private final AtomicLong writesAccepted = new AtomicLong(0);
    private final AtomicLong writesRejected = new AtomicLong(0);

    private TieringEngine tieringEngine;
    private HotStorageEngine hotStorage;
    private ReplicationServer replServer;
    private PrimaryReplicator primaryReplicator;

    // Health tracking for TUI
    private long heapUsedMB = 0;
    private long directUsedMB = 0;
    private String lastStatus = "STARTING";

    public AegisNode(int nodeId, Role role) {
        this.nodeId = nodeId;
        this.role = role;
    }

    public void start() throws IOException {
        // Step 1: Crash recovery
        CrashRecovery recovery = new CrashRecovery(nodeId);
        var result = recovery.recover();
        EventBus.get().publish(EventBus.EventType.RECOVERY_SCAN, nodeId,
            "Recovery: " + result.recordsRecovered() + " records, " +
            result.tornWritesTruncated() + " torn writes truncated");

        // Step 2: Tiering engine (MinIO client)
        tieringEngine = new TieringEngine(nodeId);

        // Step 3: Hot storage at recovered offset
        hotStorage = new HotStorageEngine(nodeId, tieringEngine);

        // Step 4: Start replication server
        replServer = new ReplicationServer(nodeId, hotStorage);
        replServer.start();

        // Step 5: If primary, connect to peers
        if (role == Role.PRIMARY) {
            primaryReplicator = new PrimaryReplicator(nodeId, hotStorage);
            primaryReplicator.connectToPeers();
        }

        alive.set(true);
        lastStatus = "HEALTHY";

        EventBus.get().publish(EventBus.EventType.NODE_JOINED, nodeId,
            "Node-" + nodeId + " (" + role + ") online port=" + AegisConfig.NODE_PORTS[nodeId]);
    }

    /**
     * Append a payload — primary uses quorum write, followers accept local only.
     */
    public long append(byte[] payload) throws Exception {
        if (!alive.get()) throw new IllegalStateException("Node-" + nodeId + " is not alive");

        try {
            long offset;
            if (role == Role.PRIMARY && primaryReplicator != null) {
                offset = primaryReplicator.appendWithQuorum(payload);
            } else {
                offset = hotStorage.append(payload);
            }
            writesAccepted.incrementAndGet();
            return offset;
        } catch (Exception e) {
            writesRejected.incrementAndGet();
            throw e;
        }
    }

    /** Kill this node (simulate crash or hard kill). */
    public void kill() {
        alive.set(false);
        lastStatus = "DEAD";
        if (replServer != null) replServer.stop();
        if (primaryReplicator != null) primaryReplicator.shutdown();
        try {
            if (hotStorage != null) hotStorage.shutdown();
        } catch (IOException ignored) {}
        EventBus.get().publish(EventBus.EventType.CHAOS_KILL, nodeId, "💀 Node-" + nodeId + " KILLED");
    }

    /** Restart a previously killed node with fresh recovery. */
    public void restart() throws IOException {
        EventBus.get().publish(EventBus.EventType.NODE_RECOVERED, nodeId,
            "Node-" + nodeId + " restarting...");
        start();
        EventBus.get().publish(EventBus.EventType.NODE_RECOVERED, nodeId,
            "Node-" + nodeId + " recovered and rejoined cluster");
    }

    /** Simulate network partition (not a kill). */
    public void partition(boolean isolated) {
        if (replServer != null) replServer.simulatePartition(isolated);
        lastStatus = isolated ? "PARTITIONED" : "HEALTHY";
    }

    /** Force manual segment roll and tier upload. */
    public void forceTierFlush() throws IOException {
        if (hotStorage != null) hotStorage.forceRoll();
    }

    // --- Metrics for TUI ---
    public void refreshMemMetrics() {
        Runtime rt = Runtime.getRuntime();
        heapUsedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    public int     nodeId()               { return nodeId; }
    public Role    role()                  { return role; }
    public boolean isAlive()               { return alive.get(); }
    public String  status()                { return alive.get() ? lastStatus : "DEAD"; }
    public long    heapUsedMB()            { return heapUsedMB; }
    public long    writesAccepted()        { return writesAccepted.get(); }
    public long    writesRejected()        { return writesRejected.get(); }
    public long    currentOffset()         { return hotStorage != null ? hotStorage.currentOffset() : 0; }
    public long    activeSegmentSize()     { return hotStorage != null ? hotStorage.activeSegmentSize() : 0; }
    public int     closedSegmentCount()    { return hotStorage != null ? hotStorage.closedSegmentCount() : 0; }
    public HotStorageEngine hotStorage()   { return hotStorage; }
    public boolean isPartitioned()         { return replServer != null && replServer.isPartitioned(); }
}
