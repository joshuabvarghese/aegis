package com.aegis.replication;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.replication.ReplicationProtocol.ReplicationMessage;
import com.aegis.storage.HotStorageEngine;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Module C: Primary Replication Client
 *
 * On each write, fans out APPEND_REQUEST to all follower nodes in parallel
 * via Virtual Threads. Collects ACKs; returns success only when quorum is met.
 *
 * Quorum: ⌊3/2⌋+1 = 2 nodes (including self) must acknowledge.
 */
public class PrimaryReplicator {

    private final int nodeId;
    private final HotStorageEngine localStorage;
    private final Map<Integer, NodeConnection> connections = new ConcurrentHashMap<>();

    // Virtual thread executor for parallel fan-out
    private final ExecutorService fanOutExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public PrimaryReplicator(int nodeId, HotStorageEngine localStorage) {
        this.nodeId = nodeId;
        this.localStorage = localStorage;
    }

    /** Connect to all peer follower nodes. */
    public void connectToPeers() {
        for (int i = 0; i < AegisConfig.NODE_PORTS.length; i++) {
            if (i == nodeId) continue; // skip self
            int peerId = i;
            Thread.ofVirtual().name("peer-connector-" + peerId).start(() -> {
                try {
                    Thread.sleep(500); // brief startup grace
                    NodeConnection conn = new NodeConnection(peerId, AegisConfig.NODE_PORTS[peerId]);
                    conn.connect();
                    connections.put(peerId, conn);
                    EventBus.get().publish(EventBus.EventType.NODE_JOINED, nodeId,
                        "Connected to peer node-" + peerId + " :" + AegisConfig.NODE_PORTS[peerId]);
                } catch (Exception e) {
                    EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                        "Cannot connect to peer-" + peerId + ": " + e.getMessage());
                }
            });
        }
    }

    /**
     * Write payload with quorum guarantee.
     * 1. Write locally (counts as 1 ACK).
     * 2. Fan out to all connected peers in parallel via Virtual Threads.
     * 3. Wait for enough ACKs to meet quorum within timeout.
     * 4. Return success or throw QuorumException.
     */
    public long appendWithQuorum(byte[] payload) throws Exception {
        // Step 1: Write to local storage
        long offset = localStorage.append(payload);

        // Step 2: Fan out to all connected peers
        List<CompletableFuture<Boolean>> peerAcks = new ArrayList<>();
        for (Map.Entry<Integer, NodeConnection> entry : connections.entrySet()) {
            int peerId = entry.getKey();
            NodeConnection conn = entry.getValue();

            CompletableFuture<Boolean> ackFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    ReplicationMessage req = ReplicationMessage.appendRequest(nodeId, offset, payload);
                    ReplicationMessage resp = conn.sendAndReceive(req);
                    boolean acked = resp.type() == ReplicationProtocol.MsgType.APPEND_ACK;
                    if (acked) {
                        EventBus.get().publish(EventBus.EventType.RECORD_REPLICATED, peerId,
                            "ACK from node-" + peerId + " offset=" + offset);
                    }
                    return acked;
                } catch (Exception e) {
                    EventBus.get().publish(EventBus.EventType.NODE_FAILED, peerId,
                        "Replication to node-" + peerId + " failed: " + e.getMessage());
                    return false;
                }
            }, fanOutExecutor);

            peerAcks.add(ackFuture);
        }

        // Step 3: Count ACKs — we already have local (=1), need QUORUM_SIZE-1 from peers
        int acksNeeded = AegisConfig.QUORUM_SIZE - 1;
        int acksReceived = 0;

        for (CompletableFuture<Boolean> future : peerAcks) {
            try {
                boolean acked = future.get(AegisConfig.REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (acked) {
                    acksReceived++;
                    if (acksReceived >= acksNeeded) break;
                }
            } catch (TimeoutException | ExecutionException e) {
                // Peer timed out — continue waiting for other peers
            }
        }

        int totalAcks = 1 + acksReceived; // local + peers
        if (totalAcks >= AegisConfig.QUORUM_SIZE) {
            EventBus.get().publish(EventBus.EventType.QUORUM_ACHIEVED, nodeId,
                "✓ QUORUM offset=" + offset + " acks=" + totalAcks + "/" + (connections.size() + 1));
            return offset;
        } else {
            EventBus.get().publish(EventBus.EventType.QUORUM_FAILED, nodeId,
                "✗ QUORUM FAILED offset=" + offset + " acks=" + totalAcks + "/" + AegisConfig.QUORUM_SIZE);
            throw new QuorumException("Quorum not met for offset " + offset +
                ": got " + totalAcks + " of " + AegisConfig.QUORUM_SIZE + " required");
        }
    }

    public void shutdown() {
        fanOutExecutor.shutdown();
        connections.values().forEach(NodeConnection::close);
    }

    /** Per-peer TCP connection with synchronized send/receive */
    private static class NodeConnection {
        private final int peerId;
        private final int port;
        private Socket socket;
        private DataOutputStream out;
        private DataInputStream  in;
        private final AtomicBoolean alive = new AtomicBoolean(false);

        NodeConnection(int peerId, int port) {
            this.peerId = peerId;
            this.port = port;
        }

        void connect() throws IOException {
            socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(AegisConfig.REPLICATION_TIMEOUT_MS);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            in  = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            alive.set(true);
        }

        synchronized ReplicationMessage sendAndReceive(ReplicationMessage msg) throws Exception {
            if (!alive.get()) throw new IOException("Connection to peer-" + peerId + " is dead");
            try {
                out.write(msg.serialize());
                out.flush();
                return ReplicationMessage.deserialize(in);
            } catch (IOException e) {
                alive.set(false);
                throw e;
            }
        }

        void close() {
            alive.set(false);
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        }

        boolean isAlive() { return alive.get(); }
    }

    public static class QuorumException extends Exception {
        public QuorumException(String message) { super(message); }
    }
}
