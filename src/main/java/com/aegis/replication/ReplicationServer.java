package com.aegis.replication;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.replication.ReplicationProtocol.ReplicationMessage;
import com.aegis.storage.HotStorageEngine;

import java.io.*;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Module C: Replication Server (Follower side)
 *
 * Listens on a dedicated port for APPEND_REQUEST messages from the Primary node.
 * Writes the payload to local storage and sends back ACK/NACK.
 * Uses Virtual Threads — one per connection, ultra-lightweight.
 */
public class ReplicationServer {

    private final int nodeId;
    private final int port;
    private final HotStorageEngine storage;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean partitioned = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private final ExecutorService acceptor =
        Executors.newVirtualThreadPerTaskExecutor();

    public ReplicationServer(int nodeId, HotStorageEngine storage) {
        this.nodeId  = nodeId;
        this.port    = AegisConfig.NODE_PORTS[nodeId];
        this.storage = storage;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running.set(true);

        acceptor.submit(() -> {
            EventBus.get().publish(EventBus.EventType.NODE_JOINED, nodeId,
                "Replication server listening on :" + port);

            while (running.get() && !serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    // Each connection gets its own Virtual Thread
                    Thread.ofVirtual()
                        .name("repl-handler-" + nodeId + "-" + client.getPort())
                        .start(() -> handleConnection(client));
                } catch (IOException e) {
                    if (running.get()) {
                        EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                            "Accept error: " + e.getMessage());
                    }
                }
            }
        });
    }

    private void handleConnection(Socket socket) {
        try (socket;
             DataInputStream  in  = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            while (running.get()) {
                ReplicationMessage msg = ReplicationMessage.deserialize(in);

                if (partitioned.get()) {
                    // Simulate network partition — silently drop
                    continue;
                }

                switch (msg.type()) {
                    case APPEND_REQUEST -> {
                        try {
                            storage.append(msg.payload());
                            ReplicationMessage ack = ReplicationMessage.ack(nodeId, msg.offset());
                            out.write(ack.serialize());
                            out.flush();
                            EventBus.get().publish(EventBus.EventType.RECORD_REPLICATED, nodeId,
                                "REPLICATED offset=" + msg.offset() + " from node-" + msg.sourceNodeId());
                        } catch (Exception e) {
                            ReplicationMessage nack = ReplicationMessage.nack(nodeId, msg.offset());
                            out.write(nack.serialize());
                            out.flush();
                        }
                    }
                    case HEARTBEAT -> {
                        out.write(ReplicationMessage.heartbeatAck(nodeId).serialize());
                        out.flush();
                    }
                    default -> { /* ignore */ }
                }
            }
        } catch (EOFException ignored) {
            // Client disconnected cleanly
        } catch (IOException e) {
            if (running.get()) {
                EventBus.get().publish(EventBus.EventType.NODE_FAILED, nodeId,
                    "Connection dropped: " + e.getMessage());
            }
        }
    }

    /** Simulate chaos: partition this node from the network */
    public void simulatePartition(boolean partitioned) {
        this.partitioned.set(partitioned);
        if (partitioned) {
            EventBus.get().publish(EventBus.EventType.CHAOS_PARTITION, nodeId,
                "⚡ PARTITION: Node-" + nodeId + " network isolated");
        } else {
            EventBus.get().publish(EventBus.EventType.NODE_RECOVERED, nodeId,
                "Node-" + nodeId + " partition healed");
        }
    }

    public void stop() {
        running.set(false);
        try { serverSocket.close(); } catch (IOException ignored) {}
        acceptor.shutdownNow();
        EventBus.get().publish(EventBus.EventType.CHAOS_KILL, nodeId,
            "💀 KILL: Node-" + nodeId + " server stopped");
    }

    public boolean isRunning() { return running.get(); }
    public boolean isPartitioned() { return partitioned.get(); }
}
