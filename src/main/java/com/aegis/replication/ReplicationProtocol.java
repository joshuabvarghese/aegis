package com.aegis.replication;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Wire protocol for inter-node replication messages.
 *
 * Message frame: [MsgType:1B][NodeId:4B][Offset:8B][PayloadLen:4B][Payload:varB]
 */
public final class ReplicationProtocol {

    public enum MsgType {
        APPEND_REQUEST((byte)0x01),
        APPEND_ACK    ((byte)0x02),
        APPEND_NACK   ((byte)0x03),
        HEARTBEAT     ((byte)0x04),
        HEARTBEAT_ACK ((byte)0x05);

        public final byte code;
        MsgType(byte code) { this.code = code; }

        public static MsgType fromCode(byte code) {
            for (MsgType t : values()) if (t.code == code) return t;
            throw new IllegalArgumentException("Unknown MsgType: " + code);
        }
    }

    public record ReplicationMessage(
        MsgType type,
        int     sourceNodeId,
        long    offset,
        byte[]  payload
    ) {
        public byte[] serialize() throws IOException {
            int payloadLen = payload == null ? 0 : payload.length;
            ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 8 + 4 + payloadLen);
            buf.put(type.code);
            buf.putInt(sourceNodeId);
            buf.putLong(offset);
            buf.putInt(payloadLen);
            if (payload != null) buf.put(payload);
            return buf.array();
        }

        public static ReplicationMessage deserialize(DataInputStream in) throws IOException {
            byte typeByte   = in.readByte();
            int  sourceNode = in.readInt();
            long offset     = in.readLong();
            int  payloadLen = in.readInt();
            byte[] payload  = null;
            if (payloadLen > 0) {
                payload = in.readNBytes(payloadLen);
            }
            return new ReplicationMessage(MsgType.fromCode(typeByte), sourceNode, offset, payload);
        }

        public static ReplicationMessage appendRequest(int nodeId, long offset, byte[] payload) {
            return new ReplicationMessage(MsgType.APPEND_REQUEST, nodeId, offset, payload);
        }
        public static ReplicationMessage ack(int nodeId, long offset) {
            return new ReplicationMessage(MsgType.APPEND_ACK, nodeId, offset, null);
        }
        public static ReplicationMessage nack(int nodeId, long offset) {
            return new ReplicationMessage(MsgType.APPEND_NACK, nodeId, offset, null);
        }
        public static ReplicationMessage heartbeat(int nodeId) {
            return new ReplicationMessage(MsgType.HEARTBEAT, nodeId, -1, null);
        }
        public static ReplicationMessage heartbeatAck(int nodeId) {
            return new ReplicationMessage(MsgType.HEARTBEAT_ACK, nodeId, -1, null);
        }
    }

    private ReplicationProtocol() {}
}
