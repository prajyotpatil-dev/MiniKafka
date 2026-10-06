package com.minikafka.storage;

import com.minikafka.exception.StorageException;
import com.minikafka.model.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Handles serialization of a MiniKafka {@link Message} to and from an array of bytes.
 * <p>
 * Payload format (for a single record excluding the frame length prefix):
 * - messageId (UUID converted to string, UTF-8, int byte length prefixed)
 * - topic (UTF-8, int byte length prefixed)
 * - partition (int)
 * - offset (long)
 * - key (hasKey boolean, if true: UTF-8, int byte length prefixed)
 * - payload (UTF-8, int byte length prefixed)
 * - timestamp (long epoch milliseconds)
 * - producerId (hasProducerId boolean, if true: UTF-8, int byte length prefixed)
 */
public class MessageSerializer {

    /**
     * Serializes a message into a byte array.
     */
    public static byte[] serialize(Message message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writeString(dos, message.getMessageId());
            writeString(dos, message.getTopic());
            dos.writeInt(message.getPartition());
            dos.writeLong(message.getOffset());

            if (message.getKey() != null) {
                dos.writeBoolean(true);
                writeString(dos, message.getKey());
            } else {
                dos.writeBoolean(false);
            }

            writeString(dos, message.getPayload());
            dos.writeLong(message.getTimestamp().toEpochMilli());

            if (message.getProducerId() != null) {
                dos.writeBoolean(true);
                writeString(dos, message.getProducerId());
            } else {
                dos.writeBoolean(false);
            }
        }
        return baos.toByteArray();
    }

    /**
     * Deserializes a byte array into a Message.
     */
    public static Message deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (DataInputStream dis = new DataInputStream(bais)) {
            String messageId = readString(dis);
            String topic = readString(dis);
            int partition = dis.readInt();
            long offset = dis.readLong();

            String key = null;
            if (dis.readBoolean()) {
                key = readString(dis);
            }

            String payload = readString(dis);
            long timestampMillis = dis.readLong();

            String producerId = null;
            if (dis.readBoolean()) {
                producerId = readString(dis);
            }

            return Message.builder()
                    .messageId(messageId)
                    .topic(topic)
                    .partition(partition)
                    .offset(offset)
                    .key(key)
                    .payload(payload)
                    .timestamp(Instant.ofEpochMilli(timestampMillis))
                    .producerId(producerId)
                    .build();
        }
    }

    // Helper to write string with 4-byte length prefix to support strings > 64k (DataOutputStream.writeUTF limitation)
    private static void writeString(DataOutputStream dos, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        if (length < 0 || length > 100 * 1024 * 1024) { // limit 100MB sanity check
            throw new StorageException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
