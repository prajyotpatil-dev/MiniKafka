package com.minikafka.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable Message model in MiniKafka.
 * Contains payload and partition/offset metadata assigned upon broker persistence.
 */
public final class Message {

    private final String messageId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final String key;
    private final String payload;
    private final Instant timestamp;
    private final String producerId;

    /**
     * Constructs an immutable Message.
     */
    public Message(
            String messageId,
            String topic,
            int partition,
            long offset,
            String key,
            String payload,
            Instant timestamp,
            String producerId
    ) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.offset = offset;
        this.key = key; // key can be null
        this.payload = Objects.requireNonNull(payload, "payload must not be null");
        this.timestamp = timestamp != null ? timestamp : Instant.now();
        this.producerId = producerId; // producerId can be null or empty
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getProducerId() {
        return producerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return partition == message.partition &&
                offset == message.offset &&
                Objects.equals(messageId, message.messageId) &&
                Objects.equals(topic, message.topic) &&
                Objects.equals(key, message.key) &&
                Objects.equals(payload, message.payload) &&
                Objects.equals(timestamp, message.timestamp) &&
                Objects.equals(producerId, message.producerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, topic, partition, offset, key, payload, timestamp, producerId);
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageId='" + messageId + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", producerId='" + producerId + '\'' +
                '}';
    }

    public static final class Builder {
        private String messageId;
        private String topic;
        private int partition;
        private long offset = -1L;
        private String key;
        private String payload;
        private Instant timestamp;
        private String producerId;

        public Builder() {
            this.messageId = UUID.randomUUID().toString();
            this.timestamp = Instant.now();
        }

        public Builder messageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder producerId(String producerId) {
            this.producerId = producerId;
            return this;
        }

        public Message build() {
            return new Message(
                    messageId != null ? messageId : UUID.randomUUID().toString(),
                    topic,
                    partition,
                    offset,
                    key,
                    payload,
                    timestamp != null ? timestamp : Instant.now(),
                    producerId
            );
        }
    }
}
