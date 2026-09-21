package com.minikafka.model;

import com.minikafka.storage.InMemoryMessageLog;
import com.minikafka.storage.MessageLog;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a single ordered partition within a Topic.
 * Holds its own {@link MessageLog} and guarantees deterministic message ordering and offset sequences.
 */
public class Partition {

    private final String topicName;
    private final int partitionId;
    private final MessageLog messageLog;

    public Partition(String topicName, int partitionId) {
        this(topicName, partitionId, new InMemoryMessageLog());
    }

    public Partition(String topicName, int partitionId, MessageLog messageLog) {
        this.topicName = Objects.requireNonNull(topicName, "topicName must not be null");
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be >= 0");
        }
        this.partitionId = partitionId;
        this.messageLog = Objects.requireNonNull(messageLog, "messageLog must not be null");
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(topicName, partitionId);
    }

    /**
     * Appends a message to this partition.
     *
     * @param message the message to append
     * @return the offset assigned to the message
     */
    public long append(Message message) {
        return messageLog.append(message);
    }

    /**
     * Reads a specific message at the given offset.
     *
     * @param offset the 0-based offset
     * @return an Optional containing the message if found
     */
    public Optional<Message> read(long offset) {
        return messageLog.read(offset);
    }

    /**
     * Reads messages starting from the given offset onwards.
     *
     * @param offset the 0-based offset
     * @return list of messages
     */
    public List<Message> readFrom(long offset) {
        return messageLog.readFrom(offset);
    }

    /**
     * Reads up to limit messages from offset onwards.
     */
    public List<Message> readFrom(long offset, int limit) {
        return messageLog.readFrom(offset, limit);
    }

    public int size() {
        return messageLog.size();
    }

    public long getLatestOffset() {
        return messageLog.getLatestOffset();
    }

    public long getNextOffset() {
        return messageLog.getNextOffset();
    }

    public boolean isEmpty() {
        return messageLog.isEmpty();
    }

    public MessageLog getMessageLog() {
        return messageLog;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "topic='" + topicName + '\'' +
                ", partitionId=" + partitionId +
                ", size=" + size() +
                ", nextOffset=" + getNextOffset() +
                '}';
    }
}
