package com.minikafka.storage;

/**
 * Factory for creating {@link MessageLog} instances when a new partition is created
 * or recovered during startup.
 */
@FunctionalInterface
public interface MessageLogFactory {

    /**
     * Creates a MessageLog for the specified topic and partition.
     *
     * @param topicName the name of the topic
     * @param partitionId the partition index
     * @return a MessageLog instance (in-memory or file-backed)
     */
    MessageLog create(String topicName, int partitionId);
}
