package com.minikafka.exception;

/**
 * Thrown when an invalid partition index or count is specified.
 */
public class InvalidPartitionException extends MiniKafkaException {
    public InvalidPartitionException(String message) {
        super(message);
    }

    public InvalidPartitionException(String topicName, int partition, int totalPartitions) {
        super(String.format("Invalid partition %d for topic '%s'. Topic has %d partition(s) (valid range: 0 to %d).",
                partition, topicName, totalPartitions, totalPartitions - 1));
    }
}
