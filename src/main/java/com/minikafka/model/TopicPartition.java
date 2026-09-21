package com.minikafka.model;

import java.util.Objects;

/**
 * Value object representing a specific partition within a topic.
 * Immutable and suitable for use as keys in maps.
 */
public record TopicPartition(String topic, int partition) {
    public TopicPartition {
        Objects.requireNonNull(topic, "topic must not be null");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
