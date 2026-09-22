package com.minikafka.producer;

import java.util.Objects;

/**
 * Represents a data record to be sent to MiniKafka by a Producer.
 * This object is strictly input data from the producer and does not
 * contain broker-assigned metadata like partition, offset, or timestamp.
 */
public record ProducerRecord(
        String topic,
        String key,
        String payload,
        String producerId
) {
    public ProducerRecord {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be null or blank");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        if (producerId == null || producerId.isBlank()) {
            throw new IllegalArgumentException("Producer ID cannot be null or blank");
        }
    }
}
