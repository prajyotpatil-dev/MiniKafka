package com.minikafka.consumer;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable record representing a message as delivered to a consumer.
 * Consumers cannot mutate broker messages; this is a read-only view.
 */
public record ConsumerRecord(
        UUID messageId,
        String topic,
        int partition,
        long offset,
        String key,
        String payload,
        Instant timestamp,
        String producerId
) {
}
