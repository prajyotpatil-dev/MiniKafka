package com.minikafka.producer;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents the broker's acknowledgement of a published record.
 * Contains broker-generated metadata like offset and partition.
 */
public record RecordMetadata(
        UUID messageId,
        String topic,
        int partition,
        long offset,
        Instant timestamp,
        String producerId
) {
}
