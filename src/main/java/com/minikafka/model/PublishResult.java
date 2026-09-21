package com.minikafka.model;

import java.time.Instant;

/**
 * Result returned upon successful message publication to the broker.
 */
public record PublishResult(
        String messageId,
        String topic,
        int partition,
        long offset,
        Instant timestamp
) {
    public static PublishResult fromMessage(Message message) {
        return new PublishResult(
                message.getMessageId(),
                message.getTopic(),
                message.getPartition(),
                message.getOffset(),
                message.getTimestamp()
        );
    }
}
