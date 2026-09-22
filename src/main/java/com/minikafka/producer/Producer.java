package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.model.PublishResult;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A producer responsible for publishing messages to the MiniKafka broker.
 */
public class Producer implements AutoCloseable {

    private final Broker broker;
    private final ProducerConfig config;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Producer(Broker broker, ProducerConfig config) {
        this.broker = Objects.requireNonNull(broker, "broker cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Publishes a record to the broker.
     *
     * @param record the record to publish
     * @return metadata representing the broker's acknowledgement
     * @throws IllegalStateException if the producer is closed
     */
    public RecordMetadata send(ProducerRecord record) {
        if (closed.get()) {
            throw new IllegalStateException("Cannot perform send after producer is closed");
        }

        Objects.requireNonNull(record, "record cannot be null");

        // The ProducerRecord requires topic, payload, and producerId to be non-null.
        PublishResult result = broker.publish(
                record.topic(),
                record.key(),
                record.payload(),
                record.producerId()
        );

        return new RecordMetadata(
                UUID.fromString(result.messageId()),
                result.topic(),
                result.partition(),
                result.offset(),
                result.timestamp(),
                record.producerId()
        );
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
