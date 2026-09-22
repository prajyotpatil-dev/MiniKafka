package com.minikafka.consumer;

import java.util.Objects;

/**
 * Configuration for a MiniKafka Consumer.
 * Immutable after construction.
 */
public class ConsumerConfig {

    /** Default number of records returned per poll. */
    public static final int DEFAULT_POLL_BATCH_SIZE = 100;

    private final String consumerId;
    private final String clientId;
    private final int defaultPollBatchSize;

    private ConsumerConfig(Builder builder) {
        this.consumerId = Objects.requireNonNull(builder.consumerId, "consumerId cannot be null");
        if (this.consumerId.isBlank()) {
            throw new IllegalArgumentException("consumerId cannot be blank");
        }
        this.clientId = builder.clientId;
        if (builder.defaultPollBatchSize <= 0) {
            throw new IllegalArgumentException("defaultPollBatchSize must be > 0, was: " + builder.defaultPollBatchSize);
        }
        this.defaultPollBatchSize = builder.defaultPollBatchSize;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getDefaultPollBatchSize() {
        return defaultPollBatchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String consumerId;
        private String clientId;
        private int defaultPollBatchSize = DEFAULT_POLL_BATCH_SIZE;

        public Builder consumerId(String consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder defaultPollBatchSize(int defaultPollBatchSize) {
            this.defaultPollBatchSize = defaultPollBatchSize;
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
