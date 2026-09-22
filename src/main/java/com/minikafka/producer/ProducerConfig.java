package com.minikafka.producer;

import java.util.Objects;

/**
 * Configuration for a MiniKafka Producer.
 * Designed to be immutable.
 */
public class ProducerConfig {
    private final String producerId;
    private final String clientId;

    private ProducerConfig(Builder builder) {
        this.producerId = Objects.requireNonNull(builder.producerId, "producerId cannot be null");
        if (this.producerId.isBlank()) {
            throw new IllegalArgumentException("producerId cannot be blank");
        }
        this.clientId = builder.clientId;
    }

    public String getProducerId() {
        return producerId;
    }

    public String getClientId() {
        return clientId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String producerId;
        private String clientId;

        public Builder producerId(String producerId) {
            this.producerId = producerId;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
}
