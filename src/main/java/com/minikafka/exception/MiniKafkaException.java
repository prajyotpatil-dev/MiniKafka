package com.minikafka.exception;

/**
 * Base exception for all MiniKafka domain and runtime exceptions.
 */
public class MiniKafkaException extends RuntimeException {
    public MiniKafkaException(String message) {
        super(message);
    }

    public MiniKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
