package com.minikafka.exception;

/**
 * Thrown when topic creation parameters are invalid (e.g. empty or null name, invalid partition count).
 */
public class InvalidTopicException extends MiniKafkaException {
    public InvalidTopicException(String message) {
        super(message);
    }
}
