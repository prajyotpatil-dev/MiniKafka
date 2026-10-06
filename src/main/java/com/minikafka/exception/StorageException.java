package com.minikafka.exception;

/**
 * Thrown when an error occurs while initializing, modifying, or reading from the persistent storage.
 */
public class StorageException extends MiniKafkaException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
