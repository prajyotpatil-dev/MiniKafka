package com.minikafka.exception;

/**
 * Thrown when corruption is detected in the middle of a partition log (NOT a truncated tail,
 * which is safely handled).
 */
public class CorruptedLogException extends StorageException {

    public CorruptedLogException(String message) {
        super(message);
    }

    public CorruptedLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
