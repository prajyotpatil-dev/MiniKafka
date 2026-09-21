package com.minikafka.exception;

/**
 * Thrown when an invalid offset (such as negative offset) is requested.
 */
public class InvalidOffsetException extends MiniKafkaException {
    private final long offset;

    public InvalidOffsetException(String message) {
        super(message);
        this.offset = -1;
    }

    public InvalidOffsetException(long offset, String details) {
        super(String.format("Invalid offset %d: %s", offset, details));
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }
}
