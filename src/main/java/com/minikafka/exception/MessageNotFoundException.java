package com.minikafka.exception;

/**
 * Thrown when a specific message at a given topic/partition/offset cannot be found.
 */
public class MessageNotFoundException extends MiniKafkaException {
    public MessageNotFoundException(String topic, int partition, long offset) {
        super(String.format("Message not found in topic '%s', partition %d at offset %d", topic, partition, offset));
    }
}
