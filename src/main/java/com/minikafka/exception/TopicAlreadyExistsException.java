package com.minikafka.exception;

/**
 * Thrown when attempting to create a topic that already exists.
 */
public class TopicAlreadyExistsException extends MiniKafkaException {
    private final String topicName;

    public TopicAlreadyExistsException(String topicName) {
        super("Topic already exists: '" + topicName + "'");
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
