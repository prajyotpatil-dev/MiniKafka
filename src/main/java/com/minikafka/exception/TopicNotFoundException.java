package com.minikafka.exception;

/**
 * Thrown when an operation targets a topic that does not exist.
 */
public class TopicNotFoundException extends MiniKafkaException {
    private final String topicName;

    public TopicNotFoundException(String topicName) {
        super("Topic not found: '" + topicName + "'");
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
