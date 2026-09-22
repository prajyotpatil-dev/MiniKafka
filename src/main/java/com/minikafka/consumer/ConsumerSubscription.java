package com.minikafka.consumer;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tracks which topics a consumer is subscribed to.
 * Phase 2 supports simple explicit subscription without consumer groups.
 */
public class ConsumerSubscription {

    private final Set<String> topics = new LinkedHashSet<>();

    public void subscribe(String... topicNames) {
        for (String t : topicNames) {
            if (t == null || t.isBlank()) {
                throw new IllegalArgumentException("Topic name cannot be null or blank");
            }
            topics.add(t);
        }
    }

    public void unsubscribe(String topic) {
        topics.remove(topic);
    }

    public void unsubscribeAll() {
        topics.clear();
    }

    public Set<String> getTopics() {
        return Collections.unmodifiableSet(topics);
    }

    public boolean isSubscribed(String topic) {
        return topics.contains(topic);
    }

    public boolean isEmpty() {
        return topics.isEmpty();
    }
}
