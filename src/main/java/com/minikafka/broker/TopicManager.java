package com.minikafka.broker;

import com.minikafka.exception.InvalidTopicException;
import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Topic;
import com.minikafka.storage.MessageLogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe manager responsible for Topic lifecycle:
 * creation, deletion, lookup, and enumeration.
 */
public class TopicManager {

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private final ConcurrentMap<String, Topic> topics = new ConcurrentHashMap<>();

    /**
     * Creates a new topic with the specified partition count.
     *
     * @param name           the topic name
     * @param partitionCount the number of partitions (>= 1)
     * @return the created Topic
     * @throws InvalidTopicException        if name is null/empty or partition count < 1
     * @throws TopicAlreadyExistsException if topic with the name already exists
     */
    public Topic createTopic(String name, int partitionCount) {
        if (name == null || name.trim().isEmpty()) {
            throw new InvalidTopicException("Topic name must not be null or blank");
        }
        if (partitionCount < 1) {
            throw new InvalidTopicException("Partition count must be at least 1, received: " + partitionCount);
        }

        String normalizedName = name.trim();
        Topic newTopic = new Topic(normalizedName, partitionCount);

        Topic existing = topics.putIfAbsent(normalizedName, newTopic);
        if (existing != null) {
            throw new TopicAlreadyExistsException(normalizedName);
        }

        log.info("Created topic '{}' with {} partition(s)", normalizedName, partitionCount);
        return newTopic;
    }

    /**
     * Creates a new topic with the specified partition count, using a custom
     * MessageLogFactory to build each partition's message log.
     *
     * @param name           the topic name
     * @param partitionCount the number of partitions (>= 1)
     * @param logFactory     the factory to produce each partition's MessageLog
     * @return the created Topic
     * @throws InvalidTopicException        if name is null/empty or partition count < 1
     * @throws TopicAlreadyExistsException if topic with the name already exists
     */
    public Topic createTopic(String name, int partitionCount, MessageLogFactory logFactory) {
        if (name == null || name.trim().isEmpty()) {
            throw new InvalidTopicException("Topic name must not be null or blank");
        }
        if (partitionCount < 1) {
            throw new InvalidTopicException("Partition count must be at least 1, received: " + partitionCount);
        }

        String normalizedName = name.trim();
        Topic newTopic = new Topic(normalizedName, partitionCount, logFactory);

        Topic existing = topics.putIfAbsent(normalizedName, newTopic);
        if (existing != null) {
            throw new TopicAlreadyExistsException(normalizedName);
        }

        log.info("Created topic '{}' with {} partition(s) [factory-backed]", normalizedName, partitionCount);
        return newTopic;
    }

    /**
     * Registers a pre-built (recovered) topic into the manager.
     * Used during startup to restore topics from persistent storage.
     *
     * @param topic the recovered topic
     * @throws TopicAlreadyExistsException if a topic with the same name is already registered
     */
    public void registerTopic(Topic topic) {
        if (topic == null) {
            throw new InvalidTopicException("Cannot register a null topic");
        }
        Topic existing = topics.putIfAbsent(topic.getName(), topic);
        if (existing != null) {
            throw new TopicAlreadyExistsException(topic.getName());
        }
        log.info("Registered recovered topic '{}' with {} partition(s)",
                topic.getName(), topic.getPartitionCount());
    }

    /**
     * Retrieves an existing topic by name.
     *
     * @param name the topic name
     * @return the Topic
     * @throws TopicNotFoundException if topic does not exist
     */
    public Topic getTopic(String name) {
        if (name == null) {
            throw new TopicNotFoundException("null");
        }
        Topic topic = topics.get(name.trim());
        if (topic == null) {
            throw new TopicNotFoundException(name.trim());
        }
        return topic;
    }

    /**
     * Checks if a topic exists.
     *
     * @param name the topic name
     * @return true if exists, false otherwise
     */
    public boolean topicExists(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }
        return topics.containsKey(name.trim());
    }

    /**
     * Deletes a topic by name.
     *
     * @param name the topic name
     * @return the removed Topic
     * @throws TopicNotFoundException if the topic does not exist
     */
    public Topic deleteTopic(String name) {
        if (name == null) {
            throw new TopicNotFoundException("null");
        }
        String normalizedName = name.trim();
        Topic removed = topics.remove(normalizedName);
        if (removed == null) {
            throw new TopicNotFoundException(normalizedName);
        }
        log.info("Deleted topic '{}'", normalizedName);
        return removed;
    }

    /**
     * Returns a snapshot list of all active topics.
     */
    public List<Topic> listTopics() {
        return new ArrayList<>(topics.values());
    }

    /**
     * Returns the total count of topics.
     */
    public int getTopicCount() {
        return topics.size();
    }
}
