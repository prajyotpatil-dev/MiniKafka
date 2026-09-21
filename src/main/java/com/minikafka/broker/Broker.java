package com.minikafka.broker;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.exception.InvalidPartitionException;
import com.minikafka.exception.MessageNotFoundException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.model.Partition;
import com.minikafka.model.PublishResult;
import com.minikafka.model.Topic;
import com.minikafka.model.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Main entry point of the MiniKafka core message broker.
 * <p>
 * Responsibilities:
 * <ul>
 *     <li>Manage topic creation and lifecycle via {@link TopicManager}</li>
 *     <li>Route incoming messages to appropriate partitions using {@link Partitioner}</li>
 *     <li>Coordinate thread-safe message appending and retrieval</li>
 *     <li>Track and coordinate offsets via {@link OffsetManager}</li>
 * </ul>
 * </p>
 */
public class Broker {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final TopicManager topicManager;
    private final OffsetManager offsetManager;
    private final Partitioner partitioner;

    public Broker() {
        this(new TopicManager(), new OffsetManager(), new Partitioner.DefaultPartitioner());
    }

    public Broker(TopicManager topicManager, OffsetManager offsetManager, Partitioner partitioner) {
        this.topicManager = Objects.requireNonNull(topicManager, "topicManager must not be null");
        this.offsetManager = Objects.requireNonNull(offsetManager, "offsetManager must not be null");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner must not be null");
        log.info("MiniKafka Broker core engine initialized");
    }

    // ==========================================
    // Topic Management APIs
    // ==========================================

    /**
     * Creates a topic with the specified partition count.
     *
     * @param topicName  the name of the topic
     * @param partitions number of partitions (>= 1)
     * @return the created Topic
     */
    public Topic createTopic(String topicName, int partitions) {
        return topicManager.createTopic(topicName, partitions);
    }

    /**
     * Deletes a topic and cleans up associated offsets.
     *
     * @param topicName the name of the topic to delete
     * @return the deleted Topic
     */
    public Topic deleteTopic(String topicName) {
        Topic deletedTopic = topicManager.deleteTopic(topicName);
        for (int i = 0; i < deletedTopic.getPartitionCount(); i++) {
            offsetManager.removePartitionOffsets(new TopicPartition(topicName, i));
        }
        return deletedTopic;
    }

    /**
     * Checks if a topic exists.
     */
    public boolean topicExists(String topicName) {
        return topicManager.topicExists(topicName);
    }

    /**
     * Retrieves a topic by name.
     */
    public Topic getTopic(String topicName) {
        return topicManager.getTopic(topicName);
    }

    /**
     * Lists all registered topics.
     */
    public List<Topic> listTopics() {
        return topicManager.listTopics();
    }

    // ==========================================
    // Publishing APIs
    // ==========================================

    /**
     * Publishes a message to a topic, automatically choosing a partition using the configured partitioner.
     *
     * @param topicName  target topic
     * @param key        optional message key for hashing
     * @param payload    message payload
     * @param producerId optional producer identifier
     * @return result containing assigned partition, offset, timestamp, and message ID
     */
    public PublishResult publish(String topicName, String key, String payload, String producerId) {
        Topic topic = topicManager.getTopic(topicName);
        int partitionId = partitioner.partition(topic, key);
        return publishToPartition(topic, partitionId, key, payload, producerId);
    }

    /**
     * Publishes a message explicitly targeting a specific partition.
     *
     * @param topicName   target topic
     * @param partitionId target partition index
     * @param key         optional message key
     * @param payload     message payload
     * @param producerId  optional producer identifier
     * @return result containing assigned partition, offset, timestamp, and message ID
     */
    public PublishResult publishToPartition(String topicName, int partitionId, String key, String payload, String producerId) {
        Topic topic = topicManager.getTopic(topicName);
        return publishToPartition(topic, partitionId, key, payload, producerId);
    }

    private PublishResult publishToPartition(Topic topic, int partitionId, String key, String payload, String producerId) {
        if (payload == null) {
            throw new IllegalArgumentException("Message payload must not be null");
        }

        Partition partition = topic.getPartition(partitionId);

        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();

        // Build provisional message - offset will be officially bound by partition log
        Message message = Message.builder()
                .messageId(messageId)
                .topic(topic.getName())
                .partition(partitionId)
                .key(key)
                .payload(payload)
                .timestamp(timestamp)
                .producerId(producerId)
                .build();

        long assignedOffset = partition.append(message);

        log.debug("Appended message [{}] to topic '{}' partition {} at offset {}",
                messageId, topic.getName(), partitionId, assignedOffset);

        return new PublishResult(messageId, topic.getName(), partitionId, assignedOffset, timestamp);
    }

    // ==========================================
    // Reading APIs
    // ==========================================

    /**
     * Reads a batch of messages starting from a given offset onward.
     * If the offset is greater than the latest message offset, returns an empty list.
     *
     * @param topicName target topic
     * @param partition partition index
     * @param offset    0-based start offset
     * @return list of messages starting from offset
     */
    public List<Message> read(String topicName, int partition, long offset) {
        return read(topicName, partition, offset, Integer.MAX_VALUE);
    }

    /**
     * Reads up to {@code limit} messages starting from a given offset.
     *
     * @param topicName target topic
     * @param partition partition index
     * @param offset    0-based start offset
     * @param limit     maximum number of messages to return
     * @return list of messages
     */
    public List<Message> read(String topicName, int partition, long offset, int limit) {
        Topic topic = topicManager.getTopic(topicName);
        Partition part = topic.getPartition(partition);
        return part.readFrom(offset, limit);
    }

    /**
     * Retrieves a single message by exact offset.
     *
     * @param topicName target topic
     * @param partition partition index
     * @param offset    0-based message offset
     * @return the Message
     * @throws MessageNotFoundException if the offset does not exist in the partition
     */
    public Message getMessage(String topicName, int partition, long offset) {
        Topic topic = topicManager.getTopic(topicName);
        Partition part = topic.getPartition(partition);
        return part.read(offset)
                .orElseThrow(() -> new MessageNotFoundException(topicName, partition, offset));
    }

    // ==========================================
    // Component Getters
    // ==========================================

    public TopicManager getTopicManager() {
        return topicManager;
    }

    public OffsetManager getOffsetManager() {
        return offsetManager;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }
}
