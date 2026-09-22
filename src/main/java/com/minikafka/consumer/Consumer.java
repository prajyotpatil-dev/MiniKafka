package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.model.Topic;
import com.minikafka.model.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A consumer responsible for reading messages from the MiniKafka broker.
 * <p>
 * Phase 2 semantics:
 * <ul>
 *     <li>Each consumer independently tracks its own in-memory position per TopicPartition.</li>
 *     <li>Multiple independent consumers reading the same topic will each receive
 *         <b>the same messages</b> — consumer groups are not implemented yet.</li>
 *     <li>Only per-partition ordering is guaranteed; global ordering across partitions is not.</li>
 * </ul>
 * <p>
 * Multi-partition polling strategy: round-robin across all partitions of all
 * subscribed topics, reading up to a fair share of each partition's available
 * messages per poll, preserving per-partition ordering and ensuring no message
 * is returned twice or silently skipped.
 */
public class Consumer implements AutoCloseable {

    private final Broker broker;
    private final ConsumerConfig config;
    private final ConsumerSubscription subscription;
    private final Map<TopicPartition, Long> positions;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Consumer(Broker broker, ConsumerConfig config) {
        this.broker = Objects.requireNonNull(broker, "broker cannot be null");
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.subscription = new ConsumerSubscription();
        this.positions = new ConcurrentHashMap<>();
    }

    // ==========================================
    // Subscription
    // ==========================================

    /**
     * Subscribes the consumer to one or more topics. The topics must exist on the broker.
     *
     * @param topics topic names
     * @throws TopicNotFoundException if any topic does not exist
     * @throws IllegalStateException  if the consumer is closed
     */
    public void subscribe(String... topics) {
        ensureOpen();
        for (String topic : topics) {
            // Validate that the topic exists on the broker
            Topic t = broker.getTopic(topic); // throws TopicNotFoundException
            subscription.subscribe(topic);
            // Initialize positions for all partitions of this topic (if not already set)
            for (int i = 0; i < t.getPartitionCount(); i++) {
                positions.putIfAbsent(new TopicPartition(topic, i), 0L);
            }
        }
    }

    /**
     * Returns the set of currently subscribed topics.
     */
    public Set<String> getSubscription() {
        return subscription.getTopics();
    }

    // ==========================================
    // Polling
    // ==========================================

    /**
     * Polls for new messages using the default batch size from config.
     *
     * @return list of consumer records; empty if no new messages
     * @throws IllegalStateException if consumer is closed or has no subscriptions
     */
    public List<ConsumerRecord> poll() {
        return poll(config.getDefaultPollBatchSize());
    }

    /**
     * Polls for up to {@code maxRecords} messages across all subscribed topic-partitions.
     * <p>
     * Uses round-robin across partitions: iterates partitions in a deterministic order
     * (alphabetical topic, then partition index), reading available messages from each
     * until the total reaches {@code maxRecords}.
     *
     * @param maxRecords maximum number of records to return
     * @return list of consumer records; empty if no new messages
     * @throws IllegalStateException    if consumer is closed or has no subscriptions
     * @throws IllegalArgumentException if maxRecords <= 0
     */
    public List<ConsumerRecord> poll(int maxRecords) {
        ensureOpen();
        if (subscription.isEmpty()) {
            throw new IllegalStateException("Consumer has no active subscriptions; call subscribe() first");
        }
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be > 0, was: " + maxRecords);
        }

        // Build a sorted list of all topic-partitions for deterministic round-robin
        List<TopicPartition> allPartitions = buildSortedPartitionList();

        List<ConsumerRecord> result = new ArrayList<>();
        int remaining = maxRecords;

        // Round-robin: keep iterating partitions until we fill maxRecords or exhaust all
        boolean madeProgress = true;
        while (remaining > 0 && madeProgress) {
            madeProgress = false;
            for (TopicPartition tp : allPartitions) {
                if (remaining <= 0) break;

                long position = positions.getOrDefault(tp, 0L);
                List<Message> messages = broker.read(tp.topic(), tp.partition(), position, 1);

                if (!messages.isEmpty()) {
                    Message msg = messages.get(0);
                    result.add(toConsumerRecord(msg));
                    positions.put(tp, position + 1);
                    remaining--;
                    madeProgress = true;
                }
            }
        }

        return Collections.unmodifiableList(result);
    }

    // ==========================================
    // Seek & Position
    // ==========================================

    /**
     * Seeks the consumer's read position for a specific topic-partition.
     *
     * @param topic     topic name
     * @param partition partition index
     * @param offset    the offset to seek to (next offset to be read)
     * @throws TopicNotFoundException   if the topic does not exist
     * @throws IllegalStateException    if not subscribed to the topic, or consumer is closed
     * @throws IllegalArgumentException if offset is negative
     */
    public void seek(String topic, int partition, long offset) {
        ensureOpen();
        if (offset < 0) {
            throw new IllegalArgumentException("Seek offset cannot be negative, was: " + offset);
        }
        // Validate topic and partition exist on broker
        Topic t = broker.getTopic(topic); // throws TopicNotFoundException
        t.getPartition(partition); // throws InvalidPartitionException

        if (!subscription.isSubscribed(topic)) {
            throw new IllegalStateException("Consumer is not subscribed to topic: " + topic);
        }

        positions.put(new TopicPartition(topic, partition), offset);
    }

    /**
     * Returns the consumer's current position (next offset to read) for a specific topic-partition.
     *
     * @param topic     topic name
     * @param partition partition index
     * @return the next offset to be read
     */
    public long position(String topic, int partition) {
        ensureOpen();
        TopicPartition tp = new TopicPartition(topic, partition);
        Long pos = positions.get(tp);
        if (pos == null) {
            throw new IllegalStateException(
                    "No position tracked for " + tp + "; consumer may not be subscribed to this topic-partition");
        }
        return pos;
    }

    /**
     * Returns an unmodifiable view of all current positions.
     */
    public Map<TopicPartition, Long> positions() {
        return Collections.unmodifiableMap(new HashMap<>(positions));
    }

    // ==========================================
    // Lifecycle
    // ==========================================

    @Override
    public void close() {
        closed.set(true);
    }

    public boolean isClosed() {
        return closed.get();
    }

    // ==========================================
    // Internals
    // ==========================================

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Consumer is closed");
        }
    }

    /**
     * Builds a deterministic, sorted list of all TopicPartitions the consumer is subscribed to.
     * Sorted by topic name (alphabetical), then partition index.
     */
    private List<TopicPartition> buildSortedPartitionList() {
        List<TopicPartition> list = new ArrayList<>();
        List<String> sortedTopics = new ArrayList<>(subscription.getTopics());
        Collections.sort(sortedTopics);
        for (String topic : sortedTopics) {
            Topic t = broker.getTopic(topic);
            for (int i = 0; i < t.getPartitionCount(); i++) {
                list.add(new TopicPartition(topic, i));
            }
        }
        return list;
    }

    private ConsumerRecord toConsumerRecord(Message msg) {
        return new ConsumerRecord(
                java.util.UUID.fromString(msg.getMessageId()),
                msg.getTopic(),
                msg.getPartition(),
                msg.getOffset(),
                msg.getKey(),
                msg.getPayload(),
                msg.getTimestamp(),
                msg.getProducerId()
        );
    }
}
