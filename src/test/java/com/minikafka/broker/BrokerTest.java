package com.minikafka.broker;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.exception.InvalidPartitionException;
import com.minikafka.exception.MessageNotFoundException;
import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.model.PublishResult;
import com.minikafka.model.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrokerTest {

    private Broker broker;

    @BeforeEach
    void setUp() {
        broker = new Broker();
    }

    @Test
    @DisplayName("Should create, list, and delete topics via broker")
    void testTopicLifecycle() {
        Topic topic = broker.createTopic("user-notifications", 3);
        assertThat(topic.getName()).isEqualTo("user-notifications");
        assertThat(topic.getPartitionCount()).isEqualTo(3);
        assertThat(broker.topicExists("user-notifications")).isTrue();

        assertThat(broker.listTopics()).hasSize(1);

        Topic deleted = broker.deleteTopic("user-notifications");
        assertThat(deleted.getName()).isEqualTo("user-notifications");
        assertThat(broker.topicExists("user-notifications")).isFalse();
    }

    @Test
    @DisplayName("Should publish message and assign correct topic, partition, offset, and id")
    void testPublishSingleMessage() {
        broker.createTopic("orders", 3);

        PublishResult result = broker.publish("orders", "order-123", "{\"amount\": 99.99}", "order-service");

        assertThat(result.messageId()).isNotBlank();
        assertThat(result.topic()).isEqualTo("orders");
        assertThat(result.partition()).isBetween(0, 2);
        assertThat(result.offset()).isEqualTo(0L);
        assertThat(result.timestamp()).isNotNull();

        Message msg = broker.getMessage("orders", result.partition(), 0L);
        assertThat(msg.getMessageId()).isEqualTo(result.messageId());
        assertThat(msg.getKey()).isEqualTo("order-123");
        assertThat(msg.getPayload()).isEqualTo("{\"amount\": 99.99}");
        assertThat(msg.getProducerId()).isEqualTo("order-service");
    }

    @Test
    @DisplayName("Should publish to specific partition and preserve continuous offsets")
    void testPublishToSpecificPartition() {
        broker.createTopic("telemetry", 2);

        PublishResult res0 = broker.publishToPartition("telemetry", 1, null, "ping-1", "sensor-1");
        PublishResult res1 = broker.publishToPartition("telemetry", 1, null, "ping-2", "sensor-1");
        PublishResult res2 = broker.publishToPartition("telemetry", 1, null, "ping-3", "sensor-1");

        assertThat(res0.partition()).isEqualTo(1);
        assertThat(res0.offset()).isEqualTo(0L);
        assertThat(res1.offset()).isEqualTo(1L);
        assertThat(res2.offset()).isEqualTo(2L);

        List<Message> messages = broker.read("telemetry", 1, 0L);
        assertThat(messages).hasSize(3);
        assertThat(messages.get(0).getPayload()).isEqualTo("ping-1");
        assertThat(messages.get(1).getPayload()).isEqualTo("ping-2");
        assertThat(messages.get(2).getPayload()).isEqualTo("ping-3");
    }

    @Test
    @DisplayName("Should route messages with same key to the same partition (hash partitioning)")
    void testKeyBasedPartitioning() {
        broker.createTopic("user-events", 5);

        PublishResult r1 = broker.publish("user-events", "user-42", "action1", "client");
        PublishResult r2 = broker.publish("user-events", "user-42", "action2", "client");
        PublishResult r3 = broker.publish("user-events", "user-42", "action3", "client");

        assertThat(r1.partition()).isEqualTo(r2.partition());
        assertThat(r2.partition()).isEqualTo(r3.partition());
    }

    @Test
    @DisplayName("Should distribute messages across partitions via round-robin when key is absent")
    void testRoundRobinPartitioning() {
        broker.createTopic("logs", 3);

        PublishResult r0 = broker.publish("logs", null, "log-0", "agent");
        PublishResult r1 = broker.publish("logs", null, "log-1", "agent");
        PublishResult r2 = broker.publish("logs", null, "log-2", "agent");
        PublishResult r3 = broker.publish("logs", null, "log-3", "agent");

        assertThat(r0.partition()).isEqualTo(0);
        assertThat(r1.partition()).isEqualTo(1);
        assertThat(r2.partition()).isEqualTo(2);
        assertThat(r3.partition()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should read messages from offset onward and handle empty offsets cleanly")
    void testReadFromOffset() {
        broker.createTopic("stocks", 1);
        for (int i = 0; i < 5; i++) {
            broker.publish("stocks", null, "tick-" + i, "exchange");
        }

        List<Message> fromOffset2 = broker.read("stocks", 0, 2L);
        assertThat(fromOffset2).hasSize(3);
        assertThat(fromOffset2.get(0).getOffset()).isEqualTo(2L);
        assertThat(fromOffset2.get(2).getOffset()).isEqualTo(4L);

        List<Message> beyond = broker.read("stocks", 0, 100L);
        assertThat(beyond).isEmpty();
    }

    @Test
    @DisplayName("Should throw appropriate exceptions for missing topic, invalid partition, and missing message")
    void testExceptions() {
        assertThatThrownBy(() -> broker.publish("nonexistent", "k", "v", "p"))
                .isInstanceOf(TopicNotFoundException.class);

        assertThatThrownBy(() -> broker.read("nonexistent", 0, 0L))
                .isInstanceOf(TopicNotFoundException.class);

        broker.createTopic("valid", 2);

        assertThatThrownBy(() -> broker.publishToPartition("valid", 5, "k", "v", "p"))
                .isInstanceOf(InvalidPartitionException.class);

        assertThatThrownBy(() -> broker.read("valid", 3, 0L))
                .isInstanceOf(InvalidPartitionException.class);

        assertThatThrownBy(() -> broker.getMessage("valid", 0, 999L))
                .isInstanceOf(MessageNotFoundException.class);

        assertThatThrownBy(() -> broker.read("valid", 0, -1L))
                .isInstanceOf(InvalidOffsetException.class);
    }

    @Test
    @DisplayName("Concurrency test: 20 threads x 100 messages to single partition (2000 total)")
    void testConcurrentPublishingSinglePartition() throws InterruptedException {
        String topicName = "concurrent-single";
        broker.createTopic(topicName, 1);

        int threads = 20;
        int messagesPerThread = 100;
        int totalExpected = threads * messagesPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        List<PublishResult> results = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int m = 0; m < messagesPerThread; m++) {
                        PublishResult res = broker.publishToPartition(
                                topicName,
                                0,
                                "key-" + threadId,
                                "payload-" + threadId + "-" + m,
                                "producer-" + threadId
                        );
                        results.add(res);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertThat(results).hasSize(totalExpected);

        // Verify all message IDs are unique
        Set<String> uniqueMsgIds = new HashSet<>();
        for (PublishResult r : results) {
            uniqueMsgIds.add(r.messageId());
        }
        assertThat(uniqueMsgIds).hasSize(totalExpected);

        // Verify stored messages in partition 0
        List<Message> storedMessages = broker.read(topicName, 0, 0L);
        assertThat(storedMessages).hasSize(totalExpected);

        // Verify strictly continuous and increasing 0-based offsets
        for (int i = 0; i < totalExpected; i++) {
            assertThat(storedMessages.get(i).getOffset()).isEqualTo(i);
        }
    }

    @Test
    @DisplayName("Concurrency test: 20 threads x 100 messages distributed across multiple partitions")
    void testConcurrentPublishingMultiPartition() throws InterruptedException {
        String topicName = "concurrent-multi";
        int partitions = 4;
        broker.createTopic(topicName, partitions);

        int threads = 20;
        int messagesPerThread = 100;
        int totalExpected = threads * messagesPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        Set<String> messageIds = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int m = 0; m < messagesPerThread; m++) {
                        // alternate between keyed and unkeyed
                        String key = (m % 2 == 0) ? ("user-" + (threadId % 10)) : null;
                        PublishResult res = broker.publish(
                                topicName,
                                key,
                                "data-" + threadId + "-" + m,
                                "producer-" + threadId
                        );
                        messageIds.add(res.messageId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertThat(messageIds).hasSize(totalExpected);

        int totalRead = 0;
        for (int p = 0; p < partitions; p++) {
            List<Message> partMsgs = broker.read(topicName, p, 0L);
            totalRead += partMsgs.size();

            // Verify offsets are continuous per partition
            for (int i = 0; i < partMsgs.size(); i++) {
                assertThat(partMsgs.get(i).getOffset()).isEqualTo(i);
            }
        }

        assertThat(totalRead).isEqualTo(totalExpected);
    }
}
