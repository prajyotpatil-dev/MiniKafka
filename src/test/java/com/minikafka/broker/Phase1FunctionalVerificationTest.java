package com.minikafka.broker;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.exception.InvalidPartitionException;
import com.minikafka.exception.InvalidTopicException;
import com.minikafka.exception.MessageNotFoundException;
import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Message;
import com.minikafka.model.Partition;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive Phase 1 Functional Verification Test Suite.
 * Validates all 11 functional scenarios, edge cases, and concurrency guarantees specified in Phase 1.
 */
class Phase1FunctionalVerificationTest {

    private Broker broker;

    @BeforeEach
    void setUp() {
        broker = new Broker();
    }

    // =========================================================================
    // Scenario 1: Topic Creation
    // =========================================================================
    @Test
    @DisplayName("Scenario 1: Topic creation with 3 partitions and partition verification")
    void testScenario1_TopicCreation() {
        Topic topic = broker.createTopic("phone-events", 3);

        assertThat(topic).isNotNull();
        assertThat(topic.getName()).isEqualTo("phone-events");
        assertThat(topic.getPartitionCount()).isEqualTo(3);
        assertThat(broker.topicExists("phone-events")).isTrue();

        // Verify partitions 0, 1, 2 exist
        Partition p0 = topic.getPartition(0);
        Partition p1 = topic.getPartition(1);
        Partition p2 = topic.getPartition(2);

        assertThat(p0.getPartitionId()).isEqualTo(0);
        assertThat(p1.getPartitionId()).isEqualTo(1);
        assertThat(p2.getPartitionId()).isEqualTo(2);

        assertThat(p0.getTopicName()).isEqualTo("phone-events");
        assertThat(p1.getTopicName()).isEqualTo("phone-events");
        assertThat(p2.getTopicName()).isEqualTo("phone-events");
    }

    // =========================================================================
    // Scenario 2: Duplicate Topic Rejection
    // =========================================================================
    @Test
    @DisplayName("Scenario 2: Duplicate topic creation throws TopicAlreadyExistsException")
    void testScenario2_DuplicateTopicException() {
        broker.createTopic("phone-events", 3);

        assertThatThrownBy(() -> broker.createTopic("phone-events", 3))
                .isInstanceOf(TopicAlreadyExistsException.class)
                .hasMessageContaining("phone-events");
    }

    // =========================================================================
    // Scenario 3: Message Publishing with Specific Data & Metadata Verification
    // =========================================================================
    @Test
    @DisplayName("Scenario 3: Message publishing with specific telemetry events and field checks")
    void testScenario3_MessagePublishing() {
        broker.createTopic("phone-events", 3);

        PublishResult res1 = broker.publish("phone-events", "device-1", "CALL_STARTED", "call-service");
        PublishResult res2 = broker.publish("phone-events", "device-2", "CALL_CONNECTED", "call-service");
        PublishResult res3 = broker.publish("phone-events", "device-1", "CALL_ENDED", "call-service");

        // Verify result objects
        assertThat(res1.messageId()).isNotBlank();
        assertThat(res2.messageId()).isNotBlank();
        assertThat(res3.messageId()).isNotBlank();

        // All IDs must be unique
        Set<String> ids = Set.of(res1.messageId(), res2.messageId(), res3.messageId());
        assertThat(ids).hasSize(3);

        assertThat(res1.topic()).isEqualTo("phone-events");
        assertThat(res2.topic()).isEqualTo("phone-events");
        assertThat(res3.topic()).isEqualTo("phone-events");

        assertThat(res1.timestamp()).isNotNull();
        assertThat(res2.timestamp()).isNotNull();
        assertThat(res3.timestamp()).isNotNull();

        // Verify stored messages match exactly
        Message msg1 = broker.getMessage("phone-events", res1.partition(), res1.offset());
        assertThat(msg1.getMessageId()).isEqualTo(res1.messageId());
        assertThat(msg1.getKey()).isEqualTo("device-1");
        assertThat(msg1.getPayload()).isEqualTo("CALL_STARTED");
        assertThat(msg1.getProducerId()).isEqualTo("call-service");

        Message msg2 = broker.getMessage("phone-events", res2.partition(), res2.offset());
        assertThat(msg2.getMessageId()).isEqualTo(res2.messageId());
        assertThat(msg2.getKey()).isEqualTo("device-2");
        assertThat(msg2.getPayload()).isEqualTo("CALL_CONNECTED");

        Message msg3 = broker.getMessage("phone-events", res3.partition(), res3.offset());
        assertThat(msg3.getMessageId()).isEqualTo(res3.messageId());
        assertThat(msg3.getKey()).isEqualTo("device-1");
        assertThat(msg3.getPayload()).isEqualTo("CALL_ENDED");

        // Keyed routing: device-1 messages must land on the exact same partition
        assertThat(res1.partition()).isEqualTo(res3.partition());
    }

    // =========================================================================
    // Scenario 4: Offset Verification (Contiguous 0-based, no gaps, no duplicates)
    // =========================================================================
    @Test
    @DisplayName("Scenario 4: Offset verification (0, 1, 2, ... contiguous, no gaps, no duplicates)")
    void testScenario4_OffsetVerification() {
        broker.createTopic("phone-events", 1);

        for (int i = 0; i < 10; i++) {
            PublishResult r = broker.publishToPartition("phone-events", 0, "k", "event-" + i, "p");
            assertThat(r.offset()).isEqualTo((long) i);
        }

        List<Message> messages = broker.read("phone-events", 0, 0L);
        assertThat(messages).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(messages.get(i).getOffset()).isEqualTo((long) i);
        }
    }

    // =========================================================================
    // Scenario 5: Message Ordering (Strict FIFO per partition)
    // =========================================================================
    @Test
    @DisplayName("Scenario 5: Message ordering (publish A,B,C,D,E and read from offset 0)")
    void testScenario5_MessageOrdering() {
        broker.createTopic("order-test", 1);

        List<String> expectedPayloads = List.of("A", "B", "C", "D", "E");
        for (String payload : expectedPayloads) {
            broker.publishToPartition("order-test", 0, null, payload, "prod-1");
        }

        List<Message> readMessages = broker.read("order-test", 0, 0L);
        assertThat(readMessages).hasSize(5);

        for (int i = 0; i < 5; i++) {
            assertThat(readMessages.get(i).getPayload()).isEqualTo(expectedPayloads.get(i));
            assertThat(readMessages.get(i).getOffset()).isEqualTo((long) i);
        }
    }

    // =========================================================================
    // Scenario 6: readFrom(5) offset filter
    // =========================================================================
    @Test
    @DisplayName("Scenario 6: readFrom(5) returns messages starting at offset 5")
    void testScenario6_ReadFromOffset5() {
        broker.createTopic("ten-messages", 1);

        for (int i = 0; i < 12; i++) {
            broker.publishToPartition("ten-messages", 0, null, "msg-" + i, "prod");
        }

        List<Message> fromOffset5 = broker.read("ten-messages", 0, 5L);
        assertThat(fromOffset5).hasSize(7); // offsets 5, 6, 7, 8, 9, 10, 11
        assertThat(fromOffset5.get(0).getOffset()).isEqualTo(5L);
        assertThat(fromOffset5.get(0).getPayload()).isEqualTo("msg-5");
        assertThat(fromOffset5.get(6).getOffset()).isEqualTo(11L);
        assertThat(fromOffset5.get(6).getPayload()).isEqualTo("msg-11");
    }

    // =========================================================================
    // Scenario 7: Read single message by topic + partition + offset
    // =========================================================================
    @Test
    @DisplayName("Scenario 7: getMessage retrieves exact single message by topic+partition+offset")
    void testScenario7_GetSingleMessage() {
        broker.createTopic("single-test", 2);

        PublishResult r0 = broker.publishToPartition("single-test", 1, "k-1", "secret-payload-42", "prod-A");
        assertThat(r0.offset()).isEqualTo(0L);

        Message fetched = broker.getMessage("single-test", 1, 0L);
        assertThat(fetched.getMessageId()).isEqualTo(r0.messageId());
        assertThat(fetched.getKey()).isEqualTo("k-1");
        assertThat(fetched.getPayload()).isEqualTo("secret-payload-42");
        assertThat(fetched.getProducerId()).isEqualTo("prod-A");
    }

    // =========================================================================
    // Scenario 8: Invalid Offset Behavior (negative, beyond latest)
    // =========================================================================
    @Test
    @DisplayName("Scenario 8: Negative offset throws InvalidOffsetException, offset > latest returns empty list")
    void testScenario8_InvalidOffsetBehavior() {
        broker.createTopic("offset-bounds", 1);
        broker.publishToPartition("offset-bounds", 0, null, "hello", "p");

        // Negative offset
        assertThatThrownBy(() -> broker.read("offset-bounds", 0, -1L))
                .isInstanceOf(InvalidOffsetException.class);

        assertThatThrownBy(() -> broker.getMessage("offset-bounds", 0, -5L))
                .isInstanceOf(InvalidOffsetException.class);

        // Offset > latest returns empty list for read
        List<Message> beyond = broker.read("offset-bounds", 0, 100L);
        assertThat(beyond).isEmpty();

        // Offset > latest throws MessageNotFoundException for getMessage
        assertThatThrownBy(() -> broker.getMessage("offset-bounds", 0, 100L))
                .isInstanceOf(MessageNotFoundException.class);
    }

    // =========================================================================
    // Scenario 9: Invalid Partition Exception
    // =========================================================================
    @Test
    @DisplayName("Scenario 9: Accessing invalid partition throws InvalidPartitionException")
    void testScenario9_InvalidPartition() {
        broker.createTopic("partition-bounds", 2); // Partitions 0 and 1

        assertThatThrownBy(() -> broker.publishToPartition("partition-bounds", 2, null, "p", "prod"))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("partition-bounds");

        assertThatThrownBy(() -> broker.publishToPartition("partition-bounds", -1, null, "p", "prod"))
                .isInstanceOf(InvalidPartitionException.class);

        assertThatThrownBy(() -> broker.read("partition-bounds", 5, 0L))
                .isInstanceOf(InvalidPartitionException.class);

        assertThatThrownBy(() -> broker.getMessage("partition-bounds", 99, 0L))
                .isInstanceOf(InvalidPartitionException.class);
    }

    // =========================================================================
    // Scenario 10: Nonexistent Topic Exception
    // =========================================================================
    @Test
    @DisplayName("Scenario 10: Accessing nonexistent topic throws TopicNotFoundException")
    void testScenario10_NonexistentTopic() {
        assertThatThrownBy(() -> broker.publish("ghost-topic", "key", "payload", "prod"))
                .isInstanceOf(TopicNotFoundException.class)
                .hasMessageContaining("ghost-topic");

        assertThatThrownBy(() -> broker.publishToPartition("ghost-topic", 0, "key", "payload", "prod"))
                .isInstanceOf(TopicNotFoundException.class);

        assertThatThrownBy(() -> broker.read("ghost-topic", 0, 0L))
                .isInstanceOf(TopicNotFoundException.class);

        assertThatThrownBy(() -> broker.getMessage("ghost-topic", 0, 0L))
                .isInstanceOf(TopicNotFoundException.class);

        assertThatThrownBy(() -> broker.deleteTopic("ghost-topic"))
                .isInstanceOf(TopicNotFoundException.class);
    }

    // =========================================================================
    // Scenario 11: Multiple Partitions Independent 0-Based Offsets
    // =========================================================================
    @Test
    @DisplayName("Scenario 11: Multiple partitions each start independently from offset 0")
    void testScenario11_MultiplePartitionsIndependentOffsets() {
        broker.createTopic("multi-part", 3);

        PublishResult p0_m0 = broker.publishToPartition("multi-part", 0, null, "p0-0", "p");
        PublishResult p0_m1 = broker.publishToPartition("multi-part", 0, null, "p0-1", "p");

        PublishResult p1_m0 = broker.publishToPartition("multi-part", 1, null, "p1-0", "p");

        PublishResult p2_m0 = broker.publishToPartition("multi-part", 2, null, "p2-0", "p");
        PublishResult p2_m1 = broker.publishToPartition("multi-part", 2, null, "p2-1", "p");
        PublishResult p2_m2 = broker.publishToPartition("multi-part", 2, null, "p2-2", "p");

        assertThat(p0_m0.offset()).isEqualTo(0L);
        assertThat(p0_m1.offset()).isEqualTo(1L);

        assertThat(p1_m0.offset()).isEqualTo(0L);

        assertThat(p2_m0.offset()).isEqualTo(0L);
        assertThat(p2_m1.offset()).isEqualTo(1L);
        assertThat(p2_m2.offset()).isEqualTo(2L);
    }

    // =========================================================================
    // Concurrency Stress Test: 20 Threads x 100 Messages = 2000 Messages
    // =========================================================================
    @Test
    @DisplayName("Concurrency: 20 threads x 100 messages (2,000 total) to single partition with zero gaps/duplicates")
    void testConcurrency_2000MessagesSinglePartition() throws InterruptedException {
        String topicName = "stress-single";
        broker.createTopic(topicName, 1);

        int threads = 20;
        int msgsPerThread = 100;
        int totalExpected = threads * msgsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        List<PublishResult> results = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int m = 0; m < msgsPerThread; m++) {
                        PublishResult res = broker.publishToPartition(
                                topicName,
                                0,
                                "t-" + threadId,
                                "data-" + threadId + "-" + m,
                                "prod-" + threadId
                        );
                        results.add(res);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(completed).isTrue();

        assertThat(results).hasSize(totalExpected);

        // Verify 2,000 unique message IDs
        Set<String> uniqueIds = new HashSet<>();
        for (PublishResult r : results) {
            uniqueIds.add(r.messageId());
        }
        assertThat(uniqueIds).hasSize(totalExpected);

        // Verify 2,000 continuous sequential offsets 0..1999
        List<Message> stored = broker.read(topicName, 0, 0L);
        assertThat(stored).hasSize(totalExpected);

        for (int i = 0; i < totalExpected; i++) {
            assertThat(stored.get(i).getOffset()).isEqualTo((long) i);
        }
    }

    // =========================================================================
    // Concurrent Multi-Partition Test
    // =========================================================================
    @Test
    @DisplayName("Concurrency: 20 threads x 100 messages distributed across 4 partitions concurrently")
    void testConcurrency_MultiPartition() throws InterruptedException {
        String topicName = "stress-multi";
        int partitions = 4;
        broker.createTopic(topicName, partitions);

        int threads = 20;
        int msgsPerThread = 100;
        int totalExpected = threads * msgsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        Set<String> publishedIds = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int m = 0; m < msgsPerThread; m++) {
                        String key = (m % 3 == 0) ? null : ("key-" + (threadId % 8));
                        PublishResult res = broker.publish(
                                topicName,
                                key,
                                "payload-" + threadId + "-" + m,
                                "prod-" + threadId
                        );
                        publishedIds.add(res.messageId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(completed).isTrue();

        assertThat(publishedIds).hasSize(totalExpected);

        int totalRead = 0;
        for (int p = 0; p < partitions; p++) {
            List<Message> partMsgs = broker.read(topicName, p, 0L);
            totalRead += partMsgs.size();

            // Verify continuous 0-based offsets per partition
            for (int i = 0; i < partMsgs.size(); i++) {
                assertThat(partMsgs.get(i).getOffset()).isEqualTo((long) i);
            }
        }
        assertThat(totalRead).isEqualTo(totalExpected);
    }

    // =========================================================================
    // Topic Deletion Test
    // =========================================================================
    @Test
    @DisplayName("Topic deletion: create, delete, verify existence is false, verify exception on access")
    void testTopicDeletion() {
        broker.createTopic("to-delete", 2);
        assertThat(broker.topicExists("to-delete")).isTrue();

        Topic deleted = broker.deleteTopic("to-delete");
        assertThat(deleted.getName()).isEqualTo("to-delete");
        assertThat(broker.topicExists("to-delete")).isFalse();

        assertThatThrownBy(() -> broker.getTopic("to-delete"))
                .isInstanceOf(TopicNotFoundException.class);
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================
    @Test
    @DisplayName("Edge cases: blank name, null name, partition count 0/negative, null/empty payload, null key")
    void testEdgeCases() {
        // Blank topic name
        assertThatThrownBy(() -> broker.createTopic("", 1))
                .isInstanceOf(InvalidTopicException.class);

        assertThatThrownBy(() -> broker.createTopic("   ", 1))
                .isInstanceOf(InvalidTopicException.class);

        // Null topic name
        assertThatThrownBy(() -> broker.createTopic(null, 1))
                .isInstanceOf(InvalidTopicException.class);

        // Partition count 0
        assertThatThrownBy(() -> broker.createTopic("zero-part", 0))
                .isInstanceOf(InvalidTopicException.class);

        // Negative partition count
        assertThatThrownBy(() -> broker.createTopic("neg-part", -3))
                .isInstanceOf(InvalidTopicException.class);

        broker.createTopic("edge-cases", 2);

        // Null payload
        assertThatThrownBy(() -> broker.publish("edge-cases", "k", null, "prod"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> broker.publishToPartition("edge-cases", 0, "k", null, "prod"))
                .isInstanceOf(IllegalArgumentException.class);

        // Empty payload (allowed, string "")
        PublishResult emptyPayloadRes = broker.publish("edge-cases", "k", "", "prod");
        assertThat(emptyPayloadRes.offset()).isEqualTo(0L);
        Message emptyPayloadMsg = broker.getMessage("edge-cases", emptyPayloadRes.partition(), 0L);
        assertThat(emptyPayloadMsg.getPayload()).isEqualTo("");

        // Null key (allowed, uses round-robin)
        PublishResult nullKeyRes = broker.publish("edge-cases", null, "payload", "prod");
        assertThat(nullKeyRes.messageId()).isNotBlank();
    }
}
