package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerTest {

    private Broker broker;
    private Consumer consumer;
    private ConsumerConfig config;

    @BeforeEach
    void setUp() {
        broker = new Broker();
        config = ConsumerConfig.builder().consumerId("c1").defaultPollBatchSize(5).build();
        consumer = new Consumer(broker, config);
    }

    @Test
    void testSubscribe() {
        broker.createTopic("t1", 2);
        broker.createTopic("t2", 1);

        consumer.subscribe("t1", "t2");

        assertTrue(consumer.getSubscription().contains("t1"));
        assertTrue(consumer.getSubscription().contains("t2"));
        assertEquals(2, consumer.getSubscription().size());

        // Positions should be initialized to 0 for all partitions
        assertEquals(0L, consumer.position("t1", 0));
        assertEquals(0L, consumer.position("t1", 1));
        assertEquals(0L, consumer.position("t2", 0));
    }

    @Test
    void testSubscribeNonExistentTopic() {
        assertThrows(TopicNotFoundException.class, () -> consumer.subscribe("missing"));
    }

    @Test
    void testPollBeforeSubscribe() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> consumer.poll());
        assertTrue(ex.getMessage().contains("no active subscriptions"));
    }

    @Test
    void testEmptyPoll() {
        broker.createTopic("t1", 1);
        consumer.subscribe("t1");

        List<ConsumerRecord> records = consumer.poll();
        assertTrue(records.isEmpty());
        // Should not advance position
        assertEquals(0L, consumer.position("t1", 0));
    }

    @Test
    void testPollAdvancesPosition() {
        broker.createTopic("t1", 1);
        broker.publish("t1", null, "msg1", "p1");
        broker.publish("t1", null, "msg2", "p1");

        consumer.subscribe("t1");
        List<ConsumerRecord> records = consumer.poll(1); // poll exactly 1

        assertEquals(1, records.size());
        assertEquals("msg1", records.get(0).payload());
        assertEquals(1L, consumer.position("t1", 0));

        records = consumer.poll(); // default batch size (5)
        assertEquals(1, records.size());
        assertEquals("msg2", records.get(0).payload());
        assertEquals(2L, consumer.position("t1", 0));
    }

    @Test
    void testMultiPartitionRoundRobinPoll() {
        broker.createTopic("t1", 3);

        // Publish one message to each partition (by forcing keys that hash to different partitions,
        // or just publishing without keys - since we use round-robin by default, 3 messages should hit all 3 partitions
        broker.publish("t1", null, "m0", "p1");
        broker.publish("t1", null, "m1", "p1");
        broker.publish("t1", null, "m2", "p1");

        consumer.subscribe("t1");
        List<ConsumerRecord> records = consumer.poll(2); // Ask for 2

        assertEquals(2, records.size());
        // Since we sort partitions deterministically (t1/0, t1/1, t1/2),
        // round-robin poll should read from partition 0 then partition 1.

        // Polling 2 more
        List<ConsumerRecord> records2 = consumer.poll(2);
        assertEquals(1, records2.size()); // only 1 left
    }

    @Test
    void testSeek() {
        broker.createTopic("t1", 1);
        broker.publish("t1", null, "msg1", "p1");
        broker.publish("t1", null, "msg2", "p1");

        consumer.subscribe("t1");

        consumer.seek("t1", 0, 1L);
        assertEquals(1L, consumer.position("t1", 0));

        List<ConsumerRecord> records = consumer.poll();
        assertEquals(1, records.size());
        assertEquals("msg2", records.get(0).payload());
        assertEquals(2L, consumer.position("t1", 0));
    }

    @Test
    void testSeekInvalidOffset() {
        broker.createTopic("t1", 1);
        consumer.subscribe("t1");
        assertThrows(IllegalArgumentException.class, () -> consumer.seek("t1", 0, -1L));
    }

    @Test
    void testSeekInvalidTopic() {
        broker.createTopic("t1", 1);
        consumer.subscribe("t1");
        assertThrows(TopicNotFoundException.class, () -> consumer.seek("missing", 0, 0L));
        assertThrows(IllegalStateException.class, () -> {
            broker.createTopic("unsub", 1);
            consumer.seek("unsub", 0, 0L);
        });
    }

    @Test
    void testClose() {
        broker.createTopic("t1", 1);
        consumer.subscribe("t1");

        consumer.close();
        assertTrue(consumer.isClosed());

        assertThrows(IllegalStateException.class, () -> consumer.poll());
        assertThrows(IllegalStateException.class, () -> consumer.subscribe("t1"));
        assertThrows(IllegalStateException.class, () -> consumer.seek("t1", 0, 0L));
        assertThrows(IllegalStateException.class, () -> consumer.position("t1", 0));
    }
}
