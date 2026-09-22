package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.exception.TopicNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ProducerTest {

    private Broker broker;
    private Producer producer;
    private ProducerConfig config;

    @BeforeEach
    void setUp() {
        broker = new Broker();
        config = ProducerConfig.builder().producerId("prod-1").build();
        producer = new Producer(broker, config);
    }

    @Test
    void testProducerRecordValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord(null, "key", "payload", "prod-1"));
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("", "key", "payload", "prod-1"));
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("  ", "key", "payload", "prod-1"));

        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("topic", "key", null, "prod-1"));

        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("topic", "key", "payload", null));
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("topic", "key", "payload", ""));
        assertThrows(IllegalArgumentException.class, () -> new ProducerRecord("topic", "key", "payload", "   "));

        // Valid record
        assertDoesNotThrow(() -> new ProducerRecord("topic", null, "payload", "prod-1"));
    }

    @Test
    void testSendSuccess() {
        broker.createTopic("test-topic", 3);

        ProducerRecord record = new ProducerRecord("test-topic", "key1", "payload1", "prod-1");
        RecordMetadata metadata = producer.send(record);

        assertNotNull(metadata);
        assertEquals("test-topic", metadata.topic());
        assertNotNull(metadata.messageId());
        assertEquals("prod-1", metadata.producerId());
        assertTrue(metadata.partition() >= 0 && metadata.partition() < 3);
        assertEquals(0, metadata.offset());
        assertNotNull(metadata.timestamp());
    }

    @Test
    void testSendNonExistentTopic() {
        ProducerRecord record = new ProducerRecord("missing-topic", null, "payload", "prod-1");
        assertThrows(TopicNotFoundException.class, () -> producer.send(record));
    }

    @Test
    void testSendAfterCloseFails() {
        producer.close();
        ProducerRecord record = new ProducerRecord("test-topic", null, "payload", "prod-1");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> producer.send(record));
        assertTrue(ex.getMessage().contains("closed"));
    }
}
