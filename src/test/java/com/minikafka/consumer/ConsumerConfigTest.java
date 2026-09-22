package com.minikafka.consumer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerConfigTest {

    @Test
    void testValidConfig() {
        ConsumerConfig config = ConsumerConfig.builder()
                .consumerId("cons-1")
                .clientId("client-1")
                .defaultPollBatchSize(50)
                .build();

        assertEquals("cons-1", config.getConsumerId());
        assertEquals("client-1", config.getClientId());
        assertEquals(50, config.getDefaultPollBatchSize());
    }

    @Test
    void testMinimumValidConfig() {
        ConsumerConfig config = ConsumerConfig.builder()
                .consumerId("cons-1")
                .build();

        assertEquals("cons-1", config.getConsumerId());
        assertNull(config.getClientId());
        assertEquals(ConsumerConfig.DEFAULT_POLL_BATCH_SIZE, config.getDefaultPollBatchSize());
    }

    @Test
    void testInvalidConsumerId() {
        assertThrows(NullPointerException.class, () ->
            ConsumerConfig.builder().build()
        );

        assertThrows(IllegalArgumentException.class, () ->
            ConsumerConfig.builder().consumerId("").build()
        );

        assertThrows(IllegalArgumentException.class, () ->
            ConsumerConfig.builder().consumerId("   ").build()
        );
    }

    @Test
    void testInvalidBatchSize() {
        assertThrows(IllegalArgumentException.class, () ->
            ConsumerConfig.builder().consumerId("c1").defaultPollBatchSize(0).build()
        );

        assertThrows(IllegalArgumentException.class, () ->
            ConsumerConfig.builder().consumerId("c1").defaultPollBatchSize(-10).build()
        );
    }
}
