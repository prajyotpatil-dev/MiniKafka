package com.minikafka.producer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ProducerConfigTest {

    @Test
    void testValidConfig() {
        ProducerConfig config = ProducerConfig.builder()
                .producerId("prod-1")
                .clientId("client-1")
                .build();

        assertEquals("prod-1", config.getProducerId());
        assertEquals("client-1", config.getClientId());
    }

    @Test
    void testMinimumValidConfig() {
        ProducerConfig config = ProducerConfig.builder()
                .producerId("prod-1")
                .build();

        assertEquals("prod-1", config.getProducerId());
        assertNull(config.getClientId());
    }

    @Test
    void testInvalidProducerId() {
        assertThrows(NullPointerException.class, () ->
            ProducerConfig.builder().build()
        );

        assertThrows(IllegalArgumentException.class, () ->
            ProducerConfig.builder().producerId("").build()
        );

        assertThrows(IllegalArgumentException.class, () ->
            ProducerConfig.builder().producerId("   ").build()
        );
    }
}
