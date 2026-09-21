package com.minikafka.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageTest {

    @Test
    @DisplayName("Should create Message with all properties correctly set")
    void testMessageCreation() {
        Instant now = Instant.now();
        Message message = new Message(
                "msg-123",
                "events-topic",
                2,
                15L,
                "user-42",
                "{\"action\":\"login\"}",
                now,
                "auth-service"
        );

        assertThat(message.getMessageId()).isEqualTo("msg-123");
        assertThat(message.getTopic()).isEqualTo("events-topic");
        assertThat(message.getPartition()).isEqualTo(2);
        assertThat(message.getOffset()).isEqualTo(15L);
        assertThat(message.getKey()).isEqualTo("user-42");
        assertThat(message.getPayload()).isEqualTo("{\"action\":\"login\"}");
        assertThat(message.getTimestamp()).isEqualTo(now);
        assertThat(message.getProducerId()).isEqualTo("auth-service");
    }

    @Test
    @DisplayName("Should generate unique message IDs via builder")
    void testMessageIdUniqueness() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            Message msg = Message.builder()
                    .topic("test")
                    .partition(0)
                    .payload("payload-" + i)
                    .build();
            ids.add(msg.getMessageId());
        }
        assertThat(ids).hasSize(1000);
    }

    @Test
    @DisplayName("Should reject null topic or payload")
    void testNullValidation() {
        assertThatThrownBy(() -> new Message("1", null, 0, 0, null, "data", Instant.now(), "prod"))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new Message("1", "test", 0, 0, null, null, Instant.now(), "prod"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Should support equals and hashCode contracts")
    void testEqualsAndHashCode() {
        Instant now = Instant.now();
        Message msg1 = new Message("1", "t", 0, 0, "k", "p", now, "prod");
        Message msg2 = new Message("1", "t", 0, 0, "k", "p", now, "prod");
        Message msg3 = new Message("2", "t", 0, 0, "k", "p", now, "prod");

        assertThat(msg1).isEqualTo(msg2);
        assertThat(msg1.hashCode()).isEqualTo(msg2.hashCode());
        assertThat(msg1).isNotEqualTo(msg3);
    }
}
