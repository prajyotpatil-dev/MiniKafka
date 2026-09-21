package com.minikafka.broker;

import com.minikafka.exception.InvalidTopicException;
import com.minikafka.exception.TopicAlreadyExistsException;
import com.minikafka.exception.TopicNotFoundException;
import com.minikafka.model.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicManagerTest {

    private TopicManager topicManager;

    @BeforeEach
    void setUp() {
        topicManager = new TopicManager();
    }

    @Test
    @DisplayName("Should successfully create and retrieve a topic")
    void testCreateAndGetTopic() {
        Topic topic = topicManager.createTopic("orders", 3);

        assertThat(topic.getName()).isEqualTo("orders");
        assertThat(topic.getPartitionCount()).isEqualTo(3);
        assertThat(topicManager.topicExists("orders")).isTrue();
        assertThat(topicManager.getTopicCount()).isEqualTo(1);

        Topic retrieved = topicManager.getTopic("orders");
        assertThat(retrieved).isEqualTo(topic);
    }

    @Test
    @DisplayName("Should reject duplicate topic creation")
    void testDuplicateTopic() {
        topicManager.createTopic("orders", 2);

        assertThatThrownBy(() -> topicManager.createTopic("orders", 4))
                .isInstanceOf(TopicAlreadyExistsException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    @DisplayName("Should validate topic name and partition count")
    void testInvalidTopicCreation() {
        assertThatThrownBy(() -> topicManager.createTopic("", 1))
                .isInstanceOf(InvalidTopicException.class);

        assertThatThrownBy(() -> topicManager.createTopic(null, 1))
                .isInstanceOf(InvalidTopicException.class);

        assertThatThrownBy(() -> topicManager.createTopic("invalid-partitions", 0))
                .isInstanceOf(InvalidTopicException.class);
    }

    @Test
    @DisplayName("Should delete topic and verify it no longer exists")
    void testDeleteTopic() {
        topicManager.createTopic("orders", 2);
        assertThat(topicManager.topicExists("orders")).isTrue();

        Topic deleted = topicManager.deleteTopic("orders");
        assertThat(deleted.getName()).isEqualTo("orders");
        assertThat(topicManager.topicExists("orders")).isFalse();
        assertThat(topicManager.getTopicCount()).isEqualTo(0);

        assertThatThrownBy(() -> topicManager.getTopic("orders"))
                .isInstanceOf(TopicNotFoundException.class);

        assertThatThrownBy(() -> topicManager.deleteTopic("orders"))
                .isInstanceOf(TopicNotFoundException.class);
    }

    @Test
    @DisplayName("Should list all created topics")
    void testListTopics() {
        topicManager.createTopic("topic1", 1);
        topicManager.createTopic("topic2", 2);
        topicManager.createTopic("topic3", 3);

        assertThat(topicManager.listTopics())
                .hasSize(3)
                .extracting(Topic::getName)
                .containsExactlyInAnyOrder("topic1", "topic2", "topic3");
    }
}
