package com.minikafka.broker;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.model.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OffsetManagerTest {

    private OffsetManager offsetManager;

    @BeforeEach
    void setUp() {
        offsetManager = new OffsetManager();
    }

    @Test
    @DisplayName("Should commit and retrieve consumer group offsets")
    void testCommitAndGetOffsets() {
        TopicPartition tp0 = new TopicPartition("payments", 0);
        TopicPartition tp1 = new TopicPartition("payments", 1);

        assertThat(offsetManager.getCommittedOffset("group-1", tp0)).isEqualTo(-1L);

        offsetManager.commitOffset("group-1", tp0, 10L);
        offsetManager.commitOffset("group-1", tp1, 5L);

        assertThat(offsetManager.getCommittedOffset("group-1", tp0)).isEqualTo(10L);
        assertThat(offsetManager.getCommittedOffset("group-1", tp1)).isEqualTo(5L);

        // Different group has independent offsets
        assertThat(offsetManager.getCommittedOffset("group-2", tp0)).isEqualTo(-1L);
    }

    @Test
    @DisplayName("Should reject negative offset commits")
    void testNegativeOffsetCommit() {
        TopicPartition tp0 = new TopicPartition("payments", 0);
        assertThatThrownBy(() -> offsetManager.commitOffset("group-1", tp0, -1L))
                .isInstanceOf(InvalidOffsetException.class);
    }

    @Test
    @DisplayName("Should return all group offsets and reset group correctly")
    void testGetGroupOffsetsAndReset() {
        TopicPartition tp0 = new TopicPartition("events", 0);
        TopicPartition tp1 = new TopicPartition("events", 1);

        offsetManager.commitOffset("analytics-group", tp0, 100L);
        offsetManager.commitOffset("analytics-group", tp1, 200L);

        Map<TopicPartition, Long> offsets = offsetManager.getGroupOffsets("analytics-group");
        assertThat(offsets).containsEntry(tp0, 100L).containsEntry(tp1, 200L);

        offsetManager.resetGroup("analytics-group");
        assertThat(offsetManager.getGroupOffsets("analytics-group")).isEmpty();
    }

    @Test
    @DisplayName("Should remove partition offsets when partition is removed")
    void testRemovePartitionOffsets() {
        TopicPartition tp0 = new TopicPartition("events", 0);
        offsetManager.commitOffset("grp1", tp0, 50L);
        offsetManager.commitOffset("grp2", tp0, 60L);

        offsetManager.removePartitionOffsets(tp0);
        assertThat(offsetManager.getCommittedOffset("grp1", tp0)).isEqualTo(-1L);
        assertThat(offsetManager.getCommittedOffset("grp2", tp0)).isEqualTo(-1L);
    }
}
