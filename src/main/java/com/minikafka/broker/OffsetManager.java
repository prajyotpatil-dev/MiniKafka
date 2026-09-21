package com.minikafka.broker;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.model.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OffsetManager maintains consumer group commit offsets and partition high watermarks.
 * <p>
 * Responsibility Model:
 * 1. Low-level physical append offsets are owned and assigned by the Partition's MessageLog (ensuring
 *    strictly serialized log consistency at append time).
 * 2. OffsetManager coordinates logical partition high watermarks and committed offsets across consumer groups.
 * </p>
 */
public class OffsetManager {

    // (consumerGroup + TopicPartition) -> committed offset
    private final ConcurrentMap<String, ConcurrentMap<TopicPartition, AtomicLong>> consumerOffsets = new ConcurrentHashMap<>();

    /**
     * Gets the next offset for a partition from the partition instance directly.
     */
    public long getCommittedOffset(String consumerGroup, TopicPartition topicPartition) {
        if (consumerGroup == null || topicPartition == null) {
            return -1L;
        }
        ConcurrentMap<TopicPartition, AtomicLong> groupMap = consumerOffsets.get(consumerGroup);
        if (groupMap == null) {
            return -1L;
        }
        AtomicLong offset = groupMap.get(topicPartition);
        return offset != null ? offset.get() : -1L;
    }

    /**
     * Commits an offset for a given consumer group and topic-partition.
     */
    public void commitOffset(String consumerGroup, TopicPartition topicPartition, long offset) {
        if (offset < 0) {
            throw new InvalidOffsetException(offset, "Committed offset must be non-negative");
        }
        consumerOffsets
                .computeIfAbsent(consumerGroup, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(topicPartition, k -> new AtomicLong())
                .set(offset);
    }

    /**
     * Resets / clears committed offsets for a consumer group.
     */
    public void resetGroup(String consumerGroup) {
        if (consumerGroup != null) {
            consumerOffsets.remove(consumerGroup);
        }
    }

    /**
     * Clears all committed offsets for a partition when a topic is deleted.
     */
    public void removePartitionOffsets(TopicPartition topicPartition) {
        for (ConcurrentMap<TopicPartition, AtomicLong> groupMap : consumerOffsets.values()) {
            groupMap.remove(topicPartition);
        }
    }

    /**
     * Returns all committed offsets for a specific consumer group.
     */
    public Map<TopicPartition, Long> getGroupOffsets(String consumerGroup) {
        ConcurrentMap<TopicPartition, AtomicLong> groupMap = consumerOffsets.get(consumerGroup);
        if (groupMap == null) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Long> result = new ConcurrentHashMap<>();
        groupMap.forEach((tp, atomicVal) -> result.put(tp, atomicVal.get()));
        return result;
    }
}
