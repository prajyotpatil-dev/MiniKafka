package com.minikafka.broker;

import com.minikafka.model.Topic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Strategy interface for selecting a partition within a topic for message publishing.
 */
public interface Partitioner {

    /**
     * Chooses a partition index for the message.
     *
     * @param topic the target Topic
     * @param key   the message key (can be null or empty)
     * @return 0-based partition index
     */
    int partition(Topic topic, String key);

    /**
     * Default partitioner implementation:
     * - If key is provided and non-empty: uses Murmur/hash-based partition selection.
     * - If key is null or empty: uses round-robin partition selection per topic.
     */
    class DefaultPartitioner implements Partitioner {

        private final ConcurrentMap<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();

        @Override
        public int partition(Topic topic, String key) {
            int numPartitions = topic.getPartitionCount();
            if (numPartitions == 1) {
                return 0;
            }

            if (key != null && !key.isEmpty()) {
                // Key-based partitioning (deterministic hashing)
                int hash = hashKey(key);
                return Math.abs(hash % numPartitions);
            } else {
                // Round-robin partitioning when key is absent
                AtomicInteger counter = roundRobinCounters.computeIfAbsent(
                        topic.getName(),
                        k -> new AtomicInteger(0)
                );
                int count = counter.getAndIncrement();
                // Ensure positive modulo even if integer overflows
                return (count & 0x7fffffff) % numPartitions;
            }
        }

        private int hashKey(String key) {
            // Standard deterministic 32-bit FNV-1a hash
            int hash = 0x811c9dc5;
            for (int i = 0; i < key.length(); i++) {
                hash ^= key.charAt(i);
                hash *= 0x01000193;
            }
            return hash;
        }
    }
}
