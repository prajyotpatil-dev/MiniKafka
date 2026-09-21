package com.minikafka.model;

import com.minikafka.exception.InvalidPartitionException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Domain entity representing a Topic in MiniKafka.
 * A Topic contains one or more ordered, numbered partitions starting from index 0.
 */
public class Topic {

    private final String name;
    private final List<Partition> partitions;
    private final Instant createdAt;

    public Topic(String name, int partitionCount) {
        this.name = validateTopicName(name);
        if (partitionCount < 1) {
            throw new InvalidPartitionException("Partition count must be at least 1, provided: " + partitionCount);
        }

        List<Partition> list = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            list.add(new Partition(this.name, i));
        }
        this.partitions = Collections.unmodifiableList(list);
        this.createdAt = Instant.now();
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitions.size();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    /**
     * Retrieves a specific partition by 0-based partition ID.
     *
     * @param partitionId 0-based partition index
     * @return the Partition
     * @throws InvalidPartitionException if partition index is out of bounds
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new InvalidPartitionException(name, partitionId, partitions.size());
        }
        return partitions.get(partitionId);
    }

    /**
     * Returns an unmodifiable list of all partitions in this topic.
     */
    public List<Partition> getPartitions() {
        return partitions;
    }

    private static String validateTopicName(String name) {
        Objects.requireNonNull(name, "Topic name must not be null");
        String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Topic name must not be blank");
        }
        // Validate valid characters: alphanumeric, dots, underscores, hyphens
        if (!trimmed.matches("^[a-zA-Z0-9._-]+$")) {
            throw new IllegalArgumentException("Topic name '" + name + "' contains invalid characters. Use alphanumeric, '.', '_' or '-'.");
        }
        return trimmed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topic topic = (Topic) o;
        return Objects.equals(name, topic.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions.size() +
                ", createdAt=" + createdAt +
                '}';
    }
}
