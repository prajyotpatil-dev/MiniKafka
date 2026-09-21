package com.minikafka.storage;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.model.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe in-memory implementation of {@link MessageLog}.
 * Uses a {@link ReentrantReadWriteLock} to allow multiple concurrent readers
 * while ensuring exclusive access during append operations.
 */
public class InMemoryMessageLog implements MessageLog {

    private final List<Message> entries = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    @Override
    public long append(Message message) {
        Objects.requireNonNull(message, "Message must not be null");

        writeLock.lock();
        try {
            long assignedOffset = entries.size();

            // Ensure the stored message carries the exact partition-assigned offset
            Message entryToStore;
            if (message.getOffset() == assignedOffset) {
                entryToStore = message;
            } else {
                entryToStore = Message.builder()
                        .messageId(message.getMessageId())
                        .topic(message.getTopic())
                        .partition(message.getPartition())
                        .offset(assignedOffset)
                        .key(message.getKey())
                        .payload(message.getPayload())
                        .timestamp(message.getTimestamp())
                        .producerId(message.getProducerId())
                        .build();
            }

            entries.add(entryToStore);
            return assignedOffset;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<Message> read(long offset) {
        validateNonNegativeOffset(offset);

        readLock.lock();
        try {
            if (offset >= entries.size()) {
                return Optional.empty();
            }
            return Optional.of(entries.get((int) offset));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<Message> readFrom(long offset) {
        return readFrom(offset, Integer.MAX_VALUE);
    }

    @Override
    public List<Message> readFrom(long offset, int limit) {
        validateNonNegativeOffset(offset);
        if (limit <= 0) {
            return Collections.emptyList();
        }

        readLock.lock();
        try {
            int total = entries.size();
            if (offset >= total) {
                return Collections.emptyList();
            }

            int fromIndex = (int) offset;
            int toIndex = (int) Math.min((long) fromIndex + limit, total);

            List<Message> sublist = new ArrayList<>(toIndex - fromIndex);
            for (int i = fromIndex; i < toIndex; i++) {
                sublist.add(entries.get(i));
            }
            return Collections.unmodifiableList(sublist);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int size() {
        readLock.lock();
        try {
            return entries.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getLatestOffset() {
        readLock.lock();
        try {
            return entries.isEmpty() ? -1L : (long) entries.size() - 1;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long getNextOffset() {
        readLock.lock();
        try {
            return entries.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return entries.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    private void validateNonNegativeOffset(long offset) {
        if (offset < 0) {
            throw new InvalidOffsetException(offset, "Offset must be non-negative (>= 0)");
        }
    }
}
