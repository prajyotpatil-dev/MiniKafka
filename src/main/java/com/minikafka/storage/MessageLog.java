package com.minikafka.storage;

import com.minikafka.model.Message;

import java.util.List;
import java.util.Optional;

/**
 * Abstraction representing an append-only log of ordered messages for a single partition.
 * Designed to support in-memory storage now and file-backed/segmented logs in future phases.
 */
public interface MessageLog {

    /**
     * Appends a message to the end of the log.
     *
     * @param message the message to append
     * @return the offset assigned to the appended message
     */
    long append(Message message);

    /**
     * Reads a single message at a specific offset.
     *
     * @param offset the 0-based offset
     * @return an Optional containing the message if found, or empty if offset is beyond latest
     */
    Optional<Message> read(long offset);

    /**
     * Reads all messages starting from a given offset up to the latest message.
     *
     * @param offset the starting 0-based offset
     * @return list of messages from the offset onward (empty if offset is beyond latest)
     */
    List<Message> readFrom(long offset);

    /**
     * Reads up to {@code limit} messages starting from a given offset.
     *
     * @param offset the starting 0-based offset
     * @param limit  the maximum number of messages to return
     * @return list of messages
     */
    List<Message> readFrom(long offset, int limit);

    /**
     * Returns the total number of messages in the log.
     */
    int size();

    /**
     * Returns the offset of the most recently appended message, or -1 if the log is empty.
     */
    long getLatestOffset();

    /**
     * Returns the next offset that will be assigned on append (starts at 0 for an empty log).
     */
    long getNextOffset();

    /**
     * Returns whether the log contains zero messages.
     */
    boolean isEmpty();
}
