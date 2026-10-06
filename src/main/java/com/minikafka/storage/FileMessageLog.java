package com.minikafka.storage;

import com.minikafka.exception.CorruptedLogException;
import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.exception.StorageException;
import com.minikafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File-backed append-only implementation of {@link MessageLog}.
 * <p>
 * Each instance manages one partition log file. The format per record is:
 * <pre>
 *   [record_length: int (4 bytes)] [serialized_message: N bytes]
 * </pre>
 * This length-prefix framing allows safe recovery from truncated tails.
 * <p>
 * Thread safety: uses a ReentrantReadWriteLock to allow concurrent reads while
 * ensuring exclusive file access during appends.
 * <p>
 * Durability: after each append the file channel is forced (fsync) to the OS.
 * This guarantees that when append() returns successfully, the data has been
 * durably written as far as the Java NIO API allows. Hardware-level guarantees
 * depend on the OS and storage hardware.
 * <p>
 * Recovery policy: on open, the log is read sequentially. Complete records are
 * loaded into the in-memory index. If the final record is truncated (incomplete),
 * it is detected and the file is truncated back to the last complete record position.
 * Corruption detected in the middle of the log (not just the tail) causes a
 * {@link CorruptedLogException} — MiniKafka does not silently skip corrupted
 * middle-of-log entries.
 */
public class FileMessageLog implements MessageLog, Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileMessageLog.class);

    /** Number of bytes for the per-record length prefix. */
    private static final int LENGTH_PREFIX_BYTES = 4;

    private final Path logFilePath;
    private final boolean flushOnWrite;

    private final List<Message> entries = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private FileChannel channel;
    private volatile boolean closed = false;

    /**
     * Opens (or creates) a FileMessageLog at the given path.
     * If the file already contains data, it is recovered immediately.
     *
     * @param logFilePath the path to the partition .log file
     * @param flushOnWrite true to call {@link FileChannel#force(boolean)} after each append
     * @throws StorageException if the log file cannot be opened or recovery fails
     */
    public FileMessageLog(Path logFilePath, boolean flushOnWrite) {
        this.logFilePath = logFilePath;
        this.flushOnWrite = flushOnWrite;
        try {
            openAndRecover();
        } catch (IOException e) {
            throw new StorageException("Failed to open partition log at " + logFilePath, e);
        }
    }

    // ==========================================
    // MessageLog interface
    // ==========================================

    @Override
    public long append(Message message) {
        ensureOpen();
        java.util.Objects.requireNonNull(message, "Message must not be null");

        writeLock.lock();
        try {
            long assignedOffset = entries.size();

            // Rebuild message with the correct log-assigned offset if needed
            Message stored;
            if (message.getOffset() == assignedOffset) {
                stored = message;
            } else {
                stored = Message.builder()
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

            // Persist to disk BEFORE updating in-memory state
            writeRecord(stored);

            // Only update in-memory index after a successful disk write
            entries.add(stored);
            return assignedOffset;
        } catch (IOException e) {
            throw new StorageException("Failed to write record to partition log at " + logFilePath, e);
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
            return Collections.unmodifiableList(new ArrayList<>(entries.subList(fromIndex, toIndex)));
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

    // ==========================================
    // Lifecycle
    // ==========================================

    @Override
    public void close() {
        if (closed) {
            return;
        }
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    log.warn("Error closing file channel for {}: {}", logFilePath, e.getMessage());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    // ==========================================
    // Internals: open and recovery
    // ==========================================

    /**
     * Opens the file channel (creating directories/file if needed) and recovers
     * existing records.
     */
    private void openAndRecover() throws IOException {
        Path parent = logFilePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        channel = FileChannel.open(
                logFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        recoverExistingRecords();
    }

    /**
     * Reads all complete records from the log file into the in-memory entries list.
     * <p>
     * Truncated tail policy: If the final record is partially written (incomplete
     * length prefix or incomplete data), the file is truncated back to the last valid
     * position. The truncated partial record is NOT exposed as a valid message.
     * <p>
     * Corrupt middle policy: If a record in the middle of the log appears corrupt
     * (e.g., an offset sequence violation), a {@link CorruptedLogException} is thrown.
     */
    private void recoverExistingRecords() throws IOException {
        long fileSize = channel.size();
        if (fileSize == 0) {
            log.debug("Partition log {} is empty, starting fresh", logFilePath);
            return;
        }

        log.info("Recovering partition log {} ({} bytes)", logFilePath, fileSize);

        long position = 0;
        long lastCompletePosition = 0;
        int recovered = 0;

        while (position < fileSize) {
            // Attempt to read the 4-byte length prefix
            if (position + LENGTH_PREFIX_BYTES > fileSize) {
                // Truncated tail: incomplete length prefix
                log.warn("Truncated tail detected at position {} in {} (incomplete length prefix). " +
                        "Truncating file to last complete record at position {}.",
                        position, logFilePath, lastCompletePosition);
                channel.truncate(lastCompletePosition);
                break;
            }

            ByteBuffer lengthBuf = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);
            readFully(channel, lengthBuf, position);
            int recordLength = lengthBuf.getInt(0);

            if (recordLength <= 0 || recordLength > 100 * 1024 * 1024) {
                // Unexpected length value
                if (entries.isEmpty() && recovered == 0) {
                    // Nothing recovered yet, file may be entirely corrupt
                    throw new CorruptedLogException(
                            "Corrupt partition log at " + logFilePath +
                            ": invalid record length " + recordLength + " at position " + position);
                }
                // We have prior good records; treat this as a truncated tail
                log.warn("Invalid record length {} at position {} in {}. " +
                        "Treating as truncated tail; truncating to position {}.",
                        recordLength, position, logFilePath, lastCompletePosition);
                channel.truncate(lastCompletePosition);
                break;
            }

            long dataStart = position + LENGTH_PREFIX_BYTES;
            if (dataStart + recordLength > fileSize) {
                // Truncated tail: incomplete record data
                log.warn("Truncated tail detected: record at position {} claims {} bytes but only {} remain in {}. " +
                        "Truncating to last complete position {}.",
                        position, recordLength, fileSize - dataStart, logFilePath, lastCompletePosition);
                channel.truncate(lastCompletePosition);
                break;
            }

            // Read full record data
            ByteBuffer dataBuf = ByteBuffer.allocate(recordLength);
            readFully(channel, dataBuf, dataStart);

            // Deserialize
            Message msg;
            try {
                msg = MessageSerializer.deserialize(dataBuf.array());
            } catch (Exception e) {
                if (entries.isEmpty()) {
                    throw new CorruptedLogException(
                            "Failed to deserialize record at position " + position + " in " + logFilePath, e);
                }
                // Prior good records exist; is this the tail or the middle?
                if (position == lastCompletePosition) {
                    // This is right after the last good record => truncated tail
                    log.warn("Failed to deserialize record at position {} (immediately after last good record) in {}. " +
                            "Treating as truncated tail.", position, logFilePath);
                    channel.truncate(lastCompletePosition);
                    break;
                }
                // Middle corruption
                throw new CorruptedLogException(
                        "Deserialization failed for record at position " + position +
                        " (after prior good records) in " + logFilePath + ". " +
                        "Cannot safely recover.", e);
            }

            // Validate offset continuity for middle-of-log corruption detection
            long expectedOffset = entries.size();
            if (msg.getOffset() != expectedOffset) {
                if (position == lastCompletePosition) {
                    // Tail issue
                    log.warn("Offset discontinuity at tail (expected {} got {}) at position {} in {}. Truncating.",
                            expectedOffset, msg.getOffset(), position, logFilePath);
                    channel.truncate(lastCompletePosition);
                    break;
                }
                throw new CorruptedLogException(
                        "Partition log " + logFilePath + " has offset discontinuity at position " + position +
                        ": expected offset " + expectedOffset + " but found " + msg.getOffset() +
                        ". This indicates middle-of-log corruption.");
            }

            entries.add(msg);
            recovered++;
            lastCompletePosition = position + LENGTH_PREFIX_BYTES + recordLength;
            position = lastCompletePosition;
        }

        if (recovered > 0) {
            log.info("Recovered {} messages from partition log {} (next offset: {})",
                    recovered, logFilePath, entries.size());
        }
    }

    /**
     * Writes a single record in length-prefixed format:
     * [4-byte record length][N bytes serialized message]
     */
    private void writeRecord(Message message) throws IOException {
        byte[] data = MessageSerializer.serialize(message);
        writeFrameAtEnd(data.length, data);
    }

    /**
     * Appends the length-prefixed record to the channel at its current end.
     */
    private void writeFrameAtEnd(int recordLength, byte[] data) throws IOException {
        ByteBuffer frame = ByteBuffer.allocate(LENGTH_PREFIX_BYTES + recordLength);
        frame.putInt(recordLength);
        frame.put(data);
        frame.flip();

        long position = channel.size();
        while (frame.hasRemaining()) {
            position += channel.write(frame, position);
        }

        if (flushOnWrite) {
            channel.force(true);
        }
    }

    /**
     * Reads exactly {@code buf.remaining()} bytes from the channel at the given position.
     */
    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int read = ch.read(buf, pos);
            if (read < 0) {
                throw new StorageException("Unexpected end of file at position " + pos);
            }
            pos += read;
        }
        buf.flip();
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException("FileMessageLog is closed: " + logFilePath);
        }
    }

    private void validateNonNegativeOffset(long offset) {
        if (offset < 0) {
            throw new InvalidOffsetException(offset, "Offset must be non-negative (>= 0)");
        }
    }

    /**
     * Visible for testing: returns the path of the underlying log file.
     */
    public Path getLogFilePath() {
        return logFilePath;
    }
}
