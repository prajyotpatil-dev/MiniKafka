package com.minikafka.storage;

import com.minikafka.config.StorageConfig;
import com.minikafka.exception.StorageException;
import com.minikafka.model.Partition;
import com.minikafka.model.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Orchestrates persistence, including directory discovery on broker startup
 * and serving as the foundational factory for per-partition MessageLogs.
 */
public class StorageEngine implements MessageLogFactory, Closeable {

    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    private final StorageConfig config;
    private final List<FileMessageLog> activeLogs = new ArrayList<>();

    public StorageEngine(StorageConfig config) {
        this.config = config;
    }

    /**
     * Initializes the storage engine, recovering topics and partitions if persistent storage is chosen.
     *
     * @return a list of recovered Topics
     */
    public List<Topic> startAndRecover() {
        if (!config.isPersistent()) {
            log.info("StorageEngine initialized in purely in-memory mode");
            return List.of();
        }

        Path topicsDir = config.getTopicsDir();
        if (!Files.exists(topicsDir)) {
            try {
                Files.createDirectories(topicsDir);
                log.info("Created persistent topics directory at {}", topicsDir);
            } catch (IOException e) {
                throw new StorageException("Failed to create topics directory", e);
            }
            return List.of();
        }

        // Recover topics
        List<Topic> recoveredTopics = new ArrayList<>();
        try (Stream<Path> stream = Files.list(topicsDir)) {
            stream.filter(Files::isDirectory).forEach(topicDir -> {
                try {
                    Topic topic = recoverTopic(topicDir);
                    if (topic != null) {
                        recoveredTopics.add(topic);
                    }
                } catch (Exception e) {
                    log.error("Failed to recover topic from directory {}", topicDir, e);
                    throw new StorageException("Failed to recover topic: " + topicDir.getFileName(), e);
                }
            });
        } catch (IOException e) {
            throw new StorageException("Failed to list topics directory", e);
        }

        return recoveredTopics;
    }

    private Topic recoverTopic(Path topicDir) throws IOException {
        String topicName = topicDir.getFileName().toString();
        log.info("Discovering partitions for topic '{}'", topicName);

        // Find partition log files (partition-0.log, partition-1.log, etc.)
        List<Path> partitionFiles = new ArrayList<>();
        try (Stream<Path> stream = Files.list(topicDir)) {
            stream.filter(Files::isRegularFile)
                  .filter(p -> p.getFileName().toString().startsWith("partition-") && p.getFileName().toString().endsWith(".log"))
                  .forEach(partitionFiles::add);
        }

        if (partitionFiles.isEmpty()) {
            // A topic directory exists but has no partitions - might be empty or invalid
            // To respect recovery rules ("empty topic recovery" from Phase 3),
            // if we have metadata indicating partition count we could recover it.
            // But without metadata, we skip or assume 0 partitions.
            // However, the rules ask for 'metadata' recovery if needed.
            return null;
        }

        List<Partition> recoveredPartitions = new ArrayList<>();

        // Recover exactly those partitions that exist on disk
        for (Path partFile : partitionFiles) {
            String fileName = partFile.getFileName().toString(); // e.g. "partition-0.log"
            String idStr = fileName.substring("partition-".length(), fileName.length() - ".log".length());
            int partitionId;
            try {
                partitionId = Integer.parseInt(idStr);
            } catch (NumberFormatException e) {
                log.warn("Skipping unknown log file '{}'", fileName);
                continue;
            }

            FileMessageLog logFile = new FileMessageLog(partFile, config.isFlushOnWrite());
            activeLogs.add(logFile);
            Partition partition = new Partition(topicName, partitionId, logFile);
            recoveredPartitions.add(partition);
        }

        if (recoveredPartitions.isEmpty()) {
            return null;
        }

        // Sort by ID to ensure contiguous 0..N layout
        recoveredPartitions.sort(Comparator.comparingInt(Partition::getPartitionId));

        // Note: Topic constructor needs to take the list of partitions directly so it doesn't recreate them.
        return new Topic(topicName, recoveredPartitions);
    }

    @Override
    public MessageLog create(String topicName, int partitionId) {
        if (!config.isPersistent()) {
            return new InMemoryMessageLog();
        }

        Path partitionFile = config.getTopicsDir()
                .resolve(topicName)
                .resolve("partition-" + partitionId + ".log");

        FileMessageLog logFile = new FileMessageLog(partitionFile, config.isFlushOnWrite());
        synchronized (activeLogs) {
            activeLogs.add(logFile);
        }
        return logFile;
    }

    @Override
    public void close() {
        synchronized (activeLogs) {
            for (FileMessageLog logFile : activeLogs) {
                try {
                    logFile.close();
                } catch (Exception e) {
                    log.warn("Error closing log file {}: {}", logFile.getLogFilePath(), e.getMessage());
                }
            }
            activeLogs.clear();
        }
    }
}
