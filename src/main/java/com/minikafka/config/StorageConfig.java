package com.minikafka.config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Configuration for MiniKafka persistent storage.
 * <p>
 * Controls the base directory where topic/partition log files are stored.
 * When persistence is disabled (dataDir is null), the broker operates
 * purely in-memory, preserving Phase 1/2 behavior.
 */
public class StorageConfig {

    /** Default data directory relative to the working directory. */
    public static final String DEFAULT_DATA_DIR = "./data/minikafka";

    private final Path dataDir;
    private final boolean flushOnWrite;

    private StorageConfig(Path dataDir, boolean flushOnWrite) {
        this.dataDir = dataDir;
        this.flushOnWrite = flushOnWrite;
    }

    /**
     * Creates a StorageConfig with persistent storage at the given directory.
     *
     * @param dataDir the base directory for persistent partition logs
     * @return a persistent StorageConfig
     */
    public static StorageConfig persistent(Path dataDir) {
        Objects.requireNonNull(dataDir, "dataDir must not be null for persistent storage");
        return new StorageConfig(dataDir, true);
    }

    /**
     * Creates a StorageConfig with persistent storage at the given path string.
     *
     * @param dataDir the base directory path
     * @return a persistent StorageConfig
     */
    public static StorageConfig persistent(String dataDir) {
        Objects.requireNonNull(dataDir, "dataDir must not be null for persistent storage");
        return persistent(Paths.get(dataDir));
    }

    /**
     * Creates a StorageConfig with the default data directory.
     *
     * @return a persistent StorageConfig using the default path
     */
    public static StorageConfig defaultPersistent() {
        return persistent(DEFAULT_DATA_DIR);
    }

    /**
     * Creates a StorageConfig for purely in-memory operation (no persistence).
     *
     * @return an in-memory StorageConfig
     */
    public static StorageConfig inMemory() {
        return new StorageConfig(null, false);
    }

    /**
     * Returns the base data directory, or null if in-memory mode.
     */
    public Path getDataDir() {
        return dataDir;
    }

    /**
     * Returns the topics directory under the data directory.
     */
    public Path getTopicsDir() {
        if (dataDir == null) {
            throw new IllegalStateException("Cannot get topics directory in in-memory mode");
        }
        return dataDir.resolve("topics");
    }

    /**
     * Returns whether persistence is enabled.
     */
    public boolean isPersistent() {
        return dataDir != null;
    }

    /**
     * Returns whether each write should be flushed/forced to disk.
     */
    public boolean isFlushOnWrite() {
        return flushOnWrite;
    }

    @Override
    public String toString() {
        return "StorageConfig{" +
                "dataDir=" + dataDir +
                ", persistent=" + isPersistent() +
                ", flushOnWrite=" + flushOnWrite +
                '}';
    }
}
