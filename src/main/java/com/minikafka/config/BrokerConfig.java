package com.minikafka.config;

/**
 * Configuration properties for MiniKafka broker defaults.
 */
public class BrokerConfig {

    private int defaultPartitions = 1;
    private int maxPartitionsPerTopic = 100;
    private int defaultReadBatchLimit = 500;
    private StorageConfig storageConfig = StorageConfig.inMemory();

    public BrokerConfig() {
    }

    public BrokerConfig(int defaultPartitions, int maxPartitionsPerTopic, int defaultReadBatchLimit) {
        this.defaultPartitions = defaultPartitions;
        this.maxPartitionsPerTopic = maxPartitionsPerTopic;
        this.defaultReadBatchLimit = defaultReadBatchLimit;
    }

    public int getDefaultPartitions() {
        return defaultPartitions;
    }

    public void setDefaultPartitions(int defaultPartitions) {
        this.defaultPartitions = defaultPartitions;
    }

    public int getMaxPartitionsPerTopic() {
        return maxPartitionsPerTopic;
    }

    public void setMaxPartitionsPerTopic(int maxPartitionsPerTopic) {
        this.maxPartitionsPerTopic = maxPartitionsPerTopic;
    }

    public int getDefaultReadBatchLimit() {
        return defaultReadBatchLimit;
    }

    public void setDefaultReadBatchLimit(int defaultReadBatchLimit) {
        this.defaultReadBatchLimit = defaultReadBatchLimit;
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    public void setStorageConfig(StorageConfig storageConfig) {
        this.storageConfig = storageConfig != null ? storageConfig : StorageConfig.inMemory();
    }
}
