# MiniKafka - Kafka-Inspired Message Broker in Java

MiniKafka is a lightweight, high-performance, Kafka-inspired message broker built in Java 21 and Spring Boot 3.4. It progresses through incremental phases — from an in-memory core engine, through rich client APIs, to durable file-backed persistent storage with crash recovery.

---

## 🏛️ Architecture Overview

Messages are organized into **Topics**, which are divided into **Partitions**. Producers write messages to partitions (round-robin or key-based), and Consumers poll messages using fair round-robin fetching. The storage layer is pluggable — each partition is backed by either an `InMemoryMessageLog` or a `FileMessageLog` depending on the configured `StorageConfig`.

```
┌─────────────────┐       ┌──────────────────────────────┐       ┌─────────────────┐
│                 │       │       MiniKafka Broker        │       │                 │
│ Producer Client ├───┬───▶  TopicManager, Partitioner   ├───────▶ Consumer Client │
│                 │   │   │  OffsetManager, StorageEngine │       │                 │
└─────────────────┘   │   └──────────────┬───────────────┘       └─────────────────┘
                      │                  │                                ▲
┌─────────────────┐   │   ┌──────────────▼───────────────┐                │
│                 │   │   │   Per-Partition MessageLog    │                │
│ Producer Client ├───┘   │  (InMemory or File-backed)   │                │
│                 │       └──────────────────────────────┘                │
└─────────────────┘                                                      │
                                                                         │
┌─────────────────┐                                                      │
│                 │                                                      │
│ Consumer Client ├──────────────────────────────────────────────────────┘
│                 │
└─────────────────┘
```

---

## 📦 Phase 1 — Core Broker Engine

The foundational in-memory broker with:

- **`Broker`**: Central entry point for topic creation, message publishing, and reading.
- **`TopicManager`**: Manages topic lifecycle (create, delete, list).
- **`Partitioner`**: Distributes messages across partitions (round-robin or key-hash based).
- **`OffsetManager`**: Tracks per-partition offsets.
- **`Message`** / **`Partition`** / **`Topic`**: Core domain models.
- Thread-safe via `ConcurrentHashMap` and `ReentrantReadWriteLock`.

---

## 📦 Phase 2 — Java Client APIs

High-level Producer and Consumer wrappers on top of the Phase 1 engine:

### Producer API

- **`ProducerConfig`**: Publisher configuration via builder pattern. Requires a `producerId`.
- **`ProducerRecord`**: Immutable message model to send. Key is optional.
- **`RecordMetadata`**: Result of a send — partition, offset, timestamp, message ID.
- **`Producer`**: Operational client. Implements `AutoCloseable`.

### Consumer API

- **`ConsumerConfig`**: Subscriber configuration via builder pattern. Requires a `consumerId`.
- **`ConsumerSubscription`**: Manages subscribed topics.
- **`ConsumerRecord`**: Message model returned to the consumer with full broker metadata.
- **`Consumer`**: Operational client — subscription, polling, position tracking, and seeking. Implements `AutoCloseable`.

### Polling & Subscription Mechanics

| Behavior | Detail |
|---|---|
| **Polling strategy** | Fair round-robin across all subscribed partitions — one message per partition per pass, prevents starvation |
| **Consumer positions** | In-memory `Map<TopicPartition, Long>`, starting at offset 0. Not persisted or committed to broker |
| **Seek** | `seek(topic, partition, offset)` — re-read or skip ahead freely |
| **Per-partition ordering** | ✅ Guaranteed |
| **Global topic ordering** | ❌ Not guaranteed across partitions |
| **Consumer groups** | ❌ Not yet — each consumer reads all messages independently |

---

## 📦 Phase 3 — Persistent Storage & Recovery

Phase 3 adds **durable, file-backed storage** so messages survive broker restarts and JVM crashes.

### Storage Architecture

```
data/minikafka/
└── topics/
    ├── orders/
    │   ├── partition-0.log
    │   ├── partition-1.log
    │   └── partition-2.log
    └── events/
        ├── partition-0.log
        └── partition-1.log
```

### Key Components

| Component | Description |
|---|---|
| **`StorageConfig`** | Configures persistent vs. in-memory mode. Factory methods: `persistent(path)`, `defaultPersistent()`, `inMemory()`. Supports `flushOnWrite` for fsync-on-every-append durability. |
| **`MessageLog`** | Interface for the append-only partition log. Defines `append`, `read`, `readFrom`, `size`, `getLatestOffset`, `getNextOffset`. |
| **`InMemoryMessageLog`** | Thread-safe `ArrayList`-backed implementation (Phase 1/2 default). |
| **`FileMessageLog`** | File-backed append-only log using `FileChannel`. Length-prefixed binary framing: `[4-byte record length][N-byte serialized message]`. Thread-safe via `ReentrantReadWriteLock`. |
| **`MessageSerializer`** | Binary serialization/deserialization of `Message` objects using `DataOutputStream`/`DataInputStream`. Handles optional fields (key, producerId) with boolean flags. |
| **`MessageLogFactory`** | `@FunctionalInterface` factory — returns `InMemoryMessageLog` or `FileMessageLog` based on config. |
| **`StorageEngine`** | Orchestrates persistence. Creates partition logs via the factory, discovers existing topic/partition directories on startup, and drives recovery. Implements `Closeable`. |

### Recovery Mechanism

On broker startup with persistent storage, the `StorageEngine.startAndRecover()` method:

1. Scans the `topics/` directory for existing topic subdirectories.
2. Discovers `partition-N.log` files within each topic directory.
3. Each `FileMessageLog` reads its log file sequentially, recovering all complete records.
4. **Truncated tail policy**: If the final record is partially written (incomplete length prefix or data), the file is truncated to the last valid position. No data loss for completed records.
5. **Middle-of-log corruption**: If a record in the middle has an offset discontinuity or fails deserialization, a `CorruptedLogException` is thrown — MiniKafka does **not** silently skip corrupted entries.
6. Recovered topics and partitions are re-registered in the `Broker` and `TopicManager`.

### Durability Guarantees

- Each `append()` writes to disk **before** updating the in-memory index — crash-safe ordering.
- When `flushOnWrite` is enabled (default for persistent mode), `FileChannel.force(true)` is called after every write — fsync to OS.
- Records use length-prefixed framing for safe boundary detection during recovery.

### New Exceptions

- **`StorageException`**: Wraps I/O failures during log read/write operations.
- **`CorruptedLogException`**: Signals unrecoverable mid-log corruption detected during recovery.

### Backward Compatibility

Phase 3 is fully backward-compatible. Pass `StorageConfig.inMemory()` to the broker for the original Phase 1/2 pure in-memory behavior — no file I/O, no directories created.

---

## 🔒 Concurrency Design & Guarantees

- Multiple `Producer` instances can send messages concurrently to the same topic without data loss or offset collisions.
- Multiple `Consumer` instances can read from the same topic simultaneously without blocking writers or each other.
- `ConcurrentHashMap` for topic/partition management; `ReentrantReadWriteLock` for per-partition log access.
- The `FileMessageLog` uses the same locking strategy — concurrent reads, exclusive writes.

---

## 🧪 Testing & Verification

The suite includes **81 JUnit 5 tests** covering all three phases:

| Suite | Coverage |
|---|---|
| `BrokerTest`, `TopicManagerTest`, `OffsetManagerTest` | Phase 1 core engine |
| `Phase1FunctionalVerificationTest` | End-to-end Phase 1 verification |
| `ProducerTest`, `ProducerConfigTest`, `ProducerIntegrationTest` | Producer API + 20-thread × 100-message concurrency stress test |
| `ConsumerTest`, `ConsumerConfigTest`, `ConsumerIntegrationTest` | Consumer API + multi-consumer isolation |
| `MessageTest` | Core message model |
| `MessageLogTest` | In-memory log operations + concurrent append stress test |
| `Phase3PersistenceTest` | File-backed storage, recovery, truncated tail handling, corruption detection, broker restart round-trips |

Run tests:
```bash
./mvnw clean test
```

Package the JAR:
```bash
./mvnw clean package
```

---

## 🗂️ Project Structure

```
src/main/java/com/minikafka/
├── MiniKafkaApplication.java         # Spring Boot entry point
├── Phase1Demo.java                   # Phase 1 demo runner
├── Phase2Demo.java                   # Phase 2 demo runner
├── broker/
│   ├── Broker.java                   # Central broker
│   ├── TopicManager.java             # Topic lifecycle
│   ├── Partitioner.java              # Partition assignment
│   └── OffsetManager.java            # Offset tracking
├── config/
│   ├── BrokerConfig.java             # Broker configuration
│   └── StorageConfig.java            # Persistent/in-memory storage config
├── consumer/
│   ├── Consumer.java                 # Consumer client
│   ├── ConsumerConfig.java           # Consumer configuration
│   ├── ConsumerRecord.java           # Consumed message model
│   └── ConsumerSubscription.java     # Subscription manager
├── exception/
│   ├── CorruptedLogException.java    # Mid-log corruption
│   ├── InvalidOffsetException.java
│   ├── InvalidPartitionException.java
│   ├── InvalidTopicException.java
│   ├── MessageNotFoundException.java
│   ├── MiniKafkaException.java       # Base exception
│   ├── StorageException.java         # Storage I/O errors
│   ├── TopicAlreadyExistsException.java
│   └── TopicNotFoundException.java
├── model/
│   ├── Message.java                  # Core message
│   ├── Partition.java                # Partition with MessageLog
│   ├── PublishResult.java
│   ├── Topic.java                    # Topic with partitions
│   └── TopicPartition.java
├── producer/
│   ├── Producer.java                 # Producer client
│   ├── ProducerConfig.java           # Producer configuration
│   ├── ProducerRecord.java           # Message to send
│   └── RecordMetadata.java           # Send result metadata
└── storage/
    ├── FileMessageLog.java           # File-backed append-only log
    ├── InMemoryMessageLog.java       # In-memory append-only log
    ├── MessageLog.java               # Log interface
    ├── MessageLogFactory.java        # Factory interface
    ├── MessageSerializer.java        # Binary message serialization
    └── StorageEngine.java            # Storage orchestrator + recovery
```

---

## ⚠️ What's Next (Not Yet Implemented)

- REST HTTP Controllers & API endpoints
- Consumer Group rebalancing & offset committing
- Log segmentation & compaction
- Networking layers (broker and clients currently run in the same JVM)
- Frontend integration with the React/Vite dashboard
