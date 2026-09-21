# MiniKafka - Core Broker Engine (Phase 1)

MiniKafka is a lightweight, high-performance, Kafka-inspired in-memory message broker built in Java 21 and Spring Boot 3.4.

This document describes the **Phase 1: Core Broker Engine** architecture, components, concurrency design, and usage.

---

## 🏛️ Architecture Overview

The core broker engine organizes messages into **Topics**, which are divided into one or more **Partitions**. Each Partition maintains an append-only, ordered **MessageLog** where messages are assigned continuous, strictly increasing, 0-based offsets.

```
Producer (Thread / Client)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│                      MiniKafka Broker                   │
│                                                         │
│  ┌─────────────────┐   ┌─────────────────────────────┐  │
│  │  TopicManager   │   │        Partitioner          │  │
│  │ (Topics Catalog)│   │(Hash-Keyed / Round-Robin RR)│  │
│  └────────┬────────┘   └──────────────┬──────────────┘  │
│           │                           │                 │
│           ▼                           ▼                 │
│  ┌───────────────────────────────────────────────────┐  │
│  │                      Topic                        │  │
│  │                                                   │  │
│  │  ┌───────────────┐ ┌───────────────┐ ┌──────────┐ │  │
│  │  │  Partition 0  │ │  Partition 1  │ │Partition 2│ │  │
│  │  │ ┌───────────┐ │ │ ┌───────────┐ │ │┌────────┐│ │  │
│  │  │ │MessageLog │ │ │ │MessageLog │ │ ││MsgLog  ││ │  │
│  │  │ │[0][1][2]..│ │ │ │[0][1][2]..│ │ ││[0][1]..││ │  │
│  │  │ └───────────┘ │ │ └───────────┘ │ │└────────┘│ │  │
│  │  └───────────────┘ └───────────────┘ └──────────┘ │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │                  OffsetManager                    │  │
│  │       (Consumer Group Committed Offsets)          │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 Core Components

### 1. `Message` (`com.minikafka.model.Message`)
An immutable record/model representing a unit of data published to MiniKafka.
- **Fields:** `messageId` (UUID), `topic`, `partition`, `offset`, `key` (optional), `payload`, `timestamp` (`Instant`), `producerId`.
- **Properties:** Thread-safe, immutable, builder pattern support, strict null-checks on `topic` and `payload`.

### 2. `Partition` (`com.minikafka.model.Partition`)
Represents an ordered stream within a topic.
- Encapsulates partition ID and an internal `MessageLog`.
- Assigns continuous 0-based offsets upon append.
- Preserves insertion order and supports bounded/unbounded reading from any offset.

### 3. `MessageLog` (`com.minikafka.storage.MessageLog` & `InMemoryMessageLog`)
The append-only storage abstraction.
- Decoupled via interface to easily allow file-backed or persistent log implementations in future phases.
- Implemented in Phase 1 with `InMemoryMessageLog` using `ReentrantReadWriteLock` for high-throughput thread safety:
  - **Write Lock:** Applied during `append()` to atomically assign offsets and append to the entry list.
  - **Read Lock:** Applied during `read()` and `readFrom()` to permit multiple concurrent readers without blocking each other.

### 4. `Topic` (`com.minikafka.model.Topic`)
A logical channel consisting of 1 or more partitions.
- Validates topic names (alphanumeric, `.`, `_`, `-`).
- Distributes writes to target partitions.

### 5. `TopicManager` (`com.minikafka.broker.TopicManager`)
Thread-safe topic catalog lifecycle manager.
- Stores topics in a `ConcurrentHashMap<String, Topic>`.
- Supports `createTopic()`, `getTopic()`, `deleteTopic()`, `listTopics()`, and `topicExists()`.
- Throws domain exceptions (`TopicAlreadyExistsException`, `TopicNotFoundException`, `InvalidTopicException`).

### 6. `OffsetManager` (`com.minikafka.broker.OffsetManager`)
Manages committed offsets for consumer groups across topic-partitions.
- Stores offsets in a `ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, Long>>`.
- Supports atomic offset commit, offset queries, consumer group resets, and partition removals.

### 7. `Partitioner` (`com.minikafka.broker.Partitioner` & `DefaultPartitioner`)
Partition selection strategy:
- **Keyed Messages:** `Math.abs(key.hashCode()) % numPartitions` (hash partitioning ensuring same-key messages land on the same partition in order).
- **Unkeyed Messages (null / empty key):** Atomic round-robin distribution via `AtomicInteger` (`(counter.getAndIncrement() & Integer.MAX_VALUE) % numPartitions`).

### 8. `Broker` (`com.minikafka.broker.Broker`)
Central coordinator providing high-level public APIs:
- `createTopic(String name, int partitions)`
- `deleteTopic(String name)`
- `publish(String topic, String key, String payload, String producerId)`
- `publishToPartition(String topic, int partition, String key, String payload, String producerId)`
- `read(String topic, int partition, long offset)` / `read(String topic, int partition, long offset, int limit)`
- `getMessage(String topic, int partition, long offset)`

---

## 🔒 Concurrency Design & Guarantees

1. **Strict Offset Contiguity:**
   - Appending to a partition acquires a `writeLock` on that partition's `MessageLog`.
   - The offset is assigned based on the current log size at the moment of insertion (`offset = entries.size()`).
   - Multiple producer threads publishing concurrently to the same partition receive unique, continuous, strictly increasing offsets with zero duplicates or gaps.
2. **Concurrent Multi-Partition Publishing:**
   - Locks are partition-granular; appending to Partition 0 does not lock or delay writes to Partition 1.
3. **Concurrent Reads:**
   - Multiple consumers can concurrently read from the same or different partitions using non-exclusive `readLock`.
4. **Thread-Safe Topic and Offset Management:**
   - `TopicManager` and `OffsetManager` use non-blocking `ConcurrentHashMap` structures.

---

## 🧪 Testing & Verification

The suite includes 27 comprehensive JUnit 5 unit and concurrency integration tests:
- **`MessageTest`:** Immutability, unique ID generation, validation.
- **`MessageLogTest`:** Contiguous offsets, bounded reads, concurrent appends (10 threads x 200 msgs = 2,000 total).
- **`TopicManagerTest`:** Lifecycle, duplicate checks, name/partition validation.
- **`OffsetManagerTest`:** Offset commits, resets, multi-group isolation.
- **`BrokerTest`:** End-to-end publish/read, key-based & round-robin routing, exception paths, and multi-threaded stress tests (20 threads x 100 msgs = 2,000 messages single-partition and multi-partition).

Run tests with:
```bash
./mvnw clean test
```

Package the JAR with:
```bash
./mvnw clean package
```

---

## ⚠️ What Phase 1 Does NOT Include

In accordance with Phase 1 boundaries, the following are intentionally deferred to subsequent phases:
- REST HTTP Controllers & API endpoints
- PostgreSQL / Disk persistence
- WebSockets & real-time streaming
- Consumer group auto-rebalancing & background polling loops
- Security, authentication, and ACLs
- Distributed clustering & replication
- Frontend web UI integration
