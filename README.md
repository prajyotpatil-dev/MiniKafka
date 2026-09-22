# MiniKafka - Java Messaging API (Phase 2)

MiniKafka is a lightweight, high-performance, Kafka-inspired in-memory message broker built in Java 21 and Spring Boot 3.4.

This document describes the Phase 2 architecture, which introduces the high-level **Producer** and **Consumer** messaging APIs on top of the Phase 1 core engine.

---

## 🏛️ Architecture Overview

The broker core organizes messages into **Topics**, which are divided into **Partitions**. Phase 2 introduces standalone client wrappers that interface with the broker using domain-specific configuration and record models.

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│                 │       │                 │       │                 │
│ Producer Client ├───┬───▶MiniKafka Broker ├───────▶ Consumer Client │
│                 │   │   │                 │       │                 │
└─────────────────┘   │   └─────────────────┘       └─────────────────┘
                      │      (TopicManager,                 ▲
                      │       Partitioner,                  │
                      │       MessageLog)                   │
┌─────────────────┐   │                                     │
│                 │   │                                     │
│ Producer Client ├───┘                                     │
│                 │                                         │
└─────────────────┘                                         │
                                                            │
┌─────────────────┐                                         │
│                 │                                         │
│ Consumer Client ├─────────────────────────────────────────┘
│                 │
└─────────────────┘
```

---

## 📦 Phase 2 Client APIs

### Producer API

- **`ProducerConfig`**: Publisher configuration built via a builder pattern. Requires a `producerId`.
- **`ProducerRecord`**: The data model for messages to be sent. Key is optional. Data is immutable.
- **`RecordMetadata`**: The result returned by a send operation, detailing the exact partition, offset, timestamp, and unique message ID assigned by the broker.
- **`Producer`**: The operational client wrapper. Implements `AutoCloseable` to cleanly stop accepting new records.

### Consumer API

- **`ConsumerConfig`**: Subscriber configuration built via a builder pattern. Requires a `consumerId`.
- **`ConsumerSubscription`**: Manages the topics the consumer is interested in.
- **`ConsumerRecord`**: The data model returned to the consumer. Contains full broker metadata alongside the payload.
- **`Consumer`**: The operational client wrapper. Handles subscription, periodic polling, position tracking, and deliberate seeking. Implements `AutoCloseable`.

---

## 🔄 Consumer Polling & Subscription Mechanics

In Phase 2, the message consumption semantics are specific and deterministic:

### 1. Polling Strategy
The `poll(maxRecords)` method fetches messages across multiple subscribed partitions. It uses a **fair round-robin strategy**:
- Partitions are sorted deterministically (by topic name alphabetically, then partition index).
- The consumer loops over these partitions reading **one message per partition per pass** until it exhausts available messages or hits `maxRecords`.
- This prevents a high-volume partition from starving other partitions during consumption.

### 2. Consumer Positions (Offsets)
- Each `Consumer` instance maintains its own in-memory `Map<TopicPartition, Long>` tracking the **next offset to read** for each partition.
- These positions start at offset `0` when a topic is subscribed.
- After a successful read during `poll()`, the position strictly advances by 1.
- Note: Positions are **not** persisted or committed to the broker in Phase 2. They live purely in the client's memory.

### 3. Seek Interface
- Consumers can explicitly move their read pointer using `seek(topic, partition, offset)`.
- Re-reading historical data or skipping ahead is fully supported as long as the topic is subscribed and the offset is valid (>= 0).

### 4. Ordering Guarantees
- **Per-partition ordering is guaranteed.** Messages published to a specific partition will be yielded by `poll()` in the exact order they were appended.
- **Global topic ordering is NOT guaranteed.** If a topic has 3 partitions, messages across these partitions may interleave during a poll.

### 5. Why No Consumer Groups Yet?
Phase 2 intentionally models **independent consumers**. If two consumers subscribe to the same topic, **both will read the exact same messages independently**. 
Load-balancing messages among workers (Consumer Groups) and rebalancing assignments are complex distributed systems concepts reserved for a later phase. 

---

## 🔒 Concurrency Design & Guarantees

The clients interface flawlessly with the Phase 1 thread-safe engine:
- Dozens of `Producer` instances can send messages concurrently to the same topic without losing data or producing offset collisions. 
- Dozens of `Consumer` instances can read from the same topic simultaneously without blocking the writers or each other.
- The `ConcurrentHashMap`-based structure means neither producers nor consumers block on global locks.

---

## 🧪 Testing & Verification

The suite includes 66 comprehensive JUnit 5 tests covering both Phase 1 core broker functionality and Phase 2 client integration:
- **`ProducerTest` & `ConsumerTest`**: Deep unit testing of configuration limits, subscription lifecycle, poll limits, and negative testing (closed states, invalid seeks).
- **`ProducerIntegrationTest`**: Concurrency stress testing. 20 producer threads sending 100 messages simultaneously (2,000 messages total). Verified 100% throughput with distinct sequential offsets.
- **`ConsumerIntegrationTest`**: Full multi-consumer round-trips ensuring complete isolation and message delivery without interference.

Run tests with:
```bash
./mvnw clean test
```

Package the JAR with:
```bash
./mvnw clean package
```

---

## ⚠️ What Phase 2 Does NOT Include

In accordance with Phase 2 boundaries, the following are deferred to subsequent phases:
- REST HTTP Controllers & API endpoints
- PostgreSQL / Disk persistence
- Consumer Group rebalancing & offset committing
- Networking layers (broker and clients run in the same JVM currently)
