package com.minikafka.producer;

import com.minikafka.broker.Broker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ProducerIntegrationTest {

    private Broker broker;

    @BeforeEach
    void setUp() {
        broker = new Broker();
    }

    /**
     * Requirement: 20 producer threads, each sending 100 messages. Total 2000 messages.
     * Verify: 2000 published, unique IDs, no duplicates, no lost messages.
     */
    @Test
    void testProducerConcurrency() throws InterruptedException {
        String topic = "concurrent-topic";
        broker.createTopic(topic, 5); // 5 partitions

        int numThreads = 20;
        int msgsPerThread = 100;
        int totalExpected = numThreads * msgsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch readyLatch = new CountDownLatch(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        // Thread-safe collection to collect all metadata
        Set<String> publishedIds = ConcurrentHashMap.newKeySet();
        List<Throwable> errors = new ArrayList<>();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    ProducerConfig config = ProducerConfig.builder().producerId("prod-" + threadId).build();
                    try (Producer producer = new Producer(broker, config)) {
                        readyLatch.countDown();
                        startLatch.await(); // wait for all threads to be ready

                        for (int j = 0; j < msgsPerThread; j++) {
                            ProducerRecord record = new ProducerRecord(topic, "key-" + threadId, "msg-" + j, config.getProducerId());
                            RecordMetadata metadata = producer.send(record);
                            publishedIds.add(metadata.messageId().toString());
                        }
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
        startLatch.countDown(); // unleash all threads
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));

        executor.shutdown();

        // Verifications
        assertTrue(errors.isEmpty(), "Exceptions occurred during concurrent produce: " + errors);
        assertEquals(totalExpected, publishedIds.size(), "Should have exactly 2000 unique message IDs");

        // Verify total size across partitions in broker equals 2000
        long brokerTotalCount = 0;
        for (int p = 0; p < broker.getTopic(topic).getPartitionCount(); p++) {
            brokerTotalCount += broker.getTopic(topic).getPartition(p).size();
        }
        assertEquals(totalExpected, brokerTotalCount, "Broker must hold exactly 2000 messages");
    }
}
