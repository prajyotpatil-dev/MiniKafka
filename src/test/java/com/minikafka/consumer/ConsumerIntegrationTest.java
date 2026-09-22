package com.minikafka.consumer;

import com.minikafka.broker.Broker;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerConfig;
import com.minikafka.producer.ProducerRecord;
import com.minikafka.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerIntegrationTest {

    private Broker broker;

    @BeforeEach
    void setUp() {
        broker = new Broker();
    }

    /**
     * Requirement: Complete integration test Producer -> Broker -> Consumer
     */
    @Test
    void testFullIntegrationProducerBrokerConsumer() {
        String topic = "phone-events";
        broker.createTopic(topic, 3); // 3 partitions

        // 1. Create Producer
        ProducerConfig pConfig = ProducerConfig.builder().producerId("phone-service").build();
        Producer producer = new Producer(broker, pConfig);

        // 2. Producer sends 10 messages
        List<RecordMetadata> sentMetadata = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ProducerRecord record = new ProducerRecord(topic, null, "EVENT-" + i, pConfig.getProducerId());
            sentMetadata.add(producer.send(record));
        }

        // 3. Create Consumer
        ConsumerConfig cConfig = ConsumerConfig.builder().consumerId("phone-consumer").defaultPollBatchSize(5).build();
        Consumer consumer = new Consumer(broker, cConfig);

        // 4. Consumer subscribes
        consumer.subscribe(topic);

        // 5. Consumer polls messages
        List<ConsumerRecord> receivedRecords = new ArrayList<>();
        List<ConsumerRecord> batch;
        while (!(batch = consumer.poll()).isEmpty()) {
            receivedRecords.addAll(batch);
        }

        // 6. Verifications
        assertEquals(10, receivedRecords.size());

        // Verify all message IDs match
        Set<String> sentIds = new HashSet<>();
        for (RecordMetadata md : sentMetadata) sentIds.add(md.messageId().toString());

        Set<String> receivedIds = new HashSet<>();
        for (ConsumerRecord cr : receivedRecords) receivedIds.add(cr.messageId().toString());

        assertEquals(sentIds, receivedIds, "Consumer should receive exactly the same messages produced");

        // Producer and Consumer closed successfully
        producer.close();
        consumer.close();
    }

    /**
     * Requirement: Multiple independent consumers reading the same topic.
     * Each should receive the exact same messages (since no consumer groups).
     */
    @Test
    void testMultipleIndependentConsumers() throws InterruptedException {
        String topic = "multi-consumer-topic";
        broker.createTopic(topic, 2);

        // Produce 100 messages
        ProducerConfig pConfig = ProducerConfig.builder().producerId("p1").build();
        try (Producer producer = new Producer(broker, pConfig)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord(topic, null, "data-" + i, "p1"));
            }
        }

        // Run 5 consumers simultaneously
        int numConsumers = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch doneLatch = new CountDownLatch(numConsumers);

        List<Throwable> errors = new ArrayList<>();
        AtomicInteger consumersCompletedSuccessfully = new AtomicInteger(0);

        for (int i = 0; i < numConsumers; i++) {
            final int consumerEpoch = i;
            executor.submit(() -> {
                try {
                    ConsumerConfig cConfig = ConsumerConfig.builder().consumerId("c-" + consumerEpoch).defaultPollBatchSize(10).build();
                    try (Consumer consumer = new Consumer(broker, cConfig)) {
                        consumer.subscribe(topic);

                        int count = 0;
                        List<ConsumerRecord> batch;
                        while (!(batch = consumer.poll()).isEmpty()) {
                            count += batch.size();
                        }

                        if (count == 100) {
                            consumersCompletedSuccessfully.incrementAndGet();
                        } else {
                            throw new IllegalStateException("Consumer " + consumerEpoch + " expected 100 but got " + count);
                        }
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertTrue(errors.isEmpty(), "Exceptions found: " + errors);
        assertEquals(numConsumers, consumersCompletedSuccessfully.get(), "All consumers should have read 100 messages independently");
    }
}
