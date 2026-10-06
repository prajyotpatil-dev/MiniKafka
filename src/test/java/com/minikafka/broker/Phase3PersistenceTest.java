package com.minikafka.broker;

import com.minikafka.config.BrokerConfig;
import com.minikafka.config.StorageConfig;
import com.minikafka.consumer.Consumer;
import com.minikafka.consumer.ConsumerConfig;
import com.minikafka.model.Message;
import com.minikafka.consumer.ConsumerRecord;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerConfig;
import com.minikafka.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class Phase3PersistenceTest {

    @TempDir
    Path tempDir;

    private Broker broker;

    private Broker startBroker() {
        BrokerConfig config = new BrokerConfig();
        config.setStorageConfig(StorageConfig.persistent(tempDir));
        Broker b = new Broker(config, new TopicManager(), new OffsetManager(), new Partitioner.DefaultPartitioner());
        return b;
    }

    private void stopBroker(Broker b) throws IOException {
        if (b != null) {
            b.close();
        }
    }

    @BeforeEach
    void setUp() {
        broker = startBroker();
    }

    @AfterEach
    void tearDown() throws IOException {
        stopBroker(broker);
    }

    @Test
    void test1_MessageSurvivesRestart() throws Exception {
        broker.createTopic("test-topic", 1);
        Producer producer = new Producer(broker, ProducerConfig.builder().producerId("prod-1").build());
        producer.send(new ProducerRecord("test-topic", "key1", "payload1", "prod-1"));
        producer.send(new ProducerRecord("test-topic", "key2", "payload2", "prod-1"));

        stopBroker(broker);

        // Restart
        broker = startBroker();

        List<Message> read = broker.read("test-topic", 0, 0);
        assertThat(read).hasSize(2);
        assertThat(read.get(0).getPayload()).isEqualTo("payload1");
        assertThat(read.get(1).getPayload()).isEqualTo("payload2");
    }

    @Test
    void test2_OffsetsSurviveRestart() throws Exception {
        broker.createTopic("offset-topic", 1);
        Producer producer = new Producer(broker, ProducerConfig.builder().producerId("prod-1").build());
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord("offset-topic", null, "msg" + i, "prod-1"));
        }

        stopBroker(broker);

        broker = startBroker();
        Producer newProducer = new Producer(broker, ProducerConfig.builder().producerId("prod-1").build());
        var result = newProducer.send(new ProducerRecord("offset-topic", null, "msg5", "prod-1"));

        assertThat(result.offset()).isEqualTo(5L);
        assertThat(broker.read("offset-topic", 0, 0)).hasSize(6);
    }

    @Test
    void test3_MultiplePartitionsSurviveRestart() throws Exception {
        broker.createTopic("multi-part", 3);
        Producer producer = new Producer(broker, ProducerConfig.builder().producerId("prod-1").build());

        // Push 10 msgs with keys, will be hashed to partitions
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord("multi-part", "key" + i, "val" + i, "prod-1"));
        }

        long p0Count = broker.getTopic("multi-part").getPartition(0).getLatestOffset() + 1;
        long p1Count = broker.getTopic("multi-part").getPartition(1).getLatestOffset() + 1;
        long p2Count = broker.getTopic("multi-part").getPartition(2).getLatestOffset() + 1;

        stopBroker(broker);

        broker = startBroker();

        assertThat(broker.getTopic("multi-part").getPartitionCount()).isEqualTo(3);
        assertThat(broker.getTopic("multi-part").getPartition(0).getLatestOffset() + 1).isEqualTo(p0Count);
        assertThat(broker.getTopic("multi-part").getPartition(1).getLatestOffset() + 1).isEqualTo(p1Count);
        assertThat(broker.getTopic("multi-part").getPartition(2).getLatestOffset() + 1).isEqualTo(p2Count);
    }

    @Test
    void test4_TopicMetadataSurvivesRestart() throws Exception {
        broker.createTopic("t-1", 1);
        broker.createTopic("t-5", 5);
        broker.createTopic("t-10", 10);

        stopBroker(broker);
        broker = startBroker();

        assertThat(broker.listTopics()).hasSize(3);
        assertThat(broker.getTopic("t-1").getPartitionCount()).isEqualTo(1);
        assertThat(broker.getTopic("t-5").getPartitionCount()).isEqualTo(5);
        assertThat(broker.getTopic("t-10").getPartitionCount()).isEqualTo(10);
    }

    @Test
    void test5_ConsumerCanReadRecoveredMessages() throws Exception {
        broker.createTopic("consumer-topic", 2);
        Producer producer = new Producer(broker, ProducerConfig.builder().producerId("prod").build());
        producer.send(new ProducerRecord("consumer-topic", "k1", "v1", "prod"));
        producer.send(new ProducerRecord("consumer-topic", "k2", "v2", "prod"));

        stopBroker(broker);
        broker = startBroker();

        Consumer consumer = new Consumer(broker, ConsumerConfig.builder().consumerId("grp1").build());
        consumer.subscribe("consumer-topic");

        List<ConsumerRecord> polled = new ArrayList<>();
        // Two polls for round-robin
        polled.addAll(consumer.poll());
        polled.addAll(consumer.poll());

        assertThat(polled).hasSize(2);
        assertThat(polled.stream().map(ConsumerRecord::payload)).containsExactlyInAnyOrder("v1", "v2");
    }

    @Test
    void test6_NewMessagesAppendAfterRecovery() throws Exception {
        broker.createTopic("t1", 1);
        Producer p = new Producer(broker, ProducerConfig.builder().producerId("prod").build());
        p.send(new ProducerRecord("t1", null, "old1", "prod"));
        p.send(new ProducerRecord("t1", null, "old2", "prod"));

        stopBroker(broker);
        broker = startBroker();

        Producer p2 = new Producer(broker, ProducerConfig.builder().producerId("prod2").build());
        p2.send(new ProducerRecord("t1", null, "new1", "prod2"));

        List<Message> all = broker.read("t1", 0, 0);
        assertThat(all).hasSize(3);
        assertThat(all.get(0).getPayload()).isEqualTo("old1");
        assertThat(all.get(1).getPayload()).isEqualTo("old2");
        assertThat(all.get(2).getPayload()).isEqualTo("new1");
        assertThat(all.get(2).getOffset()).isEqualTo(2L);
    }

    @Test
    void test7_EmptyTopicRecovery() throws Exception {
        broker.createTopic("empty-topic", 2);

        stopBroker(broker);
        broker = startBroker();

        assertThat(broker.topicExists("empty-topic")).isTrue();
        assertThat(broker.getTopic("empty-topic").getPartitionCount()).isEqualTo(2);
    }

    @Test
    void test8_CorruptedTruncatedFinalRecord() throws Exception {
        broker.createTopic("trunc-topic", 1);
        Producer p = new Producer(broker, ProducerConfig.builder().producerId("prod").build());
        p.send(new ProducerRecord("trunc-topic", null, "valid-1", "prod"));
        p.send(new ProducerRecord("trunc-topic", null, "valid-2", "prod"));

        stopBroker(broker);

        // Simulate truncation by appending some garbage bytes to the file
        // which represents an incomplete new frame
        Path partFile = tempDir.resolve("topics").resolve("trunc-topic").resolve("partition-0.log");
        try (FileChannel fc = FileChannel.open(partFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            // Write 2 bytes (incomplete 4-byte length prefix)
            ByteBuffer fake = ByteBuffer.allocate(2);
            fake.putShort((short) 100);
            fake.flip();
            fc.write(fake);
        }

        broker = startBroker();

        List<Message> recovered = broker.read("trunc-topic", 0, 0);
        assertThat(recovered).hasSize(2);
        assertThat(recovered.get(1).getPayload()).isEqualTo("valid-2");

        // Ensure we can still append correctly
        Producer p2 = new Producer(broker, ProducerConfig.builder().producerId("prod").build());
        p2.send(new ProducerRecord("trunc-topic", null, "valid-3", "prod"));

        List<Message> total = broker.read("trunc-topic", 0, 0);
        assertThat(total).hasSize(3);
        assertThat(total.get(2).getPayload()).isEqualTo("valid-3");
    }

    @Test
    void test9_ConcurrentPersistentPublishing() throws Exception {
        broker.createTopic("concurrent", 1);

        int threadCount = 20;
        int msgsPerThread = 100;
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int tId = i;
            exec.submit(() -> {
                Producer p = new Producer(broker, ProducerConfig.builder().producerId("prod-conc").build());
                try {
                    startLatch.await();
                    for (int j = 0; j < msgsPerThread; j++) {
                        p.send(new ProducerRecord("concurrent", null, "msg-" + tId + "-" + j, "prod-conc"));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);
        exec.shutdown();

        stopBroker(broker);
        broker = startBroker();

        List<Message> msgs = broker.read("concurrent", 0, 0);
        assertThat(msgs).hasSize(threadCount * msgsPerThread);

        // Ensure exactly 2000 unique sequential offsets
        for (int i = 0; i < msgs.size(); i++) {
            assertThat(msgs.get(i).getOffset()).isEqualTo((long) i);
        }
    }

    @Test
    void test10_PersistenceAcrossMultipleInstances() throws Exception {
        broker.createTopic("t-multi", 1);
        Producer p = new Producer(broker, ProducerConfig.builder().producerId("prod").build());
        p.send(new ProducerRecord("t-multi", null, "m1", "prod"));

        stopBroker(broker);

        Broker b2 = startBroker();
        Producer p2 = new Producer(b2, ProducerConfig.builder().producerId("prod2").build());
        p2.send(new ProducerRecord("t-multi", null, "m2", "prod2"));
        stopBroker(b2);

        Broker b3 = startBroker();
        List<Message> msgs = b3.read("t-multi", 0, 0);
        assertThat(msgs).hasSize(2);
        assertThat(msgs.get(0).getPayload()).isEqualTo("m1");
        assertThat(msgs.get(1).getPayload()).isEqualTo("m2");
        stopBroker(b3);
        broker = null; // Prevent AfterEach from failing if double closed
    }
}