package com.minikafka;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.consumer.ConsumerConfig;
import com.minikafka.consumer.ConsumerRecord;
import com.minikafka.producer.Producer;
import com.minikafka.producer.ProducerConfig;
import com.minikafka.producer.ProducerRecord;
import com.minikafka.producer.RecordMetadata;

import java.util.List;

public class Phase2Demo {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("       MiniKafka Phase 2 Demo");
        System.out.println("========================================");
        System.out.println();

        // 1. Start Broker and create topic
        Broker broker = new Broker();
        String topic = "phone-events";
        System.out.println("Creating topic: " + topic);
        System.out.println("Partitions: 3");
        broker.createTopic(topic, 3);
        System.out.println();

        // 2. Start Producer
        System.out.println("Producer:");
        System.out.println("phone-service");
        System.out.println();

        ProducerConfig pConfig = ProducerConfig.builder()
                .producerId("phone-service")
                .build();

        try (Producer producer = new Producer(broker, pConfig)) {
            System.out.println("Publishing messages...");
            System.out.println();

            ProducerRecord r1 = new ProducerRecord(topic, null, "CALL_STARTED", pConfig.getProducerId());
            RecordMetadata md1 = producer.send(r1);
            printMetadata(md1, "CALL_STARTED");

            ProducerRecord r2 = new ProducerRecord(topic, null, "CALL_CONNECTED", pConfig.getProducerId());
            RecordMetadata md2 = producer.send(r2);
            printMetadata(md2, "CALL_CONNECTED");
        }

        System.out.println("Consumer:");
        System.out.println("phone-consumer");
        System.out.println();

        // 3. Start Consumer
        ConsumerConfig cConfig = ConsumerConfig.builder()
                .consumerId("phone-consumer")
                .build();

        try (Consumer consumer = new Consumer(broker, cConfig)) {
            System.out.println("Subscribed:");
            System.out.println(topic);
            System.out.println();

            consumer.subscribe(topic);

            System.out.println("Polling...");
            System.out.println();

            List<ConsumerRecord> records = consumer.poll();
            for (ConsumerRecord r : records) {
                printConsumerRecord(r);
            }

            System.out.println("Consumer position:");
            consumer.positions().forEach((tp, offset) -> {
                System.out.println(tp.topic() + "/" + tp.partition() + " -> " + offset);
            });
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("       Phase 2 Demo Complete");
        System.out.println("========================================");

        // Broker has no lifecycle stop() — it is plain in-memory state.
    }

    private static void printMetadata(RecordMetadata md, String payload) {
        System.out.println("Published:");
        System.out.println("ID        : " + md.messageId());
        System.out.println("Partition : " + md.partition());
        System.out.println("Offset    : " + md.offset());
        System.out.println("Payload   : " + payload);
        System.out.println();
    }

    private static void printConsumerRecord(ConsumerRecord r) {
        System.out.println("Received:");
        System.out.println("ID        : " + r.messageId());
        System.out.println("Partition : " + r.partition());
        System.out.println("Offset    : " + r.offset());
        System.out.println("Payload   : " + r.payload());
        System.out.println();
    }
}
