package com.minikafka.storage;

import com.minikafka.exception.InvalidOffsetException;
import com.minikafka.model.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageLogTest {

    private InMemoryMessageLog messageLog;

    @BeforeEach
    void setUp() {
        messageLog = new InMemoryMessageLog();
    }

    @Test
    @DisplayName("Empty log should return correct initial metadata")
    void testEmptyLog() {
        assertThat(messageLog.isEmpty()).isTrue();
        assertThat(messageLog.size()).isEqualTo(0);
        assertThat(messageLog.getLatestOffset()).isEqualTo(-1L);
        assertThat(messageLog.getNextOffset()).isEqualTo(0L);
        assertThat(messageLog.read(0)).isEmpty();
        assertThat(messageLog.readFrom(0)).isEmpty();
    }

    @Test
    @DisplayName("Should append messages sequentially and assign contiguous 0-based offsets")
    void testAppendAndReadSequential() {
        Message msgA = Message.builder().topic("topicA").partition(0).payload("msgA").build();
        Message msgB = Message.builder().topic("topicA").partition(0).payload("msgB").build();
        Message msgC = Message.builder().topic("topicA").partition(0).payload("msgC").build();

        long offsetA = messageLog.append(msgA);
        long offsetB = messageLog.append(msgB);
        long offsetC = messageLog.append(msgC);

        assertThat(offsetA).isEqualTo(0L);
        assertThat(offsetB).isEqualTo(1L);
        assertThat(offsetC).isEqualTo(2L);

        assertThat(messageLog.size()).isEqualTo(3);
        assertThat(messageLog.getLatestOffset()).isEqualTo(2L);
        assertThat(messageLog.getNextOffset()).isEqualTo(3L);

        Optional<Message> read0 = messageLog.read(0);
        assertThat(read0).isPresent();
        assertThat(read0.get().getPayload()).isEqualTo("msgA");
        assertThat(read0.get().getOffset()).isEqualTo(0L);

        Optional<Message> read1 = messageLog.read(1);
        assertThat(read1).isPresent();
        assertThat(read1.get().getPayload()).isEqualTo("msgB");

        Optional<Message> read2 = messageLog.read(2);
        assertThat(read2).isPresent();
        assertThat(read2.get().getPayload()).isEqualTo("msgC");

        assertThat(messageLog.read(3)).isEmpty();
    }

    @Test
    @DisplayName("Should read messages from an offset onwards with optional limits")
    void testReadFrom() {
        for (int i = 0; i < 10; i++) {
            messageLog.append(Message.builder().topic("topicA").partition(0).payload("payload-" + i).build());
        }

        List<Message> from5 = messageLog.readFrom(5);
        assertThat(from5).hasSize(5);
        assertThat(from5.get(0).getOffset()).isEqualTo(5L);
        assertThat(from5.get(4).getOffset()).isEqualTo(9L);

        List<Message> from2Limit3 = messageLog.readFrom(2, 3);
        assertThat(from2Limit3).hasSize(3);
        assertThat(from2Limit3.get(0).getOffset()).isEqualTo(2L);
        assertThat(from2Limit3.get(2).getOffset()).isEqualTo(4L);

        List<Message> beyondLatest = messageLog.readFrom(15);
        assertThat(beyondLatest).isEmpty();
    }

    @Test
    @DisplayName("Should throw InvalidOffsetException on negative offset")
    void testNegativeOffsetThrows() {
        assertThatThrownBy(() -> messageLog.read(-1))
                .isInstanceOf(InvalidOffsetException.class);

        assertThatThrownBy(() -> messageLog.readFrom(-5))
                .isInstanceOf(InvalidOffsetException.class);
    }

    @Test
    @DisplayName("Should support safe concurrent appends without offset duplicates or lost messages")
    void testConcurrentAppend() throws InterruptedException {
        int threads = 10;
        int messagesPerThread = 200;
        int totalExpected = threads * messagesPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int m = 0; m < messagesPerThread; m++) {
                        messageLog.append(Message.builder()
                                .topic("concurrency-test")
                                .partition(0)
                                .payload("t-" + threadId + "-m-" + m)
                                .build());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertThat(messageLog.size()).isEqualTo(totalExpected);
        assertThat(messageLog.getLatestOffset()).isEqualTo(totalExpected - 1L);

        List<Message> allMessages = messageLog.readFrom(0);
        assertThat(allMessages).hasSize(totalExpected);

        // Verify each message has contiguous strictly increasing offsets
        for (int i = 0; i < totalExpected; i++) {
            assertThat(allMessages.get(i).getOffset()).isEqualTo(i);
        }
    }
}
