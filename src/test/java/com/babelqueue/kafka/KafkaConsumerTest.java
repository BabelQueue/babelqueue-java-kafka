package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** §6.5 consume: process-then-commit, attempts-from-header, retry-tier republish, DLQ, unknown-URN. */
class KafkaConsumerTest {

    private static final String URN = "urn:babel:orders:created";
    private static final TopicPartition TP = new TopicPartition("orders", 0);

    private static Envelope envelope(int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");
        return new Envelope(base.job(), base.traceId(), base.data(), base.meta(), attempts, base.deadLetter());
    }

    private static ConsumerRecord<byte[], byte[]> recordFor(Envelope env, long offset) {
        byte[] value = EnvelopeCodec.encode(env).getBytes(StandardCharsets.UTF_8);
        Header[] headers = KafkaHeaders.of(env).toArray(new Header[0]);
        return new ConsumerRecord<>("orders", 0, offset, env.meta().createdAt(), TimestampType.CREATE_TIME,
            -1, value.length, null, value, new RecordHeaders(headers), Optional.empty());
    }

    private static ConsumerRecord<byte[], byte[]> rawRecord(byte[] value, long offset) {
        return new ConsumerRecord<>("orders", 0, offset, 0L, TimestampType.CREATE_TIME,
            -1, value.length, null, value, new RecordHeaders(), Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[], byte[]> consumerWith(ConsumerRecord<byte[], byte[]> record) {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(record))));
        return consumer;
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static RetryTopics topology() {
        return RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(60)).build();
    }

    @Test
    void successProcessesThenCommits() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 41);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        int[] seen = {-1};

        int count = KafkaConsumer.builder(consumer)
            .handler(URN, (env, r) -> seen[0] = env.attempts())
            .build()
            .poll();

        assertEquals(1, count);
        assertEquals(0, seen[0]);
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(42)));
    }

    @Test
    void attemptsHeaderIsAuthoritative() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(2), 5);
        int[] seen = {-1};
        KafkaConsumer.builder(consumerWith(rec))
            .handler(URN, (env, r) -> seen[0] = env.attempts())
            .build()
            .poll();
        assertEquals(2, seen[0]);
    }

    @Test
    void throwingHandlerRepublishesToRetryWithAttemptsPlusOne() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 10);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();
        Throwable[] reported = {null};

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> reported[0] = e)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, reported[0]);
        assertEquals(1, producer.history().size());
        ProducerRecord<byte[], byte[]> retry = producer.history().get(0);
        assertEquals("orders.retry.1", retry.topic());
        assertEquals("1", KafkaHeaders.string(retry.headers(), KafkaHeaders.ATTEMPTS));
        assertEquals("5000", KafkaHeaders.string(retry.headers(), KafkaHeaders.DELAY));
        assertEquals("orders", KafkaHeaders.string(retry.headers(), KafkaHeaders.ORIGINAL_TOPIC));
        Envelope bumped = EnvelopeCodec.decode(new String(retry.value(), StandardCharsets.UTF_8));
        assertEquals(1, bumped.attempts());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(11)));
    }

    @Test
    void terminalFailureGoesToDlqWithDeadLetterBlock() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(2), 7); // nextAttempt 3 == maxTries
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(rec))
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        assertEquals(1, producer.history().size());
        ProducerRecord<byte[], byte[]> dlq = producer.history().get(0);
        assertEquals("orders.dlq", dlq.topic());
        Envelope dead = EnvelopeCodec.decode(new String(dlq.value(), StandardCharsets.UTF_8));
        assertEquals("failed", dead.deadLetter().reason());
    }

    @Test
    void retryWithoutTopicsOrDlqRaises() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 1);
        KafkaConsumer consumer = KafkaConsumer.builder(consumerWith(rec))
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build();
        assertThrows(BabelQueueException.class, consumer::poll);
    }

    @Test
    void unknownUrnFailRaisesAndDoesNotCommit() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 3);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        KafkaConsumer worker = KafkaConsumer.builder(consumer).build();

        assertThrows(UnknownUrnException.class, worker::poll);
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    void unknownUrnDeleteCommits() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 8);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);

        KafkaConsumer.builder(consumer).unknownUrn(UnknownUrnStrategy.DELETE).build().poll();

        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(9)));
    }

    @Test
    void unknownUrnDeadLetterGoesToDlq() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 2);
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(rec))
            .producer(producer).retryTopics(topology())
            .unknownUrn(UnknownUrnStrategy.DEAD_LETTER)
            .build()
            .poll();

        ProducerRecord<byte[], byte[]> dlq = producer.history().get(0);
        assertEquals("orders.dlq", dlq.topic());
        Envelope dead = EnvelopeCodec.decode(new String(dlq.value(), StandardCharsets.UTF_8));
        assertEquals("unknown_urn", dead.deadLetter().reason());
    }

    @Test
    void poisonBodyIsForwardedToDlqRaw() {
        ConsumerRecord<byte[], byte[]> rec = rawRecord("not-json".getBytes(StandardCharsets.UTF_8), 4);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();
        Throwable[] reported = {null};

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .onError((e, env, r) -> reported[0] = e)
            .build()
            .poll();

        assertEquals(1, producer.history().size());
        assertEquals("orders.dlq", producer.history().get(0).topic());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
        org.junit.jupiter.api.Assertions.assertNotNull(reported[0]);
    }

    @Test
    void unknownUrnReleaseRepublishesToRetry() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 6);
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(rec))
            .producer(producer).retryTopics(topology())
            .unknownUrn(UnknownUrnStrategy.RELEASE)
            .build()
            .poll();

        assertEquals("orders.retry.1", producer.history().get(0).topic());
        assertEquals("1", KafkaHeaders.string(producer.history().get(0).headers(), KafkaHeaders.ATTEMPTS));
    }

    @Test
    void terminalFailureWithDlqDisabledDropsAndCommits() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(2), 7);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();
        RetryTopics noDlq = RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).withoutDlq().build();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(noDlq).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        assertEquals(0, producer.history().size()); // dropped (no DLQ)
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(8)));
    }

    @Test
    void handlersMapAndPollTimeoutAreApplied() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 0);
        int[] seen = {-1};
        KafkaConsumer.builder(consumerWith(rec))
            .handlers(Map.of(URN, (env, r) -> seen[0] = env.attempts()))
            .pollTimeout(Duration.ofMillis(250))
            .build()
            .poll();
        assertEquals(0, seen[0]);
    }

    @Test
    void runPollsWhileSupplierIsTrue() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 0);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        boolean[] first = {true};

        KafkaConsumer.builder(consumer)
            .handler(URN, (env, r) -> { })
            .build()
            .run(() -> {
                if (first[0]) {
                    first[0] = false;
                    return true;
                }
                return false;
            });

        verify(consumer).poll(any());
    }

    @Test
    void runStopsWhenSupplierIsFalse() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        KafkaConsumer.builder(consumer).build().run(() -> false);
        verify(consumer, never()).poll(any());
    }
}
