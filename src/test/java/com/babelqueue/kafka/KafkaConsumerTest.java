package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.ArrayList;
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

    // ---- §6.7 schema-version gate -------------------------------------------------------------

    private static ConsumerRecord<byte[], byte[]> versionedRecord(byte[] value, String version, long offset) {
        RecordHeaders headers = new RecordHeaders();
        if (version != null) {
            headers.add(KafkaHeaders.header(KafkaHeaders.SCHEMA_VERSION, version));
        }
        return new ConsumerRecord<>("orders", 0, offset, 0L, TimestampType.CREATE_TIME,
            -1, value.length, null, value, headers, Optional.empty());
    }

    @Test
    void schemaVersionGateDecodesWhenHeaderMissingBlankOrSupported() {
        byte[] body = EnvelopeCodec.encode(envelope(0)).getBytes(StandardCharsets.UTF_8);
        String[] versions = {null, "", " \t", "\n\u000b\f\r", "1"};
        for (String version : versions) {
            Consumer<byte[], byte[]> consumer = consumerWith(versionedRecord(body, version, 3));
            int[] handled = {0};

            KafkaConsumer.builder(consumer).handler(URN, (env, r) -> handled[0]++).build().poll();

            assertEquals(1, handled[0], "version=" + version);
            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(4)));
        }
    }

    @Test
    void schemaVersionGateQuarantinesUnsupportedWithoutDecodingOrHandler() {
        for (String version : new String[] {"2", "x"}) {
            // A non-JSON body proves the body is never decoded (decode would take the same poison path,
            // but the reported error would be a decode error, not the gate's).
            byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
            Consumer<byte[], byte[]> consumer = consumerWith(versionedRecord(body, version, 8));
            MockProducer<byte[], byte[]> producer = producer();
            int[] handled = {0};
            List<Throwable> errors = new ArrayList<>();
            List<Envelope> envelopes = new ArrayList<>();

            KafkaConsumer.builder(consumer)
                .producer(producer).retryTopics(topology())
                .handler(URN, (env, r) -> handled[0]++)
                .onError((e, env, r) -> {
                    errors.add(e);
                    envelopes.add(env);
                })
                .build()
                .poll();

            assertEquals(0, handled[0], "version=" + version);
            assertEquals(1, errors.size());
            assertInstanceOf(BabelQueueException.class, errors.get(0));
            assertTrue(errors.get(0).getMessage().contains("unsupported"), errors.get(0).getMessage());
            assertNull(envelopes.get(0));
            assertEquals(1, producer.history().size(), "version=" + version);
            ProducerRecord<byte[], byte[]> dlq = producer.history().get(0);
            assertEquals("orders.dlq", dlq.topic());
            assertEquals("not-json", new String(dlq.value(), StandardCharsets.UTF_8));
            assertEquals(version, KafkaHeaders.string(dlq.headers(), KafkaHeaders.SCHEMA_VERSION));
            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(9)));
        }
    }

    @Test
    void schemaVersionGateDlqAsyncFailureDoesNotCommitAndRewinds() {
        byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> gated = versionedRecord(body, "2", 8);
        ConsumerRecord<byte[], byte[]> later = recordFor(envelope(0), 9); // same partition, after the gated one
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(gated, later))));
        // autoComplete=false: the send future stays pending until errorNext() fails it.
        MockProducer<byte[], byte[]> producer =
            new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        Thread failer = new Thread(() -> {
            while (producer.history().isEmpty()) {
                Thread.onSpinWait();
            }
            producer.errorNext(new RuntimeException("dlq unavailable"));
        });
        failer.start();
        int[] handled = {0};
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> handled[0]++)
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        assertEquals(0, handled[0], "the gated record and the rest of its batch must not reach the handler");
        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 8L);
        assertEquals(2, errors.size());
        assertTrue(errors.get(0).getMessage().contains("unsupported"), errors.get(0).getMessage());
        assertTrue(errors.get(1).getMessage().contains("rewound to offset 8"), errors.get(1).getMessage());
        assertEquals("dlq unavailable", errors.get(1).getCause().getCause().getMessage());
    }

    @Test
    void schemaVersionGateDlqSyncFailureDoesNotCommitAndRewinds() {
        byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
        Consumer<byte[], byte[]> consumer = consumerWith(versionedRecord(body, "2", 8));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record) {
                throw new IllegalStateException("buffer full");
            }
        };
        int[] handled = {0};
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> handled[0]++)
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        assertEquals(0, handled[0]);
        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 8L);
        assertEquals(2, errors.size());
        assertInstanceOf(IllegalStateException.class, errors.get(1).getCause());
    }

    @Test
    void schemaVersionGateRewoundRecordIsRedeliveredAndRejectedAgain() {
        byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> gated = versionedRecord(body, "2", 8);
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(gated))));
        boolean[] dlqDown = {true};
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record) {
                if (dlqDown[0]) {
                    throw new IllegalStateException("dlq down");
                }
                return super.send(record);
            }
        };
        int[] handled = {0};
        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> handled[0]++)
            .build();

        worker.poll(); // DLQ down: rewound, nothing committed
        verify(consumer, never()).commitSync(any(Map.class));
        dlqDown[0] = false;
        worker.poll(); // the same record is redelivered: rejected again, now dead-lettered and committed

        assertEquals(0, handled[0]);
        assertEquals(1, producer.history().size());
        assertEquals("orders.dlq", producer.history().get(0).topic());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(9)));
    }

    @Test
    void schemaVersionGateThrowingOnErrorStillRewindsWhenDlqFails() {
        byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
        Consumer<byte[], byte[]> consumer = consumerWith(versionedRecord(body, "2", 8));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record) {
                throw new IllegalStateException("dlq down");
            }
        };
        int[] handled = {0};

        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> handled[0]++)
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build();

        assertThrows(IllegalArgumentException.class, worker::poll);
        assertEquals(0, handled[0]);
        verify(consumer).seek(TP, 8L);
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    void schemaVersionGateThrowingOnErrorStillCommitsAfterDlqSuccess() {
        byte[] body = "not-json".getBytes(StandardCharsets.UTF_8);
        Consumer<byte[], byte[]> consumer = consumerWith(versionedRecord(body, "2", 8));
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build();

        assertThrows(IllegalArgumentException.class, worker::poll);
        assertEquals(1, producer.history().size());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(9)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }
}
