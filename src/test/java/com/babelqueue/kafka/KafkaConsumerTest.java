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
import com.babelqueue.JsonValues;
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
    void attemptsBeforeAtOrBeyondTheTierCountStayOnTheLastTier() {
        // 2 tiers, attempts_before = 4 (>= n): target is retry.<min(4, n-1) + 1> = retry.2, attempts become 5.
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(recordFor(envelope(4), 3)))
            .producer(producer).retryTopics(topology()).maxTries(10)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        ProducerRecord<byte[], byte[]> retry = producer.history().get(0);
        assertEquals("orders.retry.2", retry.topic());
        assertEquals("5", KafkaHeaders.string(retry.headers(), KafkaHeaders.ATTEMPTS));
        assertEquals("60000", KafkaHeaders.string(retry.headers(), KafkaHeaders.DELAY));
        assertEquals(5, EnvelopeCodec.decode(new String(retry.value(), StandardCharsets.UTF_8)).attempts());
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

    /** An inbound message carrying a top-level {@code extra_top} and {@code meta.vendor_flag} (ADR-0031). */
    private static Envelope envelopeWithExtras(int attempts) {
        return EnvelopeCodec.decode("{\"job\":\"" + URN + "\",\"trace_id\":\"trace-1\",\"data\":{\"order_id\":7},"
            + "\"meta\":{\"id\":\"m-1\",\"queue\":\"orders\",\"lang\":\"php\",\"schema_version\":1,"
            + "\"created_at\":1700000000000,\"vendor_flag\":true},\"attempts\":" + attempts + ",\"extra_top\":1}");
    }

    @Test
    void retryRepublishPreservesUnknownTopLevelAndMetaKeys() {
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(recordFor(envelopeWithExtras(0), 10)))
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        ProducerRecord<byte[], byte[]> retry = producer.history().get(0);
        assertEquals("orders.retry.1", retry.topic());
        Envelope bumped = EnvelopeCodec.decode(new String(retry.value(), StandardCharsets.UTF_8));
        assertEquals(1, bumped.attempts());
        assertEquals(1L, bumped.extras().get("extra_top"));
        assertEquals(true, bumped.meta().extras().get("vendor_flag"));
    }

    /** Body says attempts=0 but the {@code bq-attempts} header says 1: the header wins and the extras survive the re-encode. */
    @Test
    void headerBodyAttemptsMismatchReconcilePreservesUnknownKeysOnRepublish() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelopeWithExtras(0), 10);
        rec.headers().remove(KafkaHeaders.ATTEMPTS);
        KafkaHeaders.of(envelopeWithExtras(1)).stream()
            .filter(h -> h.key().equals(KafkaHeaders.ATTEMPTS))
            .forEach(rec.headers()::add);
        MockProducer<byte[], byte[]> producer = producer();
        Envelope[] seen = new Envelope[1];

        KafkaConsumer.builder(consumerWith(rec))
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> {
                seen[0] = env;
                throw new IllegalStateException("boom");
            })
            .build()
            .poll();

        assertEquals(1, seen[0].attempts(), "the header value is authoritative");
        assertEquals(1L, seen[0].extras().get("extra_top"), "the reconciled envelope keeps top-level extras");
        assertEquals(true, seen[0].meta().extras().get("vendor_flag"), "the reconciled envelope keeps meta extras");

        ProducerRecord<byte[], byte[]> retry = producer.history().get(0);
        assertEquals("orders.retry.2", retry.topic());
        Envelope bumped = EnvelopeCodec.decode(new String(retry.value(), StandardCharsets.UTF_8));
        assertEquals(2, bumped.attempts());
        assertEquals(1L, bumped.extras().get("extra_top"));
        assertEquals(true, bumped.meta().extras().get("vendor_flag"));
    }

    @Test
    void deadLetterPreservesUnknownKeysAndCarriesReasonAndOriginalQueue() {
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumerWith(recordFor(envelopeWithExtras(2), 7)))
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build()
            .poll();

        ProducerRecord<byte[], byte[]> dlq = producer.history().get(0);
        assertEquals("orders.dlq", dlq.topic());
        Envelope dead = EnvelopeCodec.decode(new String(dlq.value(), StandardCharsets.UTF_8));
        assertEquals(1L, dead.extras().get("extra_top"));
        assertEquals(true, dead.meta().extras().get("vendor_flag"));
        assertEquals("failed", dead.deadLetter().reason());
        assertEquals("orders", dead.deadLetter().originalQueue());
        assertEquals(URN, dead.job());
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dlqBody(ProducerRecord<byte[], byte[]> sent) {
        return (Map<String, Object>) JsonValues.decode(new String(sent.value(), StandardCharsets.UTF_8));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deadLetterOf(Map<String, Object> body) {
        return (Map<String, Object>) body.get("dead_letter");
    }

    @Test
    void poisonBodyIsWrappedAsRawWithDeadLetterBlock() {
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
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertEquals("orders.dlq", sent.topic());
        Map<String, Object> body = dlqBody(sent);
        assertEquals("not-json", body.get("raw"));
        Map<String, Object> deadLetter = deadLetterOf(body);
        assertEquals("poison", deadLetter.get("reason"));
        assertEquals("orders", deadLetter.get("original_queue"));
        assertEquals("java", deadLetter.get("lang"));
        assertEquals(0L, deadLetter.get("attempts"));
        assertEquals(BabelQueueException.class.getName(), deadLetter.get("exception"));
        assertEquals(reported[0].getMessage(), deadLetter.get("error"));
        assertTrue(((Number) deadLetter.get("failed_at")).longValue() > 0);
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
    }

    @Test
    void poisonBodyThatIsAJsonObjectKeepsItsKeys() {
        // `data` is a list, so the envelope does not decode, but the body is still one JSON object.
        String json = "{\"job\":\"urn:babel:orders:created\",\"trace_id\":\"t-1\",\"data\":[1,2],\"keep\":\"me\"}";
        ConsumerRecord<byte[], byte[]> rec = rawRecord(json.getBytes(StandardCharsets.UTF_8), 4);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> { throw new AssertionError("a poison record must never be handled"); })
            .build()
            .poll();

        Map<String, Object> body = dlqBody(producer.history().get(0));
        assertEquals(URN, body.get("job"));
        assertEquals("t-1", body.get("trace_id"));
        assertEquals(List.of(1L, 2L), body.get("data"));
        assertEquals("me", body.get("keep"));
        assertTrue(!body.containsKey("raw"));
        assertEquals("poison", deadLetterOf(body).get("reason"));
        assertEquals(URN, KafkaHeaders.string(producer.history().get(0).headers(), KafkaHeaders.JOB));
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
    }

    @Test
    void poisonBodyThatIsNotAnObjectIsWrappedAsRaw() {
        for (String text : new String[] {"[1,2]", "42", "", "{\"job\":\"x\"} trailing"}) {
            MockProducer<byte[], byte[]> producer = producer();
            KafkaConsumer.builder(consumerWith(rawRecord(text.getBytes(StandardCharsets.UTF_8), 4)))
                .producer(producer).retryTopics(topology()).build().poll();
            Map<String, Object> body = dlqBody(producer.history().get(0));
            assertEquals(text, body.get("raw"), text);
            assertEquals("poison", deadLetterOf(body).get("reason"), text);
        }
    }

    @Test
    void poisonBodyThatIsNotUtf8IsStoredWithReplacementCharacters() {
        byte[] bytes = {'a', (byte) 0xFF, 'b'};
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(consumerWith(rawRecord(bytes, 4)))
            .producer(producer).retryTopics(topology()).build().poll();
        assertEquals("a\uFFFDb", dlqBody(producer.history().get(0)).get("raw"));
    }

    @Test
    void poisonDeadLetterNeverProjectsAZeroSchemaVersionHeader() {
        // No meta.schema_version → the lenient decode yields 0, which must not become `bq-schema-version: 0`
        // (a later §6.7 gate would reject the DLQ record).
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(consumerWith(rawRecord("not-json".getBytes(StandardCharsets.UTF_8), 4)))
            .producer(producer).retryTopics(topology()).build().poll();
        assertNull(producer.history().get(0).headers().lastHeader(KafkaHeaders.SCHEMA_VERSION));
    }

    @Test
    void poisonBodyWithANumberTheCodecCannotWriteBackFallsBackToRaw() {
        // 1e400 decodes to Infinity, which cannot be written back as JSON: the body is carried verbatim as `raw`
        // instead of producing an invalid (or failing) DLQ record.
        String json = "{\"job\":\"x\",\"data\":[1],\"big\":1e400}";
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(consumerWith(rawRecord(json.getBytes(StandardCharsets.UTF_8), 4)))
            .producer(producer).retryTopics(topology()).build().poll();
        Map<String, Object> body = dlqBody(producer.history().get(0));
        assertEquals(json, body.get("raw"));
        assertEquals("poison", deadLetterOf(body).get("reason"));
    }

    @Test
    void poisonDeadLetterCarriesTheAttemptsHeaderAndTheOriginalTopic() {
        byte[] value = "not-json".getBytes(StandardCharsets.UTF_8);
        Header[] headers = {
            KafkaHeaders.header(KafkaHeaders.ATTEMPTS, "2"),
            KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, "billing")};
        ConsumerRecord<byte[], byte[]> rec = new ConsumerRecord<>("orders.retry.2", 0, 4L, 0L,
            TimestampType.CREATE_TIME, -1, value.length, null, value, new RecordHeaders(headers), Optional.empty());
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(consumerWith(rec)).producer(producer)
            .retryTopics(RetryTopics.forTopic("billing").tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(60)).build())
            .build().poll();

        Map<String, Object> deadLetter = deadLetterOf(dlqBody(producer.history().get(0)));
        assertEquals(2L, deadLetter.get("attempts"));
        assertEquals("billing", deadLetter.get("original_queue"));
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

        worker.poll(); // a throwing onError is swallowed
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

        worker.poll(); // a throwing onError is swallowed
        assertEquals(1, producer.history().size());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(9)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    // -- DLQ / retry write failure on the non-gate paths: never commit past an unconfirmed write ----------

    /** A producer whose every send fails asynchronously (the returned future completes exceptionally). */
    private static MockProducer<byte[], byte[]> failingProducer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record) {
                return java.util.concurrent.CompletableFuture.failedFuture(new RuntimeException("broker unavailable"));
            }
        };
    }

    @Test
    void terminalFailureDlqWriteFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> failing = recordFor(envelope(2), 7); // nextAttempt 3 == maxTries -> DLQ
        ConsumerRecord<byte[], byte[]> later = recordFor(envelope(0), 8); // same partition, must be skipped
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(failing, later))));
        int[] handled = {0};
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { handled[0]++; throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        assertEquals(1, handled[0], "the record after the rewound one must not be handled");
        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 7L);
        assertEquals(2, errors.size()); // settlement failure first, then the handler's own error (settle, then report)
        assertTrue(errors.get(0).getMessage().contains("rewound to offset 7"), errors.get(0).getMessage());
        assertEquals("broker unavailable", errors.get(0).getCause().getCause().getMessage());
        assertInstanceOf(IllegalStateException.class, errors.get(1));
    }

    @Test
    void synchronousSendFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> failing = recordFor(envelope(2), 7); // nextAttempt 3 == maxTries -> DLQ
        Consumer<byte[], byte[]> consumer = consumerWith(failing);
        List<Throwable> errors = new ArrayList<>();
        // send() itself throws (buffer full / max.block.ms exceeded) instead of returning a failed future.
        MockProducer<byte[], byte[]> throwingProducer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer()) {
                @Override
                public synchronized java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                    ProducerRecord<byte[], byte[]> record) {
                    throw new org.apache.kafka.common.KafkaException("buffer full");
                }
            };

        KafkaConsumer.builder(consumer)
            .producer(throwingProducer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 7L);
        assertEquals(2, errors.size()); // settlement failure first, then the handler's own error (settle, then report)
        assertTrue(errors.get(0).getMessage().contains("rewound to offset 7"), errors.get(0).getMessage());
        assertInstanceOf(IllegalStateException.class, errors.get(1));
    }

    @Test
    void retryTierRepublishFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 10);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 10L);
        assertEquals(2, errors.size());
    }

    @Test
    void poisonBodyDlqWriteFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> rec = rawRecord("not-json".getBytes(StandardCharsets.UTF_8), 4);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology())
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 4L);
        assertEquals(2, errors.size()); // decode error + DLQ write failure
    }

    @Test
    void unknownUrnDeadLetterWriteFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 2);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        List<Throwable> errors = new ArrayList<>();

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology())
            .unknownUrn(UnknownUrnStrategy.DEAD_LETTER)
            .onError((e, env, r) -> errors.add(e))
            .build()
            .poll();

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 2L);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getMessage().contains("rewound to offset 2"), errors.get(0).getMessage());
    }

    @Test
    void unknownUrnReleaseRepublishFailureDoesNotCommitAndRewinds() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(0), 6);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology())
            .unknownUrn(UnknownUrnStrategy.RELEASE)
            .build()
            .poll();

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(TP, 6L);
    }

    @Test
    void dlqWriteFailureWithThrowingOnErrorStillRewinds() {
        ConsumerRecord<byte[], byte[]> rec = recordFor(envelope(2), 7);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);

        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> { if (e instanceof BabelQueueException) { throw new IllegalArgumentException("onError blew up"); } })
            .build();

        worker.poll(); // a throwing onError is swallowed
        verify(consumer).seek(TP, 7L);
        verify(consumer, never()).commitSync(any(Map.class));
    }

    // -- a throwing onError must never bypass settlement on the report-capable paths ----------------------

    @Test
    void nonJsonBodyRejectedByConformanceCheckWithThrowingOnErrorStillParksAndCommits() {
        ConsumerRecord<byte[], byte[]> rec = rawRecord("not-json".getBytes(StandardCharsets.UTF_8), 4);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build()
            .poll();

        assertEquals("orders.dlq", producer.history().get(0).topic());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void decodeThrowingWithThrowingOnErrorStillParksTheRawRecordAndCommits() {
        // EnvelopeCodec.decode returns an empty envelope for non-JSON instead of throwing, so the decode-catch
        // branch is forced here by making decode itself fail.
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> rec = rawRecord(body, 4);
        Consumer<byte[], byte[]> consumer = consumerWith(rec);
        MockProducer<byte[], byte[]> producer = producer();

        try (org.mockito.MockedStatic<com.babelqueue.EnvelopeCodec> codec =
                 org.mockito.Mockito.mockStatic(com.babelqueue.EnvelopeCodec.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
            codec.when(() -> com.babelqueue.EnvelopeCodec.decode(org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new IllegalStateException("decode exploded"));

            KafkaConsumer.builder(consumer)
                .producer(producer).retryTopics(topology())
                .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
                .build()
                .poll();
        }

        assertEquals(1, producer.history().size());
        assertEquals("orders.dlq", producer.history().get(0).topic());
        Map<String, Object> wrapped = dlqBody(producer.history().get(0));
        assertEquals("poison", deadLetterOf(wrapped).get("reason"));
        assertEquals(IllegalStateException.class.getName(), deadLetterOf(wrapped).get("exception"));
        assertEquals("decode exploded", deadLetterOf(wrapped).get("error"));
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void typedInvalidEnvelopeWithThrowingOnErrorStillParksAndCommits() {
        // meta.id is a number: Go's typed decode rejects it, so it is poison (not a conformance-check rejection).
        String json = "{\"job\":\"" + URN + "\",\"trace_id\":\"t\",\"data\":{},\"meta\":{\"id\":5},\"attempts\":0}";
        Consumer<byte[], byte[]> consumer = consumerWith(rawRecord(json.getBytes(StandardCharsets.UTF_8), 12));
        MockProducer<byte[], byte[]> producer = producer();
        int[] handled = {0};

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology())
            .handler(URN, (env, r) -> handled[0]++)
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build()
            .poll();

        assertEquals(0, handled[0]);
        assertEquals("orders.dlq", producer.history().get(0).topic());
        assertEquals("poison", deadLetterOf(dlqBody(producer.history().get(0))).get("reason"));
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(13)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void typedInvalidEnvelopeDlqFailureWithThrowingOnErrorStillRewinds() {
        String json = "{\"job\":\"" + URN + "\",\"meta\":{\"created_at\":\"2026\"}}";
        Consumer<byte[], byte[]> consumer = consumerWith(rawRecord(json.getBytes(StandardCharsets.UTF_8), 12));

        KafkaConsumer.builder(consumer)
            .producer(failingProducer()).retryTopics(topology())
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build()
            .poll();

        verify(consumer).seek(TP, 12L);
        verify(consumer, never()).commitSync(any(Map.class));
    }

    @Test
    void throwingHandlerWithThrowingOnErrorStillRepublishesToRetryAndCommits() {
        Consumer<byte[], byte[]> consumer = consumerWith(recordFor(envelope(0), 10));
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build()
            .poll();

        assertEquals("orders.retry.1", producer.history().get(0).topic());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(11)));
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void exhaustedHandlerWithThrowingOnErrorStillDeadLettersAndCommits() {
        Consumer<byte[], byte[]> consumer = consumerWith(recordFor(envelope(2), 7));
        MockProducer<byte[], byte[]> producer = producer();

        KafkaConsumer.builder(consumer)
            .producer(producer).retryTopics(topology()).maxTries(3)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build()
            .poll();

        assertEquals("orders.dlq", producer.history().get(0).topic());
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(8)));
    }

    @Test
    void unknownUrnFailWithThrowingOnErrorStillRaisesUnknownUrn() {
        Consumer<byte[], byte[]> consumer = consumerWith(recordFor(envelope(0), 3));

        KafkaConsumer worker = KafkaConsumer.builder(consumer)
            .onError((e, env, r) -> { throw new IllegalArgumentException("onError blew up"); })
            .build();

        assertThrows(UnknownUrnException.class, worker::poll);
        verify(consumer, never()).commitSync(any(Map.class));
    }
}
