package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.JsonValues;
import com.babelqueue.UnknownUrnStrategy;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/**
 * The poison trigger set is Go's: a body is poison exactly when {@code babelqueue.Decode} (typed
 * {@code Envelope}/{@code Meta}/{@code DeadLetter}) rejects it. {@code poison-trigger-set.json} holds the bodies and the
 * verdicts produced by running {@code babelqueue.Decode} itself; the same table is checked in every SDK.
 */
class PoisonTriggerSetTest {

    private static final String URN = "urn:babel:orders:created";
    private static final TopicPartition TP = new TopicPartition("orders", 0);

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> table() throws IOException {
        try (InputStream in = PoisonTriggerSetTest.class.getResourceAsStream("/poison-trigger-set.json")) {
            assertNotNull(in, "poison-trigger-set.json");
            return (List<Map<String, Object>>) (List<?>) JsonValues.decode(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static ConsumerRecord<byte[], byte[]> record(String body) {
        byte[] value = body.getBytes(StandardCharsets.UTF_8);
        return new ConsumerRecord<>("orders", 0, 4, 0L, TimestampType.CREATE_TIME,
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
        return RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).build();
    }

    @Test
    void theShapeCheckMatchesGoOnEveryTableRow() throws IOException {
        List<String> mismatches = new ArrayList<>();
        for (Map<String, Object> row : table()) {
            boolean expected = (Boolean) row.get("poison");
            boolean actual = EnvelopeShape.violation((String) row.get("body")) != null;
            if (expected != actual) {
                mismatches.add(row.get("name") + " (Go poison=" + expected + ")");
            }
        }
        assertTrue(mismatches.isEmpty(), "differs from Go: " + mismatches);
    }

    @Test
    void theConsumerDeadLettersExactlyThePoisonRowsAndNothingElse() throws IOException {
        for (Map<String, Object> row : table()) {
            String name = (String) row.get("name");
            boolean poison = (Boolean) row.get("poison");
            Consumer<byte[], byte[]> consumer = consumerWith(record((String) row.get("body")));
            MockProducer<byte[], byte[]> producer = producer();
            int[] reported = {0};

            // No handlers and the delete strategy: a decodable record is simply dropped and committed.
            KafkaConsumer.builder(consumer)
                .producer(producer).retryTopics(topology()).unknownUrn(UnknownUrnStrategy.DELETE)
                .onError((e, env, r) -> reported[0]++)
                .build()
                .poll();

            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
            verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
            if (poison) {
                assertEquals(1, producer.history().size(), name);
                assertEquals("orders.dlq", producer.history().get(0).topic(), name);
                @SuppressWarnings("unchecked")
                Map<String, Object> body = (Map<String, Object>) JsonValues.decode(
                    new String(producer.history().get(0).value(), StandardCharsets.UTF_8));
                @SuppressWarnings("unchecked")
                Map<String, Object> deadLetter = (Map<String, Object>) body.get("dead_letter");
                assertEquals("poison", deadLetter.get("reason"), name);
                assertEquals(1, reported[0], name);
            } else {
                assertEquals(0, producer.history().size(), name);
                assertEquals(0, reported[0], name);
            }
        }
    }

    @Test
    void aTypedValidButNonConformantEnvelopeIsRoutedToItsHandlerLikeGo() {
        // Missing data, blank trace_id, a body schema_version other than 1: Go's App never calls Accepts.
        String[] bodies = {
            "{\"job\":\"" + URN + "\"}",
            "{\"job\":\"" + URN + "\",\"trace_id\":\"\",\"data\":{}}",
            "{\"job\":\"" + URN + "\",\"trace_id\":\"t\",\"data\":{},\"meta\":{\"schema_version\":2}}",
        };
        for (String body : bodies) {
            Consumer<byte[], byte[]> consumer = consumerWith(record(body));
            MockProducer<byte[], byte[]> producer = producer();
            int[] handled = {0};
            KafkaConsumer.builder(consumer)
                .producer(producer).retryTopics(topology())
                .handler(URN, (env, r) -> handled[0]++)
                .build()
                .poll();
            assertEquals(1, handled[0], body);
            assertEquals(0, producer.history().size(), body);
            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
        }
    }

    @Test
    void anEmptyJobOrABareNullGoesDownTheUnknownUrnPathNotThePoisonPath() {
        for (String body : new String[] {"{}", "null", "{\"job\":null}"}) {
            MockProducer<byte[], byte[]> producer = producer();
            KafkaConsumer.builder(consumerWith(record(body)))
                .producer(producer).retryTopics(topology()).unknownUrn(UnknownUrnStrategy.DEAD_LETTER)
                .build()
                .poll();
            assertEquals(1, producer.history().size(), body);
            @SuppressWarnings("unchecked")
            Map<String, Object> dlq = (Map<String, Object>) JsonValues.decode(
                new String(producer.history().get(0).value(), StandardCharsets.UTF_8));
            @SuppressWarnings("unchecked")
            Map<String, Object> deadLetter = (Map<String, Object>) dlq.get("dead_letter");
            assertEquals("unknown_urn", deadLetter.get("reason"), body);
            assertFalse(dlq.containsKey("raw"), body);
        }
    }

    @Test
    void aBodyNestedDeeperThanTheCoreParserCarriesIsDeadLetteredAsPoisonNotLeftToWedgeThePartition() {
        // Go accepts nesting up to 10000 and the shape scan is iterative, but the core codec parses recursively and
        // overflows the stack (an Error, not a RuntimeException). That must be a poison DLQ record + commit, never
        // an Error out of poll() (which skips the commit and redelivers the same record forever).
        for (int depth : new int[] {7_000, 9_000}) {
            String body = "{\"job\":\"" + URN + "\",\"trace_id\":\"t\",\"data\":{\"k\":"
                + "[".repeat(depth) + "]".repeat(depth) + "},\"meta\":{\"schema_version\":1}}";
            Consumer<byte[], byte[]> consumer = consumerWith(record(body));
            MockProducer<byte[], byte[]> producer = producer();
            int[] handled = {0};
            KafkaConsumer.builder(consumer)
                .producer(producer).retryTopics(topology())
                .handler(URN, (env, r) -> handled[0]++)
                .build()
                .poll();
            assertEquals(0, handled[0], "depth " + depth);
            assertEquals(1, producer.history().size(), "depth " + depth);
            assertEquals("orders.dlq", producer.history().get(0).topic(), "depth " + depth);
            String value = new String(producer.history().get(0).value(), StandardCharsets.UTF_8);
            assertTrue(value.contains("\"reason\":\"poison\""), "depth " + depth);
            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
            verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
        }
    }

    @Test
    void aBodyThatIsOneHugeNestedArrayIsAPoisonRawRecordNotAStackOverflow() {
        String body = "[".repeat(9_000) + "]".repeat(9_000);
        Consumer<byte[], byte[]> consumer = consumerWith(record(body));
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(consumer).producer(producer).retryTopics(topology()).build().poll();
        assertEquals(1, producer.history().size());
        String value = new String(producer.history().get(0).value(), StandardCharsets.UTF_8);
        assertTrue(value.contains("\"raw\":") && value.contains("\"reason\":\"poison\""));
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(5)));
    }

    @Test
    void memberNamesAreCaseSensitiveLikeTheCoreDecoder() {
        // A folded or re-cased name is an unknown member (syntax-checked only), never the typed one.
        assertNull(EnvelopeShape.violation("{\"attempt\u017f\":\"x\"}"));
        assertNull(EnvelopeShape.violation("{\"ATTEMPTS\":\"x\"}"));
        assertNull(EnvelopeShape.violation("{\"JOB\":5}"));
        assertNull(EnvelopeShape.violation("{\"META\":{\"ID\":5}}"));
        assertNull(EnvelopeShape.violation("{\"trace_\u0131d\":5}"));
        assertNull(EnvelopeShape.violation("{\"trace_\u0130d\":5}"));
        // The exact spelling is still type-checked.
        assertNotNull(EnvelopeShape.violation("{\"attempts\":\"x\"}"));
        assertNotNull(EnvelopeShape.violation("{\"meta\":{\"id\":5}}"));
        assertNotNull(EnvelopeShape.violation("{\"trace_id\":5}"));
    }

    @Test
    void aNonStringUrnAliasIsIgnoredNotPoison() {
        assertNull(EnvelopeShape.violation("{\"urn\":5}"));
        assertNotNull(EnvelopeShape.violation("{\"job\":5}"));
    }
}
