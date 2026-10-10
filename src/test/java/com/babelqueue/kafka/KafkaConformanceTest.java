package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Apache Kafka binding conformance against the vendored canonical suite's {@code kafka} block:
 * the §6 header projection and the {@code bq-attempts}-header-authoritative-else-body
 * reconciliation. The Kafka consumer is mocked with Mockito — no Kafka, no network.
 */
class KafkaConformanceTest {

    private static final String URN = "urn:babel:orders:created";

    private static String resource(String path) throws Exception {
        try (InputStream in = KafkaConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject kafkaBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("kafka");
    }

    @Test
    void propertyProjectionMatchesGolden() throws Exception {
        JSONObject projection = kafkaBlock().getJSONObject("property_projection");
        Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));
        RecordHeaders headers = new RecordHeaders(KafkaHeaders.of(envelope));

        JSONObject want = projection.getJSONObject("headers");
        for (String key : want.keySet()) {
            assertEquals(want.getString(key), KafkaHeaders.string(headers, key), key);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void attemptsReconciliationMatchesGolden() throws Exception {
        JSONArray cases = kafkaBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            Envelope base = EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null);
            Envelope env = new Envelope(
                base.job(), base.traceId(), base.data(), base.meta(),
                testCase.getInt("body_attempts"), base.deadLetter());
            byte[] value = EnvelopeCodec.encode(env).getBytes(StandardCharsets.UTF_8);

            RecordHeaders headers = new RecordHeaders();
            if (!testCase.isNull("header_attempts")) {
                headers.add(KafkaHeaders.header(KafkaHeaders.ATTEMPTS, Integer.toString(testCase.getInt("header_attempts"))));
            }

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "orders", 0, i, 0L, TimestampType.CREATE_TIME, -1, value.length, null, value, headers, Optional.empty());
            Consumer<byte[], byte[]> consumer = mock(Consumer.class);
            when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(Map.of(new TopicPartition("orders", 0), List.of(record))));

            int[] seen = {-1};
            KafkaConsumer.builder(consumer)
                .handler(URN, (envelopeIn, recordIn) -> seen[0] = envelopeIn.attempts())
                .build()
                .poll();

            assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void schemaVersionGateMatchesGolden() throws Exception {
        JSONObject gate = kafkaBlock().getJSONObject("schema_version_gate");
        String property = gate.getString("property");
        byte[] value = resource(gate.getString("fixture")).getBytes(StandardCharsets.UTF_8);
        JSONArray cases = gate.getJSONArray("cases");
        assertTrue(cases.length() > 0);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            boolean decode = "decode".equals(testCase.getString("expect"));
            boolean jsonNull = testCase.has("value") && testCase.isNull("value");
            String label = jsonNull ? "null" : testCase.has("value") ? "value=" + testCase.getString("value") : "absent";

            RecordHeaders headers = new RecordHeaders();
            if (jsonNull) {
                // A header present with a null value (§6.7: null value = header absent).
                headers.add(new RecordHeader(property, null));
            } else if (testCase.has("value")) {
                headers.add(KafkaHeaders.header(property, testCase.getString("value")));
            }
            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "orders", 0, i, 0L, TimestampType.CREATE_TIME, -1, value.length, null, value, headers, Optional.empty());
            Consumer<byte[], byte[]> consumer = mock(Consumer.class);
            when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(Map.of(new TopicPartition("orders", 0), List.of(record))));
            MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

            int[] handled = {0};
            KafkaConsumer.builder(consumer)
                .producer(producer)
                .retryTopics(RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).build())
                .handler(URN, (envelopeIn, recordIn) -> handled[0]++)
                .build()
                .poll();

            assertEquals(decode ? 1 : 0, handled[0], label);
            // Reject = poison path: the raw record lands on the DLQ and the offset is committed.
            assertEquals(decode ? 0 : 1, producer.history().size(), label);
            if (!decode) {
                assertEquals("orders.dlq", producer.history().get(0).topic(), label);
            }
            verify(consumer).commitSync(Map.of(new TopicPartition("orders", 0), new OffsetAndMetadata(i + 1L)));
        }
    }
}
