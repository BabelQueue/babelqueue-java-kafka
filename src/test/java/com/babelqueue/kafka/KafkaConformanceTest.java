package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
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
}
