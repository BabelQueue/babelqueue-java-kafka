package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.babelqueue.EnvelopeCodec;
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
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * The shared {@code kafka_poison} conformance section (ADR-0034 section 10, ADR-0032 section 6): every case's verdict
 * ({@code ok} | {@code no_identity} | {@code poison}) is recomputed from the raw record value, and its fate through the
 * transport is checked: {@code no_identity} and {@code poison} both go to {@code <topic>.dlq} with {@code reason:
 * poison} without calling the handler; {@code ok} is routed to its handler.
 */
class KafkaPoisonConformanceTest {

    private static final TopicPartition TP = new TopicPartition("orders", 0);

    private static JSONArray cases() throws IOException {
        try (InputStream in = KafkaPoisonConformanceTest.class.getResourceAsStream("/conformance/manifest.json")) {
            assertNotNull(in, "conformance manifest");
            JSONObject manifest = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            return manifest.getJSONObject("kafka_poison").getJSONArray("cases");
        }
    }

    /** The core-decoder verdict for a raw body. */
    private static String verdict(String body) {
        if (EnvelopeShape.violation(body) != null) {
            return "poison";
        }
        try {
            return EnvelopeCodec.urn(EnvelopeCodec.decode(body)).isEmpty() ? "no_identity" : "ok";
        } catch (RuntimeException | StackOverflowError e) {
            return "poison";
        }
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[], byte[]> consumerWith(String body) {
        byte[] value = body.getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("orders", 0, 4, 0L, TimestampType.CREATE_TIME,
            -1, value.length, null, value, new RecordHeaders(), Optional.empty());
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(record))));
        return consumer;
    }

    @Test
    void theSectionHasAllNinetyTwoCases() throws IOException {
        assertEquals(92, cases().length());
    }

    @Test
    void everyCaseHasTheExpectedVerdict() throws IOException {
        List<String> mismatches = new ArrayList<>();
        JSONArray cases = cases();
        for (int i = 0; i < cases.length(); i++) {
            JSONObject c = cases.getJSONObject(i);
            String actual = verdict(c.getString("body"));
            if (!actual.equals(c.getString("expect"))) {
                mismatches.add(c.getString("name") + ": expected " + c.getString("expect") + " got " + actual);
            }
        }
        assertTrue(mismatches.isEmpty(), "verdict mismatches: " + mismatches);
    }

    @Test
    void everyCaseHasTheExpectedFateThroughTheConsumer() throws IOException {
        List<String> failures = new ArrayList<>();
        JSONArray cases = cases();
        for (int i = 0; i < cases.length(); i++) {
            JSONObject c = cases.getJSONObject(i);
            String name = c.getString("name");
            String body = c.getString("body");
            boolean ok = "ok".equals(c.getString("expect"));
            MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
            int[] handled = {0};
            // The delete strategy: a decodable record with no handler is dropped, so only poison writes anything.
            KafkaConsumer.builder(consumerWith(body))
                .producer(producer)
                .retryTopics(RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).build())
                .unknownUrn(UnknownUrnStrategy.DELETE)
                .handler("urn:x", (env, r) -> handled[0]++)
                .build()
                .poll();
            if (ok) {
                if (!producer.history().isEmpty()) {
                    failures.add(name + ": ok body was dead-lettered");
                }
                continue;
            }
            if (handled[0] != 0) {
                failures.add(name + ": handler called");
            }
            if (producer.history().size() != 1 || !"orders.dlq".equals(producer.history().get(0).topic())) {
                failures.add(name + ": not exactly one orders.dlq record");
                continue;
            }
            String value = new String(producer.history().get(0).value(), StandardCharsets.UTF_8);
            @SuppressWarnings("unchecked")
            Map<String, Object> dlq = (Map<String, Object>) JsonValues.decode(value);
            @SuppressWarnings("unchecked")
            Map<String, Object> deadLetter = (Map<String, Object>) dlq.get("dead_letter");
            if (deadLetter == null || !"poison".equals(deadLetter.get("reason"))) {
                failures.add(name + ": DLQ reason is not poison");
            }
        }
        assertFalse(!failures.isEmpty(), "fate mismatches: " + failures);
    }

    @Test
    void anOkBodyIsRoutedToItsHandler() {
        int[] handled = {0};
        MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaConsumer.builder(consumerWith("{\"job\":5,\"urn\":\"urn:x\"}"))
            .producer(producer)
            .retryTopics(RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).build())
            .handler("urn:x", (env, r) -> handled[0]++)
            .build()
            .poll();
        assertEquals(1, handled[0]);
        assertTrue(producer.history().isEmpty());
    }
}
