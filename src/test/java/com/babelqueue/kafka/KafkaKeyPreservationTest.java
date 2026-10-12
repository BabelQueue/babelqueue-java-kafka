package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnStrategy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/**
 * The original record key must survive every consumer-side republish — retry-tier write (including the
 * unknown-URN release path), envelope DLQ and raw/poison DLQ. A null key stays null, an
 * empty key stays empty, and no key is ever synthesised.
 */
class KafkaKeyPreservationTest {

    private static final String URN = "urn:babel:orders:created";
    private static final TopicPartition TP = new TopicPartition("orders", 0);
    private static final byte[] KEYED = "order-7".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY = new byte[0];

    private static ConsumerRecord<byte[], byte[]> record(byte[] key, byte[] value, Header[] headers) {
        return new ConsumerRecord<>("orders", 0, 10, 0L, TimestampType.CREATE_TIME,
            key == null ? -1 : key.length, value.length, key, value, new RecordHeaders(headers), Optional.empty());
    }

    private static ConsumerRecord<byte[], byte[]> envelopeRecord(byte[] key, int attempts) {
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");
        Envelope env = new Envelope(base.job(), base.traceId(), base.data(), base.meta(), attempts, base.deadLetter());
        return record(key, EnvelopeCodec.encode(env).getBytes(StandardCharsets.UTF_8),
            KafkaHeaders.of(env).toArray(new Header[0]));
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static RetryTopics topology() {
        return RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(60)).build();
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[], byte[]> consumerWith(ConsumerRecord<byte[], byte[]> record) {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(record))));
        return consumer;
    }

    private static void assertKey(byte[] expected, ProducerRecord<byte[], byte[]> sent, String path) {
        if (expected == null) {
            assertNull(sent.key(), path + ": a null key must stay null (no key synthesised)");
        } else {
            assertNotNull(sent.key(), path + ": the original key must be carried");
            assertArrayEquals(expected, sent.key(), path + ": the original key must be carried unchanged");
        }
    }

    private static void forEachKey(Function<byte[], ProducerRecord<byte[], byte[]>> path, String name) {
        for (byte[] key : new byte[][] {KEYED, null, EMPTY}) {
            assertKey(key, path.apply(key), name + (key == null ? " [null key]" : key.length == 0 ? " [empty key]" : " [keyed]"));
        }
    }

    @Test
    void retryTierRepublishKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            KafkaConsumer.builder(consumerWith(envelopeRecord(key, 0)))
                .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals(1, producer.history().size());
            assertEquals("orders.retry.1", producer.history().get(0).topic());
            return producer.history().get(0);
        }, "retry tier");
    }

    @Test
    void envelopeDlqKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            // attempts 2 + 1 == maxTries (3): terminal, goes to the DLQ.
            KafkaConsumer.builder(consumerWith(envelopeRecord(key, 2)))
                .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals(1, producer.history().size());
            assertEquals("orders.dlq", producer.history().get(0).topic());
            return producer.history().get(0);
        }, "envelope DLQ");
    }

    @Test
    void unknownUrnDeadLetterKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            KafkaConsumer.builder(consumerWith(envelopeRecord(key, 0)))
                .unknownUrn(UnknownUrnStrategy.DEAD_LETTER)
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals("orders.dlq", producer.history().get(0).topic());
            return producer.history().get(0);
        }, "unknown_urn DLQ");
    }

    @Test
    void unknownUrnReleaseKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            KafkaConsumer.builder(consumerWith(envelopeRecord(key, 0)))
                .unknownUrn(UnknownUrnStrategy.RELEASE)
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals("orders.retry.1", producer.history().get(0).topic());
            return producer.history().get(0);
        }, "unknown_urn release");
    }

    @Test
    void schemaVersionGateRawDlqKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            byte[] body = EnvelopeCodec.encode(
                EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1")).getBytes(StandardCharsets.UTF_8);
            Header[] headers = {KafkaHeaders.header(KafkaHeaders.SCHEMA_VERSION, "2")};
            KafkaConsumer.builder(consumerWith(record(key, body, headers)))
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals("orders.dlq", producer.history().get(0).topic());
            assertArrayEquals(body, producer.history().get(0).value(), "the raw bytes are forwarded untouched");
            return producer.history().get(0);
        }, "raw DLQ (schema-version gate)");
    }

    @Test
    void nonConformantEnvelopeDlqKeepsKey() {
        forEachKey(key -> {
            MockProducer<byte[], byte[]> producer = producer();
            byte[] body = "not-json".getBytes(StandardCharsets.UTF_8); // not a JSON object -> wrapped poison record
            KafkaConsumer.builder(consumerWith(record(key, body, new Header[0])))
                .producer(producer).retryTopics(topology()).build().poll();
            assertEquals("orders.dlq", producer.history().get(0).topic());
            return producer.history().get(0);
        }, "poison DLQ (wrapped)");
    }
}
