package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** §6 produce: value = envelope, record ts = created_at, bq- headers; delay → retry tier or raise. */
class KafkaPublisherTest {

    private static final String URN = "urn:babel:orders:created";

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void publishProjectsValueHeadersAndTimestamp() {
        MockProducer<byte[], byte[]> producer = producer();

        String id = KafkaPublisher.create(producer, "orders").publish(URN, Map.of("order_id", 7), "trace-1");

        assertEquals(1, producer.history().size());
        ProducerRecord<byte[], byte[]> rec = producer.history().get(0);
        assertEquals("orders", rec.topic());
        assertNull(rec.key());
        assertEquals(URN, KafkaHeaders.string(rec.headers(), KafkaHeaders.JOB));
        assertEquals("trace-1", KafkaHeaders.string(rec.headers(), KafkaHeaders.TRACE_ID));
        assertEquals(id, KafkaHeaders.string(rec.headers(), KafkaHeaders.MESSAGE_ID));
        assertEquals("1", KafkaHeaders.string(rec.headers(), KafkaHeaders.SCHEMA_VERSION));
        assertEquals("0", KafkaHeaders.string(rec.headers(), KafkaHeaders.ATTEMPTS));

        Envelope decoded = EnvelopeCodec.decode(new String(rec.value(), StandardCharsets.UTF_8));
        assertEquals(URN, decoded.job());
        assertEquals(decoded.meta().createdAt(), rec.timestamp());
    }

    @Test
    void publishWithoutTraceMintsAFreshTrace() {
        MockProducer<byte[], byte[]> producer = producer();
        KafkaPublisher.create(producer, "orders").publish(URN, Map.of());
        assertNotNull(KafkaHeaders.string(producer.history().get(0).headers(), KafkaHeaders.TRACE_ID));
    }

    @Test
    void delayWithoutRetryTopicsRaises() {
        MockProducer<byte[], byte[]> producer = producer();
        KafkaPublisher publisher = KafkaPublisher.create(producer, "orders");
        assertThrows(BabelQueueException.class,
            () -> publisher.publish(URN, Map.of(), null, Duration.ofSeconds(30)));
    }

    @Test
    void delayRoutesToTheSmallestSufficientRetryTier() {
        MockProducer<byte[], byte[]> producer = producer();
        RetryTopics rt = RetryTopics.forTopic("orders")
            .tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(60)).build();

        KafkaPublisher.create(producer, rt).publish(URN, Map.of(), null, Duration.ofSeconds(30));

        ProducerRecord<byte[], byte[]> rec = producer.history().get(0);
        assertEquals("orders.retry.2", rec.topic());
        assertEquals("30000", KafkaHeaders.string(rec.headers(), KafkaHeaders.DELAY));
        assertEquals("orders", KafkaHeaders.string(rec.headers(), KafkaHeaders.ORIGINAL_TOPIC));
        // The envelope's meta.queue stays the work topic, not the physical retry topic.
        Envelope decoded = EnvelopeCodec.decode(new String(rec.value(), StandardCharsets.UTF_8));
        assertEquals("orders", decoded.meta().queue());
    }
}
