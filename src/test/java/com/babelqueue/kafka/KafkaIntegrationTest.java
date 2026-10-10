package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Live-broker integration tests for the Kafka transport (§6) — opt-in.
 *
 * <p>Skipped in the normal suite. Enable with {@code BQ_INTEGRATION=1} and (optionally) point at a broker with
 * {@code BQ_KAFKA_BROKERS} (default {@code localhost:9092}). The CI {@code integration} job runs an
 * {@code apache/kafka} KRaft service container. Every test uses unique topic names, so the broker can be reused.
 */
@EnabledIfEnvironmentVariable(named = "BQ_INTEGRATION", matches = "1")
class KafkaIntegrationTest {

    private static final String URN = "urn:babel:orders:created";
    private static final Duration DEADLINE = Duration.ofSeconds(60);

    private static String brokers() {
        String value = System.getenv("BQ_KAFKA_BROKERS");
        return value == null || value.isBlank() ? "localhost:9092" : value;
    }

    private static void createTopics(String... topics) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers());
        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get();
        }
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<byte[], byte[]> consumer(String group, String... topics) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topics));
        return consumer;
    }

    /** Reads {@code dlq} with a plain, separate consumer until one record arrives or the deadline passes. */
    private static ConsumerRecord<byte[], byte[]> awaitDlqRecord(String dlq) {
        try (KafkaConsumer<byte[], byte[]> reader = consumer("bq-it-dlq-" + UUID.randomUUID(), dlq)) {
            long stop = System.nanoTime() + DEADLINE.toNanos();
            while (System.nanoTime() < stop) {
                for (ConsumerRecord<byte[], byte[]> record : reader.poll(Duration.ofMillis(500))) {
                    return record;
                }
            }
        }
        throw new AssertionError("no record reached " + dlq + " within " + DEADLINE);
    }

    @Test
    void handlerErrorRetriesThenDeadLettersAfterMaxTries() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String work = "bq-it-orders-" + suffix;
        RetryTopics topology = RetryTopics.forTopic(work).tier(Duration.ofSeconds(1)).build();
        String retry = topology.tiers().get(0).topic();
        String dlq = topology.dlqTopic();
        createTopics(work, retry, dlq);

        AtomicInteger calls = new AtomicInteger();
        try (KafkaProducer<byte[], byte[]> producer = producer();
             KafkaConsumer<byte[], byte[]> rawConsumer = consumer("bq-it-group-" + suffix, work, retry)) {
            com.babelqueue.kafka.KafkaConsumer worker = com.babelqueue.kafka.KafkaConsumer.builder(rawConsumer)
                .producer(producer)
                .retryTopics(topology)
                .maxTries(3)
                .pollTimeout(Duration.ofMillis(500))
                .handler(URN, (envelope, record) -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("boom");
                })
                .build();

            KafkaPublisher.create(producer, topology).publish(URN, Map.of("order_id", 7), "trace-it-1");

            long stop = System.nanoTime() + DEADLINE.toNanos();
            while (calls.get() < 3 && System.nanoTime() < stop) {
                worker.poll();
            }
            assertEquals(3, calls.get(), "handler must run once per attempt up to maxTries");
            // One more poll settles the terminal record (DLQ write is confirmed before it returns).
            worker.poll();
        }

        ConsumerRecord<byte[], byte[]> dead = awaitDlqRecord(dlq);
        Envelope envelope = EnvelopeCodec.decode(new String(dead.value(), StandardCharsets.UTF_8));
        assertNotNull(envelope.deadLetter(), "DLQ envelope must carry a dead_letter block");
        assertEquals("failed", envelope.deadLetter().reason());
        assertEquals(work, envelope.deadLetter().originalQueue());
        assertEquals("trace-it-1", envelope.traceId());
        assertEquals(URN, EnvelopeCodec.urn(envelope));
    }

    @Test
    void schemaVersionGateRejectionForwardsRawBodyToDlq() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String work = "bq-it-gate-" + suffix;
        RetryTopics topology = RetryTopics.forTopic(work).tier(Duration.ofSeconds(1)).build();
        String dlq = topology.dlqTopic();
        createTopics(work, topology.tiers().get(0).topic(), dlq);

        // Body is deliberately not a valid v1 envelope: the gate must reject on the header alone (§6.7).
        byte[] body = "{\"job\":\"urn:babel:orders:created\",\"data\":{},\"future\":true}"
            .getBytes(StandardCharsets.UTF_8);
        AtomicInteger handled = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        try (KafkaProducer<byte[], byte[]> producer = producer();
             KafkaConsumer<byte[], byte[]> rawConsumer = consumer("bq-it-group-" + suffix, work)) {
            com.babelqueue.kafka.KafkaConsumer worker = com.babelqueue.kafka.KafkaConsumer.builder(rawConsumer)
                .producer(producer)
                .retryTopics(topology)
                .pollTimeout(Duration.ofMillis(500))
                .onError((error, env, record) -> rejected.incrementAndGet())
                .handler(URN, (envelope, record) -> handled.incrementAndGet())
                .build();

            List<Header> headers = List.of(
                new RecordHeader(KafkaHeaders.JOB, URN.getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(KafkaHeaders.SCHEMA_VERSION, "2".getBytes(StandardCharsets.UTF_8)));
            producer.send(new ProducerRecord<>(work, null, System.currentTimeMillis(), null, body, headers)).get();

            long stop = System.nanoTime() + DEADLINE.toNanos();
            while (rejected.get() == 0 && System.nanoTime() < stop) {
                worker.poll();
            }
            assertTrue(rejected.get() >= 1, "the gate must report the rejection");
        }

        ConsumerRecord<byte[], byte[]> dead = awaitDlqRecord(dlq);
        assertArrayEquals(body, dead.value(), "the raw body must reach the DLQ unmodified");
        Header version = dead.headers().lastHeader(KafkaHeaders.SCHEMA_VERSION);
        assertNotNull(version);
        assertEquals("2", new String(version.value(), StandardCharsets.UTF_8));
        assertEquals(0, handled.get(), "a gate-rejected record must never reach a handler");
    }
}
