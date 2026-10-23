package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** The retry-tier re-inject consumer (ADR-0034 §6.4; A6 clamp, A7 header policy). */
class KafkaRetryReinjectorTest {

    private static final String URN = "urn:babel:orders:created";
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    private static final TopicPartition RETRY1 = new TopicPartition("orders.retry.1", 0);

    private static RetryTopics topology() {
        return RetryTopics.forTopic("orders").tier(Duration.ofSeconds(5)).tier(Duration.ofSeconds(60)).build();
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static ConsumerRecord<byte[], byte[]> retryRecord(long offset, long timestamp, byte[] key, byte[] value,
                                                              Header... headers) {
        return new ConsumerRecord<>("orders.retry.1", 0, offset, timestamp, TimestampType.LOG_APPEND_TIME,
            -1, value == null ? 0 : value.length, key, value, new RecordHeaders(headers), Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[], byte[]> consumerReturning(ConsumerRecord<byte[], byte[]>... records) {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(RETRY1, List.of(records))));
        when(consumer.assignment()).thenReturn(Set.of(RETRY1));
        return consumer;
    }

    private static Header h(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static KafkaRetryReinjector reinjector(Consumer<byte[], byte[]> consumer,
                                                   MockProducer<byte[], byte[]> producer, AtomicLong clock) {
        return KafkaRetryReinjector.builder(consumer, producer, topology()).clock(clock::get).build();
    }

    @Test
    void failedRecordIsRepublishedToTierOneAndReturnsToTheWorkTopicByteForByte() throws Exception {
        // 1. The work consumer fails a record that carries a traceparent: it lands on orders.retry.1.
        Envelope base = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");
        byte[] value = EnvelopeCodec.encode(base).getBytes(StandardCharsets.UTF_8);
        byte[] key = "order-7".getBytes(StandardCharsets.UTF_8);
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> workConsumer = mock(Consumer.class);
        ConsumerRecord<byte[], byte[]> incoming = new ConsumerRecord<>("orders", 0, 3, 1_000L,
            TimestampType.CREATE_TIME, -1, value.length, key, value,
            new RecordHeaders(new Header[] {h("traceparent", TRACEPARENT)}), Optional.empty());
        when(workConsumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of(new TopicPartition("orders", 0), List.of(incoming))));
        MockProducer<byte[], byte[]> producer = producer();
        KafkaConsumer.builder(workConsumer).producer(producer).retryTopics(topology()).maxTries(5)
            .handler(URN, (env, r) -> { throw new IllegalStateException("boom"); })
            .build().poll();

        ProducerRecord<byte[], byte[]> retry = producer.history().get(0);
        assertEquals("orders.retry.1", retry.topic());
        assertEquals(TRACEPARENT, KafkaHeaders.string(retry.headers(), "traceparent"));

        // 2. The re-inject consumer reads it back once the tier has elapsed and returns it to the work topic.
        ConsumerRecord<byte[], byte[]> onTier = retryRecord(0, 10_000L, retry.key(), retry.value(),
            retry.headers().toArray());
        AtomicLong clock = new AtomicLong(20_000L);
        Consumer<byte[], byte[]> consumer = consumerReturning(onTier);
        MockProducer<byte[], byte[]> out = producer();

        assertEquals(1, reinjector(consumer, out, clock).poll());

        ProducerRecord<byte[], byte[]> back = out.history().get(0);
        assertEquals("orders", back.topic());
        assertArrayEquals(retry.value(), back.value());
        assertArrayEquals(key, back.key());
        assertNull(back.headers().lastHeader(KafkaHeaders.DELAY));
        assertNull(back.headers().lastHeader(KafkaHeaders.ORIGINAL_TOPIC));
        assertEquals(TRACEPARENT, KafkaHeaders.string(back.headers(), "traceparent"));
        assertEquals("1", KafkaHeaders.string(back.headers(), KafkaHeaders.ATTEMPTS)); // other bq-* headers kept
        // attempts in the body were bumped once by the work consumer; the re-injector left them alone.
        assertEquals(1, EnvelopeCodec.decode(new String(back.value(), StandardCharsets.UTF_8)).attempts());
        verify(consumer).commitSync(Map.of(RETRY1, new OffsetAndMetadata(1)));
    }

    @Test
    void recordOlderThanItsTierIsForwardedWithoutWaiting() {
        byte[] value = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        // Timestamp 100 s in the past, tier 5 s: target is far behind now -> clamped to now, forwarded at once.
        ConsumerRecord<byte[], byte[]> old = retryRecord(4, 0L, null, value);
        AtomicLong clock = new AtomicLong(100_000L);
        Consumer<byte[], byte[]> consumer = consumerReturning(old);
        MockProducer<byte[], byte[]> out = producer();

        assertEquals(1, reinjector(consumer, out, clock).poll());

        assertEquals(1, out.history().size());
        verify(consumer, never()).pause(any());
        verify(consumer, never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void notYetDueRecordPausesThePartitionThenForwardsAfterTheTierElapses() {
        byte[] value = "{\"x\":1}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> fresh = retryRecord(7, 10_000L, null, value, h(KafkaHeaders.DELAY, "5000"));
        AtomicLong clock = new AtomicLong(11_000L); // due at 15_000
        Consumer<byte[], byte[]> consumer = consumerReturning(fresh);
        MockProducer<byte[], byte[]> out = producer();
        KafkaRetryReinjector reinjector = reinjector(consumer, out, clock);

        assertEquals(0, reinjector.poll());
        verify(consumer).seek(RETRY1, 7L);
        verify(consumer).pause(Set.of(RETRY1));
        assertTrue(out.history().isEmpty());
        verify(consumer, never()).commitSync(any(Map.class));

        clock.set(15_000L);
        assertEquals(1, reinjector.poll());
        verify(consumer).resume(Set.of(RETRY1));
        assertEquals("orders", out.history().get(0).topic());
    }

    @Test
    void aTimestampFromTheFutureNeverWaitsMoreThanOneTier() {
        byte[] value = "{}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> skewed = retryRecord(0, 1_000_000L, null, value); // clock skew
        AtomicLong clock = new AtomicLong(0L);
        Consumer<byte[], byte[]> consumer = consumerReturning(skewed);
        MockProducer<byte[], byte[]> out = producer();
        KafkaRetryReinjector reinjector = reinjector(consumer, out, clock);

        assertEquals(0, reinjector.poll());
        clock.set(5_000L); // now + tier_ms at the first poll
        assertEquals(1, reinjector.poll());
    }

    @Test
    void undecodableBytesAreForwardedAsIsWithOriginalTopicHonoured() {
        byte[] garbage = {0x00, (byte) 0xff, 0x7b, 0x01};
        ConsumerRecord<byte[], byte[]> rec = retryRecord(0, 0L, new byte[] {9, 8}, garbage,
            h(KafkaHeaders.ORIGINAL_TOPIC, "orders"), h(KafkaHeaders.DELAY, "5000"), h("x-custom", "kept"));
        MockProducer<byte[], byte[]> out = producer();

        assertEquals(1, reinjector(consumerReturning(rec), out, new AtomicLong(60_000L)).poll());

        ProducerRecord<byte[], byte[]> back = out.history().get(0);
        assertArrayEquals(garbage, back.value());
        assertArrayEquals(new byte[] {9, 8}, back.key());
        assertEquals("kept", KafkaHeaders.string(back.headers(), "x-custom"));
        List<String> keys = new ArrayList<>();
        back.headers().forEach(x -> keys.add(x.key()));
        assertEquals(List.of("x-custom"), keys);
    }

    @Test
    void aTombstoneKeepsItsNullValue() {
        ConsumerRecord<byte[], byte[]> rec = retryRecord(0, 0L, new byte[] {1}, null);
        MockProducer<byte[], byte[]> out = producer();
        assertEquals(1, reinjector(consumerReturning(rec), out, new AtomicLong(60_000L)).poll());
        assertNull(out.history().get(0).value());
    }

    @Test
    void failedWriteDoesNotCommitAndRewindsThePartition() {
        ConsumerRecord<byte[], byte[]> rec = retryRecord(5, 0L, null, "{}".getBytes(StandardCharsets.UTF_8));
        Consumer<byte[], byte[]> consumer = consumerReturning(rec);
        MockProducer<byte[], byte[]> failing = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRetryReinjector reinjector = reinjector(consumer, failing, new AtomicLong(60_000L));
        // The send is not completed; fail it from another thread-free hook: errorNext after send is awaited by get().
        Thread failer = new Thread(() -> {
            while (failing.history().isEmpty()) {
                Thread.onSpinWait();
            }
            failing.errorNext(new RuntimeException("broker down"));
        });
        failer.start();

        assertThrows(BabelQueueException.class, reinjector::poll);

        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer).seek(RETRY1, 5L);
    }

    @Test
    void subscribeCoversEveryTierTopic() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        KafkaRetryReinjector.builder(consumer, producer(), topology()).build().subscribe();
        verify(consumer).subscribe(List.of("orders.retry.1", "orders.retry.2"));
    }

    @Test
    void aTopologyWithoutTiersIsRejected() {
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        RetryTopics none = RetryTopics.forTopic("orders").build();
        assertThrows(BabelQueueException.class, () -> KafkaRetryReinjector.builder(consumer, producer(), none));
    }
}
