package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.Redrive;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** Redrive over the Kafka transport keeps the DLQ record's key (contract §6.5) on every re-publish. */
class KafkaRedriveTransportTest {

    private static final TopicPartition TP = new TopicPartition("orders.dlq", 0);
    private static final byte[] KEYED = "order-7".getBytes(StandardCharsets.UTF_8);

    private static byte[] dlqValue() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "orders", "trace-1");
        return EnvelopeCodec.encode(DeadLetters.annotate(env, "failed", "orders")).getBytes(StandardCharsets.UTF_8);
    }

    private static ConsumerRecord<byte[], byte[]> dlqRecord(byte[] key, byte[] value) {
        return dlqRecord(key, value, new RecordHeaders());
    }

    private static ConsumerRecord<byte[], byte[]> dlqRecord(byte[] key, byte[] value, RecordHeaders headers) {
        return new ConsumerRecord<>("orders.dlq", 0, 41, 0L, TimestampType.CREATE_TIME,
            key == null ? -1 : key.length, value == null ? -1 : value.length, key, value, headers, Optional.empty());
    }

    @SuppressWarnings("unchecked")
    private static Consumer<byte[], byte[]> consumerWith(ConsumerRecord<byte[], byte[]> record) {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any(java.time.Duration.class)))
            .thenReturn(new ConsumerRecords<>(Map.of(TP, List.of(record))))
            .thenReturn(ConsumerRecords.empty());
        return consumer;
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void redrivenRecordKeepsTheKeyByteForByte() throws Exception {
        for (byte[] key : new byte[][] {KEYED, null, new byte[0]}) {
            Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(key, dlqValue()));
            MockProducer<byte[], byte[]> producer = producer();
            KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

            Redrive.Result result = Redrive.redrive(transport, "orders.dlq", Redrive.Options.all());

            assertEquals(1, result.redriven());
            assertEquals(1, producer.history().size());
            ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
            assertEquals("orders", sent.topic());
            if (key == null) {
                assertNull(sent.key(), "a null key must stay null");
            } else {
                assertArrayEquals(key, sent.key());
            }
            Envelope body = EnvelopeCodec.decode(new String(sent.value(), StandardCharsets.UTF_8));
            assertNull(body.deadLetter(), "the dead_letter block is reset");
            assertEquals(0, body.attempts());
            assertEquals("urn:babel:orders:created", KafkaHeaders.string(sent.headers(), KafkaHeaders.JOB));
            verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(42)));
        }
    }

    @Test
    void restoreWriteBackToTheDlqKeepsTheKey() throws Exception {
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, dlqValue()));
        MockProducer<byte[], byte[]> producer = producer();
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Redrive.redrive(transport, "orders.dlq", Redrive.Options.all().dryRun(true));

        assertEquals(1, producer.history().size());
        assertEquals("orders.dlq", producer.history().get(0).topic());
        assertArrayEquals(KEYED, producer.history().get(0).key());
    }

    @Test
    void undecodableBodyIsRestoredVerbatimWithItsKey() throws Exception {
        byte[] poison = "not json".getBytes(StandardCharsets.UTF_8);
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, poison));
        MockProducer<byte[], byte[]> producer = producer();
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Redrive.redrive(transport, "orders.dlq", Redrive.Options.all());

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertEquals("orders.dlq", sent.topic());
        assertArrayEquals(poison, sent.value());
        assertArrayEquals(KEYED, sent.key());
        assertTrue(sent.headers().toArray().length == 0, "no projection for a non-envelope body");
    }

    @Test
    void invalidUtf8RecordIsRestoredByteForByteWithItsKeyAndHeaders() throws Exception {
        byte[] poison = {(byte) 0xFF, (byte) 0xFE, 0x01};
        RecordHeaders headers = new RecordHeaders();
        headers.add("traceparent", "00-abc-def-01".getBytes(StandardCharsets.UTF_8));
        headers.add("bq-schema-version", "2".getBytes(StandardCharsets.UTF_8));
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, poison, headers));
        MockProducer<byte[], byte[]> producer = producer();
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Redrive.Result result = Redrive.redrive(transport, "orders.dlq", Redrive.Options.all());

        assertEquals(0, result.redriven());
        assertEquals(1, producer.history().size());
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertEquals("orders.dlq", sent.topic());
        assertArrayEquals(poison, sent.value(), "the original bytes survive, no U+FFFD substitution");
        assertArrayEquals(KEYED, sent.key());
        assertEquals("00-abc-def-01", KafkaHeaders.string(sent.headers(), "traceparent"));
        assertEquals("2", KafkaHeaders.string(sent.headers(), "bq-schema-version"));
        // Committed only AFTER the verbatim restore was acknowledged.
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(42)));
    }

    @Test
    void nullValueRecordIsRestoredAsNullValue() throws Exception {
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, null));
        MockProducer<byte[], byte[]> producer = producer();

        Redrive.redrive(new KafkaRedriveTransport(consumer, producer), "orders.dlq", Redrive.Options.all());

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertEquals("orders.dlq", sent.topic());
        assertNull(sent.value(), "a tombstone stays a tombstone");
        assertArrayEquals(KEYED, sent.key());
    }

    @Test
    void failedRestoreOfInvalidUtf8RecordDoesNotCommit() throws Exception {
        byte[] poison = {(byte) 0xFF, (byte) 0xFE, 0x01};
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, poison));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Thread failer = new Thread(() -> {
            while (producer.history().isEmpty()) {
                Thread.onSpinWait();
            }
            producer.errorNext(new RuntimeException("broker down"));
        });
        failer.start();
        try {
            org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all()));
        } finally {
            failer.join();
        }
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
    }

    @Test
    void failedRestoreRewindsTheConsumerToTheRecordAndNeverCommits() throws Exception {
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, dlqValue()));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        // A dry run restores the record to the DLQ; the broker refuses that write.
        Thread failer = failEveryWrite(producer);
        try {
            org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all().dryRun(true)));
        } finally {
            failer.interrupt();
            failer.join();
        }

        verify(consumer).seek(TP, 41L);
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
    }

    @Test
    void failedRedriveWriteAndFailedRestoreRewindAndReportBothErrors() throws Exception {
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, dlqValue()));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Thread failer = failEveryWrite(producer);
        Exception thrown;
        try {
            thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all()));
        } finally {
            failer.interrupt();
            failer.join();
        }

        assertEquals(1, thrown.getSuppressed().length, "the redrive-write failure rides along on the restore error");
        verify(consumer).seek(TP, 41L);
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
    }

    @Test
    void failedRewindIsReportedAlongsideTheRestoreError() throws Exception {
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, dlqValue()));
        org.mockito.Mockito.doThrow(new IllegalStateException("seek failed")).when(consumer).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Thread failer = failEveryWrite(producer);
        Exception thrown;
        try {
            thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all().dryRun(true)));
        } finally {
            failer.interrupt();
            failer.join();
        }

        assertEquals(1, thrown.getSuppressed().length);
        assertEquals("seek failed", thrown.getSuppressed()[0].getMessage());
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
    }

    private static ConsumerRecord<byte[], byte[]> dlqRecordAt(int partition, long offset) {
        return new ConsumerRecord<>("orders.dlq", partition, offset, 0L, TimestampType.CREATE_TIME,
            -1, -1, KEYED, dlqValue(), new RecordHeaders(), Optional.empty());
    }

    /** A consumer whose first poll returns the given records (partition order fixed) and later polls are empty. */
    private static Consumer<byte[], byte[]> consumerPolling(List<ConsumerRecord<byte[], byte[]>> records) {
        // LinkedHashMap, not Map.of: the poll result's partition order decides the pop order and must be stable.
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>())
                .add(record);
        }
        @SuppressWarnings("unchecked")
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any(java.time.Duration.class)))
            .thenReturn(new ConsumerRecords<>(byPartition))
            .thenReturn(ConsumerRecords.empty());
        return consumer;
    }

    @Test
    void rewindSeeksEveryPartitionBackToItsFirstUnacknowledgedRecord() {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(
            List.of(dlqRecordAt(0, 41), dlqRecordAt(0, 42), dlqRecordAt(1, 9)));
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        // The whole batch is popped before it is processed, so the consumer is already past all three records.
        Redrive.Reserved first = transport.pop("orders.dlq");
        transport.pop("orders.dlq");
        transport.pop("orders.dlq");
        assertEquals(41L, ((ConsumerRecord<?, ?>) first.handle()).offset());
        transport.rewind(first);

        verify(consumer).seek(TP, 41L);
        verify(consumer).seek(other, 9L);
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
        // The buffer was dropped: the next pop re-polls instead of handing out a stale record.
        assertNull(transport.pop("orders.dlq"));
    }

    @Test
    void rewindLeavesAlreadyAcknowledgedRecordsAlone() {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(
            List.of(dlqRecordAt(0, 41), dlqRecordAt(0, 42), dlqRecordAt(1, 9)));
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        Redrive.Reserved first = transport.pop("orders.dlq");
        Redrive.Reserved second = transport.pop("orders.dlq");
        transport.pop("orders.dlq");
        transport.ack(first);
        transport.rewind(second);

        verify(consumer).seek(TP, 42L);
        verify(consumer).seek(other, 9L);
        verify(consumer, org.mockito.Mockito.never()).seek(TP, 41L);
    }

    @Test
    void failedRestoreInAMultiPartitionBatchRewindsEveryPartition() throws Exception {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41), dlqRecordAt(1, 9)));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        Thread failer = failEveryWrite(producer);
        try {
            org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all().dryRun(true)));
        } finally {
            failer.interrupt();
            failer.join();
        }

        // The partition-1 record was popped but never processed: it must be rewound too, not just the failed one.
        verify(consumer).seek(TP, 41L);
        verify(consumer).seek(other, 9L);
        verify(consumer, org.mockito.Mockito.never()).commitSync(any(Map.class));
    }

    @Test
    void rewindOfAnAcknowledgedSoleRecordSeeksNothing() {
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41)));
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        Redrive.Reserved only = transport.pop("orders.dlq");
        transport.ack(only);
        transport.rewind(only);

        // Nothing is outstanding: seeking the acked partition back would redeliver (duplicate) the record.
        verify(consumer, org.mockito.Mockito.never()).seek(any(TopicPartition.class), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void rewindOfAnAcknowledgedRecordResumesRightAfterItInTheSamePartition() {
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41), dlqRecordAt(0, 42), dlqRecordAt(0, 43)));
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        Redrive.Reserved first = transport.pop("orders.dlq");
        transport.pop("orders.dlq");
        transport.pop("orders.dlq");
        transport.ack(first);
        transport.rewind(first);

        // 41 is acknowledged (committed up to 42): the partition resumes at 42, never at 41 (a duplicate).
        verify(consumer).seek(TP, 42L);
        verify(consumer, org.mockito.Mockito.never()).seek(TP, 41L);
    }

    @Test
    void rewindAttemptsEverySeekEvenWhenOneFails() {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41), dlqRecordAt(1, 9)));
        RuntimeException seekFailure = new IllegalStateException("seek refused");
        org.mockito.Mockito.doThrow(seekFailure).when(consumer).seek(TP, 41L);
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        Redrive.Reserved first = transport.pop("orders.dlq");
        transport.pop("orders.dlq");

        RuntimeException thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
            () -> transport.rewind(first));

        assertEquals(seekFailure, thrown);
        // The failing first partition did not stop the second one from being rewound.
        verify(consumer).seek(other, 9L);
    }

    @Test
    void rewindReportsEverySeekFailureNotOnlyTheFirst() {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41), dlqRecordAt(1, 9)));
        RuntimeException first = new IllegalStateException("seek p0 refused");
        RuntimeException second = new IllegalStateException("seek p1 refused");
        org.mockito.Mockito.doThrow(first).when(consumer).seek(TP, 41L);
        org.mockito.Mockito.doThrow(second).when(consumer).seek(other, 9L);
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer());

        Redrive.Reserved reserved = transport.pop("orders.dlq");
        transport.pop("orders.dlq");

        RuntimeException thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
            () -> transport.rewind(reserved));

        assertEquals(first, thrown);
        assertEquals(List.of(second), List.of(thrown.getSuppressed()));
    }

    @Test
    void failedWorkWriteWithASuccessfulRestoreRewindsTheRestButNotTheAcknowledgedRecord() throws Exception {
        TopicPartition other = new TopicPartition("orders.dlq", 1);
        Consumer<byte[], byte[]> consumer = consumerPolling(List.of(dlqRecordAt(0, 41), dlqRecordAt(1, 9)));
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaRedriveTransport transport = new KafkaRedriveTransport(consumer, producer);

        // The work-topic write fails, the restore to the DLQ succeeds.
        Thread failer = failWritesExcept(producer, "orders.dlq");
        try {
            org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> Redrive.redrive(transport, "orders.dlq", Redrive.Options.all()));
        } finally {
            failer.interrupt();
            failer.join();
        }

        // The first record was restored and acked: committed once, never sought back (that would duplicate it).
        verify(consumer).commitSync(Map.of(TP, new OffsetAndMetadata(42L)));
        verify(consumer, org.mockito.Mockito.never()).seek(TP, 41L);
        // The run stopped, so the popped-but-unprocessed partition-1 record is handed back.
        verify(consumer).seek(other, 9L);
    }

    /** Fails producer writes to every topic except {@code okTopic}, which succeed, until interrupted. */
    private static Thread failWritesExcept(MockProducer<byte[], byte[]> producer, String okTopic) {
        Thread failer = new Thread(() -> {
            int handled = 0;
            while (!Thread.currentThread().isInterrupted()) {
                if (producer.history().size() > handled) {
                    ProducerRecord<byte[], byte[]> sent = producer.history().get(handled);
                    handled++;
                    if (okTopic.equals(sent.topic())) {
                        producer.completeNext();
                    } else {
                        producer.errorNext(new RuntimeException("broker down"));
                    }
                } else {
                    Thread.onSpinWait();
                }
            }
        });
        failer.start();
        return failer;
    }

    /** Fails each producer write as soon as it is issued, until interrupted. */
    private static Thread failEveryWrite(MockProducer<byte[], byte[]> producer) {
        Thread failer = new Thread(() -> {
            int handled = 0;
            while (!Thread.currentThread().isInterrupted()) {
                if (producer.history().size() > handled) {
                    handled++;
                    producer.errorNext(new RuntimeException("broker down"));
                } else {
                    Thread.onSpinWait();
                }
            }
        });
        failer.start();
        return failer;
    }

    @Test
    void restoreIsStampedNowNotWithTheEnvelopeCreatedAt() throws Exception {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "orders", "trace-1");
        byte[] value = EnvelopeCodec.encode(DeadLetters.annotate(env, "failed", "orders")).getBytes(StandardCharsets.UTF_8);
        Consumer<byte[], byte[]> consumer = consumerWith(dlqRecord(KEYED, value));
        MockProducer<byte[], byte[]> producer = producer();
        long before = System.currentTimeMillis();

        Redrive.redrive(new KafkaRedriveTransport(consumer, producer), "orders.dlq", Redrive.Options.all().dryRun(true));

        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertArrayEquals(value, sent.value());
        assertTrue(sent.timestamp() >= before, "restore is stamped with the write time");
    }

    @Test
    void plainPublishCarriesNoKey() throws Exception {
        MockProducer<byte[], byte[]> producer = producer();
        KafkaRedriveTransport transport = new KafkaRedriveTransport(mock(Consumer.class), producer);

        transport.publish("orders", new String(dlqValue(), StandardCharsets.UTF_8));

        assertNull(producer.history().get(0).key());
    }

    @Test
    void emptyDlqPopsNull() {
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(consumer.poll(any(java.time.Duration.class))).thenReturn(ConsumerRecords.empty());
        assertNull(new KafkaRedriveTransport(consumer, producer()).pop("orders.dlq"));
    }
}
