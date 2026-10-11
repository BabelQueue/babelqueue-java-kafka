package com.babelqueue.kafka;

import com.babelqueue.EnvelopeCodec;
import com.babelqueue.Envelope;
import com.babelqueue.Redrive;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

/**
 * A {@link Redrive.Transport} over Apache Kafka, so {@link Redrive#redrive} (ADR-0026) can replay a
 * {@code <topic>.dlq} topic back to its work topic.
 *
 * <p>The supplied {@code consumer} must already be subscribed to (or assigned) the DLQ topic with
 * auto-commit disabled; {@link #pop} polls it and hands the records out one at a time, and
 * {@link #ack} commits the record's offset only after {@link Redrive} has re-published it (at-least-once).
 * Every write is acknowledged by the broker before it returns, so a failure surfaces and the offset
 * is never committed past it.
 *
 * <p>It implements {@link Redrive.KeyedRepublisher}: the redrive write to the work topic and the restore write
 * back to the DLQ carry the DLQ record's <b>key byte-for-byte</b> (a null key stays null, an empty key stays
 * empty, none is synthesised — contract §6.5). Partition equality is not guaranteed.
 *
 * <p>A <b>restore</b> write (the same body written back to the DLQ it was popped from: dry run, unselected,
 * undecodable, failed redrive write) re-writes the DLQ record <b>verbatim</b> — its original value bytes (a
 * null value stays null), original headers and key — with a fresh timestamp, exactly like
 * {@link KafkaConsumer}'s raw DLQ write. A record whose value is not valid UTF-8 is popped with an empty body
 * (so {@link Redrive} treats it as undecodable and restores it) and is therefore never altered or committed
 * away. A <b>redrive</b> write to the work topic re-projects the §6 {@code bq-} headers from the body being
 * published (a decodable envelope only), so the redriven record carries the reset {@code attempts} and no stale
 * {@code dead_letter} metadata. The {@code bypass} option's {@code bq-replay-bypass} header is not propagated
 * by this transport.
 */
public final class KafkaRedriveTransport implements Redrive.Transport, Redrive.KeyedRepublisher {

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;
    private final Duration pollTimeout;
    private final Deque<ConsumerRecord<byte[], byte[]>> buffer = new ArrayDeque<>();

    /** A redrive transport that polls the DLQ with a 1 second timeout. */
    public KafkaRedriveTransport(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer) {
        this(consumer, producer, Duration.ofSeconds(1));
    }

    /** A redrive transport with an explicit poll timeout (an empty poll means the DLQ is drained). */
    public KafkaRedriveTransport(
        Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer, Duration pollTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
    }

    @Override
    public Redrive.Reserved pop(String queue) {
        if (buffer.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(pollTimeout)) {
                buffer.addLast(record);
            }
        }
        ConsumerRecord<byte[], byte[]> next = buffer.pollFirst();
        if (next == null) {
            return null;
        }
        // Strict decode: a lenient one would swap invalid bytes for U+FFFD, and a restore must never alter the
        // record. An unreadable (invalid UTF-8) or null value is handed out as an empty body, which Redrive treats
        // as undecodable and restores - and publishFrom then re-writes the ORIGINAL bytes, not this body.
        String body = decodeStrict(next.value());
        return new Redrive.Reserved(body == null ? "" : body, next);
    }

    @Override
    public void publish(String queue, String body) throws Exception {
        send(queue, body, null);
    }

    @Override
    public void publishFrom(String queue, String body, Redrive.Reserved from) throws Exception {
        ConsumerRecord<byte[], byte[]> record = record(from);
        if (queue.equals(record.topic()) && body.equals(from.body())) {
            restore(record);
            return;
        }
        send(queue, body, record.key());
    }

    @Override
    public void ack(Redrive.Reserved message) {
        ConsumerRecord<byte[], byte[]> record = record(message);
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
    }

    /** Write the DLQ record back unchanged (value bytes, headers, key) with a fresh timestamp. */
    private void restore(ConsumerRecord<byte[], byte[]> record) throws Exception {
        producer.send(new ProducerRecord<>(record.topic(), null, System.currentTimeMillis(), record.key(),
            record.value(), record.headers())).get();
    }

    private void send(String topic, String body, byte[] key) throws Exception {
        byte[] value = body.getBytes(StandardCharsets.UTF_8);
        List<Header> headers = List.of();
        long timestamp = System.currentTimeMillis();
        // EnvelopeCodec.decode never throws: a body that is not an envelope yields one with no job, which is
        // published without header projection and with a fresh timestamp.
        Envelope envelope = EnvelopeCodec.decode(body);
        if (envelope.job() != null && !envelope.job().isBlank()) {
            headers = KafkaHeaders.of(envelope);
            if (envelope.meta() != null) {
                timestamp = envelope.meta().createdAt();
            }
        }
        producer.send(new ProducerRecord<>(topic, null, timestamp, key, value, headers)).get();
    }

    /** UTF-8 decode that reports malformed input: {@code null} for a null or invalid value. */
    private static String decodeStrict(byte[] value) {
        if (value == null) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value))
                .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static ConsumerRecord<byte[], byte[]> record(Redrive.Reserved message) {
        return (ConsumerRecord<byte[], byte[]>) message.handle();
    }
}
