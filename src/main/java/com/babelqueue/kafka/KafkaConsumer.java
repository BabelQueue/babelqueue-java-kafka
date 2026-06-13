package com.babelqueue.kafka;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import com.babelqueue.UnknownUrnStrategy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

/**
 * Consumes a Kafka work topic in <strong>process-then-commit</strong> mode (manual commit,
 * at-least-once): each record is decoded, validated, routed to the handler for its URN (read
 * from the {@code bq-job} header), and its offset committed only after the handler returns. A
 * throwing handler routes the envelope to a {@code <topic>.retry.<n>} tier with
 * {@code bq-attempts + 1} (the SDK-owned retry, §6.5), then commits so the partition advances;
 * once max-tries is reached the envelope goes to {@code <topic>.dlq} with a {@code dead_letter}
 * block. Kafka exposes no native delivery count, so the {@code bq-attempts} header is the
 * authoritative counter (the body's {@code attempts} is the fallback for non-BabelQueue records).
 *
 * <p>Not thread-safe — a Kafka consumer is single-threaded by design; run one per thread.
 */
public final class KafkaConsumer {

    /** Notified of a non-conformant/poison record, an unmapped URN, or a throwing handler. */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, ConsumerRecord<byte[], byte[]> record);
    }

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;
    private final Map<String, BabelHandler> handlers;
    private final RetryTopics retryTopics;
    private final int maxTries;
    private final String unknownUrn;
    private final ErrorHandler onError;
    private final Duration pollTimeout;

    private KafkaConsumer(Builder builder) {
        this.consumer = builder.consumer;
        this.producer = builder.producer;
        this.handlers = Map.copyOf(builder.handlers);
        this.retryTopics = builder.retryTopics;
        this.maxTries = builder.maxTries;
        this.unknownUrn = builder.unknownUrn;
        this.onError = builder.onError;
        this.pollTimeout = builder.pollTimeout;
    }

    public static Builder builder(Consumer<byte[], byte[]> consumer) {
        return new Builder(consumer);
    }

    /** Poll one batch, route + settle each record (process-then-commit). Returns the batch size. */
    public int poll() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            handle(record);
            count++;
        }
        return count;
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    private void handle(ConsumerRecord<byte[], byte[]> record) {
        Envelope envelope;
        try {
            envelope = reconcile(EnvelopeCodec.decode(value(record)), record);
        } catch (RuntimeException decodeError) {
            // Undecodable / poison body — cannot annotate; report and DLQ the raw record.
            report(decodeError, null, record);
            deadLetterRaw(record);
            commit(record);
            return;
        }

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Kafka."),
                envelope, record);
            deadLetter(envelope, record, "poison", null);
            commit(record);
            return;
        }

        String urn = urn(record, envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            onUnknownUrn(record, envelope, urn);
            return;
        }

        try {
            handler.handle(envelope, record);
            commit(record);
        } catch (Exception error) {
            report(error, envelope, record);
            retryOrDeadLetter(record, envelope, error);
            commit(record);
        }
    }

    // -- attempt reconciliation -------------------------------------------------------------

    /** {@code bq-attempts} header is authoritative; the body {@code attempts} is the fallback. */
    private static Envelope reconcile(Envelope envelope, ConsumerRecord<byte[], byte[]> record) {
        int attempts = KafkaHeaders.integer(record.headers(), KafkaHeaders.ATTEMPTS, envelope.attempts());
        return attempts == envelope.attempts() ? envelope : withAttempts(envelope, attempts);
    }

    // -- routing / failure paths ------------------------------------------------------------

    private void onUnknownUrn(ConsumerRecord<byte[], byte[]> record, Envelope envelope, String urn) {
        switch (unknownUrn) {
            case UnknownUrnStrategy.DELETE -> commit(record);
            case UnknownUrnStrategy.DEAD_LETTER -> {
                deadLetter(envelope, record, "unknown_urn", null);
                commit(record);
            }
            case UnknownUrnStrategy.RELEASE -> {
                republishRetry(record, envelope);
                commit(record);
            }
            // FAIL (default): surface and do NOT commit — the record redelivers on the next poll.
            default -> {
                report(new UnknownUrnException(urn), envelope, record);
                throw new UnknownUrnException(urn);
            }
        }
    }

    private void retryOrDeadLetter(ConsumerRecord<byte[], byte[]> record, Envelope envelope, Throwable error) {
        boolean hasTiers = retryTopics != null && retryTopics.hasTiers();
        boolean hasDlq = retryTopics != null && retryTopics.dlqTopic() != null;
        if (!hasTiers && !hasDlq) {
            // Nothing configured to honor the failure — raise rather than silently drop (§6.5).
            throw new BabelQueueException(
                "Kafka per-record retry requires retry topics and/or a DLQ; neither is configured.", error);
        }
        if (hasTiers && envelope.attempts() + 1 < maxTries) {
            republishRetry(record, envelope);
        } else {
            // Terminal: dead-letter, or (DLQ disabled) degrade to commit-and-drop.
            deadLetter(envelope, record, "failed", error);
        }
    }

    /** Republish the envelope to the next retry tier with {@code bq-attempts + 1} (§6.4–§6.5). */
    private void republishRetry(ConsumerRecord<byte[], byte[]> record, Envelope envelope) {
        if (retryTopics == null || !retryTopics.hasTiers()) {
            throw new BabelQueueException("Kafka retry/release requires retry topics; none are configured.");
        }
        RetryTopics.Tier tier = retryTopics.tierForAttempt(envelope.attempts());
        Envelope bumped = withAttempts(envelope, envelope.attempts() + 1);
        List<Header> headers = KafkaHeaders.of(bumped);
        headers.add(KafkaHeaders.header(KafkaHeaders.DELAY, Long.toString(tier.delay().toMillis())));
        headers.add(KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, originalTopic(record)));
        // The retry-topic consumer waits until record.timestamp + tier, so timestamp = re-entry time.
        send(tier.topic(), bumped, headers, System.currentTimeMillis());
    }

    private void deadLetter(Envelope envelope, ConsumerRecord<byte[], byte[]> record, String reason, Throwable error) {
        String dlq = retryTopics == null ? null : retryTopics.dlqTopic();
        if (dlq == null) {
            return; // dead-lettering disabled → degrade to commit-and-drop
        }
        Envelope annotated = DeadLetters.annotate(
            envelope, reason, originalTopic(record), envelope.attempts(),
            error == null ? null : error.getMessage(),
            error == null ? null : error.getClass().getName());
        List<Header> headers = KafkaHeaders.of(annotated);
        headers.add(KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, originalTopic(record)));
        send(dlq, annotated, headers, System.currentTimeMillis());
    }

    /** Forward an undecodable record's raw bytes to the DLQ (no envelope to annotate). */
    private void deadLetterRaw(ConsumerRecord<byte[], byte[]> record) {
        String dlq = retryTopics == null ? null : retryTopics.dlqTopic();
        if (dlq == null) {
            return;
        }
        requireProducer();
        producer.send(new ProducerRecord<>(dlq, null, System.currentTimeMillis(), record.key(),
            record.value(), record.headers()));
    }

    private void send(String topic, Envelope envelope, List<Header> headers, long timestamp) {
        requireProducer();
        byte[] value = EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, null, timestamp, null, value, headers));
    }

    // -- helpers ----------------------------------------------------------------------------

    private void commit(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
    }

    private static String urn(ConsumerRecord<byte[], byte[]> record, Envelope envelope) {
        String header = KafkaHeaders.string(record.headers(), KafkaHeaders.JOB);
        return header != null ? header : EnvelopeCodec.urn(envelope);
    }

    private static String originalTopic(ConsumerRecord<byte[], byte[]> record) {
        String original = KafkaHeaders.string(record.headers(), KafkaHeaders.ORIGINAL_TOPIC);
        return original != null ? original : record.topic();
    }

    private static String value(ConsumerRecord<byte[], byte[]> record) {
        return record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
    }

    private static Envelope withAttempts(Envelope e, int attempts) {
        return new Envelope(e.job(), e.traceId(), e.data(), e.meta(), attempts, e.deadLetter());
    }

    private void requireProducer() {
        if (producer == null) {
            throw new BabelQueueException("This Kafka consumer needs a producer to republish (retry/DLQ).");
        }
    }

    private void report(Throwable error, Envelope envelope, ConsumerRecord<byte[], byte[]> record) {
        if (onError != null) {
            onError.onError(error, envelope, record);
        }
    }

    /** Fluent builder for {@link KafkaConsumer}. */
    public static final class Builder {
        private final Consumer<byte[], byte[]> consumer;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private Producer<byte[], byte[]> producer;
        private RetryTopics retryTopics;
        private int maxTries = 3;
        private String unknownUrn = UnknownUrnStrategy.FAIL;
        private ErrorHandler onError;
        private Duration pollTimeout = Duration.ofMillis(1000);

        private Builder(Consumer<byte[], byte[]> consumer) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** The producer used to republish to retry/DLQ topics (required for retry/DLQ). */
        public Builder producer(Producer<byte[], byte[]> producer) {
            this.producer = producer;
            return this;
        }

        /** The retry/DLQ topology; enables per-record retry, delay, and dead-lettering. */
        public Builder retryTopics(RetryTopics retryTopics) {
            this.retryTopics = retryTopics;
            return this;
        }

        /** Attempts before terminal dead-lettering (default 3). */
        public Builder maxTries(int maxTries) {
            this.maxTries = maxTries;
            return this;
        }

        /** Strategy for a URN with no handler: {@link UnknownUrnStrategy} values (default {@code fail}). */
        public Builder unknownUrn(String strategy) {
            this.unknownUrn = strategy;
            return this;
        }

        public Builder onError(ErrorHandler onError) {
            this.onError = onError;
            return this;
        }

        public Builder pollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        public KafkaConsumer build() {
            return new KafkaConsumer(this);
        }
    }
}
