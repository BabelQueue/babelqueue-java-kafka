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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * <p>Every retry-tier / DLQ write (handler failure, poison body, non-conformant envelope, unmapped URN) is awaited
 * and the offset is committed only once the broker confirmed it. If the write fails, nothing is committed, the
 * partition is rewound ({@code seek}) to the failed record, the rest of that partition's batch is skipped and the
 * failure goes to {@code onError}; the record is redelivered on the next poll, never lost.
 *
 * <p>Per §6.7 the {@code bq-schema-version} header is checked <b>before</b> the body is decoded: when it
 * is present, not blank (empty or ASCII-whitespace-only) and not exactly
 * {@link EnvelopeCodec#SCHEMA_VERSION} (compared as sent, no trimming), the record is never decoded and
 * the handler is never called — it goes down the poison path: {@code onError} (with a {@code null}
 * envelope), the raw record forwarded to {@code <topic>.dlq} (when configured), then the offset is
 * committed — but only after the DLQ write is <b>confirmed</b>. If it fails, nothing is committed, the
 * partition is rewound ({@code seek}) to the rejected record, the rest of that partition's batch is skipped and
 * the failure is reported to {@code onError}; the record is redelivered on the next poll and rejected again,
 * never lost and never handled. An absent or blank header changes nothing, and the post-decode
 * {@link EnvelopeCodec#accepts} check still applies.
 *
 * <p>Not thread-safe — a Kafka consumer is single-threaded by design; run one per thread.
 */
public final class KafkaConsumer {

    /**
     * Notified of a non-conformant/poison record, an unmapped URN, a throwing handler, or a record rejected by
     * the §6.7 schema-version gate. {@code envelope} is {@code null} when the body was not decoded.
     */
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
        // Partitions rewound because a DLQ/retry write could not be confirmed (gate reject, poison body,
        // retry-tier republish, terminal failure): the rest of their batch is skipped (it is redelivered
        // after the seek) so no later commit can move past the rewound record.
        Set<TopicPartition> rewound = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            count++;
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            if (!rewound.contains(tp) && !handle(record)) {
                rewound.add(tp);
            }
        }
        return count;
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    /** Route + settle one record. Returns {@code false} when the partition was rewound to this record (an unconfirmed DLQ/retry write). */
    private boolean handle(ConsumerRecord<byte[], byte[]> record) {
        String declaredVersion = schemaVersionHeader(record);
        if (declaredVersion != null && !isBlankSchemaVersion(declaredVersion)
            && !declaredVersion.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION))) {
            // §6.7: reject/quarantine WITHOUT decoding — never best-effort parse an unknown schema.
            BabelQueueException rejected = new BabelQueueException("Rejected a BabelQueue record with unsupported "
                + KafkaHeaders.SCHEMA_VERSION + " header '" + declaredVersion + "' (supported: "
                + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded.");
            // A rejected record must never be lost: commit only once the DLQ write is confirmed; otherwise
            // rewind so the record is redelivered (and rejected again) instead of being skipped. Settle
            // first, report afterwards, so a throwing onError cannot bypass the commit/seek protection.
            try {
                deadLetterRawConfirmed(record);
            } catch (Exception dlqFailure) {
                if (dlqFailure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                report(rejected, null, record);
                report(new BabelQueueException("Could not dead-letter a record rejected by the "
                    + KafkaHeaders.SCHEMA_VERSION + " gate; offset not committed, partition rewound to offset "
                    + record.offset() + ".", dlqFailure), null, record);
                return false;
            }
            commit(record);
            report(rejected, null, record);
            return true;
        }

        Envelope envelope;
        try {
            envelope = reconcile(EnvelopeCodec.decode(value(record)), record);
        } catch (RuntimeException decodeError) {
            // Undecodable / poison body — cannot annotate; report and DLQ the raw record.
            report(decodeError, null, record);
            return settle(record, null, () -> deadLetterRawConfirmed(record), "dead-letter an undecodable record");
        }

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Kafka."),
                envelope, record);
            return settle(record, envelope, () -> deadLetter(envelope, record, "poison", null),
                "dead-letter a non-conformant envelope");
        }

        String urn = urn(record, envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            return onUnknownUrn(record, envelope, urn);
        }

        try {
            handler.handle(envelope, record);
            commit(record);
        } catch (Exception error) {
            report(error, envelope, record);
            return settle(record, envelope, () -> retryOrDeadLetter(record, envelope, error),
                "republish a failed record to its retry tier / DLQ");
        }
        return true;
    }

    /**
     * Run a retry/DLQ write that must be <b>confirmed</b> by the broker, then commit. If the write fails nothing is
     * committed: the partition is rewound ({@code seek}) to {@code record}, the failure is reported, and the record is
     * redelivered on the next poll — never lost. Returns {@code false} when rewound (the caller skips the rest of that
     * partition's batch). A {@link BabelQueueException} (misconfiguration, e.g. no producer) is not a write failure and
     * propagates unchanged. Settle first, report afterwards, so a throwing {@code onError} cannot bypass the seek.
     */
    private boolean settle(ConsumerRecord<byte[], byte[]> record, Envelope envelope, ConfirmedWrite write,
                           String action) {
        try {
            write.run();
        } catch (BabelQueueException misconfigured) {
            throw misconfigured;
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            report(new BabelQueueException("Could not " + action + "; offset not committed, partition rewound to offset "
                + record.offset() + ".", failure), envelope, record);
            return false;
        }
        commit(record);
        return true;
    }

    /** A broker-confirmed write that may fail with any checked or unchecked exception. */
    @FunctionalInterface
    private interface ConfirmedWrite {
        void run() throws Exception;
    }

    // -- attempt reconciliation -------------------------------------------------------------

    /** {@code bq-attempts} header is authoritative; the body {@code attempts} is the fallback. */
    private static Envelope reconcile(Envelope envelope, ConsumerRecord<byte[], byte[]> record) {
        int attempts = KafkaHeaders.integer(record.headers(), KafkaHeaders.ATTEMPTS, envelope.attempts());
        return attempts == envelope.attempts() ? envelope : withAttempts(envelope, attempts);
    }

    // -- routing / failure paths ------------------------------------------------------------

    /** Returns {@code false} when the partition was rewound because a DLQ/retry write failed. */
    private boolean onUnknownUrn(ConsumerRecord<byte[], byte[]> record, Envelope envelope, String urn) {
        switch (unknownUrn) {
            case UnknownUrnStrategy.DELETE -> {
                commit(record);
                return true;
            }
            case UnknownUrnStrategy.DEAD_LETTER -> {
                return settle(record, envelope, () -> deadLetter(envelope, record, "unknown_urn", null),
                    "dead-letter a record with an unmapped URN");
            }
            case UnknownUrnStrategy.RELEASE -> {
                return settle(record, envelope, () -> republishRetry(record, envelope),
                    "republish a record with an unmapped URN to its retry tier");
            }
            // FAIL (default): surface and do NOT commit — the record redelivers on the next poll.
            default -> {
                report(new UnknownUrnException(urn), envelope, record);
                throw new UnknownUrnException(urn);
            }
        }
    }

    private void retryOrDeadLetter(ConsumerRecord<byte[], byte[]> record, Envelope envelope, Throwable error)
        throws Exception {
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
    private void republishRetry(ConsumerRecord<byte[], byte[]> record, Envelope envelope) throws Exception {
        if (retryTopics == null || !retryTopics.hasTiers()) {
            throw new BabelQueueException("Kafka retry/release requires retry topics; none are configured.");
        }
        RetryTopics.Tier tier = retryTopics.tierForAttempt(envelope.attempts());
        Envelope bumped = withAttempts(envelope, envelope.attempts() + 1);
        List<Header> headers = KafkaHeaders.of(bumped);
        headers.add(KafkaHeaders.header(KafkaHeaders.DELAY, Long.toString(tier.delay().toMillis())));
        headers.add(KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, originalTopic(record)));
        // The retry-topic consumer waits until record.timestamp + tier, so timestamp = re-entry time.
        send(tier.topic(), record.key(), bumped, headers, System.currentTimeMillis());
    }

    private void deadLetter(Envelope envelope, ConsumerRecord<byte[], byte[]> record, String reason, Throwable error)
        throws Exception {
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
        send(dlq, record.key(), annotated, headers, System.currentTimeMillis());
    }

    /**
     * Forward an undecodable/rejected record's raw bytes to the DLQ (no envelope to annotate) and wait for the broker
     * to acknowledge the write, so the caller can refuse to commit when it failed. No DLQ configured → nothing to
     * confirm (commit-and-drop, as for every DLQ path).
     */
    private void deadLetterRawConfirmed(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String dlq = retryTopics == null ? null : retryTopics.dlqTopic();
        if (dlq == null) {
            return;
        }
        requireProducer();
        producer.send(new ProducerRecord<>(dlq, null, System.currentTimeMillis(), record.key(),
            record.value(), record.headers())).get();
    }

    /**
     * Send and wait for the broker's acknowledgement; a failure surfaces so the caller never commits past it. The
     * original record {@code key} is carried unchanged (a null key stays null, an empty key stays empty, none is
     * synthesised) so a keyed stream keeps its key across retry tiers and the DLQ.
     */
    private void send(String topic, byte[] key, Envelope envelope, List<Header> headers, long timestamp)
        throws Exception {
        requireProducer();
        byte[] value = EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, null, timestamp, key, value, headers)).get();
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

    /** The raw {@code bq-schema-version} header as UTF-8, or {@code null} when absent (or valueless). */
    private static String schemaVersionHeader(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.SCHEMA_VERSION);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * The shared cross-SDK "blank" definition for {@code bq-schema-version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
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
