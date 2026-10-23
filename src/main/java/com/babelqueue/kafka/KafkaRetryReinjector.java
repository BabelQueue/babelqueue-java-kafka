package com.babelqueue.kafka;

import com.babelqueue.BabelQueueException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * The retry-tier re-inject consumer (ADR-0034 §6.4, decisions 5c / A6 / A7): it drains every
 * {@code <topic>.retry.<n>} topic, waits until each record is due, and puts the record back on the work
 * topic so the work consumer retries it.
 *
 * <p><b>What it does to a record</b> (and nothing else):
 * <ul>
 *   <li><b>Forwarded verbatim.</b> The {@code value} and the {@code key} are written to the work topic
 *       byte-for-byte. The body is never decoded or re-encoded, {@code attempts} is not touched, and a
 *       record whose body is undecodable is forwarded as-is — judging it is the work consumer's job.</li>
 *   <li><b>Headers (A7).</b> {@code bq-delay} and {@code bq-original-topic} are dropped (they are routing
 *       state of the retry hop); every other header — {@code traceparent} included — is kept, in order.</li>
 *   <li><b>Target topic.</b> The {@code bq-original-topic} header when present, otherwise the work topic
 *       derived from the retry topic's name.</li>
 * </ul>
 *
 * <p><b>When it forwards (A6).</b> A record is due at {@code record.timestamp + tier_ms}, where {@code tier_ms}
 * is the delay of the tier topic it was read from, and the wait is measured from the <em>record timestamp</em>
 * (the moment the failed record entered the tier) — not from consumption. The due time is clamped to
 * {@code [now, now + tier_ms]}: a record older than its tier is forwarded at once, and a timestamp in the future
 * (clock skew) never delays a record by more than one tier. Operators SHOULD set
 * {@code message.timestamp.type=LogAppendTime} on the retry topics so the timestamp is the broker's, not a
 * producer's clock.
 *
 * <p><b>How it waits.</b> By partition {@code pause}/{@code resume}, never by blocking the poll loop: a record
 * that is not yet due is left unconsumed (the partition is rewound to it with {@code seek} and paused until its
 * due time), so other partitions keep flowing, the consumer keeps heart-beating, and a rebalance loses nothing.
 *
 * <p><b>At-least-once.</b> The offset is committed only after the broker acknowledged the work-topic write; a
 * failed write rewinds the partition and surfaces the exception, so the record is never lost (it may be
 * forwarded twice after a crash between the ack and the commit — the work consumer is idempotent by
 * {@code meta.id}).
 *
 * <p>The caller owns the {@link Consumer} and its configuration ({@code enable.auto.commit=false}); call
 * {@link #subscribe()} (or subscribe it yourself to {@link RetryTopics#tiers()}). Not thread-safe.
 */
public final class KafkaRetryReinjector {

    private static final Pattern RETRY_SUFFIX = Pattern.compile("\\.retry\\.\\d+$");

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;
    private final RetryTopics retryTopics;
    private final Map<String, Long> tierMillisByTopic;
    private final Duration pollTimeout;
    private final LongSupplier clock;
    /** Partitions paused until the given epoch-ms because their head record is not yet due. */
    private final Map<TopicPartition, Long> pausedUntil = new HashMap<>();
    /**
     * The due time fixed the first time a head record was parked, keyed by partition ({@code {offset, dueAt}}). A
     * record re-read after the resume keeps it: recomputing against a later {@code now} would let a future timestamp
     * (clock skew) slide the clamp forward and wait forever.
     */
    private final Map<TopicPartition, long[]> fixedDue = new HashMap<>();

    private KafkaRetryReinjector(Builder builder) {
        this.consumer = builder.consumer;
        this.producer = builder.producer;
        this.retryTopics = builder.retryTopics;
        this.pollTimeout = builder.pollTimeout;
        this.clock = builder.clock;
        Map<String, Long> tiers = new HashMap<>();
        for (RetryTopics.Tier tier : retryTopics.tiers()) {
            tiers.put(tier.topic(), tier.delay().toMillis());
        }
        this.tierMillisByTopic = Map.copyOf(tiers);
    }

    public static Builder builder(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer,
                                  RetryTopics retryTopics) {
        return new Builder(consumer, producer, retryTopics);
    }

    /** Subscribe the consumer to every {@code <topic>.retry.<n>} of the topology. */
    public void subscribe() {
        List<String> topics = new ArrayList<>(retryTopics.tiers().size());
        for (RetryTopics.Tier tier : retryTopics.tiers()) {
            topics.add(tier.topic());
        }
        consumer.subscribe(topics);
    }

    /**
     * One pass: resume partitions that became due, poll, forward every due record and park the ones that are
     * not. Returns the number of records forwarded to the work topic.
     */
    public int poll() {
        long now = clock.getAsLong();
        resumeDuePartitions(now);
        ConsumerRecords<byte[], byte[]> records = consumer.poll(boundedPollTimeout(now));
        long afterPoll = clock.getAsLong();

        int forwarded = 0;
        List<TopicPartition> partitions = new ArrayList<>(records.partitions());
        for (int p = 0; p < partitions.size(); p++) {
            TopicPartition tp = partitions.get(p);
            List<ConsumerRecord<byte[], byte[]>> batch = records.records(tp);
            for (int i = 0; i < batch.size(); i++) {
                ConsumerRecord<byte[], byte[]> record = batch.get(i);
                long[] fixed = fixedDue.get(tp);
                long dueAt = fixed != null && fixed[0] == record.offset() ? fixed[1] : dueAt(record, afterPoll);
                if (dueAt > afterPoll) {
                    // Not due yet: leave it (and the rest of the batch) unconsumed and park the partition.
                    consumer.seek(tp, record.offset());
                    consumer.pause(Set.of(tp));
                    pausedUntil.put(tp, dueAt);
                    fixedDue.put(tp, new long[] {record.offset(), dueAt});
                    break;
                }
                try {
                    forward(record, afterPoll);
                    fixedDue.remove(tp);
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(record.offset() + 1)));
                    forwarded++;
                } catch (RuntimeException | Error e) {
                    rewindFrom(tp, record, partitions.subList(p + 1, partitions.size()), records);
                    throw e;
                } catch (Exception e) {
                    rewindFrom(tp, record, partitions.subList(p + 1, partitions.size()), records);
                    throw new BabelQueueException(
                        "Could not re-inject a record from " + record.topic() + " to its work topic.", e);
                }
            }
        }
        return forwarded;
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    // -- internals --------------------------------------------------------------------------

    /** Epoch-ms at which {@code record} may be forwarded: {@code timestamp + tier_ms} clamped to {@code [now, now + tier_ms]}. */
    private long dueAt(ConsumerRecord<byte[], byte[]> record, long now) {
        long tierMillis = tierMillis(record);
        if (tierMillis <= 0 || record.timestamp() < 0) {
            return now;
        }
        long target = record.timestamp() + tierMillis;
        return Math.min(Math.max(target, now), now + tierMillis);
    }

    /** The tier delay of the topic the record was read from; the {@code bq-delay} header only for a topic the topology does not know. */
    private long tierMillis(ConsumerRecord<byte[], byte[]> record) {
        Long configured = tierMillisByTopic.get(record.topic());
        if (configured != null) {
            return configured;
        }
        String header = KafkaHeaders.string(record.headers(), KafkaHeaders.DELAY);
        if (header == null) {
            return 0L;
        }
        try {
            return Long.parseLong(header.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private void forward(ConsumerRecord<byte[], byte[]> record, long now) throws Exception {
        List<Header> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            String key = header.key();
            if (KafkaHeaders.DELAY.equals(key) || KafkaHeaders.ORIGINAL_TOPIC.equals(key)) {
                continue;
            }
            headers.add(new RecordHeader(key, header.value()));
        }
        producer.send(new ProducerRecord<>(targetTopic(record), null, now, record.key(), record.value(), headers))
            .get();
    }

    private String targetTopic(ConsumerRecord<byte[], byte[]> record) {
        Header original = record.headers().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
        if (original != null && original.value() != null && original.value().length > 0) {
            return new String(original.value(), StandardCharsets.UTF_8);
        }
        String derived = RETRY_SUFFIX.matcher(record.topic()).replaceFirst("");
        return derived.equals(record.topic()) ? retryTopics.workTopic() : derived;
    }

    private void resumeDuePartitions(long now) {
        if (pausedUntil.isEmpty()) {
            return;
        }
        Set<TopicPartition> assigned = consumer.assignment();
        Iterator<Map.Entry<TopicPartition, Long>> it = pausedUntil.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = it.next();
            TopicPartition tp = entry.getKey();
            if (!assigned.contains(tp)) {
                it.remove(); // revoked in a rebalance: nothing to resume, the new owner starts fresh
                fixedDue.remove(tp);
            } else if (entry.getValue() <= now) {
                consumer.resume(Set.of(tp));
                it.remove();
            }
        }
    }

    /** Never sleep past the earliest due time: poll for {@code min(pollTimeout, time to next due)}. */
    private Duration boundedPollTimeout(long now) {
        long soonest = Long.MAX_VALUE;
        for (long due : pausedUntil.values()) {
            soonest = Math.min(soonest, due);
        }
        if (soonest == Long.MAX_VALUE) {
            return pollTimeout;
        }
        long wait = Math.max(soonest - now, 0L);
        return wait < pollTimeout.toMillis() ? Duration.ofMillis(wait) : pollTimeout;
    }

    /** After a failed write: rewind the failing partition and every not-yet-handled one so nothing is skipped. */
    private void rewindFrom(TopicPartition failed, ConsumerRecord<byte[], byte[]> record,
                            List<TopicPartition> later, ConsumerRecords<byte[], byte[]> records) {
        consumer.seek(failed, record.offset());
        for (TopicPartition tp : later) {
            List<ConsumerRecord<byte[], byte[]>> batch = records.records(tp);
            if (!batch.isEmpty()) {
                consumer.seek(tp, batch.get(0).offset());
            }
        }
    }

    /** Fluent builder for {@link KafkaRetryReinjector}. */
    public static final class Builder {
        private final Consumer<byte[], byte[]> consumer;
        private final Producer<byte[], byte[]> producer;
        private final RetryTopics retryTopics;
        private Duration pollTimeout = Duration.ofMillis(1000);
        private LongSupplier clock = System::currentTimeMillis;

        private Builder(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer,
                        RetryTopics retryTopics) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
            this.producer = Objects.requireNonNull(producer, "producer");
            this.retryTopics = Objects.requireNonNull(retryTopics, "retryTopics");
            if (!retryTopics.hasTiers()) {
                throw new BabelQueueException(
                    "The Kafka re-inject consumer needs retry topics; none are configured for '"
                        + retryTopics.workTopic() + "'.");
            }
        }

        /** Upper bound of one {@code consumer.poll} (default 1 s); shortened to the next due time while partitions wait. */
        public Builder pollTimeout(Duration pollTimeout) {
            this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
            return this;
        }

        /** Epoch-millisecond clock (default {@code System::currentTimeMillis}); a seam for tests. */
        Builder clock(LongSupplier clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public KafkaRetryReinjector build() {
            return new KafkaRetryReinjector(this);
        }
    }
}
