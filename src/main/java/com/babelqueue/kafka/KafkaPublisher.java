package com.babelqueue.kafka;

import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Sends canonical-envelope messages to one Kafka work topic with the §6 projection: the record
 * value is the envelope JSON, the record timestamp mirrors {@code meta.created_at} (Unix ms),
 * and the contract fields are mirrored onto {@code bq-} headers so a consumer can route on
 * {@code bq-job} without decoding the body. The envelope is unchanged ({@code schema_version}
 * stays 1); Kafka is purely additive.
 *
 * <p>Kafka has no native delayed delivery: a positive delay requires a {@link RetryTopics}
 * topology ({@link #create(Producer, RetryTopics)}) and is routed to the matching
 * {@code <topic>.retry.<n>} tier; on a plain publisher a delay raises {@link BabelQueueException}
 * rather than being silently dropped (§6.4).
 */
public final class KafkaPublisher {

    private final Producer<byte[], byte[]> producer;
    private final String workTopic;
    private final RetryTopics retryTopics;

    private KafkaPublisher(Producer<byte[], byte[]> producer, String workTopic, RetryTopics retryTopics) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.workTopic = Objects.requireNonNull(workTopic, "workTopic");
        this.retryTopics = retryTopics;
    }

    /** A publisher onto {@code topic} with no retry topics (a delay raises). */
    public static KafkaPublisher create(Producer<byte[], byte[]> producer, String topic) {
        return new KafkaPublisher(producer, topic, null);
    }

    /** A publisher onto the topology's work topic, with delay routed via its retry tiers. */
    public static KafkaPublisher create(Producer<byte[], byte[]> producer, RetryTopics retryTopics) {
        Objects.requireNonNull(retryTopics, "retryTopics");
        return new KafkaPublisher(producer, retryTopics.workTopic(), retryTopics);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) {
        return publish(urn, data, null, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) {
        return publish(urn, data, traceId, null);
    }

    /**
     * Publish with an optional relative {@code delay}. With a {@link RetryTopics} topology the
     * record is routed to the matching retry tier ({@code bq-delay} + {@code bq-original-topic}
     * set); on a plain publisher a positive delay raises {@link BabelQueueException}.
     */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay) {
        Envelope envelope = EnvelopeCodec.make(urn, data, workTopic, traceId);
        if (delay != null && !delay.isZero() && !delay.isNegative()) {
            if (retryTopics == null) {
                throw new BabelQueueException(
                    "Kafka has no native delayed delivery; a delay requires retry topics (none configured).");
            }
            RetryTopics.Tier tier = retryTopics.tierForDelay(delay);
            send(tier.topic(), envelope, delay, workTopic);
        } else {
            send(workTopic, envelope, null, null);
        }
        return envelope.meta() != null ? envelope.meta().id() : "";
    }

    private void send(String topic, Envelope envelope, Duration delay, String originalTopic) {
        List<Header> headers = KafkaHeaders.of(envelope);
        if (delay != null) {
            headers.add(KafkaHeaders.header(KafkaHeaders.DELAY, Long.toString(delay.toMillis())));
        }
        if (originalTopic != null) {
            headers.add(KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, originalTopic));
        }
        byte[] value = EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8);
        long timestamp = envelope.meta() != null ? envelope.meta().createdAt() : System.currentTimeMillis();
        producer.send(new ProducerRecord<>(topic, null, timestamp, null, value, headers));
    }
}
