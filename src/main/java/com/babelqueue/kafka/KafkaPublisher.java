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
 *
 * <p>Out-of-band transport headers (e.g. the W3C {@code traceparent}, ADR-0028) ride as extra UTF-8
 * record headers <b>beside</b> the contract {@code bq-} headers — never inside the envelope (GR-1);
 * see {@link #publishWithHeaders(Envelope, Map)}, the seam for {@code com.babelqueue.otel.HeaderSender}.
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
        return publish(urn, data, traceId, delay, null);
    }

    /**
     * Publish as {@link #publish(String, Map, String, Duration)}, additionally carrying the
     * out-of-band {@code headers} (e.g. {@code traceparent}) as record headers beside the contract
     * {@code bq-} headers. A contract header already present always wins a collision; blank keys or
     * values are skipped; {@code null}/empty {@code headers} is byte-identical to the plain overload.
     */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay,
                          Map<String, String> headers) {
        Envelope envelope = EnvelopeCodec.make(urn, data, workTopic, traceId);
        if (delay != null && !delay.isZero() && !delay.isNegative()) {
            if (retryTopics == null) {
                throw new BabelQueueException(
                    "Kafka has no native delayed delivery; a delay requires retry topics (none configured).");
            }
            RetryTopics.Tier tier = retryTopics.tierForDelay(delay);
            send(tier.topic(), envelope, delay, workTopic, headers);
        } else {
            send(workTopic, envelope, null, null, headers);
        }
        return envelope.meta() != null ? envelope.meta().id() : "";
    }

    /**
     * Publish an already-built {@code envelope} to the work topic together with out-of-band transport
     * {@code headers} (e.g. a W3C {@code traceparent}, ADR-0028). This is the produce-side seam the
     * optional {@code com.babelqueue.otel.HeaderSender} wires to:
     * {@code Tracing.publish(tracer, urn, data, queue, publisher::publishWithHeaders)}. The record
     * value stays the byte-identical envelope JSON. Returns {@code meta.id}.
     */
    public String publishWithHeaders(Envelope envelope, Map<String, String> headers) {
        send(workTopic, envelope, null, null, headers);
        return envelope.meta() != null ? envelope.meta().id() : "";
    }

    private void send(String topic, Envelope envelope, Duration delay, String originalTopic,
                      Map<String, String> outOfBand) {
        List<Header> headers = KafkaHeaders.of(envelope);
        if (delay != null) {
            headers.add(KafkaHeaders.header(KafkaHeaders.DELAY, Long.toString(delay.toMillis())));
        }
        if (originalTopic != null) {
            headers.add(KafkaHeaders.header(KafkaHeaders.ORIGINAL_TOPIC, originalTopic));
        }
        KafkaHeaders.addOutOfBand(headers, outOfBand);
        byte[] value = EnvelopeCodec.encode(envelope).getBytes(StandardCharsets.UTF_8);
        long timestamp = envelope.meta() != null ? envelope.meta().createdAt() : System.currentTimeMillis();
        producer.send(new ProducerRecord<>(topic, null, timestamp, null, value, headers));
    }
}
