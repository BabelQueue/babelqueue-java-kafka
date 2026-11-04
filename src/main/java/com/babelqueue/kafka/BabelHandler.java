package com.babelqueue.kafka;

import com.babelqueue.Envelope;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Processes one decoded, validated envelope and the raw Kafka record it arrived on. Returning
 * normally acknowledges it (the consumer commits the offset past it); throwing routes it to a
 * retry topic with {@code bq-attempts + 1} (or to the DLQ once max-tries is reached) — Kafka's
 * analogue of "republish then ack".
 *
 * <p>The raw record carries the out-of-band transport headers (ADR-0028). {@link #headersOf} surfaces them
 * as a plain map, ready for the optional OpenTelemetry wrapper so the consumer span becomes a true child of
 * the producer span:
 *
 * <pre>{@code
 * KafkaConsumer.builder(consumer)
 *     .handler(urn, (env, record) -> Tracing
 *         .wrapHandler(tracer, h, () -> BabelHandler.headersOf(record))
 *         .handle(env))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface BabelHandler {
    void handle(Envelope envelope, ConsumerRecord<byte[], byte[]> record) throws Exception;

    /**
     * The record's headers as a {@code Map<String, String>} (UTF-8, blank values dropped) — the
     * delivered-headers {@code Supplier} source for {@code com.babelqueue.otel.Tracing#wrapHandler}.
     * Contains the contract {@code bq-} headers and any out-of-band header (e.g. {@code traceparent},
     * {@code tracestate}). Empty when {@code record} is {@code null}. Needs no OpenTelemetry dependency.
     */
    static Map<String, String> headersOf(ConsumerRecord<byte[], byte[]> record) {
        return record == null ? Map.of() : KafkaHeaders.toMap(record.headers());
    }
}
