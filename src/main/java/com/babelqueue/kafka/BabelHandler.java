package com.babelqueue.kafka;

import com.babelqueue.Envelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Processes one decoded, validated envelope and the raw Kafka record it arrived on. Returning
 * normally acknowledges it (the consumer commits the offset past it); throwing routes it to a
 * retry topic with {@code bq-attempts + 1} (or to the DLQ once max-tries is reached) — Kafka's
 * analogue of "republish then ack".
 */
@FunctionalInterface
public interface BabelHandler {
    void handle(Envelope envelope, ConsumerRecord<byte[], byte[]> record) throws Exception;
}
