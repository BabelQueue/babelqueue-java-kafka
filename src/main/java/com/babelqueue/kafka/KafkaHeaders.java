package com.babelqueue.kafka;

import com.babelqueue.Envelope;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Projects the envelope's contract fields onto native Kafka record headers and reads them
 * back. All values are UTF-8 byte strings (Kafka headers are bytes); the conceptually-integer
 * headers ({@code bq-attempts}, {@code bq-schema-version}) are written as their decimal string
 * (e.g. {@code "1"}) and parsed back to ints. The body stays authoritative (Contract §6.3).
 */
final class KafkaHeaders {

    static final String JOB = "bq-job";
    static final String TRACE_ID = "bq-trace-id";
    static final String MESSAGE_ID = "bq-message-id";
    static final String SCHEMA_VERSION = "bq-schema-version";
    static final String SOURCE_LANG = "bq-source-lang";
    static final String ATTEMPTS = "bq-attempts";
    static final String DELAY = "bq-delay";
    static final String ORIGINAL_TOPIC = "bq-original-topic";

    private KafkaHeaders() {}

    /** The mandatory {@code bq-} header set projected from the envelope (Contract §6.3). */
    static List<Header> of(Envelope envelope) {
        List<Header> headers = new ArrayList<>(6);
        put(headers, JOB, envelope.job());
        put(headers, TRACE_ID, envelope.traceId());
        if (envelope.meta() != null) {
            put(headers, MESSAGE_ID, envelope.meta().id());
            if (envelope.meta().schemaVersion() > 0) {
                // 0 means "absent" (a lenient decode of a body without meta.schema_version); never project it.
                put(headers, SCHEMA_VERSION, Integer.toString(envelope.meta().schemaVersion()));
            }
            put(headers, SOURCE_LANG, envelope.meta().lang());
        }
        put(headers, ATTEMPTS, Integer.toString(envelope.attempts()));
        return headers;
    }

    static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Appends the out-of-band {@code extra} headers (e.g. {@code traceparent}, ADR-0028) after the contract
     * headers already in {@code headers}. Blank keys/values are skipped, a key already present always wins
     * (the contract is never clobbered), and keys are added in sorted order so the result is deterministic.
     * A {@code null}/empty map leaves {@code headers} untouched.
     */
    static void addOutOfBand(List<Header> headers, Map<String, String> extra) {
        if (extra == null || extra.isEmpty()) {
            return;
        }
        List<String> keys = new ArrayList<>(extra.keySet());
        keys.removeIf(k -> k == null || k.isEmpty());
        keys.sort(String::compareTo);
        for (String key : keys) {
            String value = extra.get(key);
            if (value == null || value.isEmpty() || containsKey(headers, key)) {
                continue;
            }
            headers.add(header(key, value));
        }
    }

    private static boolean containsKey(List<Header> headers, String key) {
        for (Header h : headers) {
            if (h.key().equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A delivered record's headers as a flat {@code Map<String, String>} (UTF-8, last value wins per key,
     * blank values dropped) — the consume-side counterpart of {@link #addOutOfBand}. Both the contract
     * {@code bq-} headers and out-of-band ones (e.g. {@code traceparent}) surface; the consumer picks the keys
     * it needs. An empty map when the record is {@code null} or carries no headers.
     */
    static Map<String, String> toMap(Headers headers) {
        Map<String, String> out = new LinkedHashMap<>();
        if (headers == null) {
            return out;
        }
        for (Header h : headers) {
            // A Kafka header value may be null (protocol length -1): skip it. Iteration order makes the last value win.
            if (h.value() == null || h.value().length == 0) {
                continue;
            }
            out.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static void put(List<Header> headers, String key, String value) {
        if (value != null && !value.isEmpty()) {
            headers.add(header(key, value));
        }
    }

    /** The last header value for {@code key} as a UTF-8 string, or {@code null} if absent. */
    static String string(Headers headers, String key) {
        Header h = headers.lastHeader(key);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** The header value for {@code key} as an int, or {@code fallback} if absent / unparseable. */
    static int integer(Headers headers, String key, int fallback) {
        String s = string(headers, key);
        if (s == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
