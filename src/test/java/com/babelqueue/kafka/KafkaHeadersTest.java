package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

/** §6.3 header projection + parsing (no broker): all values are UTF-8 byte strings. */
class KafkaHeadersTest {

    @Test
    void projectsContractHeaders() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 7), "orders", "trace-1");
        List<Header> headers = KafkaHeaders.of(env);
        RecordHeaders rh = new RecordHeaders(headers);

        assertEquals("urn:babel:orders:created", KafkaHeaders.string(rh, KafkaHeaders.JOB));
        assertEquals("trace-1", KafkaHeaders.string(rh, KafkaHeaders.TRACE_ID));
        assertEquals(env.meta().id(), KafkaHeaders.string(rh, KafkaHeaders.MESSAGE_ID));
        assertEquals("1", KafkaHeaders.string(rh, KafkaHeaders.SCHEMA_VERSION));
        assertEquals(env.meta().lang(), KafkaHeaders.string(rh, KafkaHeaders.SOURCE_LANG));
        assertEquals("0", KafkaHeaders.string(rh, KafkaHeaders.ATTEMPTS));
    }

    @Test
    void integerParsesAndFallsBack() {
        RecordHeaders rh = new RecordHeaders();
        rh.add(KafkaHeaders.header(KafkaHeaders.ATTEMPTS, "3"));
        rh.add(KafkaHeaders.header("bq-bad", "not-a-number"));

        assertEquals(3, KafkaHeaders.integer(rh, KafkaHeaders.ATTEMPTS, -1));
        assertEquals(9, KafkaHeaders.integer(rh, "bq-missing", 9));
        assertEquals(0, KafkaHeaders.integer(rh, "bq-bad", 0));
    }

    @Test
    void outOfBandHeadersAreAppendedSortedAndNeverClobberContractHeaders() {
        Envelope env = EnvelopeCodec.make("urn:babel:orders:created", Map.of(), "orders", "trace-1");
        List<Header> headers = KafkaHeaders.of(env);

        KafkaHeaders.addOutOfBand(headers, Map.of(
            "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "tracestate", "a=b",
            KafkaHeaders.JOB, "urn:evil"));

        RecordHeaders rh = new RecordHeaders(headers);
        assertEquals("urn:babel:orders:created", KafkaHeaders.string(rh, KafkaHeaders.JOB));
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", KafkaHeaders.string(rh, "traceparent"));
        assertEquals("a=b", KafkaHeaders.string(rh, "tracestate"));
    }

    @Test
    void toMapSurfacesUtf8ValuesAndDropsBlanks() {
        RecordHeaders rh = new RecordHeaders();
        rh.add(KafkaHeaders.header("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"));
        rh.add(KafkaHeaders.header("blank", ""));

        Map<String, String> map = KafkaHeaders.toMap(rh);

        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", map.get("traceparent"));
        assertTrue(!map.containsKey("blank"));
        assertTrue(KafkaHeaders.toMap(null).isEmpty());
    }

    @Test
    void nullValuedHeadersAreSkippedByToMapAndStringAndTheLastValueWins() {
        RecordHeaders rh = new RecordHeaders();
        rh.add(new org.apache.kafka.common.header.internals.RecordHeader("x-foreign", (byte[]) null));
        rh.add(new org.apache.kafka.common.header.internals.RecordHeader(KafkaHeaders.JOB, (byte[]) null));
        rh.add(KafkaHeaders.header("traceparent", "00-old"));
        rh.add(KafkaHeaders.header("traceparent", "00-new"));

        Map<String, String> map = KafkaHeaders.toMap(rh);

        assertEquals(Map.of("traceparent", "00-new"), map);
        assertEquals(null, KafkaHeaders.string(rh, "x-foreign"));
        assertEquals(null, KafkaHeaders.string(rh, KafkaHeaders.JOB));
    }
}
