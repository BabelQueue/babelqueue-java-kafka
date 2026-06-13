package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
