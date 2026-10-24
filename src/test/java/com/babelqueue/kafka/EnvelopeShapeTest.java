package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.BabelQueueException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/**
 * Direct accept/reject table for {@link EnvelopeShape} (the poison trigger set, broker-bindings.md section 6.5):
 * the JSON lexer paths (numbers, string escapes, literals, nesting) that the shared conformance vectors exercise only
 * partially.
 */
class EnvelopeShapeTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "{}",
        "null",
        "  {\"trace_id\":null,\"attempts\":null,\"meta\":null,\"data\":null,\"dead_letter\":null}  ",
        "{\"unknown\":[]}",
        "{\"unknown\":{\"a\":[1,{\"b\":null}]}}",
        "{\"data\":{\"n\":0,\"m\":-0.5E-2,\"k\":12.5e+3,\"z\":-7,\"t\":true,\"f\":false,\"u\":null}}",
        "{\"data\":{\"s\":\"q\\\" \\\\ \\/ \\b \\f \\n \\r \\t \\u00e9 \\u0041\",\"a\":[1,[2],[]],\"o\":{}}}",
        "{\"meta\":{\"id\":\"x\",\"queue\":null,\"lang\":\"go\",\"schema_version\":1,\"created_at\":1700000000}}",
        "{\"dead_letter\":{\"reason\":\"r\",\"failed_at\":1,\"attempts\":null,\"original_queue\":\"q\"}}",
        "{\"meta\":{\"unknown\":[1,2,3]}}",
    })
    void acceptsWellFormedTypedBodies(String body) {
        assertNull(EnvelopeShape.violation(body), body);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "   ",
        "[1]",
        "7",
        "{} trailing",
        "{",
        "{\"a\"",
        "{1:2}",
        "{\"a\" 1}",
        "{\"a\":1 \"b\":2}",
        "{\"a\":}",
        "{\"unknown\":?}",
        "{\"unknown\":tru}",
        "{\"unknown\":nul}",
        "{\"data\":{\"n\":01}}",
        "{\"data\":{\"n\":-}}",
        "{\"data\":{\"n\":1.}}",
        "{\"data\":{\"n\":1e}}",
        "{\"data\":{\"n\":1e+}}",
        "{\"data\":{\"n\":1e999}}",
        "{\"data\":{\"a\":[1 2]}}",
        "{\"data\":{\"a\":[1,2}}}",
        "{\"data\":{\"a\":{\"b\":1 \"c\":2}}}",
        "{\"data\":{\"a\":{1:2}}}",
        "{\"data\":[1]}",
        "{\"data\":\"x\"}",
        "{\"meta\":[1]}",
        "{\"meta\":{\"id\":1}}",
        "{\"meta\":{\"schema_version\":\"1\"}}",
        "{\"meta\":{\"created_at\":1.5}}",
        "{\"dead_letter\":{\"attempts\":1e2}}",
        "{\"attempts\":99999999999999999999}",
        "{\"attempts\":\"x\"}",
        "{\"attempts\":1.5}",
        "{\"trace_id\":5}",
        "{\"unknown\":\"abc",
        "{\"unknown\":\"\\",
        "{\"unknown\":\"\\q\"}",
        "{\"unknown\":\"\\u12\"}",
        "{\"unknown\":\"\\u12zz\"}",
        "{\"unknown\":\"\\u00",
        "{\"unknown\":\"a\tb\"}",
    })
    void rejectsMalformedOrMistypedBodies(String body) {
        BabelQueueException violation = EnvelopeShape.violation(body);
        assertNotNull(violation, body);
        assertTrue(violation.getMessage().startsWith("Undecodable BabelQueue body: "), violation.getMessage());
    }

    @Test
    void rejectsNestingBeyondTheLimitWithoutThrowing() {
        String body = "{\"data\":" + "[".repeat(20_000) + "]".repeat(20_000) + "}";
        assertNotNull(EnvelopeShape.violation(body));
    }
}
