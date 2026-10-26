package com.babelqueue.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.PoisonRecord;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * The shared {@code poison_dlq} conformance section (ADR-0035): the DLQ record written for a poison body is a byte-level
 * splice for a strict JSON object, otherwise the {@code raw} wrapper (base64 plus {@code raw_encoding} for bytes that are
 * not valid UTF-8, never U+FFFD substitution).
 */
class PoisonDlqConformanceTest {

    private static final String DEAD_LETTER = "{\"reason\":\"poison\",\"original_queue\":\"orders\"}";
    private static final String SPLICE_TAIL = ",\"dead_letter\":" + DEAD_LETTER + "}";

    private static JSONObject section() throws IOException {
        try (InputStream in = PoisonDlqConformanceTest.class.getResourceAsStream("/conformance/manifest.json")) {
            assertNotNull(in, "conformance manifest");
            JSONObject manifest = new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            return manifest.getJSONObject("poison_dlq");
        }
    }

    private static byte[] bodyOf(JSONObject c) {
        if (c.has("body_base64")) {
            return Base64.getDecoder().decode(c.getString("body_base64"));
        }
        return c.getString("body").getBytes(StandardCharsets.UTF_8);
    }

    private static String trimJsonSpace(String s) {
        int a = 0;
        int b = s.length();
        while (a < b && " \t\r\n".indexOf(s.charAt(a)) >= 0) {
            a++;
        }
        while (b > a && " \t\r\n".indexOf(s.charAt(b - 1)) >= 0) {
            b--;
        }
        return s.substring(a, b);
    }

    @Test
    void everyPoisonDlqCaseProducesTheNormativeRecord() throws IOException {
        JSONObject section = section();
        JSONArray cases = section.getJSONArray("cases");
        assertEquals(43, cases.length());
        for (int i = 0; i < cases.length(); i++) {
            JSONObject c = cases.getJSONObject(i);
            String name = c.getString("name");
            JSONObject expect = c.getJSONObject("expect");
            String shape = expect.getString("shape");
            byte[] body = bodyOf(c);
            String record = PoisonRecord.build(body, DEAD_LETTER);
            boolean wrapper = record.startsWith("{\"raw\":");

            switch (shape) {
                case "wrapper" -> assertTrue(wrapper, name + ": expected the wrapper, got " + record);
                case "splice" -> assertFalse(wrapper && !expect.getJSONObject("record").has("raw"),
                    name + ": expected a splice, got " + record);
                case "splice_or_wrapper" -> { }
                default -> throw new AssertionError(name + ": unknown shape " + shape);
            }

            if (wrapper) {
                if (shape.equals("splice_or_wrapper")) {
                    // The wrapper is the legal fallback: raw is the exact original text.
                    JSONObject parsed = new JSONObject(record);
                    assertEquals(expect.getJSONObject("record").getString("raw"), parsed.getString("raw"), name);
                    continue;
                }
                JSONObject parsed = new JSONObject(record);
                parsed.remove("dead_letter");
                assertTrue(expect.getJSONObject("record").similar(parsed), name + ": " + record);
                assertEquals("poison", new JSONObject(record).getJSONObject("dead_letter").getString("reason"), name);
                continue;
            }

            // Splice: the original bytes (outer whitespace trimmed) plus the dead_letter member, nothing re-encoded.
            String text = trimJsonSpace(new String(body, StandardCharsets.UTF_8));
            String inner = trimJsonSpace(text.substring(1, text.length() - 1));
            String expected = inner.isEmpty()
                ? "{" + SPLICE_TAIL.substring(1)
                : text.substring(0, text.length() - 1) + SPLICE_TAIL;
            assertEquals(expected, record, name);
            if (expect.has("splice_literals")) {
                JSONArray literals = expect.getJSONArray("splice_literals");
                for (int j = 0; j < literals.length(); j++) {
                    assertTrue(record.contains(literals.getString(j)), name + ": missing literal " + literals.getString(j));
                }
            } else {
                JSONObject parsed = new JSONObject(record);
                parsed.remove("dead_letter");
                assertTrue(expect.getJSONObject("record").similar(parsed), name + ": " + record);
            }
        }
    }

    @Test
    void rejectsADeadLetterThatIsNotASingleJsonObject() {
        byte[] body = {'{', '}'};
        for (String bad : new String[] {null, "", "[]", "1", "\"x\"", "null", "{", "{} {}", "{\"a\":1},\"b\":2", "{\"a\":1}x"}) {
            assertThrows(IllegalArgumentException.class, () -> PoisonRecord.build(body, bad), String.valueOf(bad));
        }
        assertEquals("{\"dead_letter\":{\"dead_letter\":1}}", PoisonRecord.build(body, "{\"dead_letter\":1}"));
    }
}
