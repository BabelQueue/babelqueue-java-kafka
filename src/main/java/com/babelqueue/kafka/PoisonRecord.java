package com.babelqueue.kafka;

// KEEP IN SYNC with com.babelqueue.PoisonRecord in babelqueue-java (babelqueue-core): this is a verbatim copy
// (package and visibility aside), because the babelqueue-core release this module depends on predates the class.
// Delete this file and call com.babelqueue.PoisonRecord.build once the dependency is bumped to the release that
// ships it.

import com.babelqueue.JsonValues;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Builds the DLQ record of a poison body (ADR-0035, error-handling.md section 6).
 *
 * <p>A body that is valid UTF-8 and a single strict RFC 8259 JSON object without a top-level {@code dead_letter}
 * member is <b>spliced at byte level</b>: every original character is kept and only the {@code dead_letter} member is
 * inserted, so no value is ever re-encoded (a {@code 1e400} or a 64-bit integer stays as written, a repeated member
 * name is not collapsed). Anything else is wrapped as {@code {"raw": <string>, "raw_encoding"?: "base64",
 * "dead_letter": ...}}: {@code raw} is the body as plain text when it is valid UTF-8, otherwise the standard padded
 * base64 of the exact bytes with {@code "raw_encoding": "base64"}. U+FFFD substitution never happens.
 *
 * <p>The strict scan is iterative (no recursion), so a body nested thousands of levels deep cannot overflow the stack.
 */
final class PoisonRecord {

    private PoisonRecord() {
    }

    /**
     * @param body       the exact record value bytes
     * @param deadLetter the JSON text of the {@code dead_letter} block: exactly one strict JSON object, spliced into
     *                   the record verbatim
     * @return the DLQ record JSON text
     * @throws IllegalArgumentException when {@code deadLetter} is {@code null} or not a single JSON object (a
     *                                  fragment, an array, trailing members, a bare value)
     */
    static String build(byte[] body, String deadLetter) {
        if (deadLetter == null || !new Scanner(deadLetter, false).scan()) {
            throw new IllegalArgumentException("deadLetter must be the JSON text of a single object");
        }
        String text = decodeStrictUtf8(body);
        if (text != null && isSpliceableObject(text)) {
            String trimmed = trimJsonSpace(text);
            String inner = trimJsonSpace(trimmed.substring(1, trimmed.length() - 1));
            if (inner.isEmpty()) {
                return "{\"dead_letter\":" + deadLetter + "}";
            }
            return trimmed.substring(0, trimmed.length() - 1) + ",\"dead_letter\":" + deadLetter + "}";
        }
        StringBuilder out = new StringBuilder("{\"raw\":");
        if (text != null) {
            out.append(JsonValues.encode(text));
        } else {
            out.append(JsonValues.encode(Base64.getEncoder().encodeToString(body)));
            out.append(",\"raw_encoding\":\"base64\"");
        }
        return out.append(",\"dead_letter\":").append(deadLetter).append('}').toString();
    }

    /** The body as text, or {@code null} when it is not valid UTF-8. */
    private static String decodeStrictUtf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException invalid) {
            return null;
        }
    }

    private static boolean isJsonSpace(char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n';
    }

    private static String trimJsonSpace(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && isJsonSpace(s.charAt(start))) {
            start++;
        }
        while (end > start && isJsonSpace(s.charAt(end - 1))) {
            end--;
        }
        return s.substring(start, end);
    }

    /**
     * Whether {@code s} is exactly one strict RFC 8259 JSON object (no NaN/Infinity, no BOM, no trailing data, no
     * unpaired surrogate escape) that has no top-level {@code dead_letter} member.
     */
    static boolean isSpliceableObject(String s) {
        return new Scanner(s, true).scan();
    }

    private static final class Scanner {
        private static final int VALUE = 0;
        private static final int ARRAY_FIRST = 1;
        private static final int OBJECT_FIRST = 2;
        private static final int KEY = 3;
        private static final int AFTER = 4;

        private final String s;
        private final int n;
        private int i;
        private char[] stack = new char[64];
        private int sp;
        private String key;
        private final boolean rejectDeadLetterKey;

        Scanner(String s, boolean rejectDeadLetterKey) {
            this.s = s;
            this.n = s.length();
            this.rejectDeadLetterKey = rejectDeadLetterKey;
        }

        boolean scan() {
            skipSpace();
            if (i >= n || s.charAt(i) != '{') {
                return false;
            }
            int mode = VALUE;
            while (true) {
                skipSpace();
                if (mode == AFTER && sp == 0) {
                    return i == n;
                }
                if (i >= n) {
                    return false;
                }
                char c = s.charAt(i);
                switch (mode) {
                    case ARRAY_FIRST:
                        if (c == ']') {
                            sp--;
                            i++;
                            mode = AFTER;
                            break;
                        }
                        mode = VALUE;
                        continue;
                    case VALUE:
                        if (c == '{') {
                            push('{');
                            i++;
                            mode = OBJECT_FIRST;
                        } else if (c == '[') {
                            push('[');
                            i++;
                            mode = ARRAY_FIRST;
                        } else if (!scalar(c)) {
                            return false;
                        } else {
                            mode = AFTER;
                        }
                        break;
                    case OBJECT_FIRST:
                        if (c == '}') {
                            sp--;
                            i++;
                            mode = AFTER;
                            break;
                        }
                        mode = KEY;
                        continue;
                    case KEY:
                        if (c != '"' || !string(sp == 1)) {
                            return false;
                        }
                        if (rejectDeadLetterKey && sp == 1 && "dead_letter".equals(key)) {
                            return false;
                        }
                        skipSpace();
                        if (i >= n || s.charAt(i) != ':') {
                            return false;
                        }
                        i++;
                        mode = VALUE;
                        break;
                    default: // AFTER inside a container
                        char top = stack[sp - 1];
                        if (c == ',') {
                            i++;
                            mode = top == '{' ? KEY : VALUE;
                        } else if ((c == '}' && top == '{') || (c == ']' && top == '[')) {
                            sp--;
                            i++;
                        } else {
                            return false;
                        }
                        break;
                }
            }
        }

        private void push(char c) {
            if (sp == stack.length) {
                stack = java.util.Arrays.copyOf(stack, sp * 2);
            }
            stack[sp++] = c;
        }

        private void skipSpace() {
            while (i < n && isJsonSpace(s.charAt(i))) {
                i++;
            }
        }

        private boolean scalar(char c) {
            if (c == '"') {
                return string(false);
            }
            if (c == 't') {
                return literal("true");
            }
            if (c == 'f') {
                return literal("false");
            }
            if (c == 'n') {
                return literal("null");
            }
            return number();
        }

        private boolean literal(String word) {
            if (!s.startsWith(word, i)) {
                return false;
            }
            i += word.length();
            return true;
        }

        private boolean number() {
            int j = i;
            if (j < n && s.charAt(j) == '-') {
                j++;
            }
            if (j >= n) {
                return false;
            }
            if (s.charAt(j) == '0') {
                j++;
            } else if (s.charAt(j) >= '1' && s.charAt(j) <= '9') {
                j = digits(j);
            } else {
                return false;
            }
            if (j < n && s.charAt(j) == '.') {
                int next = digits(j + 1);
                if (next == j + 1) {
                    return false;
                }
                j = next;
            }
            if (j < n && (s.charAt(j) == 'e' || s.charAt(j) == 'E')) {
                int k = j + 1;
                if (k < n && (s.charAt(k) == '+' || s.charAt(k) == '-')) {
                    k++;
                }
                int next = digits(k);
                if (next == k) {
                    return false;
                }
                j = next;
            }
            i = j;
            return true;
        }

        private int digits(int from) {
            int j = from;
            while (j < n && s.charAt(j) >= '0' && s.charAt(j) <= '9') {
                j++;
            }
            return j;
        }

        /** Scan the string literal at {@code i}; with {@code capture} its decoded value lands in {@link #key}. */
        private boolean string(boolean capture) {
            StringBuilder decoded = capture ? new StringBuilder() : null;
            int j = i + 1;
            while (j < n) {
                char c = s.charAt(j);
                if (c == '"') {
                    i = j + 1;
                    key = capture ? decoded.toString() : null;
                    return true;
                }
                if (c < 0x20) {
                    return false;
                }
                if (c != '\\') {
                    if (capture) {
                        decoded.append(c);
                    }
                    j++;
                    continue;
                }
                if (j + 1 >= n) {
                    return false;
                }
                char e = s.charAt(j + 1);
                char simple;
                switch (e) {
                    case '"': simple = '"'; break;
                    case '\\': simple = '\\'; break;
                    case '/': simple = '/'; break;
                    case 'b': simple = '\b'; break;
                    case 'f': simple = '\f'; break;
                    case 'n': simple = '\n'; break;
                    case 'r': simple = '\r'; break;
                    case 't': simple = '\t'; break;
                    case 'u': simple = 0; break;
                    default: return false;
                }
                if (e != 'u') {
                    if (capture) {
                        decoded.append(simple);
                    }
                    j += 2;
                    continue;
                }
                int cp = hex4(j + 2);
                if (cp < 0) {
                    return false;
                }
                j += 6;
                if (cp >= 0xD800 && cp <= 0xDBFF) {
                    if (j + 1 >= n || s.charAt(j) != '\\' || s.charAt(j + 1) != 'u') {
                        return false;
                    }
                    int low = hex4(j + 2);
                    if (low < 0xDC00 || low > 0xDFFF) {
                        return false;
                    }
                    j += 6;
                    if (capture) {
                        decoded.append((char) cp).append((char) low);
                    }
                } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                    return false;
                } else if (capture) {
                    decoded.append((char) cp);
                }
            }
            return false;
        }

        private int hex4(int at) {
            if (at + 4 > n) {
                return -1;
            }
            int cp = 0;
            for (int k = at; k < at + 4; k++) {
                int d = Character.digit(s.charAt(k), 16);
                if (d < 0 || s.charAt(k) > 'f') {
                    return -1;
                }
                cp = (cp << 4) | d;
            }
            return cp;
        }
    }
}
