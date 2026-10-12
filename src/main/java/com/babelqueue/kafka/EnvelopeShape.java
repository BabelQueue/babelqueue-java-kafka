package com.babelqueue.kafka;

import com.babelqueue.BabelQueueException;
import java.util.Map;

/**
 * Go's typed envelope decode, reduced to its accept/reject decision: whether a body would make
 * {@code babelqueue.Decode} fail (the poison trigger set, contract §6.5).
 *
 * <p>The reference Go decoder unmarshals into typed {@code Envelope}/{@code Meta}/{@code DeadLetter} structs, so a body
 * is undecodable when it is not exactly one well-formed JSON object (a bare {@code null} is accepted as an empty
 * envelope), or when a typed member has the wrong JSON type:
 * <ul>
 *   <li>{@code job}, {@code trace_id}, {@code meta.id|queue|lang}, {@code dead_letter.reason|error|exception|original_queue|lang}
 *       — a string or {@code null};</li>
 *   <li>{@code meta.schema_version|created_at}, {@code attempts}, {@code dead_letter.failed_at|attempts} — an integer
 *       <b>literal</b> (no fraction, no exponent, no overflow of a signed 64-bit value) or {@code null};</li>
 *   <li>{@code data}, {@code meta}, {@code dead_letter} — an object or {@code null}; every number inside {@code data}
 *       must be representable as a finite double.</li>
 * </ul>
 * Member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read them;
 * this is the one deliberate difference from Go's {@code encoding/json}) and every occurrence of a duplicated member
 * is checked. Unknown members, a differently-cased name included, are only checked for well-formedness; a non-string {@code urn} alias is
 * ignored. Nothing else is poison — in particular a missing {@code data}, a blank {@code trace_id}, a
 * {@code schema_version} other than 1 or an empty {@code job} is routed like Go routes it (the conformance check is
 * not applied on the consume path).
 */
final class EnvelopeShape {

    /**
     * Go's {@code encoding/json} nesting limit. The scanner is recursive, so a thread with a small stack overflows
     * earlier; that {@code StackOverflowError} is caught in {@link #violation} and treated as poison.
     */
    private static final int MAX_DEPTH = 10_000;

    private enum Kind { STRING, INTEGER, OBJECT, DATA }

    private record Member(Kind kind, Map<String, Kind> members) {}

    private static final Map<String, Kind> META = Map.of(
        "id", Kind.STRING, "queue", Kind.STRING, "lang", Kind.STRING,
        "schema_version", Kind.INTEGER, "created_at", Kind.INTEGER);

    private static final Map<String, Kind> DEAD_LETTER = Map.of(
        "reason", Kind.STRING, "error", Kind.STRING, "exception", Kind.STRING,
        "failed_at", Kind.INTEGER, "original_queue", Kind.STRING, "attempts", Kind.INTEGER, "lang", Kind.STRING);

    private static final Map<String, Member> TOP = Map.of(
        "job", new Member(Kind.STRING, null),
        "trace_id", new Member(Kind.STRING, null),
        "attempts", new Member(Kind.INTEGER, null),
        "data", new Member(Kind.DATA, null),
        "meta", new Member(Kind.OBJECT, META),
        "dead_letter", new Member(Kind.OBJECT, DEAD_LETTER));

    private final String s;
    private int i;
    private int depth;

    private EnvelopeShape(String s) {
        this.s = s;
    }

    /** The reason {@code body} is undecodable, or {@code null} when Go's typed decode would accept it. */
    static BabelQueueException violation(String body) {
        try {
            new EnvelopeShape(body).parse();
            return null;
        } catch (Reject reject) {
            return new BabelQueueException("Undecodable BabelQueue body: " + reject.getMessage());
        } catch (StackOverflowError tooDeep) {
            // The generic value scan is recursive: nesting deeper than the thread stack carries (a few thousand
            // levels) is poison here, whereas Go allows up to 10000. An Error must never escape into poll().
            return new BabelQueueException("Undecodable BabelQueue body: nested too deeply to scan.");
        }
    }

    private static final class Reject extends RuntimeException {
        Reject(String message) {
            super(message, null, false, false);
        }
    }

    private void parse() {
        skipWhitespace();
        if (i >= s.length()) {
            throw new Reject("empty body.");
        }
        char c = s.charAt(i);
        if (c == 'n') {
            literal("null");
        } else if (c == '{') {
            object(TOP, null);
        } else {
            value(false);
            throw new Reject("expected a JSON object.");
        }
        skipWhitespace();
        if (i < s.length()) {
            throw new Reject("trailing content after the JSON value at index " + i + ".");
        }
    }

    // -- typed object ------------------------------------------------------------------------

    /** Parse an object; {@code top} (top level) or {@code flat} (meta / dead_letter) says which members are typed. */
    private void object(Map<String, Member> top, Map<String, Kind> flat) {
        enter();
        i++; // '{'
        skipWhitespace();
        if (peek() == '}') {
            i++;
            depth--;
            return;
        }
        while (true) {
            skipWhitespace();
            if (peek() != '"') {
                throw new Reject("expected an object key at index " + i + ".");
            }
            String key = string();
            skipWhitespace();
            expect(':');
            skipWhitespace();
            if (top != null) {
                Member member = lookup(top, key);
                if (member == null) {
                    value(false);
                } else {
                    typed(key, member.kind, member.members);
                }
            } else {
                Kind kind = lookup(flat, key);
                if (kind == null) {
                    value(false);
                } else {
                    typed(key, kind, null);
                }
            }
            skipWhitespace();
            char c = peek();
            if (c == ',') {
                i++;
            } else if (c == '}') {
                i++;
                depth--;
                return;
            } else {
                throw new Reject("expected ',' or '}' at index " + i + ".");
            }
        }
    }

    private void typed(String key, Kind kind, Map<String, Kind> members) {
        boolean isNull = peek() == 'n';
        switch (kind) {
            case STRING -> {
                if (isNull) {
                    literal("null");
                } else if (peek() == '"') {
                    string();
                } else {
                    throw wrongType(key, "a string");
                }
            }
            case INTEGER -> {
                if (isNull) {
                    literal("null");
                } else if (peek() == '-' || isDigit(peek())) {
                    integer(key);
                } else {
                    throw wrongType(key, "an integer");
                }
            }
            case OBJECT, DATA -> {
                if (isNull) {
                    literal("null");
                } else if (peek() == '{') {
                    if (kind == Kind.DATA) {
                        value(true);
                    } else {
                        object(null, members);
                    }
                } else {
                    throw wrongType(key, "a JSON object");
                }
            }
        }
    }

    private static Reject wrongType(String key, String expected) {
        return new Reject("'" + key + "' must be " + expected + ".");
    }

    /** Member names are matched exactly as spelled (case-sensitive), like the core decoder and the contract schema. */
    private static <T> T lookup(Map<String, T> members, String key) {
        return members.get(key);
    }

    // -- generic JSON ------------------------------------------------------------------------

    /** Parse any well-formed value; with {@code finiteNumbers} every number must fit a finite double. */
    private void value(boolean finiteNumbers) {
        char c = peek();
        switch (c) {
            case '{' -> {
                enter();
                i++;
                skipWhitespace();
                if (peek() == '}') {
                    i++;
                    depth--;
                    return;
                }
                while (true) {
                    skipWhitespace();
                    if (peek() != '"') {
                        throw new Reject("expected an object key at index " + i + ".");
                    }
                    string();
                    skipWhitespace();
                    expect(':');
                    skipWhitespace();
                    value(finiteNumbers);
                    skipWhitespace();
                    char next = peek();
                    if (next == ',') {
                        i++;
                    } else if (next == '}') {
                        i++;
                        depth--;
                        return;
                    } else {
                        throw new Reject("expected ',' or '}' at index " + i + ".");
                    }
                }
            }
            case '[' -> {
                enter();
                i++;
                skipWhitespace();
                if (peek() == ']') {
                    i++;
                    depth--;
                    return;
                }
                while (true) {
                    skipWhitespace();
                    value(finiteNumbers);
                    skipWhitespace();
                    char next = peek();
                    if (next == ',') {
                        i++;
                    } else if (next == ']') {
                        i++;
                        depth--;
                        return;
                    } else {
                        throw new Reject("expected ',' or ']' at index " + i + ".");
                    }
                }
            }
            case '"' -> string();
            case 't' -> literal("true");
            case 'f' -> literal("false");
            case 'n' -> literal("null");
            default -> {
                if (c == '-' || isDigit(c)) {
                    String token = number();
                    if (finiteNumbers && !Double.isFinite(Double.parseDouble(token))) {
                        throw new Reject("number " + token + " is out of range.");
                    }
                } else {
                    throw new Reject("unexpected character at index " + i + ".");
                }
            }
        }
    }

    private void enter() {
        if (++depth > MAX_DEPTH) {
            throw new Reject("exceeded the maximum nesting depth.");
        }
    }

    private void integer(String key) {
        String token = number();
        if (token.indexOf('.') >= 0 || token.indexOf('e') >= 0 || token.indexOf('E') >= 0) {
            throw wrongType(key, "an integer");
        }
        try {
            Long.parseLong(token);
        } catch (NumberFormatException overflow) {
            throw new Reject("'" + key + "' is out of the 64-bit integer range.");
        }
    }

    /** One strict JSON number token (RFC 8259 grammar). */
    private String number() {
        int start = i;
        if (peek() == '-') {
            i++;
        }
        if (peek() == '0') {
            i++;
        } else if (isDigit(peek())) {
            while (isDigit(peek())) {
                i++;
            }
        } else {
            throw new Reject("invalid number at index " + start + ".");
        }
        if (peek() == '.') {
            i++;
            if (!isDigit(peek())) {
                throw new Reject("invalid number at index " + start + ".");
            }
            while (isDigit(peek())) {
                i++;
            }
        }
        if (peek() == 'e' || peek() == 'E') {
            i++;
            if (peek() == '+' || peek() == '-') {
                i++;
            }
            if (!isDigit(peek())) {
                throw new Reject("invalid number at index " + start + ".");
            }
            while (isDigit(peek())) {
                i++;
            }
        }
        return s.substring(start, i);
    }

    /** One JSON string; returns its decoded content (used for member-name matching). */
    private String string() {
        i++; // opening quote
        StringBuilder out = new StringBuilder();
        while (true) {
            if (i >= s.length()) {
                throw new Reject("unterminated string.");
            }
            char c = s.charAt(i++);
            if (c == '"') {
                return out.toString();
            }
            if (c < 0x20) {
                throw new Reject("control character in a string at index " + (i - 1) + ".");
            }
            if (c != '\\') {
                out.append(c);
                continue;
            }
            if (i >= s.length()) {
                throw new Reject("unterminated string.");
            }
            char escape = s.charAt(i++);
            switch (escape) {
                case '"', '\\', '/' -> out.append(escape);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (i + 4 > s.length()) {
                        throw new Reject("invalid unicode escape.");
                    }
                    int code = 0;
                    for (int k = 0; k < 4; k++) {
                        int digit = Character.digit(s.charAt(i + k), 16);
                        if (digit < 0) {
                            throw new Reject("invalid unicode escape.");
                        }
                        code = code * 16 + digit;
                    }
                    i += 4;
                    out.append((char) code);
                }
                default -> throw new Reject("invalid escape at index " + (i - 1) + ".");
            }
        }
    }

    // -- lexing helpers ----------------------------------------------------------------------

    private void literal(String word) {
        if (!s.startsWith(word, i)) {
            throw new Reject("invalid literal at index " + i + ".");
        }
        i += word.length();
    }

    private void expect(char c) {
        if (peek() != c) {
            throw new Reject("expected '" + c + "' at index " + i + ".");
        }
        i++;
    }

    private char peek() {
        return i < s.length() ? s.charAt(i) : '\0';
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private void skipWhitespace() {
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
            } else {
                return;
            }
        }
    }
}
