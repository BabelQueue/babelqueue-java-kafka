# Changelog

All notable changes to `com.babelqueue:babelqueue-kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.3.1] - 2026-10-26

### Changed

- The poison `dead_letter` block is now built by core `PoisonRecord.build(body, originalQueue, attempts, error, exception)` (babelqueue-core 1.14.0) instead of by hand; the DLQ record is byte-for-byte unchanged (`reason: "poison"`, `failed_at`, `lang: "java"`). The local `PoisonRecord` copy was removed.

## [1.3.0] - 2026-10-24

### Added

- `KafkaRetryReinjector` (broker-bindings §6.4, ADR-0034 §5–§7): the retry-tier re-inject consumer. It drains every `<topic>.retry.<n>`, waits until `record.timestamp + tier_ms` (clamped to `[now, now + tier_ms]`) by pausing the partition — the poll loop is never blocked — and writes the record back to `bq-original-topic` with the value and key byte-for-byte. No decode/re-encode, `attempts` untouched, undecodable records forwarded as-is; `bq-delay` and `bq-original-topic` are dropped, every other header (`traceparent` included) is kept. The offset is committed only after the broker acknowledged the write.
- `RetryTopics.RECOMMENDED_TIERS` (5 s / 30 s / 5 min / 30 min) and `RetryTopics.Builder.recommendedTiers()` — opt-in; the default stays "no tiers".
- Conformance `kafka_poison` section (92 cases) is run by `KafkaPoisonConformanceTest`; the hand-copied `poison-trigger-set.json` is gone.

### Changed

- Local `PoisonRecord` copy: the `dead_letter` argument is now validated as a single JSON object and rejected otherwise. Kept in sync with `babelqueue-core`.
- Retry republish carries the failed record's non-`bq-*` headers (W3C `traceparent`) onto the retry tier so the trace stays one chain through the re-inject hop.
- A record with no usable identity (no `job`/`urn` in the body and no `bq-job` header) is dead-lettered as poison; a non-string `job` is no longer a shape violation of its own (the identity check decides, per the conformance `kafka_poison` section).
- **Poison DLQ record shape follows ADR-0035:** a strict JSON object body is spliced byte for byte with a `dead_letter` member (no value is re-encoded, so `1e400`, big integers and repeated member names survive); anything else is wrapped as `{"raw": ..., "raw_encoding"?: "base64", "dead_letter": ...}`. A body that is not valid UTF-8 is carried as standard base64 plus `raw_encoding: "base64"` instead of with U+FFFD substitution, and an object that already has a top-level `dead_letter` is wrapped. The new `poison_dlq` conformance section is run by `PoisonDlqConformanceTest`.
- Vendored conformance bumped to v1.6.0; CI pins `BQ_CONFORMANCE_VERSION=1.6.0` and verifies `fetch.sh` against `BQ_FETCH_SH_SHA256`.

## [1.2.1] - 2026-10-17

### Fixed

- Unknown top-level keys and unknown `meta` keys (ADR-0031 extension keys, e.g. `extra_top`, `meta.vendor_flag`) are no longer lost when a message is republished to a retry topic: the attempt bump now uses `Envelope.withAttempts`, which carries the extras, instead of rebuilding the envelope through the 6-argument constructor. The dead-letter copy already preserved them; regression tests now lock both paths.
- `KafkaRedriveTransport` no longer treats a job made only of Unicode whitespace (e.g. U+2003) as blank when deciding whether to project the `bq-*` headers: blank is the closed ASCII set (space, tab, LF, VT, FF, CR) of ADR-0031 (c), so it matches the other SDKs. The check is local to the transport, so it does not depend on the resolved core version.

### Changed

- Docs only: the note in the previous release that the Go reference's case-insensitive envelope-key read "is tightened separately" is now closed — Go core 1.14.0 reads envelope keys exactly (case-sensitive, last duplicate wins), so every SDK now decides poison on the same exact-case keys.
- The `babelqueue-core` dependency is raised to 1.11.0, so the ADR-0031 decode behaviour (`job` + `urn` warning, closed ASCII blank set) and encode-time extension-key rejection come in transitively.

## [1.2.0] - 2026-10-12

### Changed

- An undecodable (poison) body is now dead-lettered the way the Go reference does (§6.5) instead of as an
  annotated empty envelope that lost the original body: a body that is a single JSON object keeps its keys, anything
  else (non-JSON, a JSON scalar/array, trailing tokens, an empty value; invalid UTF-8 is stored with U+FFFD
  substitutions) is wrapped as `{"raw": "<body>"}`, and a `dead_letter` block with `reason: "poison"` is attached
  (`error` / `exception` from the decode failure, `original_queue` = the consumed topic, `attempts` from the
  `bq-attempts` header, `lang: "java"`). **Behaviour change - the poison trigger set is now exactly Go's:** a record is poison when, and only when, the Go reference's typed `babelqueue.Decode` would reject it - `data` / `meta` / `dead_letter` not an object; `job`, `trace_id`, `meta.id|queue|lang` or `dead_letter.reason|error|exception|original_queue|lang` not a string; `meta.schema_version`, `meta.created_at`, `attempts` or `dead_letter.failed_at|attempts` not an integer that fits int64 (so a string such as `"2026"`, a fraction, an exponent such as `1e2`, or an out-of-range value is poison); a number in `data` that is not a finite double (`1e400`); duplicate keys are each type-checked and member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read them; `JOB` or `Meta` is an unknown member); and a malformed body (leading zeros, `NaN`, trailing tokens, a BOM, a second object). A JSON `null` body is not poison (empty envelope, unknown-URN path); a non-string `urn` alias is ignored. Previously only a top-level subset was checked.
 A well-typed but non-conformant envelope (missing `data`, blank `trace_id`, a body `schema_version` other than 1) is **no longer dead-lettered** by `EnvelopeCodec.accepts`: like Go, which never calls `Accepts` on consume, it is routed by its URN to the handler (an empty URN takes the unknown-URN path).
  The record key is carried unchanged and the offset is
  committed only after the DLQ write is confirmed (otherwise the partition is rewound, as before). The §6.7
  schema-version-gate rejection still forwards the record raw.
- Poison DLQ records: a body holding a number the codec cannot write back (e.g. `1e400`) is carried verbatim as `{"raw": ...}` instead of failing the DLQ write, and `KafkaHeaders.of` no longer projects `bq-schema-version: 0` when the body has no `meta.schema_version` (a later §6.7 gate would have rejected the DLQ record). Remaining, intentional differences from the Go reference for a poison record: `dead_letter.attempts` is the `bq-attempts` header (Go writes 0) and `original_queue` is the `bq-original-topic` header when present (Go writes the consumed topic); the DLQ record also carries `bq-original-topic` plus the `bq-*` headers projected from the body (Go adds no headers); and a float literal such as `1.0` / `1e2` is re-formatted where the body is re-encoded. The wire envelope and the `dead_letter` shape are unchanged.
- **Any decode failure is poison; the partition is never left to wedge.** The "exactly Go's" trigger set above is corrected in two respects. (1) Any `RuntimeException` **or `StackOverflowError`** from the shape scan, the codec decode or the poison wrapper is dead-lettered as poison (a deeply nested `data` previously overflowed the recursive scanner, escaped `poll`, left the offset uncommitted and locked the partition). The nesting the recursive scanner/parser carries is bounded by the thread's stack (measured at roughly 3000-5000 levels, depending on the thread's stack size), where Go allows 10000; deeper bodies are poison. (2) Member names are matched case-sensitively, exactly as the core decoder reads them (no `encoding/json`-style folding, no `equalsIgnoreCase`); the Go reference's case-insensitive read contradicts the contract schema and is tightened separately. The wire envelope and the `dead_letter` shape are unchanged.

### Fixed

- `KafkaRedriveTransport` now implements `Redrive.Rewinder`: whenever `Redrive.redrive` stops on a failed
  message (a failed restore write back to the DLQ, a failed redrive write, a throwing `select`, a failed
  ack), the consumer is `seek`-ed back to the first unacknowledged record of every partition it has handed
  out in the batch (`Redrive.redrive` pops the whole batch before processing it), so a later cumulative
  commit can no longer skip any of them. A failing `seek` does not stop the others (every partition is
  attempted) and is reported together with the original error. An acknowledged record is never rewound:
  after a failed redrive write whose restore succeeded, only the rest of the batch is (the next record in
  the same partition resumes right after it). Requires a `babelqueue-core` that provides
  `Redrive.Rewinder`.

## [1.1.0] - 2026-10-11

### Added

- `KafkaRedriveTransport`: a `Redrive.Transport` over Apache Kafka (a subscribed DLQ `Consumer` + a `Producer`) so
  `Redrive.redrive` (ADR-0026) can replay `<topic>.dlq` back to the work topic, committing the DLQ offset only after
  the re-publish is acknowledged. It implements the core's `Redrive.KeyedRepublisher`, so the redrive write and the
  restore write back to the DLQ carry the DLQ record's key byte-for-byte (null stays null, empty stays empty, none is
  synthesised; contract §6.5). The `bq-` headers are re-projected from the body being published. The replay-bypass
  header is not propagated by this transport. A restore write (dry run, unselected, undecodable, failed redrive
  write) re-writes the DLQ record verbatim: its original value bytes (a null value stays null; a value that is not
  valid UTF-8 is never altered), headers and key, with a fresh timestamp. **Requires `babelqueue-core` 1.9.0**, which ships
  `Redrive.KeyedRepublisher`; `babelqueue-core.version` is pinned to it.

### Fixed

- `KafkaConsumer` no longer lets a throwing `onError` callback bypass settlement. Previously `onError` ran
  *before* the raw DLQ write + commit on the undecodable-body and non-conformant-envelope paths, and before the
  retry republish / DLQ write + commit on the handler-failure path, so a throwing callback skipped the settlement and
  the record was redelivered forever. These paths now settle first and report afterwards (also when settling fails),
  and `report()` swallows callback exceptions, so the commit / seek-back already performed stands and the poll loop
  keeps running. Behaviour note: a throwing `onError` on the schema-version-gate and rewind paths is now swallowed
  instead of propagating out of `poll()`. No public API change; the envelope, the `dead_letter` block and the
  headers are unchanged (`schema_version` stays 1). Patch-level behaviour fix.

## [1.0.4] - 2026-10-11

### Fixed

- The original Kafka record key is now preserved when the consumer republishes a record: the retry-tier write, the
  unknown-URN release, and the envelope DLQ write all carried a
  `null` key before. A null key stays null, an empty key stays empty and no key is synthesised; the raw/poison DLQ
  already kept it. Partition equality is not guaranteed. The envelope, the `dead_letter` block and the headers are
  unchanged (`schema_version` stays 1). Patch-level behaviour fix.

## [1.0.3] - 2026-10-11

### Fixed

- `KafkaConsumer` no longer loses a record when a retry-tier / DLQ write fails on the non-gate paths
  (handler failure -> retry tier or max-tries `<topic>.dlq`, undecodable body, non-conformant envelope,
  unmapped URN under `dead_letter` / `release`). The write was fire-and-forget and the offset was committed
  regardless; it is now awaited and the offset is committed only after the broker confirmed it. On failure
  nothing is committed, the partition is rewound (`seek`) to the failed record, the rest of that partition's
  batch is skipped and the failure is reported via `onError` - the same protection the §6.7 gate already had.

## [1.0.2] - 2026-10-10

### Fixed

- `KafkaConsumer` now applies the §6.7 schema-version gate **before** decoding the body: when the
  `bq-schema-version` header is present and is not the supported version (`1`), the record is never
  decoded and the handler is never called; it is reported via `onError` (with a `null` envelope), its raw
  bytes are forwarded to `<topic>.dlq` (when configured) and the offset is committed so the partition
  advances. A missing or blank header (empty or ASCII-whitespace-only) keeps the existing decode path.
  Cross-SDK parity (GR-5): the header is read as UTF-8 and compared exactly, with no trimming.
  The gate waits for the DLQ write to be confirmed before committing; if it fails, nothing is committed,
  the partition is rewound (`seek`) to the rejected record, the rest of that partition's batch is skipped and
  the failure goes to `onError`, so the record is redelivered and rejected again instead of being lost.
- Build: removed a duplicate `org.json:json` test dependency from `pom.xml` that made Maven reject the POM.

## [1.0.1] - 2026-10-03

### Security
- Bump `org.apache.kafka:kafka-clients` from 3.9.1 to 3.9.2 (security fix).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Kafka transport on `babelqueue-core` +
  `org.apache.kafka:kafka-clients`, implementing §6 of the broker-bindings contract. Kafka
  has no native ack/delay/DLQ/delivery-counter, so the binding absorbs all four:
  `KafkaPublisher` (value = canonical envelope, record timestamp = `meta.created_at`, the
  `bq-` header projection — `bq-job` routes; a delay routes to a `<topic>.retry.<n>` tier or
  raises without one) and `KafkaConsumer` (**process-then-commit** manual-commit consume; a
  throwing handler republishes to the next retry tier with **`bq-attempts + 1`** then commits;
  terminal failures go to `<topic>.dlq` with the additive `dead_letter` block; the
  `bq-attempts` header is the authoritative counter with the body as fallback; `fail`/`delete`/
  `release`/`dead_letter` unknown-URN strategies; poison bodies forwarded to the DLQ).
  `RetryTopics` configures the tiered retry/delay topology. Java 17, JUnit 5, JaCoCo ≥90% line
  coverage; the Kafka `Consumer` is mocked with Mockito and the producer with the official
  `MockProducer` (no Kafka, no network). The envelope is unchanged (`schema_version: 1`);
  Apache Kafka is purely additive.
