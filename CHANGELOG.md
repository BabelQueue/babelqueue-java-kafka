# Changelog

All notable changes to `com.babelqueue:babelqueue-kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

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
