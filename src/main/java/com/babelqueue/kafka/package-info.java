/**
 * Apache Kafka transport for BabelQueue — implements §6 of the broker-bindings contract.
 *
 * <p>Kafka is a partitioned append-only log with consumer-group offset commits, not a queue
 * with per-message ack. It has <strong>no native</strong> per-message ack, delayed delivery,
 * dead-letter queue, or delivery counter, so this binding absorbs all four gaps in the
 * transport layer while keeping the envelope unchanged ({@code schema_version} stays 1):
 *
 * <ul>
 *   <li>the canonical envelope JSON is the record <strong>value</strong>; the contract fields
 *       are mirrored onto {@code bq-} record headers (UTF-8 byte values) so a consumer routes
 *       on {@code bq-job} without decoding the body, and the record timestamp mirrors
 *       {@code meta.created_at};</li>
 *   <li><strong>{@code bq-attempts} is the authoritative retry counter</strong> (Kafka has no
 *       native delivery count); the body's top-level {@code attempts} is kept in sync and is
 *       the fallback for non-BabelQueue producers;</li>
 *   <li>consume is <strong>process-then-commit</strong> (manual commit, at-least-once);</li>
 *   <li>retry and delay use <strong>SDK-owned tiered retry topics</strong>
 *       ({@code <topic>.retry.<n>}); a delay or release with no retry topics configured raises
 *       rather than silently dropping;</li>
 *   <li>terminal failures go to an opt-in <strong>{@code <topic>.dlq}</strong> topic carrying
 *       the canonical envelope plus the additive {@code dead_letter} block.</li>
 * </ul>
 *
 * <p>Full spec: <a href="https://babelqueue.com">babelqueue.com</a>.
 */
package com.babelqueue.kafka;
