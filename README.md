# BabelQueue — Apache Kafka (Java)

`com.babelqueue:babelqueue-kafka` — an Apache Kafka transport for
[BabelQueue](https://babelqueue.com), built on the official `kafka-clients` and the
framework-agnostic [`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed, **process-then-commit** consumer, so a
Kafka-based Java service speaks the same wire contract (envelope shape, URN identity, trace
propagation) as the .NET, Python, Go and Node SDKs. Implements
[§6 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka).

Kafka has **no native** per-message ack, delayed delivery, dead-letter queue, or delivery
counter — this transport absorbs all four in the binding layer (the envelope stays
`schema_version: 1`):

- the envelope JSON is the record **value**; the contract fields are mirrored onto `bq-`
  headers (so a consumer routes on `bq-job` without decoding the body) and the record
  timestamp mirrors `meta.created_at`;
- **`bq-attempts` is the authoritative retry counter** (the body's `attempts` is the
  fallback for non-BabelQueue producers);
- consume is **process-then-commit** (manual commit, at-least-once);
- retry and delay use **SDK-owned tiered retry topics** `<topic>.retry.<n>`; a delay or
  release with no retry topics configured **raises** rather than silently dropping;
- terminal failures go to an opt-in `<topic>.dlq` topic carrying the canonical envelope plus
  the additive `dead_letter` block.

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-kafka</artifactId>
  <version>1.0.0</version>
</dependency>
```

It pulls `babelqueue-core` and `org.apache.kafka:kafka-clients` transitively.

## Produce

```java
Map<String, Object> cfg = Map.of(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

try (Producer<byte[], byte[]> producer = new KafkaProducer<>(cfg)) {
    String id = KafkaPublisher.create(producer, "orders")
        .publish("urn:babel:orders:created", Map.of("order_id", 1042));
}
```

`publish(urn, data)` returns the message `meta.id`; overloads add a `traceId` and a relative
`Duration delay`. Delays require a retry topology (`KafkaPublisher.create(producer, retryTopics)`);
on a plain publisher a delay raises `BabelQueueException`.

## Consume

```java
Map<String, Object> cfg = Map.of(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG, "orders-workers",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,            // manual commit is required
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]>(cfg);
consumer.subscribe(List.of("orders"));

RetryTopics retry = RetryTopics.forTopic("orders")
    .tier(Duration.ofSeconds(5)).tier(Duration.ofMinutes(1)).build(); // .retry.1, .retry.2 + orders.dlq

KafkaConsumer worker = KafkaConsumer.builder(consumer)
    .producer(producer)            // republishes retry/DLQ records
    .retryTopics(retry)
    .maxTries(3)
    .handler("urn:babel:orders:created", (env, record) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .onError((err, env, record) -> err.printStackTrace())
    .build();

worker.run(() -> true); // poll → process → commit, until you stop it
```

A throwing handler republishes the envelope to the next `<topic>.retry.<n>` tier with
`bq-attempts + 1`, then commits; once `maxTries` is reached it goes to `<topic>.dlq` with a
`dead_letter` block. The consumer routes on the `bq-job` header, so it never decodes a record
it cannot handle. Unknown-URN strategy is one of `fail` / `delete` / `release` / `dead_letter`.

> The retry/DLQ records are produced with `bq-delay` set; re-injecting a retry-topic record
> into the work topic after its tier delay is done by running a worker on the retry topics —
> the cooperative, partition-paused delay of §6.4. (A bundled re-injection runtime is a
> near-term addition.)

## Contract mapping (§6)

| Envelope | Apache Kafka |
| :--- | :--- |
| body | record `value` (byte-identical across SDKs) |
| `job` (URN) | header `bq-job` (consumer routes on this) |
| `trace_id` | header `bq-trace-id` |
| `meta.id` | header `bq-message-id` |
| `meta.schema_version` | header `bq-schema-version` (`"1"`) |
| `meta.lang` | header `bq-source-lang` |
| `meta.created_at` | record `timestamp` (Unix ms) + header mirror |
| `attempts` | header `bq-attempts` (**authoritative**; body is the fallback) |
| reserve / ack | poll → process → **commit offset** (manual) |
| retry / delay | republish to `<topic>.retry.<n>` (`bq-attempts + 1`) |
| dead-letter | `<topic>.dlq` + `dead_letter` block |

All header values are UTF-8 byte strings (integers as decimal strings, e.g. `"1"`). The
envelope is unchanged (`schema_version` stays `1`); Apache Kafka is purely additive.

## Build & test

```bash
mvn verify
```

The Kafka `Consumer` is mocked with Mockito and the producer with the official `MockProducer`
— no Kafka, no network. JUnit 5, JaCoCo ≥90% line coverage.

## License

MIT
