# krabka streams for Java

`krabka-streams-java` is the Java client library for stream processing with krabka.
It uses the Apache Kafka Streams API and adds krabka schema registry and Apache Arrow support.

The minimum Java version is 17.

```kotlin
implementation("io.krabka:krabka-streams:1.0.0")
```

## Modules

| Artifact | Purpose |
| --- | --- |
| `io.krabka:krabka-streams` | Apache Kafka Streams API and krabka defaults |
| `io.krabka:krabka-streams-schema-serde` | Avro, Protobuf, and JSON Schema serdes |
| `io.krabka:krabka-streams-columnar` | Apache Arrow batch processing |
| `io.krabka:krabka-streams-test-utils` | Test helpers for all modules |

## Build

```shell
./gradlew build
```

On Windows, use `gradlew.bat build`.

Run the broker integration test against a ready broker:

```shell
KRABKA_INTEGRATION_BOOTSTRAP=localhost:9092 \
  ./gradlew :krabka-streams-test-utils:integrationTest
```

The broker must enable the streams group protocol and finalize `streams.version=1`.
For Apache Kafka 4.3.1, set `group.streams.num.standby.replicas=1` to run the standby check.

## Schema registry example

```java
var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
var cache = new SchemaCache(client);
var serde = JsonSchemaSerde.forValue(Order.class, orderSchema, cache, true);

serde.registerSubject("orders");
cache.prewarm().join();
```

The cache resolves schema IDs before processing starts. If a consumer sees an unknown writer ID,
the cache starts one background fetch and throws `SchemaFetchPendingException`. The exception is retriable.

## Arrow columnar processing

Columnar topologies use `VectorSchemaRoot`. Each fetched topic partition batch is one processing unit.
The built-in operations are filter, select, with-columns, and group-by aggregate.

Arrow 19 needs this JVM option when it uses direct buffers:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

The reserved columns are `__key`, `__timestamp`, `__partition`, and `__offset`.
Payload schemas must not use these names. `BlobCodec` splits Arrow IPC output at a 900 KiB soft limit.

The `1.0.0` columnar API does not keep state across fetched batches. Joins, windows, and aggregates
only operate on records in the current partition batch.

## Test utilities

`ColumnarTestDriver` runs a built columnar topology without a broker. `SchemaRegistryStub` provides
a stateful local implementation of the registry endpoints used by the serdes. The artifact also
exports Apache Kafka's `TopologyTestDriver` for ordinary Kafka Streams topologies.

## Status

Version `1.0.0` is ready for release. See [PARITY.md](PARITY.md) for the parity checklist.

## License

Apache License 2.0.
