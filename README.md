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

Each module depends on `krabka-streams`, so any one of them puts the Kafka Streams API
on your classpath at the version this release pins.

## Documentation

Full documentation is in [docs/](docs/index.md).

| Document | Contents |
| --- | --- |
| [Getting started](docs/getting-started.md) | Requirements, coordinates, and first examples |
| [Configuration](docs/configuration.md) | `KrabkaStreamsConfig`, broker requirements, JVM flags |
| [Schema registry](docs/schema-registry.md) | Registry client, schema cache, prewarming |
| [Serdes](docs/serdes.md) | Avro, Protobuf, JSON Schema, and the Confluent wire format |
| [Columnar processing](docs/columnar.md) | Arrow batches, codecs, topologies, runner |
| [Columnar operators](docs/columnar-operators.md) | Built-in operators and buffer ownership |
| [Testing](docs/testing.md) | Test drivers, registry stub, integration suite |
| [API reference](docs/api-reference.md) | Every public type |
| [Architecture](docs/architecture.md) | Module layout and design decisions |
| [Limitations](docs/limitations.md) | What `1.0.0` does not do |
| [Troubleshooting](docs/troubleshooting.md) | Error messages mapped to causes |
| [Build and release](docs/build-and-release.md) | Gradle tasks, CI, publishing |

## Kafka Streams

The Kafka Streams DSL, Processor API, state stores, and interactive queries are exported
unchanged. The only krabka-specific step is the configuration helper, which enables the
streams group protocol.

```java
var settings = Map.<String, Object>of(
        StreamsConfig.APPLICATION_ID_CONFIG, "order-counter",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

var streams = new KafkaStreams(topology, KrabkaStreamsConfig.withDefaults(settings));
```

Settings you provide are never overwritten. See [Configuration](docs/configuration.md).

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

See [Schema registry](docs/schema-registry.md) and [Serdes](docs/serdes.md).

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

See [Columnar processing](docs/columnar.md) and [Columnar operators](docs/columnar-operators.md).

## Test utilities

`ColumnarTestDriver` runs a built columnar topology without a broker. `SchemaRegistryStub` provides
a stateful local implementation of the registry endpoints used by the serdes. The artifact also
exports Apache Kafka's `TopologyTestDriver` for ordinary Kafka Streams topologies.

See [Testing](docs/testing.md).

## Status

Version `1.0.0` is ready for release. See [PARITY.md](PARITY.md) for the parity checklist,
[CHANGELOG.md](CHANGELOG.md) for release notes, and [docs/limitations.md](docs/limitations.md)
for what this version does not do.

## License

Apache License 2.0.
