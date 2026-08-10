# krabka streams for Java

`krabka-streams-java` is the Java client library for stream processing with krabka.
It uses the Apache Kafka Streams API and adds krabka schema registry and Apache Arrow support.

The minimum Java version is 17.

```kotlin
implementation("io.krabka:krabka-streams:1.1.1")
```

## Modules

| Artifact                                | Purpose                                      |
| --------------------------------------- | -------------------------------------------- |
| `io.krabka:krabka-streams`              | Apache Kafka Streams API and krabka defaults |
| `io.krabka:krabka-streams-schema-serde` | Avro, Protobuf, and JSON Schema serdes       |
| `io.krabka:krabka-streams-columnar`     | Apache Arrow batch processing                |
| `io.krabka:krabka-streams-test-utils`   | Test helpers for all modules                 |
| `io.krabka:krabka-streams-bom`          | Version constraints for every module         |

Each module depends on `krabka-streams`, so any one of them puts the Kafka Streams API
on your classpath at the version this release pins.

## Documentation

Full documentation is in [docs/](docs/index.md). The API reference for the latest
release is published at <https://krabka-io.github.io/krabka-streams-java/>.

| Document                                         | Contents                                                   |
| ------------------------------------------------ | ---------------------------------------------------------- |
| [Getting started](docs/getting-started.md)       | Requirements, coordinates, and first examples              |
| [Configuration](docs/configuration.md)           | `KrabkaStreamsConfig`, broker requirements, JVM flags      |
| [Schema registry](docs/schema-registry.md)       | Registry client, schema cache, prewarming                  |
| [Serdes](docs/serdes.md)                         | Avro, Protobuf, JSON Schema, and the Confluent wire format |
| [Columnar processing](docs/columnar.md)          | Arrow batches, codecs, topologies, runner                  |
| [Columnar operators](docs/columnar-operators.md) | Built-in operators and buffer ownership                    |
| [Testing](docs/testing.md)                       | Test drivers, registry stub, integration suite             |
| [API reference](docs/api-reference.md)           | Every public type                                          |
| [Architecture](docs/architecture.md)             | Module layout and design decisions                         |
| [Runtime constraints](docs/limitations.md)       | Broker, JVM, Arrow, and packaging constraints              |
| [Troubleshooting](docs/troubleshooting.md)       | Error messages mapped to causes                            |
| [Build and release](docs/build-and-release.md)   | Gradle tasks, CI, publishing                               |

## Kafka Streams

The Kafka Streams DSL, Processor API, state stores, and interactive queries are exported
unchanged. The only krabka-specific step is the configuration helper, which enables the
streams group protocol.

```java
var settings =
    Map.<String, Object>of(
        StreamsConfig.APPLICATION_ID_CONFIG, "order-counter",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

var streams = new KafkaStreams(topology, KrabkaStreamsConfig.withDefaults(settings));
```

Settings you provide are never overwritten. See [Configuration](docs/configuration.md).

## Build

```shell
./gradlew build
bazel build //...
bazel test //...
```

On Windows, use `gradlew.bat build`.

To run Bazel builds on BuildBuddy RBE, create an ignored `user.bazelrc` containing
your BuildBuddy API key:

```text
build --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
```

Then enable the checked-in remote configuration:

```shell
bazel test //... --config=remote
```

To consume the source directly from another Bazel module, add this to its
`MODULE.bazel` (replace the commit with the revision you want to pin):

```starlark
bazel_dep(name = "krabka_streams_java", version = "1.1.1")
git_override(
    module_name = "krabka_streams_java",
    remote = "https://github.com/krabka-io/krabka-streams-java.git",
    commit = "<commit SHA>",
)
```

Then depend on any public module target:

```starlark
deps = ["@krabka_streams_java//krabka-streams:krabka-streams"]
```

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
The built-in operations are filter, select, with-columns, cumulative and windowed
group-by, plus stateful event-time joins.

Arrow 19 needs this JVM option when it uses direct buffers:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

The metadata columns are `__key`, `__timestamp`, `__partition`, `__offset`, and
`__headers`.
Colliding payload names are escaped and restored automatically. `BlobCodec` packs Arrow
IPC output under a 900 KiB hard limit. Built topologies retain processor and aggregate
state per logical partition across fetched batches. The group runner adds snapshots,
rebalance hooks, metrics, acknowledged asynchronous sends, and skip or dead-letter
error policies. `GzipBatchCodec` provides bounded per-record compression.

See [Columnar processing](docs/columnar.md) and [Columnar operators](docs/columnar-operators.md).

## Test utilities

`ColumnarTestDriver` runs a built columnar topology without a broker. `SchemaRegistryStub` provides
a stateful local implementation of the registry endpoints used by the serdes. The artifact also
exports Apache Kafka's `TopologyTestDriver` for ordinary Kafka Streams topologies.

See [Testing](docs/testing.md).

## Status

The current version is `1.1.1`. See [PARITY.md](PARITY.md) for the parity checklist,
[CHANGELOG.md](CHANGELOG.md) for release notes, and [runtime constraints](docs/limitations.md).

## License

Apache License 2.0.
