# Getting started

## Requirements

| Requirement                       | Version                                                                |
| --------------------------------- | ---------------------------------------------------------------------- |
| Java                              | 17 or later                                                            |
| Apache Kafka Streams              | 4.3.1 (a transitive `api` dependency)                                  |
| Broker                            | Apache Kafka 4.3.1 or krabka 0.3.8, with `streams.version=1` finalized |
| Gradle (to build this repository) | 9.6.1, supplied by the wrapper                                         |

The library targets Java 17 bytecode. Continuous integration compiles and tests on
Java 17 and Java 21.

## Coordinates

All artifacts share the group `io.krabka` and the version `1.0.0`.

```kotlin
dependencies {
    implementation(platform("io.krabka:krabka-streams-bom:1.0.0"))
    implementation("io.krabka:krabka-streams")
    implementation("io.krabka:krabka-streams-schema-serde")
    implementation("io.krabka:krabka-streams-columnar")
    testImplementation("io.krabka:krabka-streams-test-utils")
}
```

Maven:

```xml
<dependency>
  <groupId>io.krabka</groupId>
  <artifactId>krabka-streams</artifactId>
  <version>1.0.0</version>
</dependency>
```

Each artifact depends on `krabka-streams`, so a single dependency on
`krabka-streams-columnar` or `krabka-streams-schema-serde` also pulls in the Kafka
Streams API. `krabka-streams-test-utils` depends on all three and on
`kafka-streams-test-utils`, so a test-scoped dependency on it is enough for most test
source sets.

Dependencies are declared with the Gradle `api` configuration, which means Avro,
Protobuf, Jackson, Arrow, and the Kafka Streams API all appear on your compile
classpath. The Arrow Netty allocator (`org.apache.arrow:arrow-memory-netty`) remains a
runtime-only dependency. Every ordinary jar has a stable `Automatic-Module-Name`; an
optional `all` classifier bundles runtime dependencies for standalone classpaths.

## A first Kafka Streams application

`krabka-streams` re-exports the Apache Kafka Streams API unchanged. The DSL, the
Processor API, state stores, and interactive queries all work without a wrapper. The
only krabka-specific step is the configuration helper.

```java
import io.krabka.streams.KrabkaStreamsConfig;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

var builder = new StreamsBuilder();
builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()))
        .filter((key, value) -> value.startsWith("keep"))
        .groupByKey()
        .count(Materialized.as("counts"))
        .toStream()
        .to("order-counts", Produced.with(Serdes.String(), Serdes.Long()));

var settings = Map.<String, Object>of(
        StreamsConfig.APPLICATION_ID_CONFIG, "order-counter",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

var streams = new KafkaStreams(builder.build(), KrabkaStreamsConfig.withDefaults(settings));
streams.start();
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
```

`KrabkaStreamsConfig.withDefaults` copies your settings into a new `Properties` and
adds `group.protocol=streams` when you have not set it yourself. See
[Configuration](configuration.md) for the details.

## A first schema-aware serde

```java
import io.krabka.streams.schema.JsonSchemaSerde;
import io.krabka.streams.schema.KrabkaSchemaRegistryClient;
import io.krabka.streams.schema.SchemaCache;
import java.net.URI;

record Order(String id, long amount) {
}

String orderSchema = """
        {"type":"object",
         "properties":{"id":{"type":"string"},"amount":{"type":"integer"}},
         "required":["id"]}
        """;

var client = new KrabkaSchemaRegistryClient(URI.create("http://localhost:8081"));
var cache = new SchemaCache(client);
var serde = JsonSchemaSerde.forValue(Order.class, orderSchema, cache, true);

serde.registerSubject("orders");
cache.prewarm().join();
```

`registerSubject` records the subject that this serde needs. `prewarm` resolves every
recorded subject against the registry once, before processing starts, so that the
serializer never blocks on network I/O. See [Schema registry](schema-registry.md).

## A first columnar topology

```java
import io.krabka.streams.columnar.BlobCodec;
import io.krabka.streams.columnar.BuiltinOp;
import io.krabka.streams.columnar.ColumnarTopology;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;

try (var allocator = new RootAllocator()) {
    var codec = new BlobCodec(allocator);
    var topology = new ColumnarTopology(allocator);
    var source = topology.addSource("source", List.of("transactions"), codec);
    var large = topology.addOperator(
            "large-only",
            BuiltinOp.filter(allocator, (batch, row) ->
                    ((BigIntVector) batch.getVector("amount")).get(row) > 1_000),
            source);
    topology.addSink("sink", "large-transactions", codec, large);

    var built = topology.build();
    // Feed built.runBatch(topic, records) from your own consumer loop,
    // or use ColumnarRunner.runPartitionOnce.
}
```

Columnar processing needs one JVM flag when Arrow uses direct buffers:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

See [Columnar processing](columnar.md) for the batch model and the record layout.

## Building this repository

```shell
./gradlew build
```

On Windows use `gradlew.bat build`. See [Build and release](build-and-release.md)
for the full task list and the integration test setup.
