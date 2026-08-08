# krabka streams for Java

`krabka-streams-java` is the Java client library for stream processing with krabka.
It uses the Apache Kafka Streams API and adds krabka schema registry and Apache Arrow support.

The minimum Java version is 17.

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

## Status

Version `1.0.0` is under development. See [PARITY.md](PARITY.md) for the release checklist.

## License

Apache License 2.0.
