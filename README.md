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

## Status

Version `1.0.0` is under development. See [PARITY.md](PARITY.md) for the release checklist.

## License

Apache License 2.0.
