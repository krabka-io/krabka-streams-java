# Changelog

## 1.2.0 - 2026-08-10

- Add `krabka-streams-columnar-schema`, connecting the schema registry serdes to the
  columnar runtime: `AvroBatchCodec` and `ProtobufBatchCodec` decode registry-framed
  topics into Arrow batches whose columns follow the record schema — structs, lists,
  maps, decimals, and timestamps as native Arrow types — and encode processed batches
  back; `AvroRowBridge`, `ProtobufRowBridge`, `AvroArrowSchemas`, and
  `ProtobufArrowSchemas` expose the conversion for composition.
- Derive every bridge's Arrow schema once, at construction, from the fixed reader
  schema or message descriptor, so mid-stream writer schema evolution never changes
  the columns.
- Add the public `ArrowValues` helpers so custom row bridges and processors reuse the
  engine's type-coercing vector reads and writes, and extend the supported Arrow
  types with `Time`, `FixedSizeBinary`, and exact unsigned 64-bit reads.
- Rethrow retriable failures, such as a pending schema fetch, from the group runner
  regardless of the skip or dead-letter error policy, so transient conditions retry
  instead of discarding healthy batches.

## 1.1.1 - 2026-08-10

- Document every public type, member, and record component with full Javadoc, and add
  a usage example to every public type.
- Enforce complete API documentation by raising the Javadoc lint from
  `Xdoclint:all,-missing` to `Xdoclint:all` with warnings as errors.
- Publish the documentation site — a landing page plus one aggregated, krabka-themed
  Javadoc across every module — to GitHub Pages on each push to `main`.

## 1.1.0 - 2026-08-09

- Resolve the documented columnar, schema registry, serde, and packaging limitations.
- Add record headers, partition-scoped state, snapshots and rebalance hooks, metrics,
  acknowledged asynchronous sends, error policies, and dead-letter output.
- Add event-time windows and joins, JSON-Schema-derived Arrow fields, GZIP codecs, and
  local Avro, JSON Schema, and Protobuf compatibility checks.
- Standardize parameter matrices on TestParameterInjector and structural assertions on
  AssertJ recursive comparison; add deterministic fault injection to the columnar test driver.
- Package strict, example-backed Javadocs before Maven Central uploads and format every
  Java Markdown snippet with the Java language identifier.

## 1.0.0 - 2026-08-08

- Export the Apache Kafka Streams 4.3.1 DSL, Processor API, state stores, and query APIs.
- Enable the KIP-1071 streams group protocol through `KrabkaStreamsConfig`.
- Add native schema registry clients and Avro, Protobuf, and JSON Schema serdes.
- Add Arrow IPC codecs, row and blob modes, vector operations, topologies, and a partition runner.
- Add test utilities and live compatibility tests for Apache Kafka 4.3.1 and krabka 0.3.8.
