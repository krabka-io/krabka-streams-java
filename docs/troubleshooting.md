# Troubleshooting

Messages are quoted as they appear. Search this page for the text of the exception you
received.

## Startup and configuration

### `InaccessibleObjectException`, or an Arrow memory error on the first batch

Arrow 19 needs `--add-opens=java.base/java.nio=ALL-UNNAMED`. Add it to the JVM that runs
columnar code, including applications, tests, and any tooling. See
[Configuration](configuration.md#jvm-flags).

### The client never reaches `RUNNING`, or the group coordinator rejects the protocol

The broker does not support the streams group protocol, or `streams.version=1` is not
finalized. Check [broker requirements](configuration.md#broker-requirements). To run
against an older broker, set `group.protocol=classic` explicitly, since `withDefaults`
keeps your value.

### No standby task ever appears

Under the streams protocol the broker decides the standby count. On Apache Kafka 4.3.1
set `group.streams.num.standby.replicas=1` on the broker;
`StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG` alone is not enough.

### `integrationTest` reports zero tests

Both integration tests are gated on environment variables. Set
`KRABKA_INTEGRATION_BOOTSTRAP` or `KRABKA_INTEGRATION_SCHEMA_REGISTRY`. The task
declares both as inputs, so a changed value forces a re-run.

## Schema registry

### `schema ID for orders-value is not resolved; call registerSubject and prewarm first`

The serializer found no ID for the subject. Causes, in order of likelihood:

1. `registerSubject(topic)` was never called for this serde.
2. `prewarm()` was called but not awaited. It returns a future.
3. `prewarm()` failed; the future completed exceptionally and nothing checked it.
4. The topic at run time differs from the one passed to `registerSubject`, so the
   computed subject differs.
5. The source or sink topic passed through `RowCodec` differs from the topic registered
   during prewarm.

```java
serde.registerSubject("orders");
cache.prewarm().join();                                  // await it
assert cache.idForSubject("orders-value").isPresent();   // verify it
```

### `SchemaFetchPendingException: writer schema for id 7 is pending fetch`

Expected on the first record carrying an unseen schema ID. The fetch is already running;
retry the record. The exception extends Kafka's `RetriableException`, so a Streams
deserialization exception handler or a plain consumer retry handles it.

It becomes a problem only if it repeats indefinitely, which means the fetch keeps
failing. Check registry reachability, and whether ID `7` exists at all
(`GET /schemas/ids/7`).

### `schema registry returned HTTP 404: {"error_code":40403,...}`

Under `LOOKUP_ONLY`, the exact schema is not registered under that subject. Register it
first, or switch to `AUTO_REGISTER` in development. Remember that Avro subjects hold the
_canonical parsing form_, so a schema differing only in documentation or field order
still matches, while any structural difference does not.

`40401` means the subject itself does not exist.

### `schema registry returned HTTP 409`

The registry rejected the schema as incompatible with the subject's existing versions.
Fix the schema or set the subject's compatibility level with
`client.setCompatibility(subject, level)`.

### `schema registry request failed`, `statusCode() == -1`

A transport failure: unreachable host, TLS failure, or timeout. The cause carries the
underlying `IOException`. Configure timeouts and TLS on the `HttpClient` passed to the
three-argument constructor.

Context paths in the base URI are preserved.

### `serde role does not match the Kafka key setting`

A value serde was configured as `default.key.serde`, or the reverse. Use
`AvroSerde.forKey` / `forValue` (and the equivalents) to match the position. This check
only fires when Kafka configures the serde from configuration.

### `Protobuf messageType mismatch: writer demo.Other, local demo.Order`

The record was written by a different message type than the one this serde reads. Either
the topic carries mixed types, or the subject resolved to the wrong schema ID. Under
`USE_LATEST` the registry's `messageType` is adopted, which makes the mismatch visible
at the first record.

### `JSON Schema validation failed: ...`

The record body does not satisfy its schema. Validation applies on serialization
against the local schema and on deserialization against the writer schema. Set
`validate = false` to accept it, or fix the producer.

### `cannot serialize schema value` / `cannot deserialize schema value`

A wrapper around a format-library failure. The cause has the real message: an Avro
resolution error, a Protobuf parse failure, or a Jackson binding error.

## Wire format

### `schema frame is shorter than 5 bytes`

The record value is not Confluent-framed. Common causes: a topic written by a plain
serializer, a compacted tombstone handled as bytes, or a key deserialized with a value
serde.

### `invalid schema frame magic byte 0x01`

Same class of problem. The first byte must be `0x00`, so some other framing is in use.

### `truncated Protobuf message-index varint`

A Protobuf frame was decoded from bytes that are not one, or the value was truncated in
transit. Confirm the topic really carries Protobuf and that `ProtobufSerde` (not
`AvroSerde`) is reading it.

## Columnar

### A payload column starts with `__payload_`

The original name collided with a metadata column. Use
`BlobCodec.payloadColumn(originalName)` while processing it. Sinks restore the original
name automatically.

### `Arrow batch schemas differ`

`BlobCodec.decode` received records whose Arrow payloads have different schemas. All
records in one fetched batch must share a schema. Reuse one `JsonRowBridge`, or pass an
explicit Arrow `Schema`, to keep row batches stable.

### `decode called with an empty record batch`

A codec was called with an empty list. `ColumnarRunner` guards against this; if you drive
`runBatch` yourself, skip empty polls.

### `cannot decode Arrow record 3`

The record at index 3 in the fetched batch is not a valid Arrow IPC stream. The cause
holds the underlying failure. Check whether that topic mixes Arrow and non-Arrow values.

### `Arrow column does not exist: sku`

`select`, `groupBy`, or an aggregation named a column that is not in the batch. Remember
that `groupBy` drops metadata columns and replaces payload columns with the key and
aggregate columns, so a downstream operator may be looking at a narrower schema than you
expect.

### `no Arrow union member accepts ...`

A `withColumns` or `groupBy` value matches none of the declared union children. Return
a value compatible with one child or widen the union field.

### `groupBy requires at least one key column`

An empty key collection was passed. Whole-batch aggregation without a key is not
supported; group by a constant column added with `withColumns` if you need it.

### ``duplicate node name `same` ``

Two nodes share a name. Names must be unique across sources, operators, and sinks.

### ``node `sink` has an invalid parent``

The parent was added after the child, or the node has no parent. Add nodes in dependency
order.

### `topology has no source` / `topology has no sink`

`build()` requires at least one of each.

### `parent is not a node in this topology`

A `ColumnarNode` from a different `ColumnarTopology` was used as a parent. Nodes are not
portable between topologies.

### The allocator throws on close

Arrow reports outstanding allocations, which means a batch was not closed. Work through
the [ownership rules](columnar-operators.md#buffer-ownership): a batch you created and
did not forward is yours to close, and anything returned by `decode`,
`rowsToBatch`, or an `ArrowIpcSerde` deserializer is yours as well. The exception names
the leaked allocations.

### `runBatch` returns an empty list

Either the topic you passed matches no source, or the record list was empty. Both are
silent by design. Check `topology.sourceTopics()`.

### Output records have timestamp `0` or a null key

`groupBy` drops the metadata columns, so a downstream sink has no `__timestamp` or
`__key` to read. Group by those columns, or add them back with `withColumns`. `BlobCodec`
always produces a null key by design, because it emits batches rather than keyed rows.

### The broker rejects a produced record as too large

`BlobCodec` enforces a 900 KiB hard cap by default and rejects a single row that cannot
fit. Raise both `maxRecordBytes` and the broker's `max.message.bytes`, or reduce row
size.

### Offsets never advance

Check that the poll returned records and that `commitSync` succeeded. Group runners
commit every processed partition after producer flush; transactional runners commit
offsets through the producer transaction.

## Build

### `-Werror` fails on a warning

Compilation uses `-Xlint:all -Werror`. Fix the warning, or scope a `@SuppressWarnings`
to the smallest possible element, which is what `ColumnarRunnerTest` does for Kafka's
deprecated `MockConsumer` constructor.

### Javadoc fails

Javadoc runs with `Xdoclint:all,-missing`. Missing comments are fine; malformed HTML,
bad `@link` targets, and broken tags are not.

### A release job fails on missing credentials

The release workflow asserts `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`,
`SIGNING_KEY`, and `SIGNING_PASSWORD` before building. Signing is skipped entirely when
`SIGNING_KEY` is blank, which is why local builds do not need a key. See
[Build and release](build-and-release.md).
