# Limitations

What `1.0.0` does not do, and what to do instead. See [PARITY.md](../PARITY.md) for the
feature checklist and [CHANGELOG.md](../CHANGELOG.md) for what shipped.

## Columnar processing

### No state between batches

The columnar API keeps nothing across fetched batches. Joins, windows, and aggregates
operate only on the rows in the current partition batch. A `groupBy` on a key that
appears in two consecutive fetches produces two independent result rows.

Instead, sink the partial aggregates to a topic and combine them with a Kafka
Streams application, which does have durable state, interactive queries, and standby
replicas.

### No consumer group participation

`ColumnarRunner` assigns a single topic-partition explicitly and never subscribes. There
is no rebalancing, no group membership, and no automatic partition discovery.

Instead, run one runner per partition and assign partitions yourself, or drive
`BuiltColumnarTopology.runBatch` from your own group-managed consumer loop.

### No offset commits

`runPartitionOnce` returns the next offset and stores nothing. Delivery is at-least-once
only if you persist that offset after the producer flush.

Instead, persist the returned offset in your own store, or use a transactional
producer and write offsets inside the transaction for exactly-once.

### Sinks always re-encode

There is no pass-through sink. Every sink runs its codec, which serializes the batch
again even when nothing changed since decode.

### Fan-in is not expressed

Every node has exactly one parent. A node cannot merge two upstream branches, and one
source can only be fed by one `runBatch` call for one topic at a time.

Instead, run the branches as separate topologies and merge downstream, or write a
custom processor that reads whatever it needs from a single batch.

### Blob records are split, never merged

`BlobCodec` splits an over-sized batch into several records but never combines small
batches. A batch whose *single row* exceeds `maxRecordBytes` is emitted at full size and
may be rejected by the broker.

### Aggregate output types are fixed

`COUNT` and integral `SUM` always produce `Int(64)`; floating-point `SUM` always
produces `DOUBLE`. There is no overflow detection on integral sums, and no decimal
support.

### Writable Arrow types are limited

`withColumns` and `groupBy` write values through a fixed coercion table covering UTF-8,
binary, signed and unsigned integers, floats, and booleans. Dates, timestamps, decimals,
lists, structs, unions, and dictionaries are not writable and throw
`cannot write Arrow type ...`.

Instead, write a custom `ColumnarProcessor` that fills the vector directly, or
carry the value as UTF-8 text.

### JsonRowBridge infers types from the first sample

Column types come from the first non-null value seen for that field in the batch.
Numeric columns are always 64-bit, floating-point columns always double, all-null
columns become UTF-8, and nested objects and arrays are stored as JSON text. Two
batches of the same data can therefore produce different schemas if the first non-null
value differs, which matters because `BlobCodec` requires one schema per fetched batch.

### RowCodec passes an empty topic name to serdes

`RowCodec` calls the value serde with `""` as the topic, so any serde that derives state
from the topic name resolves the subject `-value`. That includes all of the
schema-registry serdes.

Instead, use topic-independent serdes with `RowCodec`, or seed the cache under that
subject.

### Reserved column names

`__key`, `__timestamp`, `__partition`, and `__offset` cannot appear in a payload schema.
The decode path rejects a collision rather than renaming.

### Thread confinement

`BuiltColumnarTopology`, Arrow allocators, and `VectorSchemaRoot` instances are all
single-threaded. Parallelism comes from running one topology per thread rather than
sharing one.

## Schema registry and serdes

### Protobuf schema printing is partial

`ProtobufSchemaPrinter` emits the syntax line, package, and top-level messages with
their fields, including `repeated`, `map`, and proto2 labels. It omits nested message
definitions, enum definitions, `oneof` blocks, imports, options, services, and
extensions, and it rejects group fields.

Instead, register the real `.proto` text with `KrabkaSchemaRegistryClient.register`
and pin the resulting ID with `cache.seedSubjectId`.

### Protobuf message indexes assume a top-level message

The index path is `[descriptor.getIndex()]`. A nested message needs a multi-element
path, which the serde does not compute, although `ConfluentWireFormat.encodeProtobuf`
accepts one if you frame records yourself.

### No schema compatibility checks

The client registers, looks up, and fetches. It does not read or set compatibility
levels, list subjects or versions, delete subjects, or resolve schema references.

### No registry context path

Requests resolve against the origin, so a base URI with a path prefix
(`https://host/registry`) loses that prefix. Point the client at the registry root.

### No built-in retries or authentication helpers

The client never retries. Authentication, TLS, proxies, and timeouts are configured on
the `HttpClient` you supply to the three-argument constructor; there is no
`basic.auth.user.info`-style shortcut.

### JSON Schema validation is one-directional

`validate` applies to deserialization only, against the writer's schema. Serialization
is never validated, so a producer can emit a document that does not satisfy its own
registered schema.

### JSON Schema uses one dialect

Validation always uses Draft 2020-12. Schemas written for an earlier draft are validated
under 2020-12 semantics.

### Avro reflection is not supported

Only `SpecificRecord` classes and `GenericRecord` are wired up; there is no
`ReflectDatumWriter` path.

### The subject strategy is per-cache, not per-serde

A `SchemaCache` holds one `SubjectNameStrategy`. Mixing topic-name and record-name
strategies in one application requires more than one cache.

### `prewarm` is all-or-nothing

One failing subject fails the returned future. There is no partial-success report; use
`idForSubject` afterwards to see what resolved.

## Kafka Streams surface

### The streams group protocol requires a capable broker

`group.protocol=streams` needs a broker with the streams rebalance protocol enabled and
`streams.version=1` finalized. Against an older broker, set `group.protocol=classic`
explicitly. `withDefaults` will not override you.

### Nothing else is added

State stores, punctuators, interactive queries, and processing guarantees are Apache
Kafka's, unchanged. Their limitations are upstream limitations, and their configuration
is upstream configuration.

## Build and packaging

- Java 17 is the floor; the artifacts are compiled with `--release 17`.
- No Java Platform Module System descriptors (`module-info.java`) are published.
- No BOM or version-catalog artifact is published; pin versions yourself.
- No shaded or relocated artifacts; Jackson, Avro, Protobuf, and Arrow reach your
  classpath at the versions listed in [Architecture](architecture.md#version-pinning).
