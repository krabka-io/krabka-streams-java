# Columnar processing

`krabka-streams-columnar` processes Kafka records as Apache Arrow batches instead of
one record at a time. It is a separate execution model from Kafka Streams: it does not
build a Kafka `Topology`, but its runner can participate in a consumer group and retain
partition-local state. It is meant for analytical work where per-record dispatch
dominates the cost.

## The batch model

One fetched topic-partition batch is one processing unit. Records arrive from a
consumer poll, are decoded into a single `VectorSchemaRoot`, flow through operators as
whole batches, and are encoded back into records at a sink.

```text
consumer.poll ──► List<ConsumedRecord> ──► BatchCodec.decode ──► VectorSchemaRoot
                                                                       │
                                                              operators (whole batches)
                                                                       │
                       List<ProducedToTopic) ◄── BatchCodec.encode ◄───┘
```

The processor instance created when a topology is built survives across calls to
`runBatch`. Built-in `groupBy` therefore accumulates across batches, and custom joins
or windows can keep state in their `ColumnarProcessor`. Build once and reuse the
`BuiltColumnarTopology` when state must survive polls. Processor instances are isolated
by logical partition number, so co-partitioned source topics can join without sharing
state with another partition.

## Reserved metadata columns

Every decoded batch carries five columns that hold Kafka record metadata:

| Column        | Arrow type         | Contents                                      |
| ------------- | ------------------ | --------------------------------------------- |
| `__key`       | `Binary`, nullable | record key bytes, `null` for a keyless record |
| `__timestamp` | `Int(64, signed)`  | record timestamp                              |
| `__partition` | `Int(32, signed)`  | source partition                              |
| `__offset`    | `Int(64, signed)`  | source offset                                 |
| `__headers`   | `Binary`           | ordered Kafka headers, including null values  |

They are appended after the payload columns, in that order. A colliding payload name
is escaped in the processing batch; `BlobCodec.payloadColumn(name)` returns that name.
The sink restores the original payload name before encoding.

The names are also exposed as constants (`BlobCodec.KEY_COLUMN`, `TIMESTAMP_COLUMN`,
`PARTITION_COLUMN`, `OFFSET_COLUMN`, and `HEADERS_COLUMN`) so you can reference them
without hardcoding strings.

Under `BlobCodec`, one Kafka record expands to many rows, and each of those rows
receives a copy of that record's metadata. That is what makes `__offset` useful: it
tells you which record a row came from.

## Codecs

```java
public interface BatchCodec {
  VectorSchemaRoot decode(List<ConsumedRecord> records);

  List<ProduceRecord> encode(VectorSchemaRoot batch);
}
```

`decode` turns a fetched batch into one Arrow batch. `encode` turns an Arrow batch back
into records. Two implementations ship with the module.

### BlobCodec, for records that are already Arrow

Use this when producers write Arrow IPC streams as record values, so each Kafka record
already holds many rows.

```java
var codec = new BlobCodec(allocator); // 900 KiB soft cap
var tight = new BlobCodec(allocator, 512 * 1024); // custom cap
```

Decoding reads each record value as an Arrow IPC stream, attaches metadata columns, and
concatenates the results. All records in the batch must share one payload schema;
otherwise the codec throws `Arrow batch schemas differ`. A record that fails to decode
reports its position: `cannot decode Arrow record 3`. An empty record list throws
`decode called with an empty record batch`.

Encoding drops the metadata columns and serializes the payload as Arrow IPC records.
It packs the largest consecutive rows that fit under `maxRecordBytes` and share one
key, timestamp, and header list, then applies that envelope to the output record. A
single row that exceeds the cap throws `ColumnarException` instead of sending a record
the broker will reject.

`DEFAULT_MAX_RECORD_BYTES` is `900 * 1024`, chosen to stay under a 1 MiB broker message
limit with room for headers and framing. Raise it only alongside `max.message.bytes`.

### RowCodec, for ordinary Kafka records

Use this when records hold one value each and you want a columnar view of them.

```java
var codec = new RowCodec<>(Serdes.String(), new JsonRowBridge<>(String.class), allocator);
```

Decoding deserializes each record value with the supplied `Serde`, converts the list of
values into Arrow columns through a `RowBridge`, and attaches metadata. Encoding
reverses it: the payload columns become typed rows again, each row becomes one record,
the key comes from `__key`, and the timestamp comes from `__timestamp` (or `0`).

Row count is preserved in both directions, so a batch of _n_ records decodes to _n_
rows and encodes back to _n_ records.

Topologies pass the source or sink topic into `RowCodec`, so topic-derived schema
subjects work the same way here as in ordinary Kafka consumers and producers.
Keys, timestamps, and ordered headers round-trip through their reserved columns.

Wrap any codec in `GzipBatchCodec` for per-record GZIP compression. The default
decompression ceiling is 16 MiB and can be overridden in the constructor.

### RowBridge and JsonRowBridge

```java
public interface RowBridge<T> {
  VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator);

  List<T> batchToRows(VectorSchemaRoot batch);
}
```

`JsonRowBridge<T>` implements it by routing values through Jackson's tree model.

```java
record Order(String id, long amount, List<String> tags) {}

var bridge = new JsonRowBridge<>(Order.class);
try (var batch = bridge.rowsToBatch(orders, allocator)) {
  List<Order> back = bridge.batchToRows(batch);
}
```

Column inference uses the first non-null sample and is retained by the bridge for every
later batch. To pin it up front, pass an Arrow `Schema` to the constructor.

`JsonRowBridge.fromJsonSchema(type, schema)` derives fields from JSON Schema
`properties`, `required`, local `$ref`, primitive types, arrays, objects, and base64
content. Required fields reject nulls before a batch can escape.

| JSON node         | Arrow type              | Notes                                           |
| ----------------- | ----------------------- | ----------------------------------------------- |
| text, or all-null | `Utf8`                  | all-null columns default to `Utf8`              |
| integral number   | `Int(64, signed)`       | always 64-bit                                   |
| floating point    | `FloatingPoint(DOUBLE)` | always double                                   |
| boolean           | `Bool`                  |                                                 |
| binary            | `Binary`                | tagged with metadata `krabka.binary=true`       |
| object or array   | `Utf8`                  | serialized JSON text, tagged `krabka.json=true` |

Column order follows first appearance across the rows. Nested structures survive a
round trip because the field metadata records that the column holds JSON text.

Scalar types (primitives, arrays, `CharSequence`, `Number`, and `Boolean`) have no
fields to spread across columns, so they are wrapped in a single column named `value`.
That is why the batch in `RowCodec` with `JsonRowBridge<>(String.class)` has the schema
`value, __key, __timestamp, __partition, __offset, __headers`.

A row that cannot be converted back throws
`cannot convert Arrow row 2 to com.example.Order`.

### ArrowIpcSerde

A plain Kafka `Serde<VectorSchemaRoot>` over the Arrow IPC stream format. `BlobCodec`
uses it internally, and you can use it directly to read or write Arrow-valued topics.

```java
var serde = new ArrowIpcSerde(allocator);
byte[] bytes = serde.serializer().serialize("transactions", batch);
try (var decoded = serde.deserializer().deserialize("transactions", bytes)) {
  // decoded is owned by the caller
}
```

Each serialized value contains exactly one record batch. Deserialization reads the
first batch, copies it into a root owned by your allocator, and returns it. **The
caller must close it.** A stream with no record batch, or bytes that are not an Arrow
stream, throw `SerializationException`.

The deserializer rejects being configured as a key serde: Arrow batches are record
values.

## Topologies

`ColumnarTopology` is a builder. Nodes are added in order, each non-source node names a
parent, and `build()` validates the result.

```java
var topology = new ColumnarTopology(allocator);
var source = topology.addSource("source", List.of("transactions"), codec);
var large = topology.addOperator("large", BuiltinOp.filter(allocator, predicate), source);
topology.addSink("archive", "large-transactions", codec, large);
topology.addSink("audit", "audit-log", codec, large); // fan-out
var built = topology.build();
```

| Method                                                             | Purpose                                                                      |
| ------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| `addSource(name, topics, codec)`                                   | Decodes records for any of `topics`. At least one topic is required.         |
| `addOperator(name, BuiltinOp, parent)`                             | Adds a built-in operator.                                                    |
| `addOperator(name, Supplier<? extends ColumnarProcessor>, parent)` | Adds a custom processor; its supplier is invoked once per logical partition. |
| `addMerge(name, parents)`                                          | Concatenates two or more same-schema upstream branches.                      |
| `addJoin(name, join, left, right)`                                 | Stateful co-partitioned inner equi-join within an event-time window.         |
| `addSink(name, topic, codec, parent)`                              | Encodes its parent's batches to `topic`.                                     |
| `addPassThroughSink(name, topic, source)`                          | Copies source records byte-for-byte without codec work.                      |
| `sourceTopics()`                                                   | Every topic named by any source, for subscribing a consumer.                 |
| `validate()`                                                       | Checks the graph; also called by `build()`.                                  |
| `build()`                                                          | Returns a reusable `BuiltColumnarTopology`.                                  |

`addSource`, `addOperator`, and `addSink` return a `ColumnarNode`, an opaque handle you
pass as the parent of later nodes. A node from a different topology throws
`IllegalArgumentException("parent is not a node in this topology")`.

`validate()` enforces four rules and throws `ColumnarException` otherwise:

- node names are unique: ``duplicate node name `same` ``;
- every non-source node has a parent that was added earlier:
  ``node `x` has an invalid parent``;
- at least one source exists: `topology has no source`;
- at least one sink exists: `topology has no sink`.

Because a parent must already exist when a child is added, cycles are impossible by
construction.

### Execution

`BuiltColumnarTopology.runBatch(topic, records)` evaluates nodes in the order they were
added and returns the produced records:

```java
List<ProducedToTopic> produced = built.runBatch("transactions", records);
for (var output : produced) {
  output.topic(); // sink topic
  output.record().key(); // byte[] or null
  output.record().value();
  output.record().timestamp();
}
```

`runBatches(Map<String, List<ConsumedRecord>>)` evaluates several source topics
together, which lets `addMerge` express fan-in across topics. Group runners use this
form for each consumer poll.

Each logical partition gets fresh operator and join instances. `snapshotPartition`,
`restorePartition`, and `releasePartition` expose their lifecycle; `close()` releases
every partition. Join state is bounded by its event-time window.

Semantics worth knowing:

- A source produces a batch only when `records` is non-empty **and** the source lists
  the topic you passed. Other sources yield nothing, and their descendants produce
  nothing. Passing a topic no source declares returns an empty list.
- Each operator receives a private copy of its parent's batch, so fan-out is safe: two
  children of one node cannot observe each other's mutations.
- An operator may forward zero, one, or many batches. Downstream nodes run once per
  forwarded batch.
- A sink encodes every batch its parent produced, appending one `ProducedToTopic` per
  encoded record.
- A built topology is reusable, retains processor state, and serializes concurrent
  `runBatch` calls so Arrow roots are never shared concurrently.
- All intermediate batches are closed before `runBatch` returns, including on the
  exception path.

### Records

```java
public record RecordHeader(String key, byte[] value) {}

public record ConsumedRecord(..., List<RecordHeader> headers) {}

public record ProduceRecord(..., List<RecordHeader> headers) {}

public record ProducedToTopic(String topic, ProduceRecord record) {}
```

The records copy arrays and header lists at the boundary. Backward-compatible
constructors omit `headers` and use an empty list.

## ColumnarRunner

`ColumnarRunner` supports explicit single-partition cycles and reusable consumer-group
runners.

```java
long next =
    ColumnarRunner.runPartitionOnce(
        topology, consumer, producer, "transactions", 0, offset, Duration.ofMillis(250));
```

In order, it:

1. assigns the consumer to exactly `topic-partition` and seeks to `offset`;
2. polls once with `pollTimeout` and takes the records for that partition;
3. returns `offset` unchanged if the poll was empty, without producing anything;
4. converts the records to `ConsumedRecord` (a `null` value becomes `new byte[0]`);
5. runs the topology (an overload accepts a pre-built instance);
6. sends each produced record, passing `null` for a negative timestamp so the producer
   applies its own;
7. waits for every asynchronous producer acknowledgement;
8. commits and returns the highest consumed offset plus one.

For automatic partition discovery and rebalancing, create a reusable group runner. It
subscribes to every source topic and retains one built topology across polls:

```java
var runner = ColumnarRunner.group(topology, consumer, producer);
while (running) {
  runner.runOnce(Duration.ofMillis(250));
}
```

`runner.runOnceTransactional(timeout)` sends produced records and consumed offsets in
one producer transaction. Configure and initialize the transactional producer before
creating the runner.

The full `group` overload accepts `ColumnarErrorPolicy`, `ColumnarStateStore`, and
`ColumnarMetrics`. Policies are fail-fast, explicit skip, or dead-letter; failed state
is rolled back before the policy is applied. `FileColumnarStateStore` atomically saves
partition snapshots on revocation and restores them on assignment. Lost partitions
are released without saving. `sendAsync(outputs, producer)` exposes the acknowledged,
non-flushing producer flow directly.

## Next

- [Columnar operators](columnar-operators.md): the built-in operators, custom
  processors, and the buffer ownership rules.
- [Testing](testing.md): running a columnar topology without a broker.
