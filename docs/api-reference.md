# API reference

Every public type in `1.1.0`, grouped by module. Types not listed here are
package-private implementation details and are not part of the compatibility surface.

Javadoc is published alongside each artifact (`-javadoc.jar`) and is generated with
`Xdoclint:all,-missing` plus `-Werror`. Every package summary includes a runnable-style
usage example.

---

## `krabka-streams`

Package `io.krabka.streams`.

### KrabkaStreamsConfig

`public final class`. A utility class that cannot be instantiated.

| Member                   | Signature                                                   |
| ------------------------ | ----------------------------------------------------------- |
| `GROUP_PROTOCOL_CONFIG`  | `public static final String` = `"group.protocol"`           |
| `STREAMS_GROUP_PROTOCOL` | `public static final String` = `"streams"`                  |
| `withDefaults`           | `public static Properties withDefaults(Map<?, ?> settings)` |

`withDefaults` copies `settings` into a new `Properties` and applies krabka defaults
with `putIfAbsent`. Throws `NullPointerException` for a null argument.

Everything else in this module is the Apache Kafka Streams 4.3.1 API, re-exported as an
`api` dependency.

---

## `krabka-streams-schema-serde`

Package `io.krabka.streams.schema`.

### KrabkaSchemaRegistryClient

`public final class`. An asynchronous Confluent Schema Registry REST client.

| Member       | Signature                                                                                                 |
| ------------ | --------------------------------------------------------------------------------------------------------- |
| constructor  | `KrabkaSchemaRegistryClient(URI baseUri)`                                                                 |
| constructor  | `KrabkaSchemaRegistryClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper)`               |
| constructor  | `KrabkaSchemaRegistryClient(URI baseUri, String username, String password)`                               |
| constructor  | injected client plus `int maxRetries`                                                                     |
| `register`   | `CompletableFuture<Integer> register(String subject, SchemaKind kind, String schema, String messageType)` |
| `lookup`     | `CompletableFuture<Integer> lookup(String subject, SchemaKind kind, String schema, String messageType)`   |
| `latest`     | `CompletableFuture<RegisteredSchema> latest(String subject)`                                              |
| `latestId`   | `CompletableFuture<Integer> latestId(String subject)`                                                     |
| `schemaById` | `CompletableFuture<FetchedSchema> schemaById(int schemaId)`                                               |
| management   | subjects, versions, compatibility, deletion, and reference resolution methods                             |

Nested records:

```java
public record SchemaReference(String name, String subject, int version) {}

public record RegisteredSchema(..., List<SchemaReference> references) {}

public record FetchedSchema(String schema, String messageType, List<SchemaReference> references) {}
```

Failures complete the future exceptionally with `SchemaRegistryException`.

### SchemaCache

`public final class`. A thread-safe store of resolved schema IDs and writer schemas.

| Member                  | Signature                                                                                                            |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------- |
| constructor             | `SchemaCache(KrabkaSchemaRegistryClient client)`, which uses `AUTO_REGISTER` and `TopicNameStrategy`                 |
| constructor             | `SchemaCache(KrabkaSchemaRegistryClient client, RegisterMode registerMode, SubjectNameStrategy subjectNameStrategy)` |
| `subject`               | `String subject(String topic, Role role)`                                                                            |
| `intern`                | `void intern(String subject, SchemaKind kind, String schema, String messageType)`, idempotent by subject             |
| `prewarm`               | `CompletableFuture<Void> prewarm()`                                                                                  |
| `prewarmReport`         | `CompletableFuture<PrewarmReport> prewarmReport()`                                                                   |
| `idForSubject`          | `OptionalInt idForSubject(String subject)`                                                                           |
| `writerSchema`          | `String writerSchema(int schemaId)`, which throws `SchemaFetchPendingException` on a miss                            |
| `writerMessageType`     | `String writerMessageType(int schemaId)`, `null` when unknown                                                        |
| `writerReferences`      | `Map<String, String> writerReferences(int schemaId)`                                                                 |
| `seedSubjectId`         | `void seedSubjectId(String subject, int schemaId)`                                                                   |
| `seedWriterSchema`      | `void seedWriterSchema(int schemaId, String schema)`                                                                 |
| `seedWriterMessageType` | `void seedWriterMessageType(int schemaId, String messageType)`                                                       |

### AvroSerde&lt;T&gt;

`public final class ... implements Serde<T>`

| Member                        | Signature                                                                                   |
| ----------------------------- | ------------------------------------------------------------------------------------------- |
| `forValue`                    | `static <T extends SpecificRecord> AvroSerde<T> forValue(Class<T> type, SchemaCache cache)` |
| `forKey`                      | `static <T extends SpecificRecord> AvroSerde<T> forKey(Class<T> type, SchemaCache cache)`   |
| `generic`                     | `static AvroSerde<GenericRecord> generic(Schema schema, SchemaCache cache, Role role)`      |
| `reflect`                     | `static <T> AvroSerde<T> reflect(Class<T> type, SchemaCache cache, Role role)`              |
| `registerSubject`             | `void registerSubject(String topic)`                                                        |
| `serializer` / `deserializer` | from `Serde<T>`                                                                             |

Registers the Avro canonical parsing form. Deserialization performs writer/reader schema
resolution.

### ProtobufSerde&lt;T extends Message&gt;

`public final class ... implements Serde<T>`

| Member            | Signature                                                                                    |
| ----------------- | -------------------------------------------------------------------------------------------- |
| `forValue`        | `static <T extends Message> ProtobufSerde<T> forValue(T defaultInstance, SchemaCache cache)` |
| `forKey`          | `static <T extends Message> ProtobufSerde<T> forKey(T defaultInstance, SchemaCache cache)`   |
| `registerSubject` | `void registerSubject(String topic)`                                                         |

Uses the Protobuf message-index framing and verifies the writer's `messageType`.

### JsonSchemaSerde&lt;T&gt;

`public final class ... implements Serde<T>`

| Member            | Signature                                                                                                                              |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `forValue`        | `static <T> JsonSchemaSerde<T> forValue(Class<T> type, String schema, SchemaCache cache, boolean validate)`                            |
| `forKey`          | `static <T> JsonSchemaSerde<T> forKey(Class<T> type, String schema, SchemaCache cache, boolean validate)`                              |
| `forValue`        | `static <T> JsonSchemaSerde<T> forValue(Class<T> type, String schema, SchemaCache cache, boolean validate, ObjectMapper objectMapper)` |
| `registerSubject` | `void registerSubject(String topic)`                                                                                                   |

`validate` applies in both directions. `$schema` selects Draft 4, 6, 7, 2019-09, or
2020-12; an extended factory accepts an explicit dialect, strategy, and mapper.

### ConfluentWireFormat

`public final class`. Framing helpers.

| Member           | Signature                                                                               |
| ---------------- | --------------------------------------------------------------------------------------- |
| `MAGIC`          | `public static final byte` = `0`                                                        |
| `encode`         | `static byte[] encode(int schemaId, byte[] body)`                                       |
| `decode`         | `static Frame decode(byte[] bytes)`                                                     |
| `encodeProtobuf` | `static byte[] encodeProtobuf(int schemaId, List<Integer> messageIndexes, byte[] body)` |
| `decodeProtobuf` | `static ProtobufFrame decodeProtobuf(byte[] bytes)`                                     |

```java
public record Frame(int schemaId, byte[] body) {}

public record ProtobufFrame(int schemaId, List<Integer> messageIndexes, byte[] body) {}
```

Both records copy `body` on construction and on access.

### LocalSchemaCompatibility

Network-free pairwise checks for `BACKWARD`, `FORWARD`, and `FULL` modes. `avro` and
`json` accept schema text; `protobuf` accepts two `FileDescriptor` values. Every check
returns `Result(boolean compatible, List<String> incompatibilities)`.

### Enums and interfaces

| Type                  | Values / members                                                                   |
| --------------------- | ---------------------------------------------------------------------------------- |
| `Role`                | `KEY`, `VALUE`                                                                     |
| `SchemaKind`          | `AVRO` (no wire name), `PROTOBUF`, `JSON`                                          |
| `RegisterMode`        | `AUTO_REGISTER`, `LOOKUP_ONLY`, `USE_LATEST`                                       |
| `SubjectNameStrategy` | `@FunctionalInterface String subject(String topic, Role role)`                     |
| `TopicNameStrategy`   | `implements SubjectNameStrategy`, producing `topic + "-key"` or `topic + "-value"` |

### Exceptions

| Type                          | Extends                                             | Members                                                                                                                                                           |
| ----------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SchemaRegistryException`     | `RuntimeException`                                  | `SchemaRegistryException(String)`, `(String, Throwable)`, `(int statusCode, String body)`; `int statusCode()`, which returns `-1` for transport or parsing errors |
| `SchemaFetchPendingException` | `org.apache.kafka.common.errors.RetriableException` | `SchemaFetchPendingException(int schemaId)`; `int schemaId()`                                                                                                     |

---

## `krabka-streams-columnar`

Package `io.krabka.streams.columnar`.

### ColumnarTopology

`public final class`. The topology builder.

| Member               | Signature                                                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------------- |
| constructor          | `ColumnarTopology(BufferAllocator allocator)`                                                                 |
| `addSource`          | `ColumnarNode addSource(String name, Collection<String> topics, BatchCodec codec)`                            |
| `addOperator`        | `ColumnarNode addOperator(String name, BuiltinOp operator, ColumnarNode parent)`                              |
| `addOperator`        | `ColumnarNode addOperator(String name, Supplier<? extends ColumnarProcessor> processor, ColumnarNode parent)` |
| `addMerge`           | `ColumnarNode addMerge(String name, Collection<ColumnarNode> parents)`                                        |
| `addJoin`            | `ColumnarNode addJoin(String name, ColumnarJoin join, ColumnarNode left, ColumnarNode right)`                 |
| `addSink`            | `ColumnarNode addSink(String name, String topic, BatchCodec codec, ColumnarNode parent)`                      |
| `addPassThroughSink` | `ColumnarNode addPassThroughSink(String name, String topic, ColumnarNode source)`                             |
| `sourceTopics`       | `List<String> sourceTopics()`                                                                                 |
| `validate`           | `void validate()`, which throws `ColumnarException`                                                           |
| `build`              | `BuiltColumnarTopology build()`                                                                               |

### BuiltColumnarTopology

`public final class`. Validated, stateful, reusable, and synchronized.

| Member                | Signature                                                                    |
| --------------------- | ---------------------------------------------------------------------------- |
| `runBatch`            | `List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records)` |
| `runBatches`          | `List<ProducedToTopic> runBatches(Map<String, List<ConsumedRecord>> input)`  |
| `runPartitionBatches` | evaluates one co-partitioned input map                                       |
| state lifecycle       | `snapshotPartition`, `restorePartition`, `releasePartition`, and `close`     |

### ColumnarNode

`public final class`. An opaque parent handle with no public members.

### ColumnarRunner

`public final class`. A utility class.

```java
public static long runPartitionOnce(
    ColumnarTopology topology,
    Consumer<byte[], byte[]> consumer,
    Producer<byte[], byte[]> producer,
    String topic,
    int partition,
    long offset,
    Duration pollTimeout)
```

Commits and returns the next offset. `group(...)` creates a subscribed reusable runner;
group runners provide ordinary and transactional `runOnce` methods. The full overload
accepts `ColumnarErrorPolicy`, `ColumnarStateStore`, and `ColumnarMetrics`.
`sendAsync` returns a `CompletableFuture<Void>` for broker acknowledgements.

Runner support types:

- `ColumnarErrorPolicy`: `fail()`, `skip()`, or `deadLetter(topic)`.
- `ColumnarMetrics`: lock-free counters exposed through an immutable `Snapshot`.
- `ColumnarStateStore`: partition `load` and `save`; `none()` is ephemeral.
- `FileColumnarStateStore`: atomically replaced snapshot files under a caller-owned path.

### Codecs

| Type             | Signature                                                                                                               |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `BatchCodec`     | `interface`: decode/encode methods, with default topic-aware overloads                                                  |
| `BlobCodec`      | `final class implements BatchCodec`: `BlobCodec(BufferAllocator)`, `BlobCodec(BufferAllocator, int maxRecordBytes)`     |
| `GzipBatchCodec` | `final class implements BatchCodec`: bounded GZIP decorator for another codec                                           |
| `RowCodec<T>`    | `final class implements BatchCodec`: `RowCodec(Serde<T> valueSerde, RowBridge<T> rowBridge, BufferAllocator allocator)` |
| `ArrowIpcSerde`  | `final class implements Serde<VectorSchemaRoot>`: `ArrowIpcSerde(BufferAllocator)`                                      |

`BlobCodec` constants: `DEFAULT_MAX_RECORD_BYTES` (`900 * 1024`), `KEY_COLUMN`
(`__key`), `TIMESTAMP_COLUMN` (`__timestamp`), `PARTITION_COLUMN` (`__partition`),
`OFFSET_COLUMN` (`__offset`), `HEADERS_COLUMN` (`__headers`), plus
`payloadColumn(String)` for escaped collisions.

### Row bridges

| Type               | Signature                                                                                                           |
| ------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `RowBridge<T>`     | `interface`: `VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator)`, `List<T> batchToRows(VectorSchemaRoot)` |
| `JsonRowBridge<T>` | constructors accept `Class<T>`, optional `ObjectMapper` or Arrow `Schema`; `fromJsonSchema` derives fields          |

### Operators

| Type                        | Signature                                                                                                       |
| --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `ColumnarProcessor`         | `@FunctionalInterface void process(ColumnarContext context, VectorSchemaRoot batch)`                            |
| `StatefulColumnarProcessor` | processor plus `snapshot()` and `restore(byte[])`                                                               |
| `ColumnarContext`           | `final class`: `void forward(VectorSchemaRoot batch)`                                                           |
| `RowPredicate`              | `@FunctionalInterface boolean test(VectorSchemaRoot batch, int row)`                                            |
| `RowValue`                  | `@FunctionalInterface Object value(VectorSchemaRoot batch, int row)`                                            |
| `DerivedColumn`             | `record DerivedColumn(Field field, RowValue value)`                                                             |
| `Aggregation`               | `record Aggregation(String inputColumn, String outputColumn, AggregateFunction function, ArrowType outputType)` |
| `AggregateFunction`         | `enum`: `COUNT`, `SUM`, `MIN`, `MAX`                                                                            |

`BuiltinOp` is a `public final class implements ColumnarProcessor`:

```java
static BuiltinOp filter(BufferAllocator allocator, RowPredicate predicate)
static BuiltinOp select(BufferAllocator allocator, String... columns)
static BuiltinOp withColumns(BufferAllocator allocator, DerivedColumn... columns)
static BuiltinOp groupBy(
    BufferAllocator allocator, Collection<String> keys, Aggregation... aggregations)
static BuiltinOp windowedGroupBy(
    BufferAllocator allocator,
    Collection<String> keys,
    Duration size,
    Aggregation... aggregations)
static BuiltinOp windowedGroupBy(
    BufferAllocator allocator,
    Collection<String> keys,
    Duration size,
    Duration retention,
    Aggregation... aggregations)
void process(ColumnarContext context, VectorSchemaRoot batch)
```

### Records and exceptions

```java
public record RecordHeader(String key, byte[] value) {}

public record ConsumedRecord(..., List<RecordHeader> headers) {}

public record ProduceRecord(..., List<RecordHeader> headers) {}

public record ProducedToTopic(String topic, ProduceRecord record) {}

public record ColumnarJoin(
    String leftKey, String rightKey, Duration window, String leftPrefix, String rightPrefix) {}

public final class ColumnarException extends RuntimeException {
  public ColumnarException(String message);

  public ColumnarException(String message, Throwable cause);
}
```

`ConsumedRecord` and `ProduceRecord` copy their byte arrays on construction and on
access. `value` must not be null; `key` may be.

---

## `krabka-streams-test-utils`

Package `io.krabka.streams.test`. See [Testing](testing.md) for usage.

### ColumnarTestDriver

`public final class`

| Member          | Signature                                                                       |
| --------------- | ------------------------------------------------------------------------------- |
| constructor     | `ColumnarTestDriver(BuiltColumnarTopology topology)`                            |
| `pipeInput`     | overloads accept record bytes and optional `List<RecordHeader>`                 |
| `pipeBatch`     | `void pipeBatch(String topic, List<ConsumedRecord> records)`                    |
| `failNext`      | injects one `RuntimeException` before the next batch evaluation                 |
| `isOutputEmpty` | `boolean isOutputEmpty(String topic)`                                           |
| `outputSize`    | `int outputSize(String topic)`                                                  |
| `readOutput`    | `ProduceRecord readOutput(String topic)`, which throws `NoSuchElementException` |
| `drainOutput`   | `List<ProduceRecord> drainOutput(String topic)`                                 |

### SchemaRegistryStub

`public final class ... implements AutoCloseable`

| Member         | Signature                                                                               |
| -------------- | --------------------------------------------------------------------------------------- |
| constructor    | `SchemaRegistryStub() throws IOException`, which binds `127.0.0.1` on an ephemeral port |
| `uri`          | `URI uri()`                                                                             |
| `requestCount` | `int requestCount(String method, String path)`                                          |
| `close`        | `void close()`                                                                          |

This module also re-exports Apache Kafka's `kafka-streams-test-utils`, including
`TopologyTestDriver`.

---

## Compatibility

Nothing is deprecated yet. The public surface above is what future releases will be
judged against; package-private types
(`AbstractSchemaSerde`, `ArrowBatchSupport`, `ProtobufSchemaPrinter`) may change at any
time.

Transitive `api` dependencies are part of the surface too: Kafka Streams 4.3.1, Avro
1.12.1, Protobuf 4.33.5, Jackson 2.22.0, and Arrow 19.0.0.
