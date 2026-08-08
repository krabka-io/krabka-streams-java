# API reference

Every public type in `1.0.0`, grouped by module. Types not listed here are
package-private implementation details and are not part of the compatibility surface.

Javadoc is published alongside each artifact (`-javadoc.jar`) and is generated with
`Xdoclint:all,-missing`.

---

## `krabka-streams`

Package `io.krabka.streams`.

### KrabkaStreamsConfig

`public final class`. A utility class that cannot be instantiated.

| Member | Signature |
| --- | --- |
| `GROUP_PROTOCOL_CONFIG` | `public static final String` = `"group.protocol"` |
| `STREAMS_GROUP_PROTOCOL` | `public static final String` = `"streams"` |
| `withDefaults` | `public static Properties withDefaults(Map<?, ?> settings)` |

`withDefaults` copies `settings` into a new `Properties` and applies krabka defaults
with `putIfAbsent`. Throws `NullPointerException` for a null argument.

Everything else in this module is the Apache Kafka Streams 4.3.1 API, re-exported as an
`api` dependency.

---

## `krabka-streams-schema-serde`

Package `io.krabka.streams.schema`.

### KrabkaSchemaRegistryClient

`public final class`. An asynchronous Confluent Schema Registry REST client.

| Member | Signature |
| --- | --- |
| constructor | `KrabkaSchemaRegistryClient(URI baseUri)` |
| constructor | `KrabkaSchemaRegistryClient(URI baseUri, HttpClient httpClient, ObjectMapper objectMapper)` |
| `register` | `CompletableFuture<Integer> register(String subject, SchemaKind kind, String schema, String messageType)` |
| `lookup` | `CompletableFuture<Integer> lookup(String subject, SchemaKind kind, String schema, String messageType)` |
| `latest` | `CompletableFuture<RegisteredSchema> latest(String subject)` |
| `latestId` | `CompletableFuture<Integer> latestId(String subject)` |
| `schemaById` | `CompletableFuture<FetchedSchema> schemaById(int schemaId)` |

Nested records:

```java
public record RegisteredSchema(int id, int version, String schema, String schemaType, String messageType) {}
public record FetchedSchema(String schema, String messageType) {}
```

Failures complete the future exceptionally with `SchemaRegistryException`.

### SchemaCache

`public final class`. A thread-safe store of resolved schema IDs and writer schemas.

| Member | Signature |
| --- | --- |
| constructor | `SchemaCache(KrabkaSchemaRegistryClient client)`, which uses `AUTO_REGISTER` and `TopicNameStrategy` |
| constructor | `SchemaCache(KrabkaSchemaRegistryClient client, RegisterMode registerMode, SubjectNameStrategy subjectNameStrategy)` |
| `subject` | `String subject(String topic, Role role)` |
| `intern` | `void intern(String subject, SchemaKind kind, String schema, String messageType)`, idempotent by subject |
| `prewarm` | `CompletableFuture<Void> prewarm()` |
| `idForSubject` | `OptionalInt idForSubject(String subject)` |
| `writerSchema` | `String writerSchema(int schemaId)`, which throws `SchemaFetchPendingException` on a miss |
| `writerMessageType` | `String writerMessageType(int schemaId)`, `null` when unknown |
| `seedSubjectId` | `void seedSubjectId(String subject, int schemaId)` |
| `seedWriterSchema` | `void seedWriterSchema(int schemaId, String schema)` |
| `seedWriterMessageType` | `void seedWriterMessageType(int schemaId, String messageType)` |

### AvroSerde&lt;T&gt;

`public final class ... implements Serde<T>`

| Member | Signature |
| --- | --- |
| `forValue` | `static <T extends SpecificRecord> AvroSerde<T> forValue(Class<T> type, SchemaCache cache)` |
| `forKey` | `static <T extends SpecificRecord> AvroSerde<T> forKey(Class<T> type, SchemaCache cache)` |
| `generic` | `static AvroSerde<GenericRecord> generic(Schema schema, SchemaCache cache, Role role)` |
| `registerSubject` | `void registerSubject(String topic)` |
| `serializer` / `deserializer` | from `Serde<T>` |

Registers the Avro canonical parsing form. Deserialization performs writer/reader schema
resolution.

### ProtobufSerde&lt;T extends Message&gt;

`public final class ... implements Serde<T>`

| Member | Signature |
| --- | --- |
| `forValue` | `static <T extends Message> ProtobufSerde<T> forValue(T defaultInstance, SchemaCache cache)` |
| `forKey` | `static <T extends Message> ProtobufSerde<T> forKey(T defaultInstance, SchemaCache cache)` |
| `registerSubject` | `void registerSubject(String topic)` |

Uses the Protobuf message-index framing and verifies the writer's `messageType`.

### JsonSchemaSerde&lt;T&gt;

`public final class ... implements Serde<T>`

| Member | Signature |
| --- | --- |
| `forValue` | `static <T> JsonSchemaSerde<T> forValue(Class<T> type, String schema, SchemaCache cache, boolean validate)` |
| `forKey` | `static <T> JsonSchemaSerde<T> forKey(Class<T> type, String schema, SchemaCache cache, boolean validate)` |
| `forValue` | `static <T> JsonSchemaSerde<T> forValue(Class<T> type, String schema, SchemaCache cache, boolean validate, ObjectMapper objectMapper)` |
| `registerSubject` | `void registerSubject(String topic)` |

`validate` applies to deserialization only, against the writer's schema, using the
Draft 2020-12 dialect.

### ConfluentWireFormat

`public final class`. Framing helpers.

| Member | Signature |
| --- | --- |
| `MAGIC` | `public static final byte` = `0` |
| `encode` | `static byte[] encode(int schemaId, byte[] body)` |
| `decode` | `static Frame decode(byte[] bytes)` |
| `encodeProtobuf` | `static byte[] encodeProtobuf(int schemaId, List<Integer> messageIndexes, byte[] body)` |
| `decodeProtobuf` | `static ProtobufFrame decodeProtobuf(byte[] bytes)` |

```java
public record Frame(int schemaId, byte[] body) {}
public record ProtobufFrame(int schemaId, List<Integer> messageIndexes, byte[] body) {}
```

Both records copy `body` on construction and on access.

### Enums and interfaces

| Type | Values / members |
| --- | --- |
| `Role` | `KEY`, `VALUE` |
| `SchemaKind` | `AVRO` (no wire name), `PROTOBUF`, `JSON` |
| `RegisterMode` | `AUTO_REGISTER`, `LOOKUP_ONLY`, `USE_LATEST` |
| `SubjectNameStrategy` | `@FunctionalInterface String subject(String topic, Role role)` |
| `TopicNameStrategy` | `implements SubjectNameStrategy`, producing `topic + "-key"` or `topic + "-value"` |

### Exceptions

| Type | Extends | Members |
| --- | --- | --- |
| `SchemaRegistryException` | `RuntimeException` | `SchemaRegistryException(String)`, `(String, Throwable)`, `(int statusCode, String body)`; `int statusCode()`, which returns `-1` for transport or parsing errors |
| `SchemaFetchPendingException` | `org.apache.kafka.common.errors.RetriableException` | `SchemaFetchPendingException(int schemaId)`; `int schemaId()` |

---

## `krabka-streams-columnar`

Package `io.krabka.streams.columnar`.

### ColumnarTopology

`public final class`. The topology builder.

| Member | Signature |
| --- | --- |
| constructor | `ColumnarTopology(BufferAllocator allocator)` |
| `addSource` | `ColumnarNode addSource(String name, Collection<String> topics, BatchCodec codec)` |
| `addOperator` | `ColumnarNode addOperator(String name, BuiltinOp operator, ColumnarNode parent)` |
| `addOperator` | `ColumnarNode addOperator(String name, Supplier<? extends ColumnarProcessor> processor, ColumnarNode parent)` |
| `addSink` | `ColumnarNode addSink(String name, String topic, BatchCodec codec, ColumnarNode parent)` |
| `sourceTopics` | `List<String> sourceTopics()` |
| `validate` | `void validate()`, which throws `ColumnarException` |
| `build` | `BuiltColumnarTopology build()` |

### BuiltColumnarTopology

`public final class`. Validated and reusable, but not thread-safe.

| Member | Signature |
| --- | --- |
| `runBatch` | `List<ProducedToTopic> runBatch(String topic, List<ConsumedRecord> records)` |

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

Returns the next offset to read. Does not commit offsets.

### Codecs

| Type | Signature |
| --- | --- |
| `BatchCodec` | `interface`: `VectorSchemaRoot decode(List<ConsumedRecord>)`, `List<ProduceRecord> encode(VectorSchemaRoot)` |
| `BlobCodec` | `final class implements BatchCodec`: `BlobCodec(BufferAllocator)`, `BlobCodec(BufferAllocator, int maxRecordBytes)` |
| `RowCodec<T>` | `final class implements BatchCodec`: `RowCodec(Serde<T> valueSerde, RowBridge<T> rowBridge, BufferAllocator allocator)` |
| `ArrowIpcSerde` | `final class implements Serde<VectorSchemaRoot>`: `ArrowIpcSerde(BufferAllocator)` |

`BlobCodec` constants: `DEFAULT_MAX_RECORD_BYTES` (`900 * 1024`), `KEY_COLUMN`
(`__key`), `TIMESTAMP_COLUMN` (`__timestamp`), `PARTITION_COLUMN` (`__partition`),
`OFFSET_COLUMN` (`__offset`).

### Row bridges

| Type | Signature |
| --- | --- |
| `RowBridge<T>` | `interface`: `VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator)`, `List<T> batchToRows(VectorSchemaRoot)` |
| `JsonRowBridge<T>` | `final class implements RowBridge<T>`: `JsonRowBridge(Class<T>)`, `JsonRowBridge(Class<T>, ObjectMapper)` |

### Operators

| Type | Signature |
| --- | --- |
| `ColumnarProcessor` | `@FunctionalInterface void process(ColumnarContext context, VectorSchemaRoot batch)` |
| `ColumnarContext` | `final class`: `void forward(VectorSchemaRoot batch)` |
| `RowPredicate` | `@FunctionalInterface boolean test(VectorSchemaRoot batch, int row)` |
| `RowValue` | `@FunctionalInterface Object value(VectorSchemaRoot batch, int row)` |
| `DerivedColumn` | `record DerivedColumn(Field field, RowValue value)` |
| `Aggregation` | `record Aggregation(String inputColumn, String outputColumn, AggregateFunction function)` |
| `AggregateFunction` | `enum`: `COUNT`, `SUM`, `MIN`, `MAX` |

`BuiltinOp` is a `public final class implements ColumnarProcessor`:

```java
static BuiltinOp filter(BufferAllocator allocator, RowPredicate predicate)
static BuiltinOp select(BufferAllocator allocator, String... columns)
static BuiltinOp withColumns(BufferAllocator allocator, DerivedColumn... columns)
static BuiltinOp groupBy(BufferAllocator allocator, Collection<String> keys, Aggregation... aggregations)
void process(ColumnarContext context, VectorSchemaRoot batch)
```

### Records and exceptions

```java
public record ConsumedRecord(byte[] key, byte[] value, long timestamp, int partition, long offset) {}
public record ProduceRecord(byte[] key, byte[] value, long timestamp) {}
public record ProducedToTopic(String topic, ProduceRecord record) {}

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

| Member | Signature |
| --- | --- |
| constructor | `ColumnarTestDriver(BuiltColumnarTopology topology)` |
| `pipeInput` | `void pipeInput(String topic, int partition, byte[] key, byte[] value, long timestamp)` |
| `pipeBatch` | `void pipeBatch(String topic, List<ConsumedRecord> records)` |
| `isOutputEmpty` | `boolean isOutputEmpty(String topic)` |
| `outputSize` | `int outputSize(String topic)` |
| `readOutput` | `ProduceRecord readOutput(String topic)`, which throws `NoSuchElementException` |
| `drainOutput` | `List<ProduceRecord> drainOutput(String topic)` |

### SchemaRegistryStub

`public final class ... implements AutoCloseable`

| Member | Signature |
| --- | --- |
| constructor | `SchemaRegistryStub() throws IOException`, which binds `127.0.0.1` on an ephemeral port |
| `uri` | `URI uri()` |
| `requestCount` | `int requestCount(String method, String path)` |
| `close` | `void close()` |

This module also re-exports Apache Kafka's `kafka-streams-test-utils`, including
`TopologyTestDriver`.

---

## Compatibility

`1.0.0` is the first release, so nothing is deprecated yet. The public surface above is
what future releases will be judged against; package-private types
(`AbstractSchemaSerde`, `ArrowBatchSupport`, `ProtobufSchemaPrinter`) may change at any
time.

Transitive `api` dependencies are part of the surface too: Kafka Streams 4.3.1, Avro
1.12.1, Protobuf 4.33.5, Jackson 2.22.0, and Arrow 19.0.0.
