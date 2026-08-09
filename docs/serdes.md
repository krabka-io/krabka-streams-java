# Serdes

`krabka-streams-schema-serde` provides three `org.apache.kafka.common.serialization.Serde`
implementations, for Avro, Protobuf, and JSON Schema. They produce and consume
Confluent-framed record bytes, and all three share the same lifecycle, defined by the
package-private `AbstractSchemaSerde`.

## The shared lifecycle

```java
var serde = AvroSerde.generic(schema, cache, Role.VALUE);
serde.registerSubject("orders"); // intern the subject for prewarming
cache.prewarm().join(); // resolve the subject to a schema ID
byte[] bytes = serde.serializer().serialize("orders", record);
GenericRecord back = serde.deserializer().deserialize("orders", bytes);
```

Serialization:

1. `null` values serialize to `null`. Kafka tombstones survive untouched.
2. The subject is computed from the topic and the serde's `Role`.
3. The schema ID comes from `SchemaCache.idForSubject`. If it is absent, the serde
   throws `SerializationException` with the message
   `schema ID for orders-value is not resolved; call registerSubject and prewarm first`.
4. The body is encoded by the format-specific implementation and framed.

Deserialization:

1. `null` bytes deserialize to `null`.
2. The frame is decoded into a schema ID and a body.
3. The writer schema is read from the cache, which may throw
   `SchemaFetchPendingException`. That one is retriable; see
   [Schema registry](schema-registry.md).
4. The body is decoded against that writer schema.

Any checked exception raised by a format library is wrapped in
`SerializationException` with the message `cannot serialize schema value` or
`cannot deserialize schema value`. A `SerializationException` thrown deeper down is
rethrown unchanged, so the specific message survives.

### Role checking

Serdes are constructed for a fixed `Role` (`KEY` or `VALUE`). When Kafka configures a
serde it calls `configure(configs, isKey)`, and the serde throws
`IllegalArgumentException("serde role does not match the Kafka key setting")` if the
role disagrees. This catches the common mistake of setting a value serde as
`default.key.serde`.

`configure` is only invoked when Kafka instantiates the serde from configuration. When
you pass a serde instance directly to `Consumed.with` or `Produced.with`, the role is
whatever you chose at construction time and is not cross-checked.

## The Confluent wire format

`ConfluentWireFormat` implements the framing and is public, so you can use it without
the serdes.

Standard frame (Avro and JSON Schema):

```text
+--------+------------------+-------------------+
| 0x00   | schema ID        | body              |
| 1 byte | 4 bytes, big end | remaining bytes   |
+--------+------------------+-------------------+
```

```java
byte[] frame = ConfluentWireFormat.encode(258, body);
// -> 00 00 00 01 02 <body>
ConfluentWireFormat.Frame decoded = ConfluentWireFormat.decode(frame);
decoded.schemaId(); // 258
decoded.body(); // a defensive copy
```

Protobuf frames add a message-index path between the header and the body, encoded as
zig-zag varints:

```java
byte[] frame = ConfluentWireFormat.encodeProtobuf(7, List.of(0), body);
// -> 00 00 00 00 07 00 <body>   (the single 0x00 is the shorthand for "first message")
var protobuf = ConfluentWireFormat.decodeProtobuf(frame);
protobuf.messageIndexes(); // List.of(0)
```

An index path of exactly `[0]` is written as a single zero byte, matching the Confluent
optimization for the first top-level message. Any other path is written as a count
followed by that many varints.

Decoding rejects malformed input with `SerializationException`:

| Condition                      | Message                                     |
| ------------------------------ | ------------------------------------------- |
| Fewer than 5 bytes             | `schema frame is shorter than 5 bytes`      |
| First byte is not `0x00`       | `invalid schema frame magic byte 0x01`      |
| Truncated index varint         | `truncated Protobuf message-index varint`   |
| Varint longer than 10 bytes    | `Protobuf message-index varint is too long` |
| Index count out of `int` range | `invalid Protobuf message-index count: ...` |

Both `Frame` and `ProtobufFrame` are records that copy their `body` on construction
and on access, so a frame never aliases a caller's array.

## AvroSerde

```java
// Generated SpecificRecord classes
AvroSerde<Order> values = AvroSerde.forValue(Order.class, cache);
AvroSerde<OrderKey> keys = AvroSerde.forKey(OrderKey.class, cache);

// GenericRecord against a parsed schema
Schema schema = new Schema.Parser().parse(schemaJson);
AvroSerde<GenericRecord> generic = AvroSerde.generic(schema, cache, Role.VALUE);
```

The specific factories read the schema from the generated class through
`SpecificData.get().getSchema(type)`.

The schema registered with the registry is the Avro _canonical parsing form_
(`SchemaNormalization.toParsingForm`), which strips documentation, ordering, and
aliases. Two schemas that differ only in those respects therefore resolve to the same
registry ID.

Writing uses binary encoding against the serde's own schema. Reading parses the writer
schema from the cache and hands both schemas to Avro's `DatumReader`, so full Avro
schema resolution applies: added fields with defaults, dropped fields, promoted numeric
types, and renamed records with aliases all work as Avro specifies.

```java
var cache = new SchemaCache(client);
cache.seedSubjectId("orders-value", 11);
cache.seedWriterSchema(11, schema.toString());
var serde = AvroSerde.generic(schema, cache, Role.VALUE);

var order = new GenericData.Record(schema);
order.put("id", "o-1");
var bytes = serde.serializer().serialize("orders", order);
assert serde.deserializer().deserialize("orders", bytes).get("id").toString().equals("o-1");
```

Ordinary Java classes use Avro reflection:

```java
var reflected = AvroSerde.reflect(Order.class, cache, Role.VALUE);
```

## ProtobufSerde

```java
ProtobufSerde<Order> values = ProtobufSerde.forValue(Order.getDefaultInstance(), cache);
ProtobufSerde<OrderKey> keys = ProtobufSerde.forKey(OrderKey.getDefaultInstance(), cache);
```

The serde derives three things from the default instance:

- the schema text, printed from the message's `FileDescriptor`;
- the `messageType`, which is the fully qualified message name;
- the full message-index path from the top-level parent to a nested message.

Bodies are `Message.toByteArray()` and are parsed with the message's own `Parser`.

Deserialization enforces a type check: if the cache holds a `messageType` for the
writer's schema ID and it differs from the local message's full name, the serde throws

```text
Protobuf messageType mismatch: writer demo.Other, local google.protobuf.StringValue
```

That check is skipped when the registry supplied no `messageType`.

`ProtobufSchemaPrinter` reconstructs imports, options, nested definitions, enums,
`oneof`, services, extensions, maps, and proto2 groups from the generated descriptor.
Schema references can also be supplied to `register` and are recursively resolved when
reading.

## JsonSchemaSerde

```java
JsonSchemaSerde<Order> values = JsonSchemaSerde.forValue(Order.class, schemaJson, cache, true);
JsonSchemaSerde<Order> keys = JsonSchemaSerde.forKey(Order.class, schemaJson, cache, true);
JsonSchemaSerde<Order> custom =
    JsonSchemaSerde.forValue(Order.class, schemaJson, cache, true, objectMapper);
```

The `validate` flag checks serialization against the local schema and deserialization
against the fetched writer schema. A failure throws

```text
JSON Schema validation failed: <first message from the validator>
```

The serde detects Draft 4, 6, 7, 2019-09, or 2020-12 from `$schema`; schemas without it
default to 2020-12. An extended factory can set the dialect, subject strategy, and
`ObjectMapper` explicitly. Compiled validators are cached per schema ID.

The fourth factory takes an `ObjectMapper`, which is how you register modules
(`JavaTimeModule`), change naming strategies, or relax `FAIL_ON_UNKNOWN_PROPERTIES`.
A custom mapper is available for values only; keys use the default mapper.

## Choosing between them

### Local compatibility before registration

`LocalSchemaCompatibility` checks a previous and candidate schema without contacting a
registry. Choose `BACKWARD`, `FORWARD`, or `FULL`:

```java
var result = LocalSchemaCompatibility.avro(previous, candidate, Mode.BACKWARD);
if (!result.compatible()) {
  throw new IllegalArgumentException(result.incompatibilities().toString());
}
```

Avro delegates to Avro's reader/writer compatibility checker. JSON Schema checks type
narrowing, required fields, and closed object properties. Protobuf compares
`FileDescriptor` message fields by wire type,
cardinality, and required-field presence.

|                                | Avro                          | Protobuf                  | JSON Schema          |
| ------------------------------ | ----------------------------- | ------------------------- | -------------------- |
| Payload size                   | smallest                      | small                     | largest              |
| Schema evolution               | full reader/writer resolution | field numbers             | validation only      |
| Registry `schemaType`          | omitted                       | `PROTOBUF`                | `JSON`               |
| Needs generated code           | for `SpecificRecord`          | yes                       | no                   |
| Reads writer schema per record | yes                           | ID checked, schema unused | yes, when validating |

Avro is the only one of the three that performs true schema resolution at read time.
Protobuf relies on wire-level compatibility rules and only verifies the message type.
JSON Schema verifies the document shape but does not reshape it.

## Writing your own serde

`AbstractSchemaSerde` is package-private, so a custom format cannot extend it from
outside `io.krabka.streams.schema`. Implement `Serde<T>` directly and reuse the public
pieces:

```java
final class CborSerde<T> implements Serde<T> {
  private final SchemaCache cache;
  private final String subject;

  @Override
  public Serializer<T> serializer() {
    return (topic, value) ->
        value == null
            ? null
            : ConfluentWireFormat.encode(
                cache
                    .idForSubject(subject)
                    .orElseThrow(
                        () ->
                            new SerializationException(
                                "schema ID for " + subject + " is not resolved")),
                encodeCbor(value));
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, bytes) -> {
      if (bytes == null) {
        return null;
      }
      var frame = ConfluentWireFormat.decode(bytes);
      return decodeCbor(cache.writerSchema(frame.schemaId()), frame.body());
    };
  }
}
```

Keep the two rules that make the built-in serdes safe: never block on I/O inside
`serialize`/`deserialize`, and let `SchemaFetchPendingException` propagate so the
caller can retry.
