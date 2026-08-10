package io.krabka.streams.schema;

/**
 * Schema formats supported by the registry client.
 *
 * <p>The kind is sent as the Confluent {@code schemaType} field when registering or
 * looking up a schema. Avro is the registry default and is therefore transmitted
 * without an explicit type name.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * int id = client.register("orders-value", SchemaKind.JSON, orderJsonSchema, null).join();
 * }</pre>
 */
public enum SchemaKind {
    /** An Apache Avro schema, the registry default format. */
    AVRO(null),

    /** A Protocol Buffers file descriptor rendered as {@code .proto} text. */
    PROTOBUF("PROTOBUF"),

    /** A JSON Schema document. */
    JSON("JSON");

    private final String wireName;

    SchemaKind(String wireName) {
        this.wireName = wireName;
    }

    String wireName() {
        return wireName;
    }
}
