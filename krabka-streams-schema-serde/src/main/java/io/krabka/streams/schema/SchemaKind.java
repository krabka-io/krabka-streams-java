package io.krabka.streams.schema;

/** Schema formats supported by the registry client. */
public enum SchemaKind {
    AVRO(null),
    PROTOBUF("PROTOBUF"),
    JSON("JSON");

    private final String wireName;

    SchemaKind(String wireName) {
        this.wireName = wireName;
    }

    String wireName() {
        return wireName;
    }
}
