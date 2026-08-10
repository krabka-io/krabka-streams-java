package io.krabka.streams.columnar.schema;

/**
 * Arrow field metadata keys the bridges stamp on converted columns.
 *
 * <p>The keys extend the {@code krabka.*} namespace that
 * {@link io.krabka.streams.columnar.JsonRowBridge} established with
 * {@code krabka.json}. They let the write-back path reverse a conversion that is not
 * implied by the Arrow type alone.
 */
final class BridgeMetadata {
    /** JSON text column holding a subtree the Arrow type system cannot express. */
    static final String JSON = "krabka.json";

    /** The Avro enum's full name on a symbol column. */
    static final String AVRO_ENUM = "krabka.avro.enum";

    /** Comma-separated symbols of the Avro enum. */
    static final String AVRO_ENUM_SYMBOLS = "krabka.avro.enum.symbols";

    /** Marks a struct column that spreads a multi-branch Avro union. */
    static final String AVRO_UNION = "krabka.avro.union";

    /** The Avro fixed type's full name on a fixed-size binary column. */
    static final String AVRO_FIXED = "krabka.avro.fixed";

    /** The Avro logical type's name where the Arrow type does not imply it. */
    static final String AVRO_LOGICAL = "krabka.avro.logical";

    /** The Protobuf enum's full name on a symbol column. */
    static final String PROTO_ENUM = "krabka.proto.enum";

    /** The oneof name shared by the columns of one Protobuf oneof. */
    static final String PROTO_ONEOF = "krabka.proto.oneof";

    /** The wrapper type's full name on an unwrapped well-known wrapper column. */
    static final String PROTO_WRAPPER = "krabka.proto.wrapper";

    /** The message full name on a JSON text column holding a Protobuf subtree. */
    static final String PROTO_MESSAGE = "krabka.proto.message";

    private BridgeMetadata() {
    }
}
