package io.krabka.streams.columnar.schema;

import com.google.protobuf.Descriptors;
import io.krabka.streams.columnar.ColumnarException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Translates Protobuf message descriptors into Arrow schemas.
 *
 * <p>Scalar fields map to their Arrow counterparts, with {@code uint32} and
 * {@code fixed32} widened to signed 64-bit integers and {@code uint64} and
 * {@code fixed64} kept exact as unsigned 64-bit columns. Nested messages become
 * nullable {@code Struct} columns, {@code repeated} fields become {@code List},
 * and map fields become {@code Map}. Enums become {@code Utf8} symbol columns
 * tagged {@code krabka.proto.enum}. Fields with presence — message fields,
 * {@code optional} scalars, and oneof members — are nullable; proto3
 * implicit-presence scalars are not, so an unset scalar reads as its default value.
 *
 * <p>Well-known types get special treatment: {@code google.protobuf.Timestamp}
 * becomes a UTC microsecond timestamp, the wrapper types unwrap to nullable
 * scalars tagged {@code krabka.proto.wrapper}, and the dynamic
 * {@code google.protobuf.Struct}, {@code Value}, and {@code ListValue} — which
 * recurse and therefore cannot form a finite Arrow tree — are stored as their
 * canonical Protobuf JSON text in a {@code Utf8} column tagged {@code krabka.json}
 * and {@code krabka.proto.message}. The same JSON fallback applies to any recursive
 * message reference.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * org.apache.arrow.vector.types.pojo.Schema arrow =
 *     ProtobufArrowSchemas.toArrowSchema(Order.getDescriptor());
 * // e.g. columns: id (Utf8), total_cents (Int(64, true)), placed_at
 * // (Timestamp(MICROSECOND, "UTC"), nullable)
 * }</pre>
 */
public final class ProtobufArrowSchemas {
    private static final String TIMESTAMP = "google.protobuf.Timestamp";
    private static final Set<String> JSON_FALLBACK = Set.of(
            "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue");
    private static final Map<String, ArrowType> WRAPPERS = Map.of(
            "google.protobuf.DoubleValue", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            "google.protobuf.FloatValue", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            "google.protobuf.Int64Value", new ArrowType.Int(64, true),
            "google.protobuf.UInt64Value", new ArrowType.Int(64, false),
            "google.protobuf.Int32Value", new ArrowType.Int(32, true),
            "google.protobuf.UInt32Value", new ArrowType.Int(64, true),
            "google.protobuf.BoolValue", new ArrowType.Bool(),
            "google.protobuf.StringValue", new ArrowType.Utf8(),
            "google.protobuf.BytesValue", new ArrowType.Binary());

    private ProtobufArrowSchemas() {
    }

    /**
     * Translates a message descriptor into the Arrow schema its batches use.
     *
     * @param descriptor the descriptor of the message type
     * @return the Arrow schema with one column per message field
     * @throws ColumnarException if a field uses an unsupported shape
     */
    public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Descriptors.Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return new org.apache.arrow.vector.types.pojo.Schema(topLevelFields(descriptor));
    }

    /**
     * Translates one message field into one Arrow field.
     *
     * @param field the descriptor of the message field
     * @return the Arrow field, nullable when the message field tracks presence
     * @throws ColumnarException if the field uses an unsupported shape
     */
    public static Field toArrowField(Descriptors.FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        var visiting = new ArrayDeque<String>();
        visiting.push(field.getContainingType().getFullName());
        return field(field, visiting);
    }

    static List<Field> topLevelFields(Descriptors.Descriptor descriptor) {
        return descriptor.getFields().stream()
                .map(field -> {
                    var visiting = new ArrayDeque<String>();
                    visiting.push(descriptor.getFullName());
                    return field(field, visiting);
                })
                .toList();
    }

    private static Field field(Descriptors.FieldDescriptor field, Deque<String> visiting) {
        if (field.isMapField()) {
            return mapField(field, visiting);
        }
        if (field.isRepeated()) {
            return new Field(
                    field.getName(),
                    new FieldType(false, new ArrowType.List(), null, oneofMetadata(field, Map.of())),
                    List.of(element("item", field, visiting)));
        }
        var element = element(field.getName(), field, visiting);
        return new Field(
                field.getName(),
                new FieldType(
                        field.hasPresence(),
                        element.getType(),
                        null,
                        oneofMetadata(field, element.getMetadata())),
                element.getChildren().isEmpty() ? null : element.getChildren());
    }

    private static Field element(String name, Descriptors.FieldDescriptor field, Deque<String> visiting) {
        return switch (field.getType()) {
            case DOUBLE -> leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case FLOAT -> leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            case INT32, SINT32, SFIXED32 -> leaf(name, new ArrowType.Int(32, true));
            case INT64, SINT64, SFIXED64 -> leaf(name, new ArrowType.Int(64, true));
            case UINT32, FIXED32 -> leaf(name, new ArrowType.Int(64, true));
            case UINT64, FIXED64 -> leaf(name, new ArrowType.Int(64, false));
            case BOOL -> leaf(name, new ArrowType.Bool());
            case STRING -> leaf(name, new ArrowType.Utf8());
            case BYTES -> leaf(name, new ArrowType.Binary());
            case ENUM -> new Field(
                    name,
                    new FieldType(false, new ArrowType.Utf8(), null, Map.of(
                            BridgeMetadata.PROTO_ENUM, field.getEnumType().getFullName())),
                    null);
            case MESSAGE, GROUP -> messageField(name, field.getMessageType(), visiting);
        };
    }

    private static Field messageField(String name, Descriptors.Descriptor message, Deque<String> visiting) {
        var fullName = message.getFullName();
        if (TIMESTAMP.equals(fullName)) {
            return leaf(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
        }
        var wrapped = WRAPPERS.get(fullName);
        if (wrapped != null) {
            return new Field(
                    name,
                    new FieldType(false, wrapped, null, Map.of(BridgeMetadata.PROTO_WRAPPER, fullName)),
                    null);
        }
        if (JSON_FALLBACK.contains(fullName) || visiting.contains(fullName)) {
            return new Field(
                    name,
                    new FieldType(false, new ArrowType.Utf8(), null, Map.of(
                            BridgeMetadata.JSON, "true",
                            BridgeMetadata.PROTO_MESSAGE, fullName)),
                    null);
        }
        visiting.push(fullName);
        try {
            var children = message.getFields().stream()
                    .map(child -> field(child, visiting))
                    .toList();
            return new Field(name, new FieldType(false, new ArrowType.Struct(), null, Map.of()), children);
        } finally {
            visiting.pop();
        }
    }

    private static Field mapField(Descriptors.FieldDescriptor field, Deque<String> visiting) {
        var entryType = field.getMessageType();
        var key = element("key", entryType.findFieldByName("key"), visiting);
        var keyField = new Field(
                "key", new FieldType(false, key.getType(), null, key.getMetadata()), null);
        var value = field(entryType.findFieldByName("value"), visiting);
        var valueField = new Field(
                "value",
                new FieldType(true, value.getType(), null, value.getMetadata()),
                value.getChildren().isEmpty() ? null : value.getChildren());
        var entries = new Field(
                "entries",
                FieldType.notNullable(new ArrowType.Struct()),
                List.of(keyField, valueField));
        return new Field(
                field.getName(),
                new FieldType(false, new ArrowType.Map(false), null, oneofMetadata(field, Map.of())),
                List.of(entries));
    }

    private static Field leaf(String name, ArrowType type) {
        return new Field(name, new FieldType(false, type, null, Map.of()), null);
    }

    private static Map<String, String> oneofMetadata(
            Descriptors.FieldDescriptor field, Map<String, String> metadata) {
        var oneof = field.getRealContainingOneof();
        if (oneof == null) {
            return metadata;
        }
        var result = new HashMap<>(metadata);
        result.put(BridgeMetadata.PROTO_ONEOF, oneof.getName());
        return Map.copyOf(result);
    }
}
