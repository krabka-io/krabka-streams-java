package io.krabka.streams.columnar.schema;

import io.krabka.streams.columnar.ColumnarException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Translates Avro schemas into Arrow schemas.
 *
 * <p>Records become one column per field, nested records become {@code Struct}
 * columns, arrays become {@code List}, maps become {@code Map} with {@code Utf8}
 * keys, and the {@code decimal}, {@code date}, {@code time}, and {@code timestamp}
 * logical types become their native Arrow counterparts. Enums become {@code Utf8}
 * symbol columns and {@code fixed} becomes {@code FixedSizeBinary}, both tagged with
 * {@code krabka.avro.*} field metadata so the value conversion is reversible. A
 * union of {@code null} with one branch is that branch made nullable; a union with
 * several non-null branches becomes a {@code Struct} with one nullable child per
 * branch of which exactly one is set. A recursive record reference falls back to its
 * JSON text in a {@code Utf8} column tagged {@code krabka.json}, because an Arrow
 * schema is a finite tree. A non-record top-level schema maps to a single column
 * named {@code value}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var avro = new org.apache.avro.Schema.Parser().parse("""
 *     {"type": "record", "name": "Order", "fields": [
 *       {"name": "id", "type": "string"},
 *       {"name": "total", "type": {"type": "bytes", "logicalType": "decimal",
 *                                  "precision": 10, "scale": 2}}]}""");
 * org.apache.arrow.vector.types.pojo.Schema arrow = AvroArrowSchemas.toArrowSchema(avro);
 * // columns: id (Utf8, not null), total (Decimal(10, 2, 128), not null)
 * }</pre>
 */
public final class AvroArrowSchemas {
    private AvroArrowSchemas() {
    }

    /**
     * Translates an Avro schema into the Arrow schema its batches use.
     *
     * @param schema the Avro reader schema
     * @return the Arrow schema; one column per field for a record schema, one
     *     {@code value} column otherwise
     * @throws ColumnarException if the schema uses an unsupported shape, such as a
     *     decimal precision above 76
     */
    public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        return new org.apache.arrow.vector.types.pojo.Schema(topLevelFields(schema));
    }

    /**
     * Translates one Avro schema into one Arrow field.
     *
     * @param name the field name the column uses
     * @param schema the Avro schema of the value
     * @return the Arrow field, nullable when the schema is a union with {@code null}
     * @throws ColumnarException if the schema uses an unsupported shape
     */
    public static Field toArrowField(String name, Schema schema) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(schema, "schema");
        return field(name, schema, false, new ArrayDeque<>());
    }

    static List<Field> topLevelFields(Schema schema) {
        if (schema.getType() == Schema.Type.RECORD) {
            return schema.getFields().stream()
                    .map(field -> {
                        var visiting = new ArrayDeque<String>();
                        visiting.push(schema.getFullName());
                        return field(field.name(), field.schema(), false, visiting);
                    })
                    .toList();
        }
        return List.of(field("value", schema, false, new ArrayDeque<>()));
    }

    private static Field field(String name, Schema schema, boolean nullable, Deque<String> visiting) {
        if (schema.getType() == Schema.Type.UNION) {
            var branches = schema.getTypes().stream()
                    .filter(branch -> branch.getType() != Schema.Type.NULL)
                    .toList();
            boolean hasNull = branches.size() < schema.getTypes().size();
            if (branches.isEmpty()) {
                throw new ColumnarException("Avro field has only null branches: " + name);
            }
            if (branches.size() == 1) {
                return field(name, branches.get(0), nullable || hasNull, visiting);
            }
            var children = branches.stream()
                    .map(branch -> field(branchName(branch), branch, true, visiting))
                    .toList();
            var names = children.stream().map(Field::getName).collect(Collectors.toSet());
            if (names.size() != children.size()) {
                throw new ColumnarException("Avro union branch names collide in field " + name);
            }
            return new Field(
                    name,
                    new FieldType(
                            nullable || hasNull,
                            new ArrowType.Struct(),
                            null,
                            Map.of(BridgeMetadata.AVRO_UNION, "true")),
                    children);
        }
        return switch (schema.getType()) {
            case NULL -> throw new ColumnarException("Avro field is the null type: " + name);
            case BOOLEAN -> leaf(name, new ArrowType.Bool(), nullable);
            case INT -> intField(name, schema, nullable);
            case LONG -> longField(name, schema, nullable);
            case FLOAT -> leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), nullable);
            case DOUBLE -> leaf(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), nullable);
            case BYTES -> schema.getLogicalType() instanceof LogicalTypes.Decimal decimal
                    ? leaf(name, decimalType(name, decimal), nullable)
                    : leaf(name, new ArrowType.Binary(), nullable);
            case STRING -> stringField(name, schema, nullable);
            case FIXED -> fixedField(name, schema, nullable);
            case ENUM -> new Field(
                    name,
                    new FieldType(nullable, new ArrowType.Utf8(), null, Map.of(
                            BridgeMetadata.AVRO_ENUM, schema.getFullName(),
                            BridgeMetadata.AVRO_ENUM_SYMBOLS, String.join(",", schema.getEnumSymbols()))),
                    null);
            case RECORD -> recordField(name, schema, nullable, visiting);
            case ARRAY -> new Field(
                    name,
                    new FieldType(nullable, new ArrowType.List(), null, Map.of()),
                    List.of(field("item", schema.getElementType(), false, visiting)));
            case MAP -> mapField(name, schema, nullable, visiting);
            case UNION -> throw new AssertionError("unreachable");
        };
    }

    private static Field leaf(String name, ArrowType type, boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null, Map.of()), null);
    }

    private static Field intField(String name, Schema schema, boolean nullable) {
        LogicalType logical = schema.getLogicalType();
        if (logical instanceof LogicalTypes.Date) {
            return leaf(name, new ArrowType.Date(DateUnit.DAY), nullable);
        }
        if (logical instanceof LogicalTypes.TimeMillis) {
            return leaf(name, new ArrowType.Time(TimeUnit.MILLISECOND, 32), nullable);
        }
        return leaf(name, new ArrowType.Int(32, true), nullable);
    }

    private static Field longField(String name, Schema schema, boolean nullable) {
        LogicalType logical = schema.getLogicalType();
        if (logical instanceof LogicalTypes.TimeMicros) {
            return leaf(name, new ArrowType.Time(TimeUnit.MICROSECOND, 64), nullable);
        }
        if (logical instanceof LogicalTypes.TimestampMillis) {
            return leaf(name, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), nullable);
        }
        if (logical instanceof LogicalTypes.TimestampMicros) {
            return leaf(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), nullable);
        }
        if (logical instanceof LogicalTypes.LocalTimestampMillis) {
            return leaf(name, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), nullable);
        }
        if (logical instanceof LogicalTypes.LocalTimestampMicros) {
            return leaf(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), nullable);
        }
        return leaf(name, new ArrowType.Int(64, true), nullable);
    }

    private static Field stringField(String name, Schema schema, boolean nullable) {
        if (schema.getLogicalType() != null && "uuid".equals(schema.getLogicalType().getName())) {
            return new Field(
                    name,
                    new FieldType(nullable, new ArrowType.Utf8(), null, Map.of(
                            BridgeMetadata.AVRO_LOGICAL, "uuid")),
                    null);
        }
        return leaf(name, new ArrowType.Utf8(), nullable);
    }

    private static Field fixedField(String name, Schema schema, boolean nullable) {
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
            return leaf(name, decimalType(name, decimal), nullable);
        }
        return new Field(
                name,
                new FieldType(nullable, new ArrowType.FixedSizeBinary(schema.getFixedSize()), null, Map.of(
                        BridgeMetadata.AVRO_FIXED, schema.getFullName())),
                null);
    }

    private static ArrowType decimalType(String name, LogicalTypes.Decimal decimal) {
        if (decimal.getPrecision() > 76) {
            throw new ColumnarException(
                    "Avro decimal precision above 76 is unsupported in field " + name
                            + ": " + decimal.getPrecision());
        }
        int width = decimal.getPrecision() > 38 ? 256 : 128;
        return new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale(), width);
    }

    private static Field recordField(String name, Schema schema, boolean nullable, Deque<String> visiting) {
        if (visiting.contains(schema.getFullName())) {
            return new Field(
                    name,
                    new FieldType(nullable, new ArrowType.Utf8(), null, Map.of(BridgeMetadata.JSON, "true")),
                    null);
        }
        visiting.push(schema.getFullName());
        try {
            var children = schema.getFields().stream()
                    .map(child -> field(child.name(), child.schema(), false, visiting))
                    .toList();
            return new Field(name, new FieldType(nullable, new ArrowType.Struct(), null, Map.of()), children);
        } finally {
            visiting.pop();
        }
    }

    private static Field mapField(String name, Schema schema, boolean nullable, Deque<String> visiting) {
        var entries = new Field(
                "entries",
                FieldType.notNullable(new ArrowType.Struct()),
                List.of(
                        new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                        field("value", schema.getValueType(), false, visiting)));
        return new Field(name, new FieldType(nullable, new ArrowType.Map(false), null, Map.of()), List.of(entries));
    }

    private static String branchName(Schema branch) {
        return switch (branch.getType()) {
            case RECORD, ENUM, FIXED -> branch.getFullName();
            default -> branch.getType().getName();
        };
    }

    static Map<String, Schema> unionBranches(Schema union) {
        var result = new LinkedHashMap<String, Schema>();
        var branches = new ArrayList<Schema>();
        union.getTypes().stream()
                .filter(branch -> branch.getType() != Schema.Type.NULL)
                .forEach(branches::add);
        branches.forEach(branch -> result.put(branchName(branch), branch));
        return result;
    }
}
