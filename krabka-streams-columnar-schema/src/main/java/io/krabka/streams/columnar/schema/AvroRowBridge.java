package io.krabka.streams.columnar.schema;

import io.krabka.streams.columnar.ArrowValues;
import io.krabka.streams.columnar.ColumnarException;
import io.krabka.streams.columnar.RowBridge;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/**
 * Converts Avro records to and from Arrow batches with schema-derived columns.
 *
 * <p>The Arrow schema comes from {@link AvroArrowSchemas#toArrowSchema} once, at
 * construction, so every batch — including an empty one — carries the same columns.
 * Nested records become {@code Struct} columns, arrays {@code List}, maps
 * {@code Map}, and the decimal and temporal logical types their native Arrow
 * counterparts, unlike {@link io.krabka.streams.columnar.JsonRowBridge}, which
 * stores nested data as JSON text.
 *
 * <p>Combine the bridge with an {@code AvroSerde} in a
 * {@link io.krabka.streams.columnar.RowCodec} to read a registry-framed topic, or
 * use {@link AvroBatchCodec}, which packages exactly that composition.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var bridge = AvroRowBridge.generic(orderSchema);
 * try (var batch = bridge.rowsToBatch(List.of(order), allocator)) {
 *     List<GenericRecord> back = bridge.batchToRows(batch);
 * }
 * }</pre>
 *
 * @param <T> the Avro record type the bridge converts
 */
public final class AvroRowBridge<T extends IndexedRecord> implements RowBridge<T> {
    private final Schema readerSchema;
    private final GenericData model;
    private final Class<T> specificType;
    private final List<Field> fields;

    private AvroRowBridge(Schema readerSchema, GenericData model, Class<T> specificType) {
        if (readerSchema.getType() != Schema.Type.RECORD) {
            throw new ColumnarException("the top-level Avro schema must be a record: " + readerSchema);
        }
        this.readerSchema = readerSchema;
        this.model = model;
        this.specificType = specificType;
        this.fields = AvroArrowSchemas.topLevelFields(readerSchema);
    }

    /**
     * Creates a bridge for schema-driven {@link GenericRecord} values.
     *
     * @param schema the record schema every row follows
     * @return a generic-record bridge for {@code schema}
     * @throws ColumnarException if the schema is not a record or uses an unsupported
     *     shape
     */
    public static AvroRowBridge<GenericRecord> generic(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        return new AvroRowBridge<>(schema, GenericData.get(), null);
    }

    /**
     * Creates a bridge for a generated Avro class.
     *
     * @param <T> the generated Avro record type
     * @param type the generated class, whose embedded schema drives the columns
     * @return a bridge that returns instances of {@code type}
     * @throws ColumnarException if the embedded schema is not a record or uses an
     *     unsupported shape
     */
    public static <T extends SpecificRecord> AvroRowBridge<T> forSpecific(Class<T> type) {
        Objects.requireNonNull(type, "type");
        return new AvroRowBridge<>(SpecificData.get().getSchema(type), SpecificData.get(), type);
    }

    /**
     * Returns the Arrow schema every batch of this bridge uses.
     *
     * @return the Arrow schema derived from the record schema
     */
    public org.apache.arrow.vector.types.pojo.Schema arrowSchema() {
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    /**
     * Builds one Arrow batch with one row per record.
     *
     * @param rows the records to convert, in record order
     * @param allocator the allocator that owns the batch's buffers
     * @return the payload batch; the caller must close it
     * @throws ColumnarException if a required field is null or a value cannot be
     *     converted
     */
    @Override
    public VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator) {
        var root = ArrowValues.createRoot(fields, rows.size(), allocator);
        try {
            for (int column = 0; column < fields.size(); column++) {
                var field = fields.get(column);
                var avroField = readerSchema.getFields().get(column);
                var vector = root.getVector(column);
                for (int row = 0; row < rows.size(); row++) {
                    ArrowValues.set(
                            vector, row, toArrow(rows.get(row).get(column), avroField.schema(), field));
                }
            }
            ArrowValues.finish(root);
            return root;
        } catch (RuntimeException error) {
            root.close();
            throw error;
        }
    }

    /**
     * Converts an Arrow payload batch back into one record per row.
     *
     * @param batch the payload batch to convert; the bridge reads it and leaves it
     *     open
     * @return the records in row order
     * @throws ColumnarException if a row cannot be converted back to the record
     *     schema
     */
    @Override
    public List<T> batchToRows(VectorSchemaRoot batch) {
        var rows = new ArrayList<T>(batch.getRowCount());
        for (int row = 0; row < batch.getRowCount(); row++) {
            var record = new GenericData.Record(readerSchema);
            for (int column = 0; column < fields.size(); column++) {
                var field = fields.get(column);
                var avroField = readerSchema.getFields().get(column);
                var vector = batch.getVector(field.getName());
                if (vector == null) {
                    throw new ColumnarException("Arrow batch has no column " + field.getName());
                }
                record.put(column, fromArrow(ArrowValues.get(vector, row), avroField.schema(), field));
            }
            rows.add(toRow(record));
        }
        return List.copyOf(rows);
    }

    @SuppressWarnings("unchecked")
    private T toRow(GenericData.Record record) {
        if (specificType == null) {
            return (T) record;
        }
        try {
            var output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new GenericDatumWriter<IndexedRecord>(readerSchema).write(record, encoder);
            encoder.flush();
            var decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(output.toByteArray()), null);
            return (T) new SpecificDatumReader<>(readerSchema, readerSchema).read(null, decoder);
        } catch (java.io.IOException error) {
            throw new ColumnarException("cannot convert Arrow row to " + specificType.getName(), error);
        }
    }

    private Object toArrow(Object value, Schema schema, Field field) {
        if (schema.getType() == Schema.Type.UNION) {
            if ("true".equals(field.getMetadata().get(BridgeMetadata.AVRO_UNION))) {
                if (value == null) {
                    return requireNullable(value, field);
                }
                int index = model.resolveUnion(schema, value);
                var branch = schema.getTypes().get(index);
                var branches = AvroArrowSchemas.unionBranches(schema);
                var childName = branches.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(branch))
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElseThrow(() -> new ColumnarException(
                                "no union branch column for value in field " + field.getName()));
                var child = child(field, childName);
                var result = new LinkedHashMap<String, Object>();
                result.put(childName, toArrow(value, branch, child));
                return result;
            }
            var branch = schema.getTypes().stream()
                    .filter(candidate -> candidate.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow();
            return value == null ? requireNullable(value, field) : toArrow(value, branch, field);
        }
        if (value == null) {
            return requireNullable(value, field);
        }
        if ("true".equals(field.getMetadata().get(BridgeMetadata.JSON))) {
            return jsonText(value, schema);
        }
        return switch (schema.getType()) {
            case RECORD -> {
                var record = (IndexedRecord) value;
                var result = new LinkedHashMap<String, Object>();
                var children = field.getChildren();
                for (int index = 0; index < children.size(); index++) {
                    var child = children.get(index);
                    var avroField = schema.getFields().get(index);
                    result.put(child.getName(), toArrow(record.get(index), avroField.schema(), child));
                }
                yield result;
            }
            case ENUM, STRING -> value.toString();
            case BYTES -> schema.getLogicalType() instanceof LogicalTypes.Decimal
                    ? new Conversions.DecimalConversion()
                            .fromBytes((ByteBuffer) value, schema, schema.getLogicalType())
                    : value;
            case FIXED -> schema.getLogicalType() instanceof LogicalTypes.Decimal
                    ? new Conversions.DecimalConversion()
                            .fromFixed((GenericFixed) value, schema, schema.getLogicalType())
                    : ((GenericFixed) value).bytes();
            case ARRAY -> {
                var items = new ArrayList<Object>();
                var itemField = field.getChildren().get(0);
                for (var item : (Collection<?>) value) {
                    items.add(toArrow(item, schema.getElementType(), itemField));
                }
                yield items;
            }
            case MAP -> {
                var valueField = field.getChildren().get(0).getChildren().get(1);
                var result = new LinkedHashMap<String, Object>();
                ((Map<?, ?>) value).forEach((key, entry) ->
                        result.put(key.toString(), toArrow(entry, schema.getValueType(), valueField)));
                yield result;
            }
            default -> value;
        };
    }

    private Object fromArrow(Object value, Schema schema, Field field) {
        if (schema.getType() == Schema.Type.UNION) {
            if ("true".equals(field.getMetadata().get(BridgeMetadata.AVRO_UNION))) {
                if (value == null) {
                    return null;
                }
                var struct = (Map<?, ?>) value;
                var branches = AvroArrowSchemas.unionBranches(schema);
                for (var child : field.getChildren()) {
                    var branchValue = struct.get(child.getName());
                    if (branchValue != null) {
                        return fromArrow(branchValue, branches.get(child.getName()), child);
                    }
                }
                return null;
            }
            var branch = schema.getTypes().stream()
                    .filter(candidate -> candidate.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow();
            return value == null ? null : fromArrow(value, branch, field);
        }
        if (value == null) {
            return null;
        }
        if ("true".equals(field.getMetadata().get(BridgeMetadata.JSON))) {
            return parseJson(value.toString(), schema);
        }
        return switch (schema.getType()) {
            case RECORD -> {
                var struct = (Map<?, ?>) value;
                var record = new GenericData.Record(schema);
                var children = field.getChildren();
                for (int index = 0; index < children.size(); index++) {
                    var child = children.get(index);
                    record.put(index, fromArrow(struct.get(child.getName()), schema.getFields().get(index).schema(), child));
                }
                yield record;
            }
            case ENUM -> new GenericData.EnumSymbol(schema, value.toString());
            case STRING -> value.toString();
            case BYTES -> schema.getLogicalType() instanceof LogicalTypes.Decimal
                    ? new Conversions.DecimalConversion()
                            .toBytes(decimal(value, schema), schema, schema.getLogicalType())
                    : ByteBuffer.wrap(bytes(value));
            case FIXED -> schema.getLogicalType() instanceof LogicalTypes.Decimal
                    ? new Conversions.DecimalConversion()
                            .toFixed(decimal(value, schema), schema, schema.getLogicalType())
                    : new GenericData.Fixed(schema, bytes(value));
            case INT -> intValue(value);
            case LONG -> longValue(value, schema);
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BOOLEAN -> value;
            case ARRAY -> {
                var items = new ArrayList<Object>();
                var itemField = field.getChildren().get(0);
                for (var item : (Collection<?>) value) {
                    items.add(fromArrow(item, schema.getElementType(), itemField));
                }
                yield items;
            }
            case MAP -> {
                var valueField = field.getChildren().get(0).getChildren().get(1);
                var result = new LinkedHashMap<String, Object>();
                for (var entry : (Collection<?>) value) {
                    var pair = (Map<?, ?>) entry;
                    result.put(
                            String.valueOf(pair.get("key")),
                            fromArrow(pair.get("value"), schema.getValueType(), valueField));
                }
                yield result;
            }
            default -> throw new ColumnarException(
                    "cannot convert Arrow value back to Avro type " + schema.getType());
        };
    }

    private static Object requireNullable(Object value, Field field) {
        if (!field.isNullable()) {
            throw new ColumnarException("required Avro field is null: " + field.getName());
        }
        return value;
    }

    private static Field child(Field field, String name) {
        return field.getChildren().stream()
                .filter(candidate -> candidate.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new ColumnarException(
                        "Arrow field " + field.getName() + " has no child " + name));
    }

    private static BigDecimal decimal(Object value, Schema schema) {
        var scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
        return (value instanceof BigDecimal number ? number : new BigDecimal(value.toString()))
                .setScale(scale);
    }

    private static byte[] bytes(Object value) {
        if (value instanceof ByteBuffer buffer) {
            var copy = buffer.duplicate();
            var result = new byte[copy.remaining()];
            copy.get(result);
            return result;
        }
        return (byte[]) value;
    }

    private static int intValue(Object value) {
        if (value instanceof LocalTime time) {
            return Math.toIntExact(time.toNanoOfDay() / 1_000_000L);
        }
        if (value instanceof LocalDateTime dateTime) {
            return Math.toIntExact(dateTime.toLocalTime().toNanoOfDay() / 1_000_000L);
        }
        return ((Number) value).intValue();
    }

    private static long longValue(Object value, Schema schema) {
        if (value instanceof LocalDateTime dateTime) {
            var logical = schema.getLogicalType();
            var instant = dateTime.toInstant(ZoneOffset.UTC);
            if (logical instanceof LogicalTypes.LocalTimestampMicros
                    || logical instanceof LogicalTypes.TimestampMicros) {
                return Math.addExact(
                        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
            }
            return instant.toEpochMilli();
        }
        return ((Number) value).longValue();
    }

    private String jsonText(Object value, Schema schema) {
        try {
            var output = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().jsonEncoder(schema, output);
            new GenericDatumWriter<Object>(schema, model).write(value, encoder);
            encoder.flush();
            return output.toString(java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException error) {
            throw new ColumnarException("cannot encode recursive Avro value as JSON", error);
        }
    }

    private Object parseJson(String text, Schema schema) {
        try {
            var decoder = DecoderFactory.get().jsonDecoder(schema, text);
            return new GenericDatumReader<>(schema, schema, model).read(null, decoder);
        } catch (java.io.IOException error) {
            throw new ColumnarException("cannot decode recursive Avro value from JSON", error);
        }
    }
}
