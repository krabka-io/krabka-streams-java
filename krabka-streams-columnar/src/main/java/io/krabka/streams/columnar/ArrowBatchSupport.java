package io.krabka.streams.columnar;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;

final class ArrowBatchSupport {
    static final String KEY = "__key";
    static final String TIMESTAMP = "__timestamp";
    static final String PARTITION = "__partition";
    static final String OFFSET = "__offset";
    static final String HEADERS = "__headers";
    static final List<String> RESERVED = List.of(KEY, TIMESTAMP, PARTITION, OFFSET, HEADERS);
    private static final String PAYLOAD_NAME = "krabka.payload.name";
    private static final String PAYLOAD_PREFIX = "__payload_";

    private static final List<Field> METADATA_FIELDS = List.of(
            new Field(KEY, FieldType.nullable(new ArrowType.Binary()), null),
            new Field(TIMESTAMP, FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field(PARTITION, FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field(OFFSET, FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field(HEADERS, FieldType.nullable(new ArrowType.Binary()), null));

    private ArrowBatchSupport() {
    }

    static void rejectReservedPayloadColumns(Collection<String> names) {
        for (var name : names) {
            if (RESERVED.contains(name)) {
                throw new ColumnarException("payload column `" + name + "` collides with a reserved metadata column");
            }
        }
    }

    static VectorSchemaRoot withMetadata(
            VectorSchemaRoot payload, List<RowMetadata> metadata, BufferAllocator allocator) {
        if (payload.getRowCount() != metadata.size()) {
            throw new ColumnarException("payload and metadata row counts differ");
        }
        var fields = new ArrayList<Field>();
        payload.getSchema().getFields().forEach(field -> fields.add(escapedPayloadField(field)));
        fields.addAll(METADATA_FIELDS);
        var result = create(fields, payload.getRowCount(), allocator);
        copyVectors(payload, result, range(payload.getRowCount()), 0, payload.getFieldVectors().size());

        var key = (VarBinaryVector) result.getVector(KEY);
        var timestamp = (BigIntVector) result.getVector(TIMESTAMP);
        var partition = (IntVector) result.getVector(PARTITION);
        var offset = (BigIntVector) result.getVector(OFFSET);
        var headers = (VarBinaryVector) result.getVector(HEADERS);
        for (int row = 0; row < metadata.size(); row++) {
            var value = metadata.get(row);
            if (value.key() == null) {
                key.setNull(row);
            } else {
                key.setSafe(row, value.key());
            }
            timestamp.setSafe(row, value.timestamp());
            partition.setSafe(row, value.partition());
            offset.setSafe(row, value.offset());
            headers.setSafe(row, encodeHeaders(value.headers()));
        }
        setValueCounts(result);
        return result;
    }

    static VectorSchemaRoot concatenate(List<VectorSchemaRoot> batches, BufferAllocator allocator) {
        if (batches.isEmpty()) {
            throw new ColumnarException("cannot concatenate an empty batch list");
        }
        var schema = batches.get(0).getSchema();
        int rows = 0;
        for (var batch : batches) {
            if (!schema.equals(batch.getSchema())) {
                throw new ColumnarException("Arrow batch schemas differ");
            }
            rows = Math.addExact(rows, batch.getRowCount());
        }
        var result = create(schema.getFields(), rows, allocator);
        int destinationRow = 0;
        for (var batch : batches) {
            copyVectors(batch, result, range(batch.getRowCount()), destinationRow, batch.getFieldVectors().size());
            destinationRow += batch.getRowCount();
        }
        setValueCounts(result);
        return result;
    }

    static VectorSchemaRoot joinRows(
            VectorSchemaRoot left,
            VectorSchemaRoot right,
            List<RowPair> pairs,
            String leftPrefix,
            String rightPrefix,
            BufferAllocator allocator) {
        var leftPayload = left.getSchema().getFields().stream()
                .filter(field -> !RESERVED.contains(field.getName()))
                .toList();
        var rightPayload = right.getSchema().getFields().stream()
                .filter(field -> !RESERVED.contains(field.getName()))
                .toList();
        var fields = new ArrayList<Field>();
        leftPayload.forEach(field -> fields.add(prefixedPayloadField(field, leftPrefix)));
        rightPayload.forEach(field -> fields.add(prefixedPayloadField(field, rightPrefix)));
        fields.addAll(METADATA_FIELDS);
        var names = new HashSet<String>();
        fields.forEach(field -> {
            if (!names.add(field.getName())) {
                throw new ColumnarException("joined Arrow column name collides: " + field.getName());
            }
        });

        var result = create(fields, pairs.size(), allocator);
        for (int outputRow = 0; outputRow < pairs.size(); outputRow++) {
            var pair = pairs.get(outputRow);
            int outputColumn = 0;
            for (var field : leftPayload) {
                result.getVector(outputColumn++).copyFromSafe(
                        pair.leftRow(), outputRow, left.getVector(field.getName()));
            }
            for (var field : rightPayload) {
                result.getVector(outputColumn++).copyFromSafe(
                        pair.rightRow(), outputRow, right.getVector(field.getName()));
            }
            for (var name : RESERVED) {
                var source = left.getVector(name);
                if (source == null || source.isNull(pair.leftRow())) {
                    result.getVector(outputColumn++).setNull(outputRow);
                } else {
                    result.getVector(outputColumn++).copyFromSafe(pair.leftRow(), outputRow, source);
                }
            }
        }
        setValueCounts(result);
        return result;
    }

    static VectorSchemaRoot payload(VectorSchemaRoot root, BufferAllocator allocator) {
        var source = root.getSchema().getFields().stream()
                .filter(field -> !RESERVED.contains(field.getName()))
                .toList();
        var fields = source.stream().map(ArrowBatchSupport::restoredPayloadField).toList();
        var result = create(fields, root.getRowCount(), allocator);
        for (int column = 0; column < source.size(); column++) {
            var sourceVector = root.getVector(source.get(column).getName());
            var targetVector = result.getVector(column);
            for (int row = 0; row < root.getRowCount(); row++) {
                targetVector.copyFromSafe(row, row, sourceVector);
            }
        }
        setValueCounts(result);
        return result;
    }

    static String payloadColumn(String name) {
        return RESERVED.contains(name) ? PAYLOAD_PREFIX + name : name;
    }

    static List<RecordHeader> headers(FieldVector vector, int row) {
        if (!(vector instanceof VarBinaryVector) || vector.isNull(row)) {
            return List.of();
        }
        return decodeHeaders(((VarBinaryVector) vector).get(row));
    }

    private static byte[] encodeHeaders(List<RecordHeader> headers) {
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var output = new java.io.DataOutputStream(bytes)) {
                output.writeInt(headers.size());
                for (var header : headers) {
                    var key = header.key().getBytes(StandardCharsets.UTF_8);
                    var value = header.value();
                    output.writeInt(key.length);
                    output.write(key);
                    output.writeInt(value == null ? -1 : value.length);
                    if (value != null) {
                        output.write(value);
                    }
                }
            }
            return bytes.toByteArray();
        } catch (java.io.IOException error) {
            throw new AssertionError(error);
        }
    }

    private static List<RecordHeader> decodeHeaders(byte[] bytes) {
        try (var input = new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
            int count = input.readInt();
            if (count < 0 || count > bytes.length / 8) {
                throw new ColumnarException("invalid Kafka header count");
            }
            var result = new ArrayList<RecordHeader>(count);
            for (int index = 0; index < count; index++) {
                int keyLength = input.readInt();
                if (keyLength < 0) {
                    throw new ColumnarException("negative Kafka header key length");
                }
                var key = new String(readExact(input, keyLength), StandardCharsets.UTF_8);
                int valueLength = input.readInt();
                if (valueLength < -1) {
                    throw new ColumnarException("invalid Kafka header value length");
                }
                result.add(new RecordHeader(key, valueLength < 0 ? null : readExact(input, valueLength)));
            }
            if (input.available() != 0) {
                throw new ColumnarException("trailing bytes in Kafka headers");
            }
            return List.copyOf(result);
        } catch (java.io.IOException error) {
            throw new ColumnarException("cannot decode Kafka headers", error);
        }
    }

    private static byte[] readExact(java.io.DataInputStream input, int length) throws java.io.IOException {
        var bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new ColumnarException("truncated Kafka headers");
        }
        return bytes;
    }

    static VectorSchemaRoot select(VectorSchemaRoot root, Collection<String> names, BufferAllocator allocator) {
        var unique = new HashSet<String>();
        var fields = new ArrayList<Field>();
        var sourceVectors = new ArrayList<FieldVector>();
        for (var name : names) {
            if (!unique.add(name)) {
                continue;
            }
            var vector = root.getVector(name);
            if (vector == null) {
                throw new ColumnarException("Arrow column does not exist: " + name);
            }
            fields.add(vector.getField());
            sourceVectors.add(vector);
        }
        var result = create(fields, root.getRowCount(), allocator);
        for (int column = 0; column < sourceVectors.size(); column++) {
            var source = sourceVectors.get(column);
            var target = result.getVector(column);
            for (int row = 0; row < root.getRowCount(); row++) {
                target.copyFromSafe(row, row, source);
            }
        }
        setValueCounts(result);
        return result;
    }

    static VectorSchemaRoot copyRows(VectorSchemaRoot root, int[] rows, BufferAllocator allocator) {
        var result = create(root.getSchema().getFields(), rows.length, allocator);
        copyVectors(root, result, rows, 0, root.getFieldVectors().size());
        setValueCounts(result);
        return result;
    }

    static VectorSchemaRoot copyRange(VectorSchemaRoot root, int start, int length, BufferAllocator allocator) {
        var rows = new int[length];
        for (int index = 0; index < length; index++) {
            rows[index] = start + index;
        }
        return copyRows(root, rows, allocator);
    }

    static VectorSchemaRoot create(List<Field> fields, int rows, BufferAllocator allocator) {
        var root = VectorSchemaRoot.create(new org.apache.arrow.vector.types.pojo.Schema(fields), allocator);
        root.setRowCount(rows);
        for (var vector : root.getFieldVectors()) {
            vector.setInitialCapacity(Math.max(rows, 1));
            vector.allocateNew();
        }
        return root;
    }

    static Object value(FieldVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        var value = vector.getObject(row);
        if (value instanceof Text text) {
            return text.toString();
        }
        if (value instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes.clone()).asReadOnlyBuffer();
        }
        return value;
    }

    static void setValue(FieldVector vector, int row, Object value) {
        if (value == null) {
            vector.setNull(row);
        } else if (vector instanceof VarCharVector target) {
            target.setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof VarBinaryVector target) {
            if (value instanceof ByteBuffer buffer) {
                var copy = buffer.duplicate();
                var bytes = new byte[copy.remaining()];
                copy.get(bytes);
                target.setSafe(row, bytes);
            } else {
                target.setSafe(row, (byte[]) value);
            }
        } else if (vector instanceof BigIntVector target) {
            target.setSafe(row, exactLong(value));
        } else if (vector instanceof IntVector target) {
            target.setSafe(row, Math.toIntExact(exactLong(value)));
        } else if (vector instanceof SmallIntVector target) {
            long number = exactLong(value);
            if (number < Short.MIN_VALUE || number > Short.MAX_VALUE) {
                throw new ArithmeticException("short overflow: " + number);
            }
            target.setSafe(row, (short) number);
        } else if (vector instanceof TinyIntVector target) {
            long number = exactLong(value);
            if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) {
                throw new ArithmeticException("byte overflow: " + number);
            }
            target.setSafe(row, (byte) number);
        } else if (vector instanceof UInt1Vector target) {
            target.setSafe(row, Math.toIntExact(unsigned(value, 8)));
        } else if (vector instanceof UInt2Vector target) {
            target.setSafe(row, Math.toIntExact(unsigned(value, 16)));
        } else if (vector instanceof UInt4Vector target) {
            target.setSafe(row, (int) unsigned(value, 32));
        } else if (vector instanceof UInt8Vector target) {
            var number = new java.math.BigInteger(value.toString());
            if (number.signum() < 0 || number.bitLength() > 64) {
                throw new ArithmeticException("unsigned 64-bit overflow: " + number);
            }
            target.setSafe(row, number.longValue());
        } else if (vector instanceof Float4Vector target) {
            target.setSafe(row, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector target) {
            target.setSafe(row, ((Number) value).doubleValue());
        } else if (vector instanceof BitVector target) {
            target.setSafe(row, Boolean.TRUE.equals(value) ? 1 : 0);
        } else if (vector instanceof DateDayVector target) {
            target.setSafe(row, value instanceof java.time.LocalDate date
                    ? Math.toIntExact(date.toEpochDay())
                    : ((Number) value).intValue());
        } else if (vector instanceof DateMilliVector target) {
            target.setSafe(row, epochMillis(value));
        } else if (vector instanceof TimeStampVector target) {
            target.setSafe(row, timestampValue(value, vector.getField().getType()));
        } else if (vector instanceof DecimalVector target) {
            target.setSafe(row, decimal(value));
        } else if (vector instanceof Decimal256Vector target) {
            target.setSafe(row, decimal(value));
        } else if (vector instanceof MapVector target && value instanceof Map<?, ?> map) {
            var entries = map.entrySet().stream()
                    .map(entry -> {
                        var result = new java.util.HashMap<String, Object>();
                        result.put("key", entry.getKey());
                        result.put("value", entry.getValue());
                        return result;
                    })
                    .toList();
            setList(target, row, entries);
        } else if (vector instanceof ListVector target && value instanceof Collection<?> values) {
            setList(target, row, values);
        } else if (vector instanceof FixedSizeListVector target && value instanceof Collection<?> values) {
            if (values.size() != target.getListSize()) {
                throw new ColumnarException("fixed-size list requires " + target.getListSize() + " values");
            }
            int start = target.startNewValue(row);
            int index = 0;
            for (var item : values) {
                setValue(target.getDataVector(), start + index++, item);
            }
        } else if (vector instanceof StructVector target && value instanceof Map<?, ?> values) {
            target.setIndexDefined(row);
            for (var child : target.getChildrenFromFields()) {
                setValue(child, row, values.get(child.getName()));
            }
        } else if (vector instanceof DenseUnionVector target) {
            var children = target.getChildrenFromFields();
            int childIndex = java.util.stream.IntStream.range(0, children.size())
                    .filter(index -> accepts(children.get(index), value))
                    .findFirst()
                    .orElseThrow(() -> new ColumnarException("no Arrow union member accepts " + value.getClass()));
            var child = children.get(childIndex);
            int offset = child.getValueCount();
            setValue(child, offset, value);
            child.setValueCount(offset + 1);
            var typeIds = ((ArrowType.Union) vector.getField().getType()).getTypeIds();
            target.setTypeId(row, (byte) typeIds[childIndex]);
            target.setOffset(row, offset);
        } else if (vector instanceof UnionVector target) {
            var child = target.getChildrenFromFields().stream()
                    .filter(candidate -> accepts(candidate, value))
                    .findFirst()
                    .orElseThrow(() -> new ColumnarException("no Arrow union member accepts " + value.getClass()));
            setValue(child, row, value);
            target.setType(row, child.getMinorType());
        } else {
            throw new ColumnarException("cannot write Arrow type " + vector.getField().getType());
        }
    }

    private static void setList(ListVector vector, int row, Collection<?> values) {
        int start = vector.startNewValue(row);
        int index = 0;
        for (var value : values) {
            setValue(vector.getDataVector(), start + index++, value);
        }
        vector.endValue(row, values.size());
    }

    private static boolean accepts(FieldVector vector, Object value) {
        return (value instanceof CharSequence && vector instanceof VarCharVector)
                || (value instanceof byte[] || value instanceof ByteBuffer) && vector instanceof VarBinaryVector
                || value instanceof Boolean && vector instanceof BitVector
                || value instanceof Float && vector instanceof Float4Vector
                || value instanceof Double && vector instanceof Float8Vector
                || value instanceof java.math.BigDecimal
                        && (vector instanceof DecimalVector || vector instanceof Decimal256Vector)
                || value instanceof Number && !(value instanceof java.math.BigDecimal) && (vector instanceof BigIntVector
                        || vector instanceof IntVector
                        || vector instanceof SmallIntVector
                        || vector instanceof TinyIntVector
                        || vector instanceof UInt1Vector
                        || vector instanceof UInt2Vector
                        || vector instanceof UInt4Vector
                        || vector instanceof UInt8Vector)
                || value instanceof java.time.LocalDate
                        && (vector instanceof DateDayVector || vector instanceof DateMilliVector)
                || (value instanceof java.time.Instant || value instanceof java.time.LocalDateTime)
                        && vector instanceof TimeStampVector
                || value instanceof Collection<?> && (vector instanceof ListVector
                        || vector instanceof FixedSizeListVector)
                || value instanceof Map<?, ?> && (vector instanceof StructVector || vector instanceof MapVector);
    }

    private static java.math.BigDecimal decimal(Object value) {
        return value instanceof java.math.BigDecimal decimal
                ? decimal
                : new java.math.BigDecimal(value.toString());
    }

    private static long unsigned(Object value, int bits) {
        var number = new java.math.BigInteger(value.toString());
        var max = java.math.BigInteger.ONE.shiftLeft(bits).subtract(java.math.BigInteger.ONE);
        if (number.signum() < 0 || number.compareTo(max) > 0) {
            throw new ArithmeticException("unsigned " + bits + "-bit overflow: " + number);
        }
        return number.longValue();
    }

    private static long exactLong(Object value) {
        return value instanceof java.math.BigInteger integer
                ? integer.longValueExact()
                : new java.math.BigDecimal(value.toString()).longValueExact();
    }

    private static long epochMillis(Object value) {
        if (value instanceof java.time.Instant instant) {
            return instant.toEpochMilli();
        }
        if (value instanceof java.time.LocalDateTime dateTime) {
            return dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        }
        if (value instanceof java.time.LocalDate date) {
            return date.atStartOfDay().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
        }
        return ((Number) value).longValue();
    }

    private static long timestampValue(Object value, ArrowType type) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        long nanos = value instanceof java.time.Instant instant
                ? Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano())
                : Math.multiplyExact(epochMillis(value), 1_000_000L);
        var unit = ((ArrowType.Timestamp) type).getUnit();
        return switch (unit) {
            case SECOND -> nanos / 1_000_000_000L;
            case MILLISECOND -> nanos / 1_000_000L;
            case MICROSECOND -> nanos / 1_000L;
            case NANOSECOND -> nanos;
        };
    }

    static void setValueCounts(VectorSchemaRoot root) {
        for (var vector : root.getFieldVectors()) {
            vector.setValueCount(root.getRowCount());
        }
    }

    private static void copyVectors(
            VectorSchemaRoot source,
            VectorSchemaRoot target,
            int[] sourceRows,
            int destinationStart,
            int columns) {
        for (int column = 0; column < columns; column++) {
            var sourceVector = source.getVector(column);
            var targetVector = target.getVector(column);
            for (int index = 0; index < sourceRows.length; index++) {
                targetVector.copyFromSafe(sourceRows[index], destinationStart + index, sourceVector);
            }
        }
    }

    private static int[] range(int size) {
        var result = new int[size];
        for (int index = 0; index < size; index++) {
            result[index] = index;
        }
        return result;
    }

    private static Field escapedPayloadField(Field field) {
        if (!RESERVED.contains(field.getName())) {
            return field;
        }
        var metadata = new java.util.HashMap<>(field.getMetadata());
        metadata.put(PAYLOAD_NAME, field.getName());
        return renamed(field, payloadColumn(field.getName()), metadata);
    }

    private static Field restoredPayloadField(Field field) {
        var original = field.getMetadata().get(PAYLOAD_NAME);
        if (original == null) {
            return field;
        }
        var metadata = new java.util.HashMap<>(field.getMetadata());
        metadata.remove(PAYLOAD_NAME);
        return renamed(field, original, metadata);
    }

    private static Field prefixedPayloadField(Field field, String prefix) {
        var restored = restoredPayloadField(field);
        return renamed(restored, prefix + restored.getName(), restored.getMetadata());
    }

    private static Field renamed(Field field, String name, java.util.Map<String, String> metadata) {
        var type = field.getFieldType();
        return new Field(
                name,
                new FieldType(type.isNullable(), type.getType(), type.getDictionary(), metadata),
                field.getChildren());
    }

    record RowMetadata(byte[] key, long timestamp, int partition, long offset, List<RecordHeader> headers) {
        RowMetadata(byte[] key, long timestamp, int partition, long offset) {
            this(key, timestamp, partition, offset, List.of());
        }

        RowMetadata {
            key = key == null ? null : key.clone();
            headers = List.copyOf(headers);
        }

        @Override
        public byte[] key() {
            return key == null ? null : key.clone();
        }
    }

    record RowPair(int leftRow, int rightRow) {
    }
}
