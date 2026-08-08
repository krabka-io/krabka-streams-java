package io.krabka.streams.columnar;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;

final class ArrowBatchSupport {
    static final String KEY = "__key";
    static final String TIMESTAMP = "__timestamp";
    static final String PARTITION = "__partition";
    static final String OFFSET = "__offset";
    static final List<String> RESERVED = List.of(KEY, TIMESTAMP, PARTITION, OFFSET);

    private static final List<Field> METADATA_FIELDS = List.of(
            new Field(KEY, FieldType.nullable(new ArrowType.Binary()), null),
            new Field(TIMESTAMP, FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field(PARTITION, FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field(OFFSET, FieldType.nullable(new ArrowType.Int(64, true)), null));

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
        rejectReservedPayloadColumns(payload.getSchema().getFields().stream().map(Field::getName).toList());
        var fields = new ArrayList<Field>(payload.getSchema().getFields());
        fields.addAll(METADATA_FIELDS);
        var result = create(fields, payload.getRowCount(), allocator);
        copyVectors(payload, result, range(payload.getRowCount()), 0, payload.getFieldVectors().size());

        var key = (VarBinaryVector) result.getVector(KEY);
        var timestamp = (BigIntVector) result.getVector(TIMESTAMP);
        var partition = (IntVector) result.getVector(PARTITION);
        var offset = (BigIntVector) result.getVector(OFFSET);
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

    static VectorSchemaRoot payload(VectorSchemaRoot root, BufferAllocator allocator) {
        var names = root.getSchema().getFields().stream()
                .map(Field::getName)
                .filter(name -> !RESERVED.contains(name))
                .toList();
        return select(root, names, allocator);
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
            target.setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof IntVector target) {
            target.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof SmallIntVector target) {
            target.setSafe(row, ((Number) value).shortValue());
        } else if (vector instanceof TinyIntVector target) {
            target.setSafe(row, ((Number) value).byteValue());
        } else if (vector instanceof UInt1Vector target) {
            target.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof UInt2Vector target) {
            target.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof UInt4Vector target) {
            target.setSafe(row, ((Number) value).intValue());
        } else if (vector instanceof UInt8Vector target) {
            target.setSafe(row, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector target) {
            target.setSafe(row, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector target) {
            target.setSafe(row, ((Number) value).doubleValue());
        } else if (vector instanceof BitVector target) {
            target.setSafe(row, Boolean.TRUE.equals(value) ? 1 : 0);
        } else {
            throw new ColumnarException("cannot write Arrow type " + vector.getField().getType());
        }
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

    record RowMetadata(byte[] key, long timestamp, int partition, long offset) {
        RowMetadata {
            key = key == null ? null : key.clone();
        }

        @Override
        public byte[] key() {
            return key == null ? null : key.clone();
        }
    }
}
