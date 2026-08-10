package io.krabka.streams.columnar;

import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Type-coercing Arrow batch helpers for {@link RowBridge} and
 * {@link ColumnarProcessor} implementations.
 *
 * <p>These are the same conversions the built-in operators and bridges use: a write
 * dispatches on the vector's type and accepts natural Java values ({@code Number},
 * {@code CharSequence}, {@code byte[]} or {@code ByteBuffer}, {@code Boolean},
 * {@code BigDecimal}, {@code java.time} types, {@code Collection} for lists,
 * {@code Map} for maps and structs), and a read returns the matching Java value with
 * {@code Text} normalized to {@link String}, binary to a read-only
 * {@code ByteBuffer}, and unsigned 64-bit integers to {@code BigInteger}. Using them
 * keeps custom code consistent with the rest of the engine instead of reimplementing
 * raw vector access.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var fields = List.of(
 *     new Field("user", FieldType.nullable(new ArrowType.Utf8()), null));
 * VectorSchemaRoot root = ArrowValues.createRoot(fields, 2, allocator);
 * ArrowValues.set(root.getVector("user"), 0, "ada");
 * ArrowValues.set(root.getVector("user"), 1, null);
 * ArrowValues.finish(root);
 * Object first = ArrowValues.get(root.getVector("user"), 0); // "ada"
 * }</pre>
 */
public final class ArrowValues {
    private ArrowValues() {
    }

    /**
     * Creates a batch with allocated vectors and the given row count.
     *
     * @param fields the columns of the batch
     * @param rows the number of rows the batch holds
     * @param allocator the allocator that owns the batch's buffers
     * @return the batch; the caller must close it
     */
    public static VectorSchemaRoot createRoot(List<Field> fields, int rows, BufferAllocator allocator) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(allocator, "allocator");
        return ArrowBatchSupport.create(fields, rows, allocator);
    }

    /**
     * Writes one value into a vector, coercing it to the vector's Arrow type.
     *
     * @param vector the vector to write to
     * @param row the row index to write at
     * @param value the value to write; null marks the row null
     * @throws ColumnarException if the vector's Arrow type is unsupported or the
     *     value does not fit it
     */
    public static void set(FieldVector vector, int row, Object value) {
        Objects.requireNonNull(vector, "vector");
        ArrowBatchSupport.setValue(vector, row, value);
    }

    /**
     * Reads one value from a vector as its natural Java representation.
     *
     * @param vector the vector to read from
     * @param row the row index to read
     * @return the value, or null if the row is null
     */
    public static Object get(FieldVector vector, int row) {
        Objects.requireNonNull(vector, "vector");
        return ArrowBatchSupport.value(vector, row);
    }

    /**
     * Sets every vector's value count to the batch's row count.
     *
     * <p>Call this once after the last {@link #set} so readers observe every written
     * row.
     *
     * @param root the batch to finish
     */
    public static void finish(VectorSchemaRoot root) {
        Objects.requireNonNull(root, "root");
        ArrowBatchSupport.setValueCounts(root);
    }
}
