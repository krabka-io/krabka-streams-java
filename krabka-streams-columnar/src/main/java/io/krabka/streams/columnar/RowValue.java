package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Computes one derived Arrow column value.
 *
 * <p>Used inside a {@link DerivedColumn} passed to
 * {@link BuiltinOp#withColumns(org.apache.arrow.memory.BufferAllocator, DerivedColumn...)}.
 * The returned object is coerced to the derived column's declared Arrow type;
 * returning null writes a null. Numeric coercions are overflow-checked and a value no
 * conversion accepts throws {@link ColumnarException}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RowValue doubled = (batch, row) ->
 *     ((BigIntVector) batch.getVector("amount")).get(row) * 2;
 * var column = new DerivedColumn(
 *     new Field("double_amount", FieldType.nullable(new ArrowType.Int(64, true)), null),
 *     doubled);
 * }</pre>
 */
@FunctionalInterface
public interface RowValue {
    /**
     * Computes the derived value for one row.
     *
     * @param batch the input batch; the function must not modify or close it
     * @param row the row index to compute the value for
     * @return the value to write, coerced to the declared Arrow type, or null
     */
    Object value(VectorSchemaRoot batch, int row);
}
