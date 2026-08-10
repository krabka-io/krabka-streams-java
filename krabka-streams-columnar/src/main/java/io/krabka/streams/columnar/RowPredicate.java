package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Tests one row in an Arrow batch.
 *
 * <p>Used by {@link BuiltinOp#filter(org.apache.arrow.memory.BufferAllocator, RowPredicate)},
 * which calls the predicate once per row and copies the passing rows into a new
 * batch. Read vectors by name, cast to the concrete Arrow vector type, and check
 * {@code isNull(row)} before reading a nullable column: the typed accessors do not.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * RowPredicate largeAmount = (batch, row) -> {
 *     var amounts = (BigIntVector) batch.getVector("amount");
 *     return !amounts.isNull(row) && amounts.get(row) > 4;
 * };
 * var filter = BuiltinOp.filter(allocator, largeAmount);
 * }</pre>
 */
@FunctionalInterface
public interface RowPredicate {
    /**
     * Decides whether a row is kept.
     *
     * @param batch the batch the row belongs to; the predicate must not modify or
     *     close it
     * @param row the row index to test
     * @return true to keep the row, false to drop it
     */
    boolean test(VectorSchemaRoot batch, int row);
}
