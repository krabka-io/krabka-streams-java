package io.krabka.streams.columnar;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Defines a column that a with-columns operator adds or replaces.
 *
 * <p>A derived column whose field name matches an existing payload column replaces it
 * in place, keeping its position; a new name is appended after the existing columns.
 * Reserved metadata names such as {@code __timestamp} are rejected when the operator
 * is constructed.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var doubled = new DerivedColumn(
 *     new Field("double_amount", FieldType.nullable(new ArrowType.Int(64, true)), null),
 *     (batch, row) -> ((BigIntVector) batch.getVector("amount")).get(row) * 2);
 * var op = BuiltinOp.withColumns(allocator, doubled);
 * }</pre>
 *
 * @param field the Arrow field declaring the column's name, type, and nullability
 * @param value the function that computes the column's value for each row
 */
public record DerivedColumn(Field field, RowValue value) {
    /**
     * Validates that both components are present.
     *
     * @param field the Arrow field declaring the column's name, type, and nullability
     * @param value the function that computes the column's value for each row
     * @throws NullPointerException if {@code field} or {@code value} is null
     */
    public DerivedColumn {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(value, "value");
    }
}
