package io.krabka.streams.columnar;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Field;

/** Defines a column that a with-columns operator adds or replaces. */
public record DerivedColumn(Field field, RowValue value) {
    public DerivedColumn {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(value, "value");
    }
}
