package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/** Tests one row in an Arrow batch. */
@FunctionalInterface
public interface RowPredicate {
    boolean test(VectorSchemaRoot batch, int row);
}
