package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/** Computes one derived Arrow column value. */
@FunctionalInterface
public interface RowValue {
    Object value(VectorSchemaRoot batch, int row);
}
