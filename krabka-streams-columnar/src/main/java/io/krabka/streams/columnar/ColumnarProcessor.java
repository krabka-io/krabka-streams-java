package io.krabka.streams.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/** Processes one Arrow batch and forwards zero or more output batches. */
@FunctionalInterface
public interface ColumnarProcessor {
    void process(ColumnarContext context, VectorSchemaRoot batch);
}
