package io.krabka.streams.columnar;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Converts typed rows to and from an Arrow payload batch. */
public interface RowBridge<T> {
    VectorSchemaRoot rowsToBatch(List<T> rows, BufferAllocator allocator);

    List<T> batchToRows(VectorSchemaRoot batch);
}
