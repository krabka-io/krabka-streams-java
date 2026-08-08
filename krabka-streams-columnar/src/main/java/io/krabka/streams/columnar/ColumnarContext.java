package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Collects batches that a columnar processor forwards. */
public final class ColumnarContext {
    private final List<VectorSchemaRoot> forwarded = new ArrayList<>();

    public void forward(VectorSchemaRoot batch) {
        forwarded.add(Objects.requireNonNull(batch, "batch"));
    }

    List<VectorSchemaRoot> drain() {
        var result = List.copyOf(forwarded);
        forwarded.clear();
        return result;
    }

    boolean contains(VectorSchemaRoot batch) {
        return forwarded.stream().anyMatch(candidate -> candidate == batch);
    }
}
