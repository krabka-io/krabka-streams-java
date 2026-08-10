package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Collects batches that a columnar processor forwards.
 *
 * <p>A fresh context is handed to {@link ColumnarProcessor#process} for every input
 * batch. Calling {@link #forward(VectorSchemaRoot)} zero times drops the batch, once
 * passes one batch downstream, and several times fans the input out into several
 * downstream batches, delivered in forwarding order.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * ColumnarProcessor splitBySize = (context, batch) -> {
 *     context.forward(smallRows(batch));
 *     context.forward(largeRows(batch));
 * };
 * }</pre>
 */
public final class ColumnarContext {
    private final List<VectorSchemaRoot> forwarded = new ArrayList<>();

    /**
     * Creates an empty context.
     *
     * <p>The framework creates one per processed batch; create one directly only to
     * unit-test a {@link ColumnarProcessor} outside a topology.
     */
    public ColumnarContext() {
    }

    /**
     * Forwards one batch to every downstream node.
     *
     * <p>Ownership of a forwarded batch transfers to the framework, which closes it
     * once downstream processing finishes. Forwarding the input batch itself is
     * allowed.
     *
     * @param batch the batch to pass downstream
     * @throws NullPointerException if {@code batch} is null
     */
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
