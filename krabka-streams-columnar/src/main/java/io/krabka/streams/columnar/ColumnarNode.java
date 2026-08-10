package io.krabka.streams.columnar;

/**
 * An opaque handle to a columnar topology node.
 *
 * <p>Returned by every {@code add*} method of {@link ColumnarTopology} and accepted
 * as the parent argument of downstream nodes. A handle is only valid within the
 * topology that created it; passing it to another topology throws
 * {@link IllegalArgumentException}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * ColumnarNode source = topology.addSource("source", List.of("transactions"), codec);
 * ColumnarNode filtered = topology.addOperator("large", filter, source);
 * topology.addSink("sink", "large-transactions", codec, filtered);
 * }</pre>
 */
public final class ColumnarNode {
    private final ColumnarTopology owner;
    private final int index;

    ColumnarNode(ColumnarTopology owner, int index) {
        this.owner = owner;
        this.index = index;
    }

    boolean belongsTo(ColumnarTopology topology) {
        return owner == topology;
    }

    int index() {
        return index;
    }
}
