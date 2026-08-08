package io.krabka.streams.columnar;

/** An opaque handle to a columnar topology node. */
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
