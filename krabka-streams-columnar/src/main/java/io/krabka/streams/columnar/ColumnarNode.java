package io.krabka.streams.columnar;

/** An opaque handle to a columnar topology node. */
public final class ColumnarNode {
    private final int index;

    ColumnarNode(int index) {
        this.index = index;
    }

    int index() {
        return index;
    }
}
