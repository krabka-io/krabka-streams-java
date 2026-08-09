package io.krabka.streams.columnar;

/** A processor whose partition-local state can be snapshotted and restored. */
public interface StatefulColumnarProcessor extends ColumnarProcessor {
    byte[] snapshot();

    void restore(byte[] snapshot);
}
