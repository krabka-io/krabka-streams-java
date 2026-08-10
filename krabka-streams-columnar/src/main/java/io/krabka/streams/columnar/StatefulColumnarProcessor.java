package io.krabka.streams.columnar;

/**
 * A processor whose partition-local state can be snapshotted and restored.
 *
 * <p>The topology snapshots stateful processors through
 * {@link BuiltColumnarTopology#snapshotPartition(int)} and restores them through
 * {@link BuiltColumnarTopology#restorePartition(int, java.util.Map)}. Group runners
 * do this automatically around rebalances using a {@link ColumnarStateStore}, and
 * roll a partition back to its pre-batch snapshot when processing fails.
 *
 * <p>The snapshot format is the processor's own; it only needs to be stable between
 * the versions of the application that write and read it. Snapshots should be
 * self-validating: {@link #restore(byte[])} throws {@link ColumnarException} for
 * bytes it does not understand.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * final class RunningCount implements StatefulColumnarProcessor {
 *     private long count;
 *
 *     public void process(ColumnarContext context, VectorSchemaRoot batch) {
 *         count += batch.getRowCount();
 *     }
 *
 *     public byte[] snapshot() {
 *         return ByteBuffer.allocate(Long.BYTES).putLong(count).array();
 *     }
 *
 *     public void restore(byte[] snapshot) {
 *         count = ByteBuffer.wrap(snapshot).getLong();
 *     }
 * }
 * }</pre>
 */
public interface StatefulColumnarProcessor extends ColumnarProcessor {
    /**
     * Serializes the processor's current state.
     *
     * @return the state as bytes; never null, empty for "no state yet"
     */
    byte[] snapshot();

    /**
     * Replaces the processor's state with a previously taken snapshot.
     *
     * @param snapshot bytes previously returned by {@link #snapshot()}
     * @throws ColumnarException if the bytes cannot be restored
     */
    void restore(byte[] snapshot);
}
