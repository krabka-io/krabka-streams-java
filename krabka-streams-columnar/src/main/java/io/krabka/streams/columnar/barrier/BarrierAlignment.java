package io.krabka.streams.columnar.barrier;

import java.util.Objects;

/**
 * Tells a group runner which barrier group to align on.
 *
 * <p>Pass one to the {@code ColumnarRunner.group} overload that takes it. The runner
 * then reads the group's cuts from {@code __barrier_state}, holds back every record at
 * or above each partition's marker offset, and fires the barrier when every assigned
 * partition reaches the cut. At that point it snapshots each owned partition under the
 * cut's epoch, commits the cut offsets, and calls the listener.
 *
 * <p>Give the reader a consumer of its own. The runner's consumer is subscribed to the
 * topology's source topics, and the reader replaces the assignment of the consumer it
 * drives.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var alignment = BarrierAlignment.on("transactions", new BarrierCutReader(cutConsumer))
 *     .withListener(cut -> System.out.println("epoch " + cut.epoch()));
 *
 * try (var runner = ColumnarRunner.group(topology, consumer, producer,
 *         ColumnarErrorPolicy.fail(), new FileColumnarStateStore(directory),
 *         new ColumnarMetrics(), alignment)) {
 *     runner.runOnce(Duration.ofSeconds(1));
 * }
 * }</pre>
 *
 * @param group the barrier group whose cuts the runner aligns on
 * @param reader the reader the runner pulls cuts from
 * @param listener the callback the runner calls when a barrier fires
 */
public record BarrierAlignment(String group, BarrierCutReader reader, BarrierListener listener) {
    /**
     * Rejects null components.
     *
     * @param group the barrier group whose cuts the runner aligns on
     * @param reader the reader the runner pulls cuts from
     * @param listener the callback the runner calls when a barrier fires
     */
    public BarrierAlignment {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(listener, "listener");
    }

    /**
     * Creates an alignment with no listener.
     *
     * @param group the barrier group whose cuts the runner aligns on
     * @param reader the reader the runner pulls cuts from
     * @return the alignment
     */
    public static BarrierAlignment on(String group, BarrierCutReader reader) {
        return new BarrierAlignment(group, reader, BarrierListener.none());
    }

    /**
     * Returns a copy of this alignment with another listener.
     *
     * @param newListener the callback the runner calls when a barrier fires
     * @return the copy
     */
    public BarrierAlignment withListener(BarrierListener newListener) {
        return new BarrierAlignment(group, reader, newListener);
    }
}
