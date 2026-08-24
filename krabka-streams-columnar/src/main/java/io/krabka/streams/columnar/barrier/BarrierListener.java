package io.krabka.streams.columnar.barrier;

/**
 * Receives a cut when a runner reaches the barrier.
 *
 * <p>The runner calls the listener once per epoch, after it snapshots every owned
 * partition under the cut's epoch and commits the cut offsets. The call runs on the
 * thread that drives the runner, so keep the work short. A listener that throws fails
 * the cycle, and the snapshot and the commit have already happened.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * BarrierListener listener = cut ->
 *     System.out.println("aligned on epoch " + cut.epoch() + " at " + cut.offsets());
 * }</pre>
 */
@FunctionalInterface
public interface BarrierListener {
    /**
     * Reports that every assigned partition reached the cut.
     *
     * @param cut the cut the runner aligned on
     */
    void onBarrier(BarrierCut cut);

    /**
     * Returns a listener that does nothing.
     *
     * @return a listener that ignores every barrier
     */
    static BarrierListener none() {
        return cut -> {
            // Intentionally silent.
        };
    }
}
