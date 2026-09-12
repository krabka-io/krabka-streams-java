package io.krabka.streams.columnar;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Persists operator snapshots for one logical Kafka partition and one epoch.
 *
 * <p>A group runner saves a partition's snapshots when the partition is revoked and
 * loads them when it is assigned, so stateful operators survive rebalances and
 * restarts. That rebalance state uses {@link #LIVE_EPOCH}. A runner that aligns on a
 * barrier group also saves a snapshot under the epoch of each cut it reaches, and
 * {@code ColumnarRunner.GroupRunner.restoreToEpoch} reads it back. Snapshots are keyed
 * by operator name inside the map, which is why node names should stay stable across
 * application versions. {@link FileColumnarStateStore} persists to local files;
 * {@link #none()} disables persistence.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var stateStore = new FileColumnarStateStore(Path.of("/var/lib/app/state"));
 * var runner = ColumnarRunner.group(
 *     topology, consumer, producer,
 *     ColumnarErrorPolicy.fail(), stateStore, new ColumnarMetrics());
 *
 * Map<String, byte[]> live = stateStore.load(0, ColumnarStateStore.LIVE_EPOCH);
 * }</pre>
 */
public interface ColumnarStateStore {
    /**
     * The epoch of the running state, which no barrier cut owns.
     *
     * <p>Rebalance saves and loads use it. A barrier epoch is never negative, so the
     * two never collide.
     */
    long LIVE_EPOCH = -1L;

    /**
     * Loads the stored snapshots for a partition and epoch.
     *
     * @param partition the logical partition number
     * @param epoch the barrier epoch, or {@link #LIVE_EPOCH} for the running state
     * @return operator name to snapshot bytes; empty when nothing is stored
     */
    Map<String, byte[]> load(int partition, long epoch);

    /**
     * Stores the snapshots for a partition and epoch, replacing what was stored
     * before.
     *
     * @param partition the logical partition number
     * @param epoch the barrier epoch, or {@link #LIVE_EPOCH} for the running state
     * @param snapshot operator name to snapshot bytes
     */
    void save(int partition, long epoch, Map<String, byte[]> snapshot);

    /**
     * Lists the barrier epochs currently available for a partition.
     *
     * <p>An empty optional means the store cannot enumerate epochs.
     *
     * @param partition the logical partition number
     * @return the retained epochs in ascending order, when enumeration is supported
     */
    default Optional<List<Long>> retainedEpochs(int partition) {
        return Optional.empty();
    }

    /**
     * Prevents an epoch from being reclaimed while a restore is reading it.
     *
     * @param epoch the barrier epoch in use
     * @return a lease that releases the epoch when closed
     */
    default EpochLease retain(long epoch) {
        return () -> { };
    }

    /** A closeable snapshot-retention lease without checked exceptions. */
    interface EpochLease extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Returns a store that keeps nothing.
     *
     * <p>Loads are empty and saves are discarded, so operator state is ephemeral and
     * lost on rebalance or restart.
     *
     * @return the no-op store
     */
    static ColumnarStateStore none() {
        return new ColumnarStateStore() {
            @Override
            public Map<String, byte[]> load(int partition, long epoch) {
                return Map.of();
            }

            @Override
            public void save(int partition, long epoch, Map<String, byte[]> snapshot) {
                // Intentionally ephemeral.
            }

            @Override
            public Optional<List<Long>> retainedEpochs(int partition) {
                return Optional.of(List.of());
            }
        };
    }
}
