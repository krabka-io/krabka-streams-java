package io.krabka.streams.columnar;

import java.util.Map;

/**
 * Persists operator snapshots for one logical Kafka partition.
 *
 * <p>A group runner saves a partition's snapshots when the partition is revoked and
 * loads them when it is assigned, so stateful operators survive rebalances and
 * restarts. Snapshots are keyed by operator name, which is why node names should stay
 * stable across application versions. {@link FileColumnarStateStore} persists to
 * local files; {@link #none()} disables persistence.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var stateStore = new FileColumnarStateStore(Path.of("/var/lib/app/state"));
 * var runner = ColumnarRunner.group(
 *     topology, consumer, producer,
 *     ColumnarErrorPolicy.fail(), stateStore, new ColumnarMetrics());
 * }</pre>
 */
public interface ColumnarStateStore {
    /**
     * Loads the stored snapshots for a partition.
     *
     * @param partition the logical partition number
     * @return operator name to snapshot bytes; empty when nothing is stored
     */
    Map<String, byte[]> load(int partition);

    /**
     * Stores the snapshots for a partition, replacing what was stored before.
     *
     * @param partition the logical partition number
     * @param snapshot operator name to snapshot bytes
     */
    void save(int partition, Map<String, byte[]> snapshot);

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
            public Map<String, byte[]> load(int partition) {
                return Map.of();
            }

            @Override
            public void save(int partition, Map<String, byte[]> snapshot) {
                // Intentionally ephemeral.
            }
        };
    }
}
