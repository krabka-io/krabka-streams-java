package io.krabka.streams.columnar;

import java.util.Map;

/** Persists operator snapshots for one logical Kafka partition. */
public interface ColumnarStateStore {
    Map<String, byte[]> load(int partition);

    void save(int partition, Map<String, byte[]> snapshot);

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
