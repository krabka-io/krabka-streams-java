package io.krabka.streams.columnar;

import java.util.List;

/**
 * One input record in a partition batch.
 *
 * <p>This is the broker-independent form of a fetched Kafka record: the runner and
 * the test driver both build it, and {@link BatchCodec#decode(java.util.List)} turns a
 * list of them into one Arrow batch. Key and value bytes are defensively copied in
 * both directions, so a record can be shared freely between tests and threads.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var record = new ConsumedRecord(key, arrowIpcBytes, 1_700_000_000_000L, 0, 42L);
 * try (var batch = codec.decode("transactions", List.of(record))) {
 *     // process the batch
 * }
 * }</pre>
 *
 * @param key the record key bytes, or null for a keyless record
 * @param value the record value bytes
 * @param timestamp the record timestamp in epoch milliseconds
 * @param partition the source partition number
 * @param offset the record offset within the partition
 * @param headers the ordered Kafka headers
 */
public record ConsumedRecord(
        byte[] key,
        byte[] value,
        long timestamp,
        int partition,
        long offset,
        List<RecordHeader> headers) {
    /**
     * Creates a record without headers.
     *
     * @param key the record key bytes, or null for a keyless record
     * @param value the record value bytes
     * @param timestamp the record timestamp in epoch milliseconds
     * @param partition the source partition number
     * @param offset the record offset within the partition
     */
    public ConsumedRecord(byte[] key, byte[] value, long timestamp, int partition, long offset) {
        this(key, value, timestamp, partition, offset, List.of());
    }

    /**
     * Copies the mutable components so the record is immutable.
     *
     * @param key the record key bytes, or null for a keyless record
     * @param value the record value bytes
     * @param timestamp the record timestamp in epoch milliseconds
     * @param partition the source partition number
     * @param offset the record offset within the partition
     * @param headers the ordered Kafka headers
     */
    public ConsumedRecord {
        key = key == null ? null : key.clone();
        value = value.clone();
        headers = List.copyOf(headers);
    }

    /**
     * Returns a copy of the record key.
     *
     * @return a fresh copy of the key bytes, or null for a keyless record
     */
    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    /**
     * Returns a copy of the record value.
     *
     * @return a fresh copy of the value bytes
     */
    @Override
    public byte[] value() {
        return value.clone();
    }
}
