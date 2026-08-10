package io.krabka.streams.columnar;

import java.util.List;

/**
 * One output record from a columnar sink.
 *
 * <p>Sinks emit these through {@link BatchCodec#encode(org.apache.arrow.vector.VectorSchemaRoot)},
 * and {@link ColumnarRunner#sendAsync(java.util.List, org.apache.kafka.clients.producer.Producer)}
 * converts them to Kafka producer records. A timestamp below zero means "no
 * timestamp": the runner then lets the broker or producer assign one. Key and value
 * bytes are defensively copied in both directions.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * ProduceRecord output = driver.readOutput("processed-transactions");
 * assertThat(output.value()).isNotEmpty();
 * assertThat(output.headers()).contains(new RecordHeader("trace-id", traceId));
 * }</pre>
 *
 * @param key the record key bytes, or null for a keyless record
 * @param value the record value bytes
 * @param timestamp the record timestamp in epoch milliseconds, or a negative value
 *     for none
 * @param headers the ordered Kafka headers
 */
public record ProduceRecord(byte[] key, byte[] value, long timestamp, List<RecordHeader> headers) {
    /**
     * Creates a record without headers.
     *
     * @param key the record key bytes, or null for a keyless record
     * @param value the record value bytes
     * @param timestamp the record timestamp in epoch milliseconds, or a negative
     *     value for none
     */
    public ProduceRecord(byte[] key, byte[] value, long timestamp) {
        this(key, value, timestamp, List.of());
    }

    /**
     * Copies the mutable components so the record is immutable.
     *
     * @param key the record key bytes, or null for a keyless record
     * @param value the record value bytes
     * @param timestamp the record timestamp in epoch milliseconds, or a negative
     *     value for none
     * @param headers the ordered Kafka headers
     */
    public ProduceRecord {
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
