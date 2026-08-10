package io.krabka.streams.columnar;

import java.util.Objects;

/**
 * One immutable Kafka record header.
 *
 * <p>Unlike Kafka's own header type, this record defensively copies its value bytes
 * in both directions and implements value-based {@link #equals(Object)} and
 * {@link #hashCode()}, which makes it safe to use in assertions and as map or set
 * elements.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var traceId = new RecordHeader("trace-id", "abc123".getBytes(UTF_8));
 * driver.pipeInput("input", 0, key, value, 0L, List.of(traceId));
 * }</pre>
 *
 * @param key the header key
 * @param value the header value bytes, or null for a null-valued header
 */
public record RecordHeader(String key, byte[] value) {
    /**
     * Copies the value so later mutation of the input array cannot change the header.
     *
     * @param key the header key
     * @param value the header value bytes, or null for a null-valued header
     * @throws NullPointerException if {@code key} is null
     */
    public RecordHeader {
        Objects.requireNonNull(key, "key");
        value = value == null ? null : value.clone();
    }

    /**
     * Returns a copy of the header value.
     *
     * @return a fresh copy of the value bytes, or null for a null-valued header
     */
    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    /**
     * Compares headers by key and value content.
     *
     * @param other the object to compare with
     * @return true when {@code other} is a header with an equal key and equal value bytes
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof RecordHeader header
                && key.equals(header.key)
                && java.util.Arrays.equals(value, header.value);
    }

    /**
     * Hashes the key and value content consistently with {@link #equals(Object)}.
     *
     * @return the content-based hash code
     */
    @Override
    public int hashCode() {
        return 31 * key.hashCode() + java.util.Arrays.hashCode(value);
    }
}
