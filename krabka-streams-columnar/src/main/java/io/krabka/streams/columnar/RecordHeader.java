package io.krabka.streams.columnar;

import java.util.Objects;

/** One immutable Kafka record header. */
public record RecordHeader(String key, byte[] value) {
    public RecordHeader {
        Objects.requireNonNull(key, "key");
        value = value == null ? null : value.clone();
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RecordHeader header
                && key.equals(header.key)
                && java.util.Arrays.equals(value, header.value);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + java.util.Arrays.hashCode(value);
    }
}
