package io.krabka.streams.columnar;

import java.util.List;

/** One output record from a columnar sink. */
public record ProduceRecord(byte[] key, byte[] value, long timestamp, List<RecordHeader> headers) {
    public ProduceRecord(byte[] key, byte[] value, long timestamp) {
        this(key, value, timestamp, List.of());
    }

    public ProduceRecord {
        key = key == null ? null : key.clone();
        value = value.clone();
        headers = List.copyOf(headers);
    }

    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
