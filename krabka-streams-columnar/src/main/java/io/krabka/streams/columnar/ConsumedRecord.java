package io.krabka.streams.columnar;

import java.util.List;

/** One input record in a partition batch. */
public record ConsumedRecord(
        byte[] key,
        byte[] value,
        long timestamp,
        int partition,
        long offset,
        List<RecordHeader> headers) {
    public ConsumedRecord(byte[] key, byte[] value, long timestamp, int partition, long offset) {
        this(key, value, timestamp, partition, offset, List.of());
    }

    public ConsumedRecord {
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
