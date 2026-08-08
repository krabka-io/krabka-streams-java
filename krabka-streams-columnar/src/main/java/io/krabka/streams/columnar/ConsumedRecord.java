package io.krabka.streams.columnar;

/** One input record in a partition batch. */
public record ConsumedRecord(byte[] key, byte[] value, long timestamp, int partition, long offset) {
    public ConsumedRecord {
        key = key == null ? null : key.clone();
        value = value.clone();
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
