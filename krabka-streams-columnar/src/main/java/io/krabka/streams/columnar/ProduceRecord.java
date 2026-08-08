package io.krabka.streams.columnar;

/** One output record from a columnar sink. */
public record ProduceRecord(byte[] key, byte[] value, long timestamp) {
    public ProduceRecord {
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
