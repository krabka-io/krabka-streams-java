package io.krabka.streams.columnar;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.common.serialization.Serde;

/** Converts ordinary Kafka value records through a typed row bridge. */
public final class RowCodec<T> implements BatchCodec {
    private final Serde<T> valueSerde;
    private final RowBridge<T> rowBridge;
    private final BufferAllocator allocator;

    public RowCodec(Serde<T> valueSerde, RowBridge<T> rowBridge, BufferAllocator allocator) {
        this.valueSerde = Objects.requireNonNull(valueSerde, "valueSerde");
        this.rowBridge = Objects.requireNonNull(rowBridge, "rowBridge");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        return decode("", records);
    }

    @Override
    public VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        var values = new ArrayList<T>(records.size());
        var metadata = new ArrayList<ArrowBatchSupport.RowMetadata>(records.size());
        for (var record : records) {
            values.add(valueSerde.deserializer().deserialize(topic, record.value()));
            metadata.add(new ArrowBatchSupport.RowMetadata(
                    record.key(), record.timestamp(), record.partition(), record.offset()));
        }
        try (var payload = rowBridge.rowsToBatch(values, allocator)) {
            return ArrowBatchSupport.withMetadata(payload, metadata, allocator);
        }
    }

    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        return encode("", batch);
    }

    @Override
    public List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        try (var payload = ArrowBatchSupport.payload(batch, allocator)) {
            var rows = rowBridge.batchToRows(payload);
            var output = new ArrayList<ProduceRecord>(rows.size());
            var keys = batch.getVector(ArrowBatchSupport.KEY);
            var timestamps = batch.getVector(ArrowBatchSupport.TIMESTAMP);
            for (int row = 0; row < rows.size(); row++) {
                output.add(new ProduceRecord(
                        key(keys, row),
                        valueSerde.serializer().serialize(topic, rows.get(row)),
                        timestamp(timestamps, row)));
            }
            return List.copyOf(output);
        }
    }

    private static byte[] key(org.apache.arrow.vector.FieldVector vector, int row) {
        if (!(vector instanceof VarBinaryVector) || vector.isNull(row)) {
            return null;
        }
        var value = ArrowBatchSupport.value(vector, row);
        if (value instanceof ByteBuffer buffer) {
            var copy = buffer.duplicate();
            var bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        return null;
    }

    private static long timestamp(org.apache.arrow.vector.FieldVector vector, int row) {
        if (vector instanceof BigIntVector timestamps && !timestamps.isNull(row)) {
            return timestamps.get(row);
        }
        return 0;
    }
}
