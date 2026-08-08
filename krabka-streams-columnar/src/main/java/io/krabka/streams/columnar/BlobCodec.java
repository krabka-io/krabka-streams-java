package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Converts records whose values are Arrow IPC batches. */
public final class BlobCodec implements BatchCodec {
    public static final int DEFAULT_MAX_RECORD_BYTES = 900 * 1024;
    public static final String KEY_COLUMN = ArrowBatchSupport.KEY;
    public static final String TIMESTAMP_COLUMN = ArrowBatchSupport.TIMESTAMP;
    public static final String PARTITION_COLUMN = ArrowBatchSupport.PARTITION;
    public static final String OFFSET_COLUMN = ArrowBatchSupport.OFFSET;

    private final BufferAllocator allocator;
    private final ArrowIpcSerde serde;
    private final int maxRecordBytes;

    public BlobCodec(BufferAllocator allocator) {
        this(allocator, DEFAULT_MAX_RECORD_BYTES);
    }

    public BlobCodec(BufferAllocator allocator, int maxRecordBytes) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (maxRecordBytes < 1) {
            throw new IllegalArgumentException("maxRecordBytes must be positive");
        }
        this.maxRecordBytes = maxRecordBytes;
        this.serde = new ArrowIpcSerde(allocator);
    }

    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        if (records.isEmpty()) {
            throw new ColumnarException("decode called with an empty record batch");
        }
        var batches = new ArrayList<VectorSchemaRoot>(records.size());
        try {
            for (int recordIndex = 0; recordIndex < records.size(); recordIndex++) {
                var record = records.get(recordIndex);
                try (var payload = serde.deserialize(record.value())) {
                    var metadata = new ArrayList<ArrowBatchSupport.RowMetadata>(payload.getRowCount());
                    for (int row = 0; row < payload.getRowCount(); row++) {
                        metadata.add(new ArrowBatchSupport.RowMetadata(
                                record.key(), record.timestamp(), record.partition(), record.offset()));
                    }
                    batches.add(ArrowBatchSupport.withMetadata(payload, metadata, allocator));
                } catch (RuntimeException error) {
                    throw new ColumnarException("cannot decode Arrow record " + recordIndex, error);
                }
            }
            return ArrowBatchSupport.concatenate(batches, allocator);
        } finally {
            batches.forEach(VectorSchemaRoot::close);
        }
    }

    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        long timestamp = lastTimestamp(batch);
        try (var payload = ArrowBatchSupport.payload(batch, allocator)) {
            var result = new ArrayList<ProduceRecord>();
            encodeChunks(payload, timestamp, result);
            return List.copyOf(result);
        }
    }

    private void encodeChunks(VectorSchemaRoot batch, long timestamp, List<ProduceRecord> output) {
        var bytes = serde.serialize(batch);
        if (bytes.length <= maxRecordBytes || batch.getRowCount() <= 1) {
            output.add(new ProduceRecord(null, bytes, timestamp));
            return;
        }
        int midpoint = batch.getRowCount() / 2;
        try (var left = ArrowBatchSupport.copyRange(batch, 0, midpoint, allocator);
                var right = ArrowBatchSupport.copyRange(
                        batch, midpoint, batch.getRowCount() - midpoint, allocator)) {
            encodeChunks(left, timestamp, output);
            encodeChunks(right, timestamp, output);
        }
    }

    private static long lastTimestamp(VectorSchemaRoot batch) {
        var vector = batch.getVector(TIMESTAMP_COLUMN);
        if (!(vector instanceof BigIntVector timestamps) || batch.getRowCount() == 0) {
            return 0;
        }
        int row = batch.getRowCount() - 1;
        return timestamps.isNull(row) ? 0 : timestamps.get(row);
    }
}
