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
    public static final String HEADERS_COLUMN = ArrowBatchSupport.HEADERS;

    /** Returns the in-batch name used when a payload column collides with metadata. */
    public static String payloadColumn(String name) {
        return ArrowBatchSupport.payloadColumn(name);
    }

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
                                record.key(), record.timestamp(), record.partition(), record.offset(), record.headers()));
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
        try (var payload = ArrowBatchSupport.payload(batch, allocator)) {
            var result = new ArrayList<ProduceRecord>();
            int start = 0;
            while (start < payload.getRowCount()) {
                var chunk = largestChunk(payload, start, envelopeRows(batch, start));
                int row = start + chunk.rows() - 1;
                result.add(new ProduceRecord(
                        key(batch, row),
                        chunk.bytes(),
                        timestamp(batch, row),
                        ArrowBatchSupport.headers(batch.getVector(HEADERS_COLUMN), row)));
                start += chunk.rows();
            }
            return List.copyOf(result);
        }
    }

    private EncodedChunk largestChunk(VectorSchemaRoot batch, int start, int availableRows) {
        int low = 1;
        int high = availableRows;
        EncodedChunk best = null;
        while (low <= high) {
            int rows = low + (high - low) / 2;
            byte[] bytes;
            try (var candidate = ArrowBatchSupport.copyRange(batch, start, rows, allocator)) {
                bytes = serde.serialize(candidate);
            }
            if (bytes.length <= maxRecordBytes) {
                best = new EncodedChunk(rows, bytes);
                low = rows + 1;
            } else {
                high = rows - 1;
            }
        }
        if (best == null) {
            throw new ColumnarException("one Arrow row exceeds maxRecordBytes=" + maxRecordBytes);
        }
        return best;
    }

    private static int envelopeRows(VectorSchemaRoot batch, int start) {
        var expectedKey = key(batch, start);
        long expectedTimestamp = timestamp(batch, start);
        var expectedHeaders = ArrowBatchSupport.headers(batch.getVector(HEADERS_COLUMN), start);
        int end = start + 1;
        while (end < batch.getRowCount()
                && java.util.Arrays.equals(expectedKey, key(batch, end))
                && expectedTimestamp == timestamp(batch, end)
                && expectedHeaders.equals(ArrowBatchSupport.headers(batch.getVector(HEADERS_COLUMN), end))) {
            end++;
        }
        return end - start;
    }

    private static byte[] key(VectorSchemaRoot batch, int row) {
        var vector = batch.getVector(KEY_COLUMN);
        if (!(vector instanceof org.apache.arrow.vector.VarBinaryVector keys) || keys.isNull(row)) {
            return null;
        }
        return keys.get(row);
    }

    private static long timestamp(VectorSchemaRoot batch, int row) {
        var vector = batch.getVector(TIMESTAMP_COLUMN);
        if (!(vector instanceof BigIntVector timestamps) || timestamps.isNull(row)) {
            return 0;
        }
        return timestamps.get(row);
    }

    private record EncodedChunk(int rows, byte[] bytes) {
    }
}
