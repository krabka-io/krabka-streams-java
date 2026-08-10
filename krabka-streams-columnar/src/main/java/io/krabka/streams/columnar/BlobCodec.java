package io.krabka.streams.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Converts records whose values are Arrow IPC batches.
 *
 * <p>Use this codec when producers already write Arrow IPC streams as record values,
 * so one Kafka record holds many rows. Decoding reads each record value as an IPC
 * stream, attaches that record's key, timestamp, partition, offset, and headers to
 * every row through the reserved metadata columns, and concatenates the results; all
 * records in a batch must share one payload schema.
 *
 * <p>Encoding drops the metadata columns and serializes the payload back into IPC
 * records: it packs the largest run of consecutive rows that fits under
 * {@code maxRecordBytes} and shares one key, timestamp, and header list, and applies
 * that envelope to the produced record. A single row larger than the cap throws
 * {@link ColumnarException} instead of producing a record the broker would reject.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var codec = new BlobCodec(allocator); // 900 KiB cap per record
 *
 * try (var batch = codec.decode("transactions", records)) {
 *     var amounts = (BigIntVector) batch.getVector("amount");
 *     var offsets = (BigIntVector) batch.getVector(BlobCodec.OFFSET_COLUMN);
 * }
 * }</pre>
 */
public final class BlobCodec implements BatchCodec {
    /** The default {@code maxRecordBytes}: 900 KiB, safely under a 1 MiB broker limit. */
    public static final int DEFAULT_MAX_RECORD_BYTES = 900 * 1024;

    /** The reserved column holding each row's record key, {@code __key}. */
    public static final String KEY_COLUMN = ArrowBatchSupport.KEY;

    /** The reserved column holding each row's record timestamp, {@code __timestamp}. */
    public static final String TIMESTAMP_COLUMN = ArrowBatchSupport.TIMESTAMP;

    /** The reserved column holding each row's source partition, {@code __partition}. */
    public static final String PARTITION_COLUMN = ArrowBatchSupport.PARTITION;

    /** The reserved column holding each row's source offset, {@code __offset}. */
    public static final String OFFSET_COLUMN = ArrowBatchSupport.OFFSET;

    /** The reserved column holding each row's encoded Kafka headers, {@code __headers}. */
    public static final String HEADERS_COLUMN = ArrowBatchSupport.HEADERS;

    /**
     * Returns the in-batch name used when a payload column collides with metadata.
     *
     * <p>A payload column named like a reserved column, for example {@code __offset},
     * is escaped inside processing batches; sinks restore the original name before
     * encoding. Use this method to address such a column inside operators.
     *
     * @param name the original payload column name
     * @return the escaped in-batch name, or {@code name} itself when it does not collide
     */
    public static String payloadColumn(String name) {
        return ArrowBatchSupport.payloadColumn(name);
    }

    private final BufferAllocator allocator;
    private final ArrowIpcSerde serde;
    private final int maxRecordBytes;

    /**
     * Creates a codec with the {@value #DEFAULT_MAX_RECORD_BYTES}-byte record cap.
     *
     * @param allocator the allocator that owns decoded batches
     */
    public BlobCodec(BufferAllocator allocator) {
        this(allocator, DEFAULT_MAX_RECORD_BYTES);
    }

    /**
     * Creates a codec with an explicit record size cap.
     *
     * <p>Raise the cap only together with the broker's {@code max.message.bytes};
     * leave room for headers and framing.
     *
     * @param allocator the allocator that owns decoded batches
     * @param maxRecordBytes the largest encoded record value the codec may produce
     * @throws IllegalArgumentException if {@code maxRecordBytes} is not positive
     */
    public BlobCodec(BufferAllocator allocator, int maxRecordBytes) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (maxRecordBytes < 1) {
            throw new IllegalArgumentException("maxRecordBytes must be positive");
        }
        this.maxRecordBytes = maxRecordBytes;
        this.serde = new ArrowIpcSerde(allocator);
    }

    /**
     * Decodes Arrow IPC record values into one batch with metadata columns.
     *
     * @param records the non-empty records of one topic partition batch
     * @return the concatenated batch; the caller must close it
     * @throws ColumnarException if the record list is empty, a record is not a
     *     readable IPC stream, or the records' payload schemas differ
     */
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

    /**
     * Encodes a batch's payload into size-capped Arrow IPC records.
     *
     * <p>Each produced record's key, timestamp, and headers come from the reserved
     * metadata columns of the rows it carries; a missing or null {@code __timestamp}
     * becomes {@code 0} and a missing or null {@code __key} becomes a null key.
     *
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     * @throws ColumnarException if one row alone exceeds {@code maxRecordBytes}
     */
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
