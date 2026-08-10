package io.krabka.streams.columnar;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Converts one partition batch between Kafka records and Arrow vectors.
 *
 * <p>{@code decode} turns a fetched batch into one Arrow batch whose payload columns
 * are followed by the five reserved metadata columns ({@code __key},
 * {@code __timestamp}, {@code __partition}, {@code __offset}, {@code __headers});
 * {@code encode} turns an Arrow batch back into records, dropping the metadata
 * columns and applying their values to the produced records. Two implementations
 * ship with the module: {@link BlobCodec} for records that already hold Arrow IPC
 * batches and {@link RowCodec} for ordinary one-value-per-record topics.
 * {@link GzipBatchCodec} wraps any codec with per-record compression.
 *
 * <p>The caller owns and must close every root returned by {@code decode}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * BatchCodec codec = new BlobCodec(allocator);
 * try (VectorSchemaRoot batch = codec.decode("transactions", records)) {
 *     List<ProduceRecord> output = codec.encode("processed", batch);
 * }
 * }</pre>
 */
public interface BatchCodec {
    /**
     * Decodes a fetched partition batch into one Arrow batch.
     *
     * @param records the non-empty records of one topic partition batch
     * @return the decoded batch with metadata columns appended; the caller must close it
     * @throws ColumnarException if a record cannot be decoded
     */
    VectorSchemaRoot decode(List<ConsumedRecord> records);

    /**
     * Decodes a fetched partition batch with the source topic available to the codec.
     *
     * <p>Topologies call this form so serde-backed codecs can derive registry
     * subjects from the topic. The default implementation ignores the topic and
     * delegates to {@link #decode(List)}.
     *
     * @param topic the topic the records were fetched from
     * @param records the non-empty records of one topic partition batch
     * @return the decoded batch with metadata columns appended; the caller must close it
     * @throws ColumnarException if a record cannot be decoded
     */
    default VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        return decode(records);
    }

    /**
     * Encodes an Arrow batch into producible records.
     *
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     * @throws ColumnarException if the batch cannot be encoded
     */
    List<ProduceRecord> encode(VectorSchemaRoot batch);

    /**
     * Encodes an Arrow batch with the sink topic available to the codec.
     *
     * <p>Topologies call this form so serde-backed codecs can derive registry
     * subjects from the topic. The default implementation ignores the topic and
     * delegates to {@link #encode(VectorSchemaRoot)}.
     *
     * @param topic the topic the records will be produced to
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     * @throws ColumnarException if the batch cannot be encoded
     */
    default List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        return encode(batch);
    }
}
