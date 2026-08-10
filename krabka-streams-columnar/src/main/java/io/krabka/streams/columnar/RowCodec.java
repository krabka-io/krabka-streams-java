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

/**
 * Converts ordinary Kafka value records through a typed row bridge.
 *
 * <p>Use this codec when each record holds one value and you want a columnar view of
 * a topic. Decoding deserializes every value with the supplied {@link Serde},
 * converts the values into Arrow columns through the {@link RowBridge}, and appends
 * the reserved metadata columns. Encoding reverses it: payload columns become typed
 * rows, each row becomes one record, and the key, timestamp, and headers are taken
 * from the {@code __key}, {@code __timestamp}, and {@code __headers} columns. Row
 * count is preserved in both directions.
 *
 * <p>The topic passed through {@link #decode(String, List)} and
 * {@link #encode(String, VectorSchemaRoot)} is forwarded to the serde, so
 * topic-derived schema registry subjects work the same way as in ordinary consumers
 * and producers.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * record Transaction(String user, long amount) {}
 *
 * var codec = new RowCodec<>(
 *     transactionSerde, new JsonRowBridge<>(Transaction.class), allocator);
 *
 * try (var batch = codec.decode("transactions", records)) {
 *     // one row per record; columns "user" and "amount" plus metadata
 * }
 * }</pre>
 *
 * @param <T> the record value type
 */
public final class RowCodec<T> implements BatchCodec {
    private final Serde<T> valueSerde;
    private final RowBridge<T> rowBridge;
    private final BufferAllocator allocator;

    /**
     * Creates a codec from a value serde and a row bridge.
     *
     * @param valueSerde the serde that reads and writes record values
     * @param rowBridge the bridge between values and Arrow columns
     * @param allocator the allocator that owns decoded batches
     */
    public RowCodec(Serde<T> valueSerde, RowBridge<T> rowBridge, BufferAllocator allocator) {
        this.valueSerde = Objects.requireNonNull(valueSerde, "valueSerde");
        this.rowBridge = Objects.requireNonNull(rowBridge, "rowBridge");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
    }

    /**
     * Decodes records with an empty topic name.
     *
     * @param records the records of one topic partition batch
     * @return the decoded batch; the caller must close it
     */
    @Override
    public VectorSchemaRoot decode(List<ConsumedRecord> records) {
        return decode("", records);
    }

    /**
     * Decodes records, forwarding the topic to the value serde.
     *
     * @param topic the topic the records were fetched from
     * @param records the records of one topic partition batch
     * @return the decoded batch, one row per record, with metadata columns appended;
     *     the caller must close it
     */
    @Override
    public VectorSchemaRoot decode(String topic, List<ConsumedRecord> records) {
        var values = new ArrayList<T>(records.size());
        var metadata = new ArrayList<ArrowBatchSupport.RowMetadata>(records.size());
        for (var record : records) {
            values.add(valueSerde.deserializer().deserialize(
                    topic, kafkaHeaders(record.headers()), record.value()));
            metadata.add(new ArrowBatchSupport.RowMetadata(
                    record.key(), record.timestamp(), record.partition(), record.offset(), record.headers()));
        }
        try (var payload = rowBridge.rowsToBatch(values, allocator)) {
            return ArrowBatchSupport.withMetadata(payload, metadata, allocator);
        }
    }

    /**
     * Encodes a batch with an empty topic name.
     *
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     */
    @Override
    public List<ProduceRecord> encode(VectorSchemaRoot batch) {
        return encode("", batch);
    }

    /**
     * Encodes a batch into one record per row, forwarding the topic to the serde.
     *
     * <p>A missing or null {@code __key} becomes a null record key, and a missing or
     * null {@code __timestamp} becomes {@code 0}.
     *
     * @param topic the topic the records will be produced to
     * @param batch the batch to encode; the codec reads it and leaves it open
     * @return the encoded records in row order
     */
    @Override
    public List<ProduceRecord> encode(String topic, VectorSchemaRoot batch) {
        try (var payload = ArrowBatchSupport.payload(batch, allocator)) {
            var rows = rowBridge.batchToRows(payload);
            var output = new ArrayList<ProduceRecord>(rows.size());
            var keys = batch.getVector(ArrowBatchSupport.KEY);
            var timestamps = batch.getVector(ArrowBatchSupport.TIMESTAMP);
            var headers = batch.getVector(ArrowBatchSupport.HEADERS);
            for (int row = 0; row < rows.size(); row++) {
                var recordHeaders = ArrowBatchSupport.headers(headers, row);
                output.add(new ProduceRecord(
                        key(keys, row),
                        valueSerde.serializer().serialize(
                                topic, kafkaHeaders(recordHeaders), rows.get(row)),
                        timestamp(timestamps, row),
                        recordHeaders));
            }
            return List.copyOf(output);
        }
    }

    private static org.apache.kafka.common.header.Headers kafkaHeaders(List<RecordHeader> headers) {
        var result = new org.apache.kafka.common.header.internals.RecordHeaders();
        headers.forEach(header -> result.add(header.key(), header.value()));
        return result;
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
